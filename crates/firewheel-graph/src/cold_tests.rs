use std::{
    num::NonZeroU32,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
};

use firewheel_core::node::{
    AudioNode, AudioNodeInfo, AudioNodeProcessor, ConstructProcessorContext, EmptyConfig,
    NodeError, ProcBuffers, ProcExtra, ProcInfo, ProcStreamCtx, ProcessStatus,
};

use crate::{ActivateInfo, FirewheelContext};

#[derive(Default)]
pub(crate) struct Probe {
    pub(crate) constructed: AtomicUsize,
    pub(crate) succeeded: AtomicUsize,
    pub(crate) dropped: AtomicUsize,
    pub(crate) refuse_factory: AtomicBool,
    pub(crate) refuse_rate: AtomicU32,
    pub(crate) active_rate: AtomicU32,
    pub(crate) prepared: Mutex<Vec<Descriptor>>,
    pub(crate) committed: Mutex<Vec<Descriptor>>,
    pub(crate) processed: AtomicUsize,
}

pub(crate) struct TestNode(pub(crate) Arc<Probe>);
pub(crate) struct TestProcessor {
    pub(crate) probe: Arc<Probe>,
    pub(crate) prepared: Option<Descriptor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Descriptor {
    pub(crate) rate: u32,
    pub(crate) previous: u32,
    pub(crate) block: u32,
    pub(crate) inputs: u32,
    pub(crate) outputs: u32,
    pub(crate) latency: u64,
    pub(crate) reciprocal: u64,
    pub(crate) declick: u32,
}

impl From<&firewheel_core::StreamInfo> for Descriptor {
    fn from(info: &firewheel_core::StreamInfo) -> Self {
        Self {
            rate: info.sample_rate.get(),
            previous: info.prev_sample_rate.get(),
            block: info.max_block_frames.get(),
            inputs: info.num_stream_in_channels,
            outputs: info.num_stream_out_channels,
            latency: info.input_to_output_latency_seconds.to_bits(),
            reciprocal: info.sample_rate_recip.to_bits(),
            declick: info.declick_frames.get(),
        }
    }
}

struct MainLifetime(Arc<Probe>);

impl Drop for MainLifetime {
    fn drop(&mut self) {
        assert_eq!(
            self.0.succeeded.load(Ordering::SeqCst),
            self.0.dropped.load(Ordering::SeqCst),
            "processors must drop before their custom state"
        );
    }
}

#[derive(Debug)]
pub(crate) struct Refusal;

impl std::fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("controlled refusal")
    }
}

impl std::error::Error for Refusal {}

impl AudioNode for TestNode {
    type Configuration = EmptyConfig;

    fn info(&self, _: &Self::Configuration) -> Result<AudioNodeInfo, NodeError> {
        Ok(AudioNodeInfo::new()
            .channel_config(firewheel_core::channel_config::ChannelConfig {
                num_inputs: firewheel_core::channel_config::ChannelCount::ZERO,
                num_outputs: firewheel_core::channel_config::ChannelCount::ZERO,
            })
            .custom_state(MainLifetime(self.0.clone())))
    }

    fn construct_processor(
        &self,
        _: &Self::Configuration,
        context: ConstructProcessorContext,
    ) -> Result<impl AudioNodeProcessor, NodeError> {
        self.0.constructed.fetch_add(1, Ordering::SeqCst);
        if self.0.refuse_factory.load(Ordering::SeqCst) {
            return Err(NodeError(Box::new(Refusal)));
        }
        self.0.succeeded.fetch_add(1, Ordering::SeqCst);
        self.0
            .active_rate
            .store(context.stream_info.sample_rate.get(), Ordering::SeqCst);
        Ok(TestProcessor {
            probe: self.0.clone(),
            prepared: None,
        })
    }
}

impl AudioNodeProcessor for TestProcessor {
    fn prepare_new_stream(&mut self, info: &firewheel_core::StreamInfo) -> Result<(), NodeError> {
        self.prepared = None;
        self.probe.prepared.lock().unwrap().push(info.into());
        if self.probe.refuse_rate.load(Ordering::SeqCst) == info.sample_rate.get() {
            return Err(NodeError(Box::new(Refusal)));
        }
        self.prepared = Some(info.into());
        Ok(())
    }
    fn new_stream(&mut self, info: &firewheel_core::StreamInfo, _: &mut ProcStreamCtx) {
        let descriptor = self.prepared.take().expect("prepared exactly once");
        assert_eq!(descriptor, Descriptor::from(info));
        self.probe
            .active_rate
            .store(descriptor.rate, Ordering::SeqCst);
        self.probe.committed.lock().unwrap().push(descriptor);
    }
    fn process(&mut self, _: &ProcInfo, _: ProcBuffers, _: &mut ProcExtra) -> ProcessStatus {
        self.probe.processed.fetch_add(1, Ordering::SeqCst);
        ProcessStatus::Bypass
    }
}

impl Drop for TestProcessor {
    fn drop(&mut self) {
        self.probe.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

pub(crate) fn descriptor(rate: u32, block: u32) -> ActivateInfo {
    ActivateInfo {
        sample_rate: NonZeroU32::new(rate).unwrap(),
        max_block_frames: NonZeroU32::new(block).unwrap(),
        num_stream_in_channels: 0,
        num_stream_out_channels: 2,
        input_to_output_latency_seconds: 0.0,
    }
}

#[test]
fn failed_factory_retains_prior_processors() {
    let first = Arc::new(Probe::default());
    let second = Arc::new(Probe::default());
    second.refuse_factory.store(true, Ordering::SeqCst);
    let mut context = FirewheelContext::new(Default::default());
    context.add_node(TestNode(first.clone()), None).unwrap();
    context.add_node(TestNode(second.clone()), None).unwrap();
    assert!(context.activate(descriptor(44_100, 64)).is_err());
    assert_eq!(first.constructed.load(Ordering::SeqCst), 1);
    assert_eq!(first.dropped.load(Ordering::SeqCst), 0);
    second.refuse_factory.store(false, Ordering::SeqCst);
    let processor = context.activate(descriptor(48_000, 128)).unwrap();
    assert_eq!(first.constructed.load(Ordering::SeqCst), 1);
    assert_eq!(second.constructed.load(Ordering::SeqCst), 2);
    let prepared = first.prepared.lock().unwrap();
    assert_eq!(prepared.len(), 1);
    assert_eq!(prepared[0].rate, 48_000);
    assert_eq!(prepared[0].previous, 48_000);
    assert_eq!(prepared[0].block, 128);
    drop(prepared);
    drop(processor);
    drop(context);
    assert_eq!(first.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(second.dropped.load(Ordering::SeqCst), 1);
}

pub(crate) fn stream_info(rate: u32, block: u32) -> firewheel_core::StreamInfo {
    let info = descriptor(rate, block);
    firewheel_core::StreamInfo {
        sample_rate: info.sample_rate,
        sample_rate_recip: f64::from(rate).recip(),
        prev_sample_rate: info.sample_rate,
        max_block_frames: info.max_block_frames,
        num_stream_in_channels: 0,
        num_stream_out_channels: 2,
        input_to_output_latency_seconds: 0.0,
        declick_frames: NonZeroU32::new(128).unwrap(),
    }
}

#[test]
fn boxed_processor_forwards_preparation_refusal() {
    let probe = Arc::new(Probe::default());
    probe.refuse_rate.store(48_000, Ordering::SeqCst);
    let mut processor: Box<dyn AudioNodeProcessor> = Box::new(TestProcessor {
        probe: probe.clone(),
        prepared: None,
    });
    let error = AudioNodeProcessor::prepare_new_stream(&mut processor, &stream_info(48_000, 128))
        .unwrap_err();
    assert!(error.0.is::<Refusal>());
    assert_eq!(probe.prepared.lock().unwrap().len(), 1);
    assert!(probe.committed.lock().unwrap().is_empty());
}

#[test]
fn removing_a_staged_processor_drops_it_before_custom_state() {
    let first = Arc::new(Probe::default());
    let second = Arc::new(Probe::default());
    second.refuse_factory.store(true, Ordering::SeqCst);
    let mut context = FirewheelContext::new(Default::default());
    let id = context.add_node(TestNode(first.clone()), None).unwrap();
    context.add_node(TestNode(second), None).unwrap();
    assert!(context.activate(descriptor(44_100, 64)).is_err());
    assert_eq!(first.dropped.load(Ordering::SeqCst), 0);
    context.remove_node(id).unwrap();
    assert_eq!(first.dropped.load(Ordering::SeqCst), 1);
}
