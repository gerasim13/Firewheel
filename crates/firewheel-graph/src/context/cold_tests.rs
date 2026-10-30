use std::sync::{Arc, atomic::Ordering};

use audioadapter_buffers::direct::InterleavedSlice;
use firewheel_core::node::StreamStatus;
use ringbuf::traits::Observer;

use super::*;
use crate::{
    backend::BackendProcessInfo,
    cold_tests::{Probe, Refusal, TestNode, descriptor},
};

fn process(processor: &mut FirewheelProcessor, frames: usize) {
    let mut output = vec![0.0; frames * 2];
    processor.process(
        &InterleavedSlice::new(&[], 0, 0).unwrap(),
        &mut InterleavedSlice::new_mut(&mut output, 2, frames).unwrap(),
        BackendProcessInfo {
            frames,
            process_timestamp: None,
            duration_since_stream_start: Duration::ZERO,
            input_stream_status: StreamStatus::empty(),
            output_stream_status: StreamStatus::empty(),
            dropped_frames: 0,
            process_to_playback_delay: None,
        },
    );
}

#[test]
fn queued_owners_prepare_before_any_commit_and_install_with_new_geometry() {
    let first = Arc::new(Probe::default());
    let second = Arc::new(Probe::default());
    let mut context = FirewheelContext::new(Default::default());
    context.add_node(TestNode(first.clone()), None).unwrap();
    let second_id = context.add_node(TestNode(second.clone()), None).unwrap();
    let processor = context.activate(descriptor(44_100, 64)).unwrap();
    drop(processor);
    second.refuse_rate.store(48_000, Ordering::SeqCst);
    let mut next = descriptor(48_000, 128);
    next.input_to_output_latency_seconds = 0.125;
    let error = context.activate(next).err().unwrap();
    let ActivateError::StreamPreparationFailed { node, error } = error else {
        panic!("typed hook error")
    };
    assert_eq!(node, second_id);
    assert!(error.0.is::<Refusal>());
    for probe in [&first, &second] {
        assert_eq!(probe.active_rate.load(Ordering::SeqCst), 44_100);
        assert!(probe.committed.lock().unwrap().is_empty());
        assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
    }
    assert_eq!(context.sample_rate.get(), 44_100);
    second.refuse_rate.store(0, Ordering::SeqCst);
    let mut processor = context.activate(next).unwrap();
    for probe in [&first, &second] {
        let prepared = probe.prepared.lock().unwrap();
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared[0], prepared[1]);
        assert_eq!(prepared[1].rate, 48_000);
        assert_eq!(prepared[1].previous, 44_100);
        assert_eq!(prepared[1].block, 128);
        assert_eq!(prepared[1].latency, 0.125_f64.to_bits());
        assert_eq!(probe.committed.lock().unwrap().as_slice(), &prepared[1..]);
        assert_eq!(probe.constructed.load(Ordering::SeqCst), 1);
    }
    process(&mut processor, 128);
    assert!(first.processed.load(Ordering::SeqCst) > 0);
    assert!(second.processed.load(Ordering::SeqCst) > 0);
}

#[test]
fn full_queue_refuses_before_commit_and_retains_staged_factory_owners() {
    let mut context = FirewheelContext::new(FirewheelConfig {
        channel_capacity: 2,
        ..Default::default()
    });
    let probe = Arc::new(Probe::default());
    context.add_node(TestNode(probe.clone()), None).unwrap();
    context
        .set_flags(FirewheelFlags {
            hard_clip_outputs: true,
            ..Default::default()
        })
        .unwrap();
    context.set_flags(FirewheelFlags::default()).unwrap();
    let before = context.pending_processor_channel.as_ref().unwrap();
    assert_eq!(before.from_context_rx.occupied_len(), 2);
    assert_eq!(before.from_context_rx.vacant_len(), 0);
    let read = before.from_context_rx.read_index();
    let write = before.from_context_rx.write_index();
    let store = std::ptr::from_ref(&before.store);
    let error = context.activate(descriptor(48_000, 128)).err().unwrap();
    assert!(matches!(error, ActivateError::MsgChannelFull));
    assert_eq!(probe.constructed.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
    assert!(probe.committed.lock().unwrap().is_empty());
    let after = context.pending_processor_channel.as_ref().unwrap();
    assert_eq!(after.from_context_rx.read_index(), read);
    assert_eq!(after.from_context_rx.write_index(), write);
    assert_eq!(std::ptr::from_ref(&after.store), store);
    assert!(context.stream_info.is_none());
    drop(context);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn initial_factory_error_preserves_its_generation_and_typed_payload() {
    let probe = Arc::new(Probe::default());
    probe.refuse_factory.store(true, Ordering::SeqCst);
    let mut context = FirewheelContext::new(Default::default());
    let id = context.add_node(TestNode(probe.clone()), None).unwrap();
    let error = context.activate(descriptor(44_100, 64)).err().unwrap();
    let ActivateError::GraphCompileError(CompileGraphError::ProcessorConstructionFailed {
        node,
        error,
    }) = error
    else {
        panic!("typed factory error")
    };
    assert_eq!(node, id);
    assert!(error.0.is::<Refusal>());
    assert!(context.pending_processor_channel.is_some());
    assert!(context.stream_info.is_none());
}

#[test]
fn staged_removal_and_structural_rollback_keep_the_same_owner() {
    let first = Arc::new(Probe::default());
    let second = Arc::new(Probe::default());
    second.refuse_factory.store(true, Ordering::SeqCst);
    let mut context = FirewheelContext::new(Default::default());
    let id = context.add_node(TestNode(first.clone()), None).unwrap();
    context.add_node(TestNode(second.clone()), None).unwrap();
    assert!(context.activate(descriptor(44_100, 64)).is_err());
    let result: Result<(), Refusal> = context.try_modify_graph(|context| {
        context.remove_node(id).unwrap();
        Err(Refusal)
    });
    assert!(result.is_err());
    assert!(context.node_info(id).is_some());
    assert_eq!(first.dropped.load(Ordering::SeqCst), 0);
    second.refuse_factory.store(false, Ordering::SeqCst);
    let mut processor = context.activate(descriptor(48_000, 128)).unwrap();
    assert_eq!(first.constructed.load(Ordering::SeqCst), 1);
    process(&mut processor, 128);
    context.remove_node(id).unwrap();
    let third = Arc::new(Probe::default());
    let next = context.add_node(TestNode(third.clone()), None).unwrap();
    assert_eq!(id.0.slot(), next.0.slot());
    assert_ne!(id, next);
    context.update().unwrap();
    drop(processor);
    let mut processor = context.activate(descriptor(44_100, 256)).unwrap();
    process(&mut processor, 256);
    context.update().unwrap();
    assert_eq!(first.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(third.dropped.load(Ordering::SeqCst), 0);
    assert!(context.node_info(next).is_some());
}
