use bevy_platform::collections::HashSet;
use firewheel_core::{
    StreamInfo,
    dsp::{
        buffer::ConstSequentialBuffer,
        declick::{DeclickValues, Declicker},
    },
    node::{NodeID, ProcStreamCtx},
};
use ringbuf::traits::{Consumer, Producer};

#[cfg(not(feature = "std"))]
use bevy_platform::prelude::Box;

#[cfg(feature = "musical_transport")]
use firewheel_core::clock::TransportState;

use crate::{
    error::{ActivateError, CompileGraphError},
    graph::{AudioGraph, NodeHeapData, ScheduleHeapData},
    processor::{
        ContextToProcessorMsg, FirewheelProcessorInner, NodeEntry, NodeEventSchedulerData,
        ProcessorToContextMsg,
    },
};

pub(crate) fn validate_owner(
    id: NodeID,
    seen: &mut HashSet<NodeID>,
    graph: &AudioGraph,
) -> Result<(), ActivateError> {
    if !seen.insert(id) {
        return Err(CompileGraphError::NodeIDNotUnique(id).into());
    }
    if !graph.owns_transferred(id) {
        return Err(CompileGraphError::ProcessorOwnerNotFound(id).into());
    }
    Ok(())
}

pub(crate) fn prepare_schedule(
    data: &mut ScheduleHeapData,
    info: &StreamInfo,
    preceding: Option<usize>,
    seen: &mut HashSet<NodeID>,
    graph: &AudioGraph,
) -> Result<usize, ActivateError> {
    for node in &mut data.new_node_processors {
        validate_owner(node.id, seen, graph)?;
        if node.needs_stream_update {
            node.processor.prepare_new_stream(info).map_err(|error| {
                ActivateError::StreamPreparationFailed {
                    node: node.id,
                    error,
                }
            })?;
        }
    }
    if let Some(arena) = &mut data.new_node_arena {
        for (index, node) in arena.iter_mut() {
            let id = NodeID(index);
            validate_owner(id, seen, graph)?;
            node.processor
                .prepare_new_stream(info)
                .map_err(|error| ActivateError::StreamPreparationFailed { node: id, error })?;
        }
    }
    let frames = info.max_block_frames.get() as usize;
    data.schedule.prepare_geometry(frames, preceding);
    Ok(frames)
}

pub(crate) fn prepare_queue(
    receiver: &mut ringbuf::HeapCons<ContextToProcessorMsg>,
    info: &StreamInfo,
    mut preceding: Option<usize>,
    seen: &mut HashSet<NodeID>,
    graph: &AudioGraph,
) -> Result<Option<usize>, ActivateError> {
    for message in receiver.iter_mut() {
        if let ContextToProcessorMsg::NewSchedule(data) = message {
            for node in &mut data.new_node_processors {
                node.needs_stream_update = true;
            }
            preceding = Some(prepare_schedule(data, info, preceding, seen, graph)?);
        }
    }
    Ok(preceding)
}

pub(crate) fn commit_queue(
    receiver: &mut ringbuf::HeapCons<ContextToProcessorMsg>,
    info: &StreamInfo,
    context: &mut ProcStreamCtx,
) {
    for message in receiver.iter_mut() {
        if let ContextToProcessorMsg::NewSchedule(data) = message {
            data.schedule.apply_prepared_geometry();
            for node in &mut data.new_node_processors {
                if node.needs_stream_update {
                    node.processor.new_stream(info, context);
                    node.needs_stream_update = false;
                }
            }
            if let Some(arena) = &mut data.new_node_arena {
                for (_, node) in arena.iter_mut() {
                    node.processor.new_stream(info, context);
                }
            }
        }
    }
}

impl FirewheelProcessorInner {
    pub(crate) fn prepare_stream(
        &mut self,
        info: &StreamInfo,
        seen: &mut HashSet<NodeID>,
        graph: &AudioGraph,
    ) -> Result<Option<usize>, ActivateError> {
        self.prepared_stream = None;
        for (index, node) in self.nodes.iter_mut() {
            let id = NodeID(index);
            validate_owner(id, seen, graph)?;
            node.processor
                .prepare_new_stream(info)
                .map_err(|error| ActivateError::StreamPreparationFailed { node: id, error })?;
        }
        let preceding = self
            .schedule_data
            .as_ref()
            .map(|data| data.schedule.max_block_frames());
        let preceding = prepare_queue(&mut self.from_graph_rx, info, preceding, seen, graph)?;
        self.prepared_stream = Some(super::PreparedStream {
            scratch: (self.max_block_frames != info.max_block_frames.get() as usize)
                .then(|| ConstSequentialBuffer::new(info.max_block_frames.get() as usize)),
            declick: (self.sample_rate != info.sample_rate)
                .then(|| DeclickValues::new(info.declick_frames)),
        });
        Ok(preceding)
    }

    pub(crate) fn clear_pending_owners(&mut self) {
        self.from_graph_rx.clear();
    }
    pub fn poll_messages(&mut self) {
        while let Some(msg) = self.from_graph_rx.try_pop() {
            match msg {
                ContextToProcessorMsg::EventGroup(mut event_group) => {
                    self.event_scheduler.push_event_group(
                        &mut event_group,
                        &mut self.nodes,
                        &mut self.extra.logger,
                        #[cfg(feature = "scheduled_events")]
                        self.sample_rate,
                        #[cfg(feature = "scheduled_events")]
                        self.clock_samples,
                        #[cfg(feature = "musical_transport")]
                        &self.proc_transport_state,
                    );

                    let _ = self
                        .to_graph_tx
                        .try_push(ProcessorToContextMsg::DropEventGroup(event_group));
                }
                ContextToProcessorMsg::NewSchedule(new_schedule_data) => {
                    self.new_schedule(new_schedule_data);
                }
                ContextToProcessorMsg::SetFlags(flags) => {
                    self.flags = flags;
                }
                #[cfg(feature = "musical_transport")]
                ContextToProcessorMsg::SetTransportState(new_transport_state) => {
                    self.set_transport_state(new_transport_state);
                }
                #[cfg(feature = "scheduled_events")]
                ContextToProcessorMsg::ClearScheduledEvents(msgs) => {
                    self.event_scheduler
                        .handle_clear_scheduled_events_event(&msgs, &mut self.nodes);

                    let _ = self
                        .to_graph_tx
                        .try_push(ProcessorToContextMsg::DropClearScheduledEvents(msgs));
                }
            }
        }
    }

    fn new_schedule(&mut self, mut new_schedule_data: Box<ScheduleHeapData>) {
        assert_eq!(
            new_schedule_data.schedule.max_block_frames(),
            self.max_block_frames
        );

        if let Some(new_arena) = &mut new_schedule_data.new_node_arena {
            // A new arena with a larger allocated capacity was sent.

            for (index, node_entry) in self.nodes.drain() {
                let _ = new_arena.insert_at(index, node_entry);
            }

            core::mem::swap(&mut self.nodes, new_arena);
        }

        #[cfg(feature = "scheduled_events")]
        let mut remove_old_scheduled_events = false;

        if let Some(mut old_schedule_data) = self.schedule_data.take() {
            core::mem::swap(
                &mut old_schedule_data.removed_nodes,
                &mut new_schedule_data.removed_nodes,
            );

            for node_id in new_schedule_data.nodes_to_remove.iter() {
                if let Some(node_entry) = self.nodes.remove(node_id.0) {
                    #[cfg(feature = "scheduled_events")]
                    if self.event_scheduler.node_has_scheduled_events(&node_entry) {
                        remove_old_scheduled_events = true;
                    }

                    old_schedule_data.removed_nodes.push(NodeHeapData {
                        id: *node_id,
                        processor: node_entry.processor,
                        is_pre_process: false,
                        in_place_buffers: false,
                        needs_stream_update: false,
                    });
                }
            }

            new_schedule_data
                .schedule
                .sync_new_buffers(&mut old_schedule_data.schedule);

            let _ = self
                .to_graph_tx
                .try_push(ProcessorToContextMsg::DropSchedule(old_schedule_data));
        }

        for n in new_schedule_data.new_node_processors.drain(..) {
            assert!((n.id.0.slot() as usize) < self.nodes.capacity());

            assert!(
                self.nodes
                    .insert_at(
                        n.id.0,
                        NodeEntry {
                            processor: n.processor,
                            event_data: NodeEventSchedulerData::new(n.is_pre_process),
                            bypass_declick: Declicker::SettledAt1,
                            is_bypassed: false,
                            is_first_process: true,
                            in_place_buffers: n.in_place_buffers,
                        }
                    )
                    .is_none()
            );
        }

        #[cfg(feature = "scheduled_events")]
        if remove_old_scheduled_events {
            self.event_scheduler
                .remove_events_from_removed_nodes(&self.nodes);
        }

        self.profiler_tx.new_schedule(
            &new_schedule_data.schedule,
            &mut new_schedule_data.new_profiler_heap_data,
        );

        self.schedule_data = Some(new_schedule_data);
    }

    #[cfg(feature = "musical_transport")]
    fn set_transport_state(&mut self, new_transport_state: Box<TransportState>) {
        let old_transport_state = self.proc_transport_state.set_transport_state(
            new_transport_state,
            self.clock_samples,
            self.sample_rate,
            self.sample_rate_recip,
        );

        self.event_scheduler.sync_scheduled_events_to_transport(
            self.proc_transport_state.transport_sync_info(),
            self.sample_rate,
        );

        let _ = self
            .to_graph_tx
            .try_push(ProcessorToContextMsg::DropTransportState(
                old_transport_state,
            ));
    }

    pub fn stream_stopped(&mut self) {
        for (_, node) in self.nodes.iter_mut() {
            node.processor.stream_stopped(&mut ProcStreamCtx {
                store: &mut self.extra.store,
                logger: &mut self.extra.logger,
            });
        }
    }

    /// Called when a new audio stream has been started to replace the old one.
    ///
    /// Note, this method gets called on the main thread, not the audio thread.
    pub fn new_stream(&mut self, stream_info: &StreamInfo) {
        if let Some(prepared) = self.prepared_stream.take() {
            if let Some(scratch) = prepared.scratch {
                self.extra.scratch_buffers = scratch;
            }
            if let Some(declick) = prepared.declick {
                self.extra.declick_values = declick;
            }
        }
        for (_, node) in self.nodes.iter_mut() {
            node.processor.new_stream(
                stream_info,
                &mut ProcStreamCtx {
                    store: &mut self.extra.store,
                    logger: &mut self.extra.logger,
                },
            );
        }

        if self.sample_rate != stream_info.sample_rate {
            self.clock_samples = self
                .clock_samples
                .to_seconds(self.sample_rate, self.sample_rate_recip)
                .to_samples(stream_info.sample_rate);

            #[cfg(feature = "musical_transport")]
            self.proc_transport_state.update_sample_rate(
                self.sample_rate,
                self.sample_rate_recip,
                stream_info.sample_rate,
            );

            #[cfg(feature = "scheduled_events")]
            self.event_scheduler.sample_rate_changed(
                self.sample_rate,
                self.sample_rate_recip,
                stream_info.sample_rate,
            );

            self.sample_rate = stream_info.sample_rate;
            self.sample_rate_recip = stream_info.sample_rate_recip;
        }

        if self.max_block_frames != stream_info.max_block_frames.get() as usize {
            self.max_block_frames = stream_info.max_block_frames.get() as usize;
        }
        commit_queue(
            &mut self.from_graph_rx,
            stream_info,
            &mut ProcStreamCtx {
                store: &mut self.extra.store,
                logger: &mut self.extra.logger,
            },
        );
    }
}

#[cfg(all(test, feature = "std"))]
mod queue_tests {
    use super::*;
    use crate::context::FirewheelBitFlags;
    use crate::{
        FirewheelConfig,
        cold_tests::{Probe, TestNode, stream_info},
    };
    use firewheel_core::{
        event::{NodeEvent, NodeEventType},
        log::{RealtimeLoggerConfig, realtime_logger},
        node::ProcStore,
    };
    use ringbuf::traits::{Observer, Split};
    use std::sync::{Arc, atomic::Ordering};

    #[derive(Debug, PartialEq)]
    enum Message {
        Flags(FirewheelBitFlags),
        Schedule(Vec<NodeID>, Vec<NodeID>, usize),
        Events(Vec<(NodeID, bool)>),
    }

    fn signature(receiver: &ringbuf::HeapCons<ContextToProcessorMsg>) -> Vec<Message> {
        receiver
            .iter()
            .map(|message| match message {
                ContextToProcessorMsg::SetFlags(flags) => Message::Flags(*flags),
                ContextToProcessorMsg::NewSchedule(data) => Message::Schedule(
                    data.new_node_processors
                        .iter()
                        .map(|node| node.id)
                        .collect(),
                    data.nodes_to_remove.clone(),
                    data.schedule.max_block_frames(),
                ),
                ContextToProcessorMsg::EventGroup(events) => Message::Events(
                    events
                        .iter()
                        .map(|event| match &event.event {
                            NodeEventType::SetBypassed(value) => (event.node_id, *value),
                            _ => panic!("controlled event"),
                        })
                        .collect(),
                ),
                #[cfg(feature = "musical_transport")]
                ContextToProcessorMsg::SetTransportState(_) => panic!("controlled queue"),
                #[cfg(feature = "scheduled_events")]
                ContextToProcessorMsg::ClearScheduledEvents(_) => panic!("controlled queue"),
            })
            .collect()
    }

    #[test]
    fn queued_schedules_and_events_keep_order_and_cursors_through_refusal() {
        let mut graph = AudioGraph::new(&FirewheelConfig::default());
        let probe = Arc::new(Probe::default());
        let id = graph.add_node(TestNode(probe.clone()), None).unwrap();
        let old = stream_info(44_100, 64);
        let (mut sender, mut receiver) = ringbuf::HeapRb::new(8).split();
        let first = graph.compile(&old).unwrap();
        let capacity = first.schedule.buffer_capacity();
        assert!(
            sender
                .try_push(ContextToProcessorMsg::NewSchedule(first))
                .is_ok()
        );
        graph.on_schedule_sent(capacity, old.max_block_frames);
        assert!(
            sender
                .try_push(ContextToProcessorMsg::SetFlags(FirewheelBitFlags::empty()))
                .is_ok()
        );
        assert!(
            sender
                .try_push(ContextToProcessorMsg::EventGroup(vec![NodeEvent::new(
                    id,
                    NodeEventType::SetBypassed(true)
                )]))
                .is_ok()
        );
        let second = graph.compile(&old).unwrap();
        assert!(
            sender
                .try_push(ContextToProcessorMsg::NewSchedule(second))
                .is_ok()
        );
        let before = signature(&receiver);
        let read = receiver.read_index();
        let write = receiver.write_index();
        let next = stream_info(48_000, 128);
        probe.refuse_rate.store(48_000, Ordering::SeqCst);
        assert!(
            prepare_queue(&mut receiver, &next, None, &mut HashSet::default(), &graph).is_err()
        );
        assert_eq!(signature(&receiver), before);
        assert_eq!(receiver.read_index(), read);
        assert_eq!(receiver.write_index(), write);
        assert!(probe.committed.lock().unwrap().is_empty());
        probe.refuse_rate.store(0, Ordering::SeqCst);
        prepare_queue(&mut receiver, &next, None, &mut HashSet::default(), &graph).unwrap();
        assert_eq!(signature(&receiver), before);
        let mut store = ProcStore::with_capacity(1);
        let (mut logger, _messages) = realtime_logger(RealtimeLoggerConfig::default());
        commit_queue(
            &mut receiver,
            &next,
            &mut ProcStreamCtx {
                store: &mut store,
                logger: &mut logger,
            },
        );
        let expected: Vec<_> = before
            .into_iter()
            .map(|message| match message {
                Message::Schedule(nodes, removed, _) => Message::Schedule(nodes, removed, 128),
                other => other,
            })
            .collect();
        assert_eq!(signature(&receiver), expected);
        assert_eq!(receiver.read_index(), read);
        assert_eq!(receiver.write_index(), write);
        assert_eq!(probe.committed.lock().unwrap().len(), 1);
        assert_eq!(probe.constructed.load(Ordering::SeqCst), 1);
        receiver.clear();
        drop(graph);
        assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn owner_validation_distinguishes_generations_and_rejects_duplicates() {
        let mut graph = AudioGraph::new(&FirewheelConfig::default());
        let first = Arc::new(Probe::default());
        let old = graph.add_node(TestNode(first), None).unwrap();
        let info = stream_info(44_100, 64);
        let old_schedule = graph.compile(&info).unwrap();
        graph.remove_node(old, false).unwrap();
        let current = graph
            .add_node(TestNode(Arc::new(Probe::default())), None)
            .unwrap();
        let current_schedule = graph.compile(&info).unwrap();
        assert_eq!(old.0.slot(), current.0.slot());
        assert_ne!(old, current);
        let mut seen = HashSet::default();
        validate_owner(old, &mut seen, &graph).unwrap();
        validate_owner(current, &mut seen, &graph).unwrap();
        assert!(matches!(validate_owner(old, &mut seen, &graph),
            Err(ActivateError::GraphCompileError(CompileGraphError::NodeIDNotUnique(id))) if id == old));
        assert!(
            matches!(validate_owner(NodeID::DANGLING, &mut HashSet::default(), &graph),
            Err(ActivateError::GraphCompileError(CompileGraphError::ProcessorOwnerNotFound(id))) if id == NodeID::DANGLING)
        );
        drop(current_schedule);
        drop(old_schedule);
    }

    #[test]
    fn occupied_queued_arena_is_prepared_and_committed_without_installing_it() {
        let mut graph = AudioGraph::new(&FirewheelConfig::default());
        let probe = Arc::new(Probe::default());
        let id = graph.add_node(TestNode(probe.clone()), None).unwrap();
        let old = stream_info(44_100, 64);
        let mut schedule = graph.compile(&old).unwrap();
        let index = schedule
            .new_node_processors
            .iter()
            .position(|node| node.id == id)
            .unwrap();
        let node = schedule.new_node_processors.remove(index);
        assert!(
            schedule
                .new_node_arena
                .as_mut()
                .unwrap()
                .insert_at(
                    id.0,
                    crate::processor::NodeEntry {
                        processor: node.processor,
                        event_data: NodeEventSchedulerData::new(node.is_pre_process),
                        bypass_declick: Declicker::SettledAt1,
                        is_bypassed: false,
                        is_first_process: true,
                        in_place_buffers: node.in_place_buffers,
                    }
                )
                .is_none()
        );
        let (mut sender, mut receiver) = ringbuf::HeapRb::new(2).split();
        assert!(
            sender
                .try_push(ContextToProcessorMsg::NewSchedule(schedule))
                .is_ok()
        );
        let next = stream_info(48_000, 128);
        prepare_queue(&mut receiver, &next, None, &mut HashSet::default(), &graph).unwrap();
        assert_eq!(probe.active_rate.load(Ordering::SeqCst), 44_100);
        assert!(probe.committed.lock().unwrap().is_empty());
        let mut store = ProcStore::with_capacity(1);
        let (mut logger, _messages) = realtime_logger(RealtimeLoggerConfig::default());
        commit_queue(
            &mut receiver,
            &next,
            &mut ProcStreamCtx {
                store: &mut store,
                logger: &mut logger,
            },
        );
        assert_eq!(probe.active_rate.load(Ordering::SeqCst), 48_000);
        assert_eq!(probe.prepared.lock().unwrap().len(), 1);
        assert_eq!(probe.committed.lock().unwrap().len(), 1);
        assert_eq!(probe.processed.load(Ordering::SeqCst), 0);
        receiver.clear();
        drop(graph);
        assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    }
}
