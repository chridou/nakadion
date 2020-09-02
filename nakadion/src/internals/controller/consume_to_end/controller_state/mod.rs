use std::time::{Duration, Instant};

use crate::consumer::StreamDeadPolicy;
use crate::logging::Logger;
use crate::{
    components::connector::EventStreamBatch, instrumentation::Instruments, internals::StreamState,
    nakadi_types::subscription::EventTypePartition, Error,
};

use partition_tracker::PartitionTracker;

mod partition_tracker;

pub struct ControllerState {
    /// We might want to abort if we do not receive data from Nakadi in time
    /// This is basically to prevents us from being locked in a dead stream.
    pub stream_dead_policy: StreamDeadPolicy,
    pub warn_no_frames: Duration,
    pub warn_no_events: Duration,

    pub stream_started_at: Instant,
    pub last_events_received_at: Instant,
    pub last_frame_received_at: Instant,
    /// If streaming ends, we use this to correct the in flight metrics
    pub batches_sent_to_dispatcher: usize,
    partition_tracker: PartitionTracker,
    stream_state: StreamState,
    events_received: bool,
}

impl ControllerState {
    pub(crate) fn new(stream_state: StreamState) -> Self {
        let now = Instant::now();
        Self {
            stream_dead_policy: stream_state.config().stream_dead_policy,
            warn_no_frames: stream_state.config().warn_no_frames.into_duration(),
            warn_no_events: stream_state.config().warn_no_events.into_duration(),

            stream_started_at: now,
            last_events_received_at: now,
            last_frame_received_at: now,
            batches_sent_to_dispatcher: 0,
            partition_tracker: PartitionTracker::new(stream_state.clone()),
            stream_state,
            events_received: false,
        }
    }

    pub fn received_frame(
        &mut self,
        event_type_partition: &EventTypePartition,
        frame: &EventStreamBatch,
    ) {
        let frame_started_at = frame.frame_started_at();
        let frame_completed_at = frame.frame_completed_at();
        let now = Instant::now();
        self.last_frame_received_at = now;

        self.partition_tracker.activity(event_type_partition);

        if let Some(info_str) = frame.info_str() {
            self.stream_state
                .instrumentation
                .info_frame_received(frame_started_at, frame_completed_at);
            self.stream_state.info(format_args!(
                "Received info line for with frame #{} on {}: {}",
                frame.frame_id(),
                event_type_partition,
                info_str
            ));
        }

        if frame.is_keep_alive_line() {
            self.stream_state
                .instrumentation
                .keep_alive_frame_received(frame_started_at, frame_completed_at);
        } else {
            let bytes = frame.bytes().len();
            self.stream_state.instrumentation.batch_frame_received(
                frame_started_at,
                frame_completed_at,
                bytes,
            );

            // Only measure if we already have a previous batch
            if self.events_received {
                let events_batch_gap = now - self.last_events_received_at;
                self.stream_state
                    .instrumentation
                    .batch_frame_gap(events_batch_gap);
            } else {
                self.events_received = true;
            }

            self.last_events_received_at = now;
        }
    }

    pub fn received_tick(&mut self, _tick_timestamp: Instant) -> Result<(), Error> {
        if let Some(dead_for) = self
            .stream_dead_policy
            .is_stream_dead(self.last_frame_received_at, self.last_events_received_at)
        {
            self.stream_state.instrumentation().stream_dead(dead_for);
            return Err(Error::new(format!(
                "The stream is dead boys... for {:?}",
                dead_for
            )));
        }

        let elapsed = self.last_frame_received_at.elapsed();
        if elapsed >= self.warn_no_frames {
            self.stream_state
                .warn(format_args!("No frames for {:?}.", elapsed));
            self.stream_state
                .instrumentation()
                .no_frames_warning(elapsed);
        }

        let elapsed = self.last_events_received_at.elapsed();
        if elapsed >= self.warn_no_events {
            self.stream_state
                .warn(format_args!("No events received for {:?}.", elapsed));
            self.stream_state
                .instrumentation()
                .no_events_warning(elapsed);
        }

        self.partition_tracker.check_for_inactivity(Instant::now());
        Ok(())
    }
}
