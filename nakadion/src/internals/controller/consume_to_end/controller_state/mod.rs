use std::time::{Duration, Instant};

use crate::consumer::StreamDeadPolicy;
use crate::logging::Logger;
use crate::{
    instrumentation::Instruments, internals::StreamState,
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
    partition_tracker: PartitionTracker,
    stream_state: StreamState,
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
            partition_tracker: PartitionTracker::new(stream_state.clone()),
            stream_state,
        }
    }

    pub fn received_batch_frame(&mut self, event_type_partition: &EventTypePartition) {
        let now = Instant::now();
        self.last_frame_received_at = now;

        self.partition_tracker.activity(event_type_partition);

        self.last_events_received_at = now;
    }

    pub fn received_keep_alive_frame(&mut self) {
        let now = Instant::now();
        self.last_frame_received_at = now;
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
            self.stream_state.warn(format_args!(
                "No frames for {:?}. {} uncommitted batch(es).",
                elapsed,
                self.stream_state.uncommitted_batches()
            ));
            self.stream_state
                .instrumentation()
                .no_frames_warning(elapsed);
        }

        let elapsed = self.last_events_received_at.elapsed();
        if elapsed >= self.warn_no_events {
            self.stream_state.warn(format_args!(
                "No events received for {:?}. {} uncommitted batch(es).",
                elapsed,
                self.stream_state.uncommitted_batches()
            ));
            self.stream_state
                .instrumentation()
                .no_events_warning(elapsed);
        }

        self.partition_tracker.check_for_inactivity(Instant::now());
        Ok(())
    }
}
