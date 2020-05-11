use std::collections::BTreeMap;
use std::fmt::Arguments;
use std::time::{Duration, Instant};

use crate::consumer::{LogPartitionEventsMode, StreamDeadPolicy};
use crate::logging::Logger;
use crate::{
    components::connector::BatchLine, instrumentation::Instruments, internals::StreamState,
    nakadi_types::subscription::EventTypePartition, Error,
};

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
        }
    }

    pub fn received_frame(&mut self, event_type_partition: &EventTypePartition, frame: &BatchLine) {
        let frame_received_at = frame.received_at();
        let now = Instant::now();
        self.last_frame_received_at = now;

        self.partition_tracker.activity(event_type_partition);

        if let Some(info_str) = frame.info_str() {
            self.stream_state
                .instrumentation
                .info_frame_received(frame_received_at);
            self.stream_state.info(format_args!(
                "Received info line for {}: {}",
                event_type_partition, info_str
            ));
        }

        if frame.is_keep_alive_line() {
            self.stream_state
                .instrumentation
                .keep_alive_frame_received(frame_received_at);
        } else {
            let bytes = frame.bytes().len();
            self.stream_state
                .instrumentation
                .batch_frame_received(frame_received_at, bytes);
            self.last_events_received_at = now;
        }
    }

    pub fn received_tick(&mut self, _tick_timestamp: Instant) -> Result<(), Error> {
        if self
            .stream_dead_policy
            .is_stream_dead(self.last_frame_received_at, self.last_events_received_at)
        {
            return Err(Error::new("The stream is dead boys..."));
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

struct PartitionTracker {
    partitions: BTreeMap<EventTypePartition, Entry>,
    stream_state: StreamState,
    inactivity_timeout: Duration,
    mode: LogPartitionEventsMode,
}

impl PartitionTracker {
    pub fn new(stream_state: StreamState) -> Self {
        let inactivity_timeout = stream_state.config().partition_inactivity_timeout.into();
        let mode = stream_state.config().log_partition_events_mode;

        Self {
            partitions: BTreeMap::new(),
            stream_state,
            inactivity_timeout,
            mode,
        }
    }

    /// Call when something was received
    pub fn activity(&mut self, partition: &EventTypePartition) {
        let now = Instant::now();
        if let Some(entry) = self.partitions.get_mut(partition) {
            if let Some(was_inactive_for) = entry.activity(now) {
                log_activity(
                    &self.stream_state,
                    format_args!(
                        "Event type partition {} is active again after {:?} of inactivity",
                        partition, was_inactive_for,
                    ),
                    self.mode,
                );
                self.stream_state
                    .instrumentation()
                    .event_type_partition_activated();
            }
        } else {
            let entry = Entry {
                state: PartitionActivationState::ActiveSince(now),
                last_activity_at: now,
            };
            self.partitions.insert(partition.clone(), entry);
            self.log_after_connect(format_args!(
                "New active event type partition {}",
                partition
            ));
            self.stream_state
                .instrumentation
                .event_type_partition_activated();
        }
    }

    /// Call on tick to check inactivity
    pub fn check_for_inactivity(&mut self, now: Instant) {
        for (partition, entry) in self.partitions.iter_mut() {
            if let Some(was_active_for) = entry.check_for_inactivity(now, self.inactivity_timeout) {
                log_activity(
                    &self.stream_state,
                    format_args!(
                        "Partition {} became inactive after {:?}",
                        partition, was_active_for
                    ),
                    self.mode,
                );
                self.stream_state
                    .instrumentation
                    .event_type_partition_deactivated(was_active_for)
            }
        }
    }

    fn log_after_connect(&self, args: Arguments) {
        match self.mode {
            LogPartitionEventsMode::All | LogPartitionEventsMode::AfterConnect => {
                self.stream_state.info(args);
            }
            _ => {
                self.stream_state.debug(args);
            }
        }
    }
}

fn log_activity(stream_state: &StreamState, args: Arguments, mode: LogPartitionEventsMode) {
    match mode {
        LogPartitionEventsMode::All | LogPartitionEventsMode::ActivityChange => {
            stream_state.info(args);
        }
        _ => {
            stream_state.debug(args);
        }
    }
}

impl Drop for PartitionTracker {
    fn drop(&mut self) {
        for entry in self.partitions.iter().map(|(_, entry)| entry) {
            match entry.state {
                PartitionActivationState::ActiveSince(when) => self
                    .stream_state
                    .instrumentation()
                    .event_type_partition_deactivated(when.elapsed()),
                PartitionActivationState::InactiveSince(_when) => {}
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum PartitionActivationState {
    ActiveSince(Instant),
    InactiveSince(Instant),
}

struct Entry {
    state: PartitionActivationState,
    last_activity_at: Instant,
}

impl Entry {
    /// Returns Some(inactive for) if the partition was reactivated
    pub fn activity(&mut self, now: Instant) -> Option<Duration> {
        match self.state {
            PartitionActivationState::ActiveSince(_) => {
                self.last_activity_at = now;
                None
            }
            PartitionActivationState::InactiveSince(when) => {
                self.state = PartitionActivationState::ActiveSince(now);
                Some(when.elapsed())
            }
        }
    }

    /// Returns `Some(active_for)` if the partition was deactivated.
    pub fn check_for_inactivity(
        &mut self,
        now: Instant,
        inactive_after: Duration,
    ) -> Option<Duration> {
        match self.state {
            PartitionActivationState::ActiveSince(when) => {
                if self.last_activity_at + inactive_after < now {
                    self.state = PartitionActivationState::InactiveSince(now);
                    Some(when.elapsed())
                } else {
                    None
                }
            }
            PartitionActivationState::InactiveSince(_) => None,
        }
    }
}
