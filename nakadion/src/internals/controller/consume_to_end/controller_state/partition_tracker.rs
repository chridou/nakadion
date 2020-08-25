use std::collections::BTreeMap;
use std::fmt::Arguments;
use std::time::{Duration, Instant};

use crate::consumer::LogPartitionEventsMode;
use crate::logging::Logger;
use crate::{
    instrumentation::Instruments, internals::StreamState,
    nakadi_types::subscription::EventTypePartition,
};
pub(crate) struct PartitionTracker {
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
                    partition,
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
            self.log_after_connect(
                format_args!("New active event type partition {}", partition),
                partition,
            );
            self.stream_state
                .instrumentation
                .event_type_partition_activated();
        }
    }

    /// Call on tick to check for inactivity
    pub fn check_for_inactivity(&mut self, now: Instant) {
        let inactivity_timeout = self.inactivity_timeout;
        for (partition, entry) in self.partitions.iter_mut() {
            if let Some(was_active_for) = entry.check_for_inactivity(now, inactivity_timeout) {
                log_activity(
                    &self.stream_state,
                    format_args!(
                        "Partition {} became inactive after {:?} of activity. \
                        Inactivity timeout is {:?}.",
                        partition, was_active_for, inactivity_timeout
                    ),
                    partition,
                    self.mode,
                );
                self.stream_state
                    .instrumentation
                    .event_type_partition_deactivated(was_active_for)
            }
        }
    }

    fn log_after_connect(&self, args: Arguments, etp: &EventTypePartition) {
        let logger = self
            .stream_state
            .logger()
            .partition_id(etp.partition().clone())
            .event_type(etp.event_type().clone());
        match self.mode {
            LogPartitionEventsMode::All | LogPartitionEventsMode::AfterConnect => {
                logger.info(args);
            }
            _ => {
                logger.debug(args);
            }
        }
    }
}

fn log_activity(
    stream_state: &StreamState,
    args: Arguments,
    etp: &EventTypePartition,
    mode: LogPartitionEventsMode,
) {
    let logger = stream_state
        .logger()
        .partition_id(etp.partition().clone())
        .event_type(etp.event_type().clone());
    match mode {
        LogPartitionEventsMode::All | LogPartitionEventsMode::ActivityChange => {
            logger.info(args);
        }
        _ => {
            logger.debug(args);
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
        self.last_activity_at = now;
        match self.state {
            PartitionActivationState::ActiveSince(_) => None,
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
                    Some(
                        self.last_activity_at
                            .checked_duration_since(when)
                            .unwrap_or(Duration::from_secs(0)),
                    )
                } else {
                    None
                }
            }
            PartitionActivationState::InactiveSince(_) => None,
        }
    }
}
