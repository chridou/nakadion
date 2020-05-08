use std::collections::BTreeMap;
use std::fmt::Arguments;
use std::time::{Duration, Instant};

use crate::consumer::LogPartitionEventsMode;
use crate::instrumentation::{Instrumentation, Instruments};
use crate::logging::Logger;
use crate::nakadi_types::subscription::EventTypePartition;

pub(crate) struct PartitionTracker {
    partitions: BTreeMap<EventTypePartition, Entry>,
    instrumentation: Instrumentation,
    logger: Box<dyn Logger + Send>,
    inactivity_after: Duration,
    mode: LogPartitionEventsMode,
}

impl PartitionTracker {
    pub fn new<L>(
        instrumentation: Instrumentation,
        inactivity_after: Duration,
        logger: L,
        mode: LogPartitionEventsMode,
    ) -> Self
    where
        L: Logger + Send + 'static,
    {
        Self {
            partitions: BTreeMap::new(),
            instrumentation,
            logger: Box::new(logger),
            inactivity_after,
            mode,
        }
    }

    /// Call when something was received
    pub fn activity(&mut self, partition: &EventTypePartition) {
        let now = Instant::now();
        if let Some(entry) = self.partitions.get_mut(partition) {
            if let Some(was_inactive_for) = entry.activity(now) {
                log_activity(
                    self.logger.as_ref(),
                    format_args!(
                        "Event type partition {} is active again after {:?} of inactivity",
                        partition, was_inactive_for,
                    ),
                    self.mode,
                );
                self.instrumentation.controller_partition_activated();
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
            self.instrumentation.controller_partition_activated();
        }
    }

    /// Call on tick to check inactivity
    pub fn check_for_inactivity(&mut self, now: Instant) {
        for (partition, entry) in self.partitions.iter_mut() {
            if let Some(was_active_for) = entry.check_for_inactivity(now, self.inactivity_after) {
                log_activity(
                    self.logger.as_ref(),
                    format_args!(
                        "Partition {} became inactive after {:?}",
                        partition, was_active_for
                    ),
                    self.mode,
                );
                self.instrumentation
                    .controller_partition_deactivated(was_active_for)
            }
        }
    }

    fn log_after_connect(&self, args: Arguments) {
        match self.mode {
            LogPartitionEventsMode::All | LogPartitionEventsMode::AfterConnect => {
                self.logger.info(args);
            }
            _ => {
                self.logger.debug(args);
            }
        }
    }
}

fn log_activity(logger: &dyn Logger, args: Arguments, mode: LogPartitionEventsMode) {
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
                    .instrumentation
                    .controller_partition_deactivated(when.elapsed()),
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
