use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::instrumentation::{Instrumentation, Instruments};
use crate::logging::Logger;
use crate::nakadi_types::subscription::EventTypePartition;

pub(crate) struct PartitionTracker {
    partitions: BTreeMap<EventTypePartition, Entry>,
    instrumentation: Instrumentation,
    logger: Box<dyn Logger + Send>,
    inactivity_after: Duration,
}

impl PartitionTracker {
    pub fn new<L>(instrumentation: Instrumentation, inactivity_after: Duration, logger: L) -> Self
    where
        L: Logger + Send + 'static,
    {
        Self {
            partitions: BTreeMap::new(),
            instrumentation,
            logger: Box::new(logger),
            inactivity_after,
        }
    }

    pub fn activity(&mut self, partition: &EventTypePartition) {
        let now = Instant::now();
        if let Some(entry) = self.partitions.get_mut(partition) {
            if entry.activity(now) {
                self.logger.info(format_args!(
                    "Known partition {} is active again",
                    partition
                ));
                self.instrumentation.controller_partition_activated();
            }
        } else {
            let entry = Entry {
                activated_at: Some(now),
                last_activity_at: now,
            };
            self.partitions.insert(partition.clone(), entry);
            self.logger
                .info(format_args!("New active partition {}", partition));
            self.instrumentation.controller_partition_activated();
        }
    }

    pub fn check_for_inactivity(&mut self, now: Instant) {
        for (partition, entry) in self.partitions.iter_mut() {
            if let Some(activated_at) = entry.check_for_inactivity(now, self.inactivity_after) {
                self.logger.info(format_args!(
                    "Known partition {} became inactive",
                    partition
                ));
                self.instrumentation
                    .controller_partition_deactivated(activated_at.elapsed())
            }
        }
    }
}

impl Drop for PartitionTracker {
    fn drop(&mut self) {
        for activated_at in self
            .partitions
            .iter()
            .filter_map(|(_, entry)| entry.activated_at)
        {
            self.instrumentation
                .controller_partition_deactivated(activated_at.elapsed())
        }
    }
}

struct Entry {
    activated_at: Option<Instant>,
    last_activity_at: Instant,
}

impl Entry {
    /// Returns true if the partition was reactivated
    pub fn activity(&mut self, now: Instant) -> bool {
        let was_inactive = self.activated_at.is_none();
        if was_inactive {
            self.activated_at = Some(now);
        }
        self.last_activity_at = now;

        was_inactive
    }

    /// Returns `Some(Instant)` if the partition was deactivated.
    /// The timestamp is when the partition was activated.
    pub fn check_for_inactivity(
        &mut self,
        now: Instant,
        inactive_after: Duration,
    ) -> Option<Instant> {
        if let Some(activated_at) = self.activated_at {
            if activated_at + inactive_after < now {
                self.activated_at = None;
                Some(activated_at)
            } else {
                None
            }
        } else {
            None
        }
    }
}
