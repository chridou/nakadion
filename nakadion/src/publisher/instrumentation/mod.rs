use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::nakadi_types::publishing::BatchStats;

pub trait Instruments {
    /// All batch items have been successfully submitted
    fn published_ok(&self, elapsed: Duration);
    /// Not all batch items have been successfully submitted
    fn published_not_ok(&self, elapsed: Duration);
    /// Stats after each attempt to submit batches
    fn batch_items(&self, stats: BatchStats);
}

#[derive(Clone)]
enum InstrumentationSelection {
    Off,
    Custom(Arc<dyn Instruments + Send + Sync>),
    //   #[cfg(feature = "metrix")]
    //   Metrix(()),
}

impl Default for InstrumentationSelection {
    fn default() -> Self {
        Self::Off
    }
}

/// Used by the publisher to notify on measurable state changes
#[derive(Default, Clone)]
pub struct Instrumentation {
    instr: InstrumentationSelection,
}

impl Instruments for Instrumentation {
    fn published_ok(&self, elapsed: Duration) {}
    fn published_not_ok(&self, elapsed: Duration) {}
    fn batch_items(&self, stats: BatchStats) {}
}
