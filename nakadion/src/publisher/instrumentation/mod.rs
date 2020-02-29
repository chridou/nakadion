use std::sync::Arc;
use std::time::Duration;

use crate::nakadi_types::publishing::BatchStats;

#[cfg(feature = "metrix")]
pub use metrix::{
    driver::{DriverBuilder, TelemetryDriver},
    processor::ProcessorMount,
    AggregatesProcessors,
};

#[cfg(feature = "metrix")]
mod metrix_impl;
#[cfg(feature = "metrix")]
pub use metrix_impl::*;

/// Instruments a `Publisher`
pub trait Instruments {
    /// All batch items have been successfully submitted
    fn published(&self, elapsed: Duration);
    /// Not all batch items have been successfully submitted
    fn publish_failed(&self, elapsed: Duration);
    /// Stats after each attempt to submit a batch
    fn batch_stats(&self, stats: BatchStats);
}

#[derive(Clone)]
enum InstrumentationSelection {
    Off,
    Custom(Arc<dyn Instruments + Send + Sync>),
    #[cfg(feature = "metrix")]
    Metrix(Metrix),
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

impl Instrumentation {
    /// Do not collect any data.
    ///
    /// This is the default.
    pub fn off() -> Self {
        {
            Instrumentation {
                instr: InstrumentationSelection::Off,
            }
        }
    }

    /// Use the given implementation of `Instruments`
    pub fn new<I>(instruments: I) -> Self
    where
        I: Instruments + Send + Sync + 'static,
    {
        Instrumentation {
            instr: InstrumentationSelection::Custom(Arc::new(instruments)),
        }
    }

    #[cfg(feature = "metrix")]
    pub fn metrix(metrix: Metrix) -> Self {
        Instrumentation {
            instr: InstrumentationSelection::Metrix(metrix),
        }
    }

    #[cfg(feature = "metrix")]
    pub fn metrix_mounted<A: AggregatesProcessors>(processor: &mut A) -> Self {
        let metrix = Metrix::new(processor);
        Self::metrix(metrix)
    }

    #[cfg(feature = "metrix")]
    pub fn metrix_mountable(mountable_name: Option<&str>) -> (Self, ProcessorMount) {
        let (metrix, mount) = Metrix::new_mountable(mountable_name);
        (Self::metrix(metrix), mount)
    }
}

impl Instruments for Instrumentation {
    fn published(&self, elapsed: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.published(elapsed),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.published(elapsed),
        }
    }
    fn publish_failed(&self, elapsed: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.publish_failed(elapsed),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.publish_failed(elapsed),
        }
    }
    fn batch_stats(&self, stats: BatchStats) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.batch_stats(stats),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.batch_stats(stats),
        }
    }
}
