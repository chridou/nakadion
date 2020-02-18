//! Types for implementing custom instrumentation
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "metrix")]
mod metrix;

#[cfg(feature = "metrix")]
pub use self::metrix::{Metrix, MetrixConfig, MetrixTrackingSecs};

pub trait Instruments {
    // === CONSUMER ===
    fn consumer_batches_in_flight_inc(&self);
    fn consumer_batches_in_flight_dec(&self);
    fn consumer_batches_in_flight_dec_by(&self, by: usize);

    // === STREAM ===
    fn stream_connect_attempt_success(&self, time: Duration);
    fn stream_connect_attempt_failed(&self, time: Duration);
    fn stream_connected(&self, time: Duration);
    fn stream_not_connected(&self, time: Duration);

    fn stream_chunk_received(&self, n_bytes: usize);
    fn stream_frame_received(&self, n_bytes: usize);
    fn stream_tick_emitted(&self);

    // === CONTROLLER ===
    fn controller_batch_received(&self, frame_received_at: Instant, events_bytes: usize);
    fn controller_info_received(&self, frame_received_at: Instant);
    fn controller_keep_alive_received(&self, frame_received_at: Instant);

    // === DISPATCHER ===

    // === WORKERS ===

    fn worker_batch_processed(&self, n_bytes: usize, n_events: Option<usize>, time: Duration);
    fn worker_deserialization_time(&self, n_bytes: usize, time: Duration);

    // === HANDLERS ===

    // === COMMITTER ===

    fn committer_cursors_committed(&self, n_cursors: usize, time: Duration);
    fn committer_cursors_not_committed(&self, n_cursors: usize, time: Duration);
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum MetricsDetailLevel {
    Low,
    Medium,
    High,
}

impl Default for MetricsDetailLevel {
    fn default() -> Self {
        MetricsDetailLevel::Medium
    }
}

#[derive(Clone)]
pub struct Instrumentation {
    instr: InstrumentationSelection,
    detail: MetricsDetailLevel,
}

impl Instrumentation {
    pub fn off() -> Self {
        {
            Instrumentation {
                instr: InstrumentationSelection::Off,
                detail: MetricsDetailLevel::default(),
            }
        }
    }

    pub fn new<I>(instruments: I, detail: MetricsDetailLevel) -> Self
    where
        I: Instruments + Send + Sync + 'static,
    {
        Instrumentation {
            instr: InstrumentationSelection::Custom(Arc::new(instruments)),
            detail,
        }
    }
}

impl Default for Instrumentation {
    fn default() -> Self {
        Instrumentation::off()
    }
}

impl fmt::Debug for Instrumentation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Instrumentation")?;
        Ok(())
    }
}

impl Instruments for Instrumentation {
    // === CONSUMER ===

    fn consumer_batches_in_flight_inc(&self) {}
    fn consumer_batches_in_flight_dec(&self) {}
    fn consumer_batches_in_flight_dec_by(&self, by: usize) {}

    // === STREAM ===
    fn stream_connect_attempt_success(&self, time: Duration) {
        if self.detail >= MetricsDetailLevel::Medium {}
    }
    fn stream_connect_attempt_failed(&self, time: Duration) {}
    fn stream_connected(&self, time: Duration) {}
    fn stream_not_connected(&self, time: Duration) {}

    fn stream_chunk_received(&self, n_bytes: usize) {
        if self.detail == MetricsDetailLevel::High {}
    }
    fn stream_frame_received(&self, n_bytes: usize) {
        if self.detail >= MetricsDetailLevel::Medium {}
    }
    fn stream_tick_emitted(&self) {
        if self.detail >= MetricsDetailLevel::Medium {}
    }

    // === CONTROLLER ===
    fn controller_batch_received(&self, frame_received_at: Instant, events_bytes: usize) {}
    fn controller_info_received(&self, frame_received_at: Instant) {}
    fn controller_keep_alive_received(&self, frame_received_at: Instant) {}

    // === DISPATCHER ===

    // === WORKERS ===

    fn worker_batch_processed(&self, n_bytes: usize, n_events: Option<usize>, time: Duration) {}
    fn worker_deserialization_time(&self, n_bytes: usize, time: Duration) {
        if self.detail >= MetricsDetailLevel::Medium {}
    }

    // === HANDLERS ===

    // === COMMITTER ===

    fn committer_cursors_committed(&self, n_cursors: usize, time: Duration) {}
    fn committer_cursors_not_committed(&self, n_cursors: usize, time: Duration) {}
}

#[derive(Clone)]
enum InstrumentationSelection {
    Off,
    Custom(Arc<dyn Instruments + Send + Sync>),
}
