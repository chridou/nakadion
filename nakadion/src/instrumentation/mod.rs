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
    fn controller_partition_activated(&self);
    fn controller_partition_deactivated(&self, active_for: Duration);

    // === DISPATCHER ===

    // === WORKERS ===

    fn handler_batch_processed_1(&self, n_events: Option<usize>, time: Duration);
    fn handler_batch_processed_2(&self, frame_received_at: Instant, n_bytes: usize);
    fn handler_deserialization(&self, n_bytes: usize, time: Duration);

    // === HANDLERS ===

    // === COMMITTER ===
    fn committer_cursor_received(&self, frame_received_at: Instant);
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

    #[cfg(feature = "metrix")]
    pub fn metrix(metrix: Metrix, detail: MetricsDetailLevel) -> Self {
        Instrumentation {
            instr: InstrumentationSelection::Metrix(metrix),
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

    fn consumer_batches_in_flight_inc(&self) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.consumer_batches_in_flight_inc(),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.consumer_batches_in_flight_inc(),
        }
    }
    fn consumer_batches_in_flight_dec(&self) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.consumer_batches_in_flight_dec(),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.consumer_batches_in_flight_dec(),
        }
    }
    fn consumer_batches_in_flight_dec_by(&self, by: usize) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.consumer_batches_in_flight_dec_by(by)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.consumer_batches_in_flight_dec_by(by)
            }
        }
    }

    // === STREAM ===
    fn stream_connect_attempt_success(&self, time: Duration) {
        if self.detail >= MetricsDetailLevel::Medium {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => {
                    instr.stream_connect_attempt_success(time)
                }
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => {
                    instr.stream_connect_attempt_success(time)
                }
            }
        }
    }
    fn stream_connect_attempt_failed(&self, time: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.stream_connect_attempt_failed(time)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.stream_connect_attempt_failed(time)
            }
        }
    }
    fn stream_connected(&self, time: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.stream_connected(time),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.stream_connected(time),
        }
    }
    fn stream_not_connected(&self, time: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.stream_not_connected(time),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.stream_not_connected(time),
        }
    }

    fn stream_chunk_received(&self, n_bytes: usize) {
        if self.detail == MetricsDetailLevel::High {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => instr.stream_chunk_received(n_bytes),
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => instr.stream_chunk_received(n_bytes),
            }
        }
    }
    fn stream_frame_received(&self, n_bytes: usize) {
        if self.detail >= MetricsDetailLevel::Medium {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => instr.stream_frame_received(n_bytes),
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => instr.stream_frame_received(n_bytes),
            }
        }
    }
    fn stream_tick_emitted(&self) {
        if self.detail >= MetricsDetailLevel::Medium {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => instr.stream_tick_emitted(),
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => instr.stream_tick_emitted(),
            }
        }
    }

    // === CONTROLLER ===
    fn controller_batch_received(&self, frame_received_at: Instant, events_bytes: usize) {
        if self.detail >= MetricsDetailLevel::Medium {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => {
                    instr.controller_batch_received(frame_received_at, events_bytes)
                }
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => {
                    instr.controller_batch_received(frame_received_at, events_bytes)
                }
            }
        }
    }
    fn controller_info_received(&self, frame_received_at: Instant) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.controller_info_received(frame_received_at)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.controller_info_received(frame_received_at)
            }
        }
    }
    fn controller_keep_alive_received(&self, frame_received_at: Instant) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.controller_keep_alive_received(frame_received_at)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.controller_keep_alive_received(frame_received_at)
            }
        }
    }

    fn controller_partition_activated(&self) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.controller_partition_activated(),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.controller_partition_activated(),
        }
    }
    fn controller_partition_deactivated(&self, active_for: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.controller_partition_deactivated(active_for)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.controller_partition_deactivated(active_for)
            }
        }
    }

    // === DISPATCHER ===

    // === WORKERS ===

    fn handler_batch_processed_1(&self, n_events: Option<usize>, time: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.handler_batch_processed_1(n_events, time)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.handler_batch_processed_1(n_events, time)
            }
        }
    }
    fn handler_batch_processed_2(&self, frame_received_at: Instant, n_bytes: usize) {
        if self.detail >= MetricsDetailLevel::Medium {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => {
                    instr.handler_batch_processed_2(frame_received_at, n_bytes)
                }
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => {
                    instr.handler_batch_processed_2(frame_received_at, n_bytes)
                }
            }
        }
    }
    fn handler_deserialization(&self, n_bytes: usize, time: Duration) {
        if self.detail >= MetricsDetailLevel::High {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => {
                    instr.handler_deserialization(n_bytes, time)
                }
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => {
                    instr.handler_deserialization(n_bytes, time)
                }
            }
        }
    }

    // === HANDLERS ===

    // === COMMITTER ===
    fn committer_cursor_received(&self, frame_received_at: Instant) {
        if self.detail >= MetricsDetailLevel::Medium {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => {
                    instr.committer_cursor_received(frame_received_at)
                }
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => {
                    instr.committer_cursor_received(frame_received_at)
                }
            }
        }
    }

    fn committer_cursors_committed(&self, n_cursors: usize, time: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.committer_cursors_committed(n_cursors, time)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.committer_cursors_committed(n_cursors, time)
            }
        }
    }
    fn committer_cursors_not_committed(&self, n_cursors: usize, time: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.committer_cursors_not_committed(n_cursors, time)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.committer_cursors_not_committed(n_cursors, time)
            }
        }
    }
}

#[derive(Clone)]
enum InstrumentationSelection {
    Off,
    Custom(Arc<dyn Instruments + Send + Sync>),
    #[cfg(feature = "metrix")]
    Metrix(Metrix),
}
