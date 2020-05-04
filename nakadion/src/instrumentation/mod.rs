//! Types for implementing custom instrumentation
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::nakadi_types::Error;

#[cfg(feature = "metrix")]
mod metrix_impl;

#[cfg(feature = "metrix")]
pub use self::metrix_impl::{Metrix, MetrixConfig, MetrixGaugeTrackingSecs};
#[cfg(feature = "metrix")]
pub use metrix::{
    driver::{DriverBuilder, TelemetryDriver},
    processor::ProcessorMount,
    AggregatesProcessors,
};

/// An interface on which `Nakadion` exposes metrics
///
/// An implementor of this interface can be used with
/// `Instrumentation::new`
pub trait Instruments {
    // === CONSUMER ===

    /// Triggered when a new batch with events was received
    fn consumer_batches_in_flight_inc(&self);
    /// Triggered when a batch with events was processed
    fn consumer_batches_in_flight_dec(&self);
    /// Usually triggered when there are still batches in flight and the stream aborts.
    ///
    /// This is a correction for the inflight metrics.
    fn consumer_batches_in_flight_dec_by(&self, by: usize);

    // === STREAM ===

    /// Triggered when a single connect attempt for a stream was successful
    ///
    /// `time` is the time for the request
    fn stream_connect_attempt_success(&self, time: Duration);
    /// Triggered when a single connect attempt for a stream failed
    ///
    /// `time` is the time for the request
    fn stream_connect_attempt_failed(&self, time: Duration);
    /// Triggered when a stream was finally connect after maybe multiple attempts
    ///
    /// `time` is the time for the whole cycle until a connection was made
    fn stream_connected(&self, time: Duration);
    /// Triggered when connecting to a stream finally failed after maybe multiple attempts
    ///
    /// `time` is the time for the whole cycle until a connection attempts finally failed
    fn stream_not_connected(&self, time: Duration);

    /// A chunk of data with `n_bytes` was received over the network
    fn stream_chunk_received(&self, n_bytes: usize);
    /// Chunks have been assembled to a complete frame containing all required data
    fn stream_frame_received(&self, n_bytes: usize);
    /// An internal tick signal has been emitted
    fn stream_tick_emitted(&self);

    // === CONTROLLER ===

    /// The controller received a frame which contained events
    ///
    /// The time it took from receiving the first chunk until it reached the controller are passed
    /// along with the complete bytes of the frame
    fn controller_batch_received(&self, frame_received_at: Instant, events_bytes: usize);
    /// The controller received a frame which contained info data
    ///
    /// The time it took from receiving the first chunk until it reached the controller are passed
    /// along with the complete bytes of the frame
    fn controller_info_received(&self, frame_received_at: Instant);
    /// The controller received a frame which contained no events
    ///
    /// The time it took from receiving the first chunk until it reached the controller are passed
    /// along with the complete bytes of the frame
    fn controller_keep_alive_received(&self, frame_received_at: Instant);
    /// A partition which was formerly not known to be active was sent
    /// data on
    fn controller_partition_activated(&self);
    /// A partition did not receive data for some time is is therefore considered inactive.
    fn controller_partition_deactivated(&self, active_for: Duration);

    /// No frames have been received for the given time and the warning has already  threshold elapsed
    fn controller_no_frames_warning(&self, no_frames_for: Duration);
    /// No events have been received for the given time and the warning threshold has already elapsed
    fn controller_no_events_warning(&self, no_events_for: Duration);

    // === DISPATCHER ===

    // === WORKERS ===

    /// Events were processed.
    ///
    /// The time the processing took for all events and the number of events are passed.
    fn handler_batch_processed_1(&self, n_events: Option<usize>, time: Duration);
    /// Events were processed.
    ///
    /// The bytes for all events and time elapsed from receiving the first chunk
    /// of the frame are passed.
    fn handler_batch_processed_2(&self, frame_received_at: Instant, n_bytes: usize);
    /// Events have been deserialized.
    ///
    /// The amount of bytes deserialized and the time it took are passed.
    fn handler_deserialization(&self, n_bytes: usize, time: Duration);

    // === HANDLERS ===

    // === COMMITTER ===

    /// Cursors to be committed have reached the commit stage.
    ///
    /// The time it took from receiving the first chunk until it reached the commit stage are passed
    fn committer_cursor_received(&self, frame_received_at: Instant);
    /// Cursors were successfully committed.
    ///
    /// The time request took is passed
    fn committer_cursors_committed(&self, n_cursors: usize, time: Duration);
    /// Cursors were not successfully committed.
    ///
    /// The time request took is passed
    fn committer_cursors_not_committed(&self, n_cursors: usize, time: Duration);
}

/// This defines the level of metrics being collected
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum MetricsDetailLevel {
    Low,
    Medium,
    High,
}

impl MetricsDetailLevel {
    env_funs!("METRICS_DETAIL_LEVEL");
}

impl Default for MetricsDetailLevel {
    fn default() -> Self {
        MetricsDetailLevel::Medium
    }
}

impl FromStr for MetricsDetailLevel {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "low" => Ok(MetricsDetailLevel::Low),
            "medium" => Ok(MetricsDetailLevel::Medium),
            "high" => Ok(MetricsDetailLevel::High),
            s => Err(Error::new(format!(
                "{} is not a valid MetricsDetailLevel",
                s
            ))),
        }
    }
}

/// Used by the consumer to notify on measurable state changes
#[derive(Clone)]
pub struct Instrumentation {
    instr: InstrumentationSelection,
    detail: MetricsDetailLevel,
}

impl Instrumentation {
    /// Do not collect any data.
    ///
    /// This is the default.
    pub fn off() -> Self {
        {
            Instrumentation {
                instr: InstrumentationSelection::Off,
                detail: MetricsDetailLevel::default(),
            }
        }
    }

    /// Use the given implementation of `Instruments`
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

    #[cfg(feature = "metrix")]
    pub fn metrix_mounted<A: AggregatesProcessors>(
        config: &MetrixConfig,
        detail: MetricsDetailLevel,
        processor: &mut A,
    ) -> Self {
        let metrix = Metrix::new(config, processor);
        Self::metrix(metrix, detail)
    }

    #[cfg(feature = "metrix")]
    pub fn metrix_mountable(
        config: &MetrixConfig,
        detail: MetricsDetailLevel,
        mountable_name: Option<&str>,
    ) -> (Self, ProcessorMount) {
        let (metrix, mount) = Metrix::new_mountable(config, mountable_name);
        (Self::metrix(metrix, detail), mount)
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

    fn controller_no_frames_warning(&self, no_frames_for: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.controller_no_frames_warning(no_frames_for)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.controller_no_frames_warning(no_frames_for)
            }
        }
    }

    fn controller_no_events_warning(&self, no_events_for: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.controller_no_events_warning(no_events_for)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.controller_no_events_warning(no_events_for)
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
