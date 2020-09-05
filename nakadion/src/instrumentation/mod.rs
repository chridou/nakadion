//! Types for implementing custom instrumentation
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::nakadi_types::Error;

use crate::components::{
    committer::{CommitError, CommitTrigger},
    connector::ConnectError,
    streams::EventStreamError,
};

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
///
/// Implementations of this trait should not be shared with multiple consumers
/// since they are stateful e.g. in flight batches.
pub trait Instruments {
    fn consumer_started(&self);
    fn consumer_stopped(&self, ran_for: Duration);
    fn streaming_ended(&self, streamed_for: Duration);

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
    fn stream_not_connected(&self, time: Duration, err: &ConnectError);

    /// A chunk of data with `n_bytes` was received over the network
    fn stream_chunk_received(&self, n_bytes: usize);
    /// Chunks have been assembled to a complete frame containing all required data
    fn stream_frame_completed(&self, n_bytes: usize, time: Duration);
    /// An internal tick signal has been emitted
    fn stream_tick_emitted(&self);

    /// The controller received a frame which contained info data
    ///
    /// The time it took from receiving the first chunk until it reached the controller are passed
    /// along with the complete bytes of the frame
    fn info_frame_received(&self, frame_started_at: Instant, frame_completed_at: Instant);
    /// The controller received a frame which contained no events
    ///
    /// The time it took from receiving the first chunk until it reached the controller are passed
    /// along with the complete bytes of the frame
    fn keep_alive_frame_received(&self, frame_started_at: Instant, frame_completed_at: Instant);
    /// A partition which was formerly not known to be active was sent
    /// data on

    /// The controller received a frame which contained events
    ///
    /// The time it took from receiving the first chunk until it reached the controller are passed
    /// along with the complete bytes of the frame
    fn batch_frame_received(
        &self,
        frame_started_at: Instant,
        frame_completed_at: Instant,
        events_bytes: usize,
    );

    /// The time elapsed between the reception of 2 batches with events
    fn batch_frame_gap(&self, gap: Duration);

    /// No frames have been received for the given time and the warning has already  threshold elapsed
    fn no_frames_warning(&self, no_frames_for: Duration);
    /// No events have been received for the given time and the warning threshold has already elapsed
    fn no_events_warning(&self, no_events_for: Duration);

    fn stream_dead(&self, after: Duration);

    /// The stream was aborted due to a streaming related error
    fn stream_error(&self, err: &EventStreamError);

    /// Tracks the number of unconsumed events.
    ///
    /// Only available if the `Consumer` was created with a clients that
    /// supports the `SubscriptionApi`
    fn stream_unconsumed_events(&self, n_unconsumed: usize);

    /// Triggered when a new batch with events was received
    fn batches_in_flight_inc(&self);
    /// Triggered when a batch with events was processed
    fn batches_in_flight_dec(&self);
    /// Usually triggered when there are still batches in flight and the stream aborts.
    ///
    /// This is a correction for the inflight metrics.
    fn batches_in_flight_reset(&self);

    fn event_type_partition_activated(&self);

    /// A partition did not receive data for some time is is therefore considered inactive.
    fn event_type_partition_deactivated(&self, active_for: Duration);

    fn batch_processing_started(&self, frame_started_at: Instant, frame_completed_at: Instant);

    /// Events were processed.
    fn batch_processed(&self, n_bytes: usize, time: Duration);
    /// Events were processed.
    fn batch_processed_n_events(&self, n_events: usize);
    /// Events have been deserialized.
    ///
    /// The amount of bytes deserialized and the time it took are passed.
    fn batch_deserialized(&self, n_bytes: usize, time: Duration);

    /// Cursors to be committed have reached the commit stage.
    ///
    /// The time it took from receiving the first chunk until it reached the commit stage are passed
    fn cursor_to_commit_received(&self, frame_started_at: Instant, frame_completed_at: Instant);
    /// Cursors commit was triggered.
    fn cursors_commit_triggered(&self, trigger: CommitTrigger);
    /// Ages of the cursors when making a commit attempt to Nakadi
    ///
    /// This ages are measured before making an attempt to make a call to Nakadi.
    /// `first_cursor_age` is the critical age of the first cursor which might be commited
    /// with a later cursor which has the age `last_cursor_age`.
    ///
    /// `` should be true, when the first cursor reached a critical age which
    /// might endanger or even cause the commit to fail.
    fn cursor_ages_on_commit_attempt(
        &self,
        first_cursor_age: Duration,
        last_cursor_age: Duration,
        first_cursor_age_warning: bool,
    );
    /// Cursors were successfully committed.
    ///
    /// The time request took is passed
    fn cursors_committed(&self, n_cursors: usize, time: Duration);
    /// Cursors were not successfully committed.
    ///
    /// The time request took is passed
    fn cursors_not_committed(&self, n_cursors: usize, time: Duration, err: &CommitError);
    /// An attempt to commit cursors failed. There might still be a retry
    ///
    /// The time request took is passed
    fn commit_cursors_attempt_failed(&self, n_cursors: usize, time: Duration);
}

impl<T> crate::tools::subscription_stats::Instruments for T
where
    T: Instruments,
{
    fn unconsumed_events(&self, n_unconsumed: usize) {
        self.stream_unconsumed_events(n_unconsumed);
    }
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
    fn consumer_started(&self) {
        if self.detail >= MetricsDetailLevel::Medium {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => instr.consumer_started(),
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => instr.consumer_started(),
            }
        }
    }

    fn consumer_stopped(&self, ran_for: Duration) {
        if self.detail >= MetricsDetailLevel::Medium {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => instr.consumer_stopped(ran_for),
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => instr.consumer_stopped(ran_for),
            }
        }
    }

    fn streaming_ended(&self, streamed_for: Duration) {
        if self.detail >= MetricsDetailLevel::Medium {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => instr.streaming_ended(streamed_for),
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => instr.streaming_ended(streamed_for),
            }
        }
    }

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

    fn stream_not_connected(&self, time: Duration, err: &ConnectError) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.stream_not_connected(time, err),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.stream_not_connected(time, err),
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
    fn stream_frame_completed(&self, n_bytes: usize, time: Duration) {
        if self.detail == MetricsDetailLevel::High {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => {
                    instr.stream_frame_completed(n_bytes, time)
                }
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => {
                    instr.stream_frame_completed(n_bytes, time)
                }
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

    fn info_frame_received(&self, frame_started_at: Instant, frame_completed_at: Instant) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.info_frame_received(frame_started_at, frame_completed_at)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.info_frame_received(frame_started_at, frame_completed_at)
            }
        }
    }
    fn keep_alive_frame_received(&self, frame_started_at: Instant, frame_completed_at: Instant) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.keep_alive_frame_received(frame_started_at, frame_completed_at)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.keep_alive_frame_received(frame_started_at, frame_completed_at)
            }
        }
    }

    fn batch_frame_received(
        &self,
        frame_started_at: Instant,
        frame_completed_at: Instant,
        events_bytes: usize,
    ) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.batch_frame_received(frame_started_at, frame_completed_at, events_bytes)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.batch_frame_received(frame_started_at, frame_completed_at, events_bytes)
            }
        }
    }

    fn batch_frame_gap(&self, gap: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.batch_frame_gap(gap),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.batch_frame_gap(gap),
        }
    }

    fn no_frames_warning(&self, no_frames_for: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.no_frames_warning(no_frames_for),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.no_frames_warning(no_frames_for),
        }
    }

    fn no_events_warning(&self, no_events_for: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.no_events_warning(no_events_for),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.no_events_warning(no_events_for),
        }
    }

    fn stream_dead(&self, after: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.stream_dead(after),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.stream_dead(after),
        }
    }

    fn stream_error(&self, err: &EventStreamError) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.stream_error(err),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.stream_error(err),
        }
    }

    fn stream_unconsumed_events(&self, n_unconsumed: usize) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.stream_unconsumed_events(n_unconsumed)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.stream_unconsumed_events(n_unconsumed)
            }
        }
    }

    fn batches_in_flight_inc(&self) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.batches_in_flight_inc(),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.batches_in_flight_inc(),
        }
    }
    fn batches_in_flight_dec(&self) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.batches_in_flight_dec(),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.batches_in_flight_dec(),
        }
    }
    fn batches_in_flight_reset(&self) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.batches_in_flight_reset(),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.batches_in_flight_reset(),
        }
    }

    fn event_type_partition_activated(&self) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.event_type_partition_activated(),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.event_type_partition_activated(),
        }
    }

    fn event_type_partition_deactivated(&self, active_for: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.event_type_partition_deactivated(active_for)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.event_type_partition_deactivated(active_for)
            }
        }
    }

    fn batch_processing_started(&self, frame_started_at: Instant, frame_completed_at: Instant) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.batch_processing_started(frame_started_at, frame_completed_at)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.batch_processing_started(frame_started_at, frame_completed_at)
            }
        }
    }

    fn batch_processed(&self, n_bytes: usize, time: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.batch_processed(n_bytes, time),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.batch_processed(n_bytes, time),
        }
    }
    fn batch_processed_n_events(&self, n_events: usize) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.batch_processed_n_events(n_events),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.batch_processed_n_events(n_events),
        }
    }
    fn batch_deserialized(&self, n_bytes: usize, time: Duration) {
        if self.detail >= MetricsDetailLevel::Medium {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => {
                    instr.batch_deserialized(n_bytes, time)
                }
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => {
                    instr.batch_deserialized(n_bytes, time)
                }
            }
        }
    }

    fn cursor_to_commit_received(&self, frame_started_at: Instant, frame_completed_at: Instant) {
        if self.detail >= MetricsDetailLevel::Medium {
            match self.instr {
                InstrumentationSelection::Off => {}
                InstrumentationSelection::Custom(ref instr) => {
                    instr.cursor_to_commit_received(frame_started_at, frame_completed_at)
                }
                #[cfg(feature = "metrix")]
                InstrumentationSelection::Metrix(ref instr) => {
                    instr.cursor_to_commit_received(frame_started_at, frame_completed_at)
                }
            }
        }
    }

    fn cursors_commit_triggered(&self, trigger: CommitTrigger) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.cursors_commit_triggered(trigger),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.cursors_commit_triggered(trigger),
        }
    }

    fn cursor_ages_on_commit_attempt(
        &self,
        first_cursor_age: Duration,
        last_cursor_age: Duration,
        first_cursor_age_warning: bool,
    ) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.cursor_ages_on_commit_attempt(
                first_cursor_age,
                last_cursor_age,
                first_cursor_age_warning,
            ),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.cursor_ages_on_commit_attempt(
                first_cursor_age,
                last_cursor_age,
                first_cursor_age_warning,
            ),
        }
    }

    fn cursors_committed(&self, n_cursors: usize, time: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => instr.cursors_committed(n_cursors, time),
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => instr.cursors_committed(n_cursors, time),
        }
    }
    fn cursors_not_committed(&self, n_cursors: usize, time: Duration, err: &CommitError) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.cursors_not_committed(n_cursors, time, err)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.cursors_not_committed(n_cursors, time, err)
            }
        }
    }

    fn commit_cursors_attempt_failed(&self, n_cursors: usize, time: Duration) {
        match self.instr {
            InstrumentationSelection::Off => {}
            InstrumentationSelection::Custom(ref instr) => {
                instr.commit_cursors_attempt_failed(n_cursors, time)
            }
            #[cfg(feature = "metrix")]
            InstrumentationSelection::Metrix(ref instr) => {
                instr.commit_cursors_attempt_failed(n_cursors, time)
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
