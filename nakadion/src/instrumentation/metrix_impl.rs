use std::time::{Duration, Instant};

use metrix::instruments::Gauge;
use metrix::{
    processor::ProcessorMount, AggregatesProcessors, Decrement, DecrementBy, Increment,
    TelemetryTransmitter, TimeUnit, TransmitsTelemetryData,
};

use serde::{Deserialize, Serialize};

use super::Instruments;

/// Instrumentation with Metrix
#[derive(Clone)]
pub struct Metrix {
    tx: TelemetryTransmitter<Metric>,
}

impl Metrix {
    /// Initializes the metrics.
    ///
    /// Adds them directly into the given `processor` without creating an additional group.
    pub fn new<A: AggregatesProcessors>(config: &MetrixConfig, processor: &mut A) -> Self {
        let (tx, global_proc) = instr::create(&config);
        processor.add_processor(global_proc);
        Metrix { tx }
    }

    /// Creates new Metrix instrumentation and returns a mount that can be plugged with metrix
    /// and the instrumentation which can be plugged into the `Consumer`
    pub fn new_mountable(config: &MetrixConfig, name: Option<&str>) -> (Metrix, ProcessorMount) {
        let mut mount = if let Some(name) = name {
            ProcessorMount::new(name)
        } else {
            ProcessorMount::default()
        };

        let me = Self::new(config, &mut mount);

        (me, mount)
    }
}

new_type! {
    #[doc="The time a gauge will track values.\n\nDefault is 60s.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct MetrixGaugeTrackingSecs(u32, env="METRIX_GAUGE_TRACKING_SECS");
}
impl Default for MetrixGaugeTrackingSecs {
    fn default() -> Self {
        60.into()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MetrixConfig {
    /// Enables tracking for gauges for the given amount of seconds
    pub gauge_tracking_secs: Option<MetrixGaugeTrackingSecs>,
}

impl MetrixConfig {
    env_ctors!();
    fn fill_from_env_prefixed_internal<T: AsRef<str>>(
        &mut self,
        prefix: T,
    ) -> Result<(), crate::Error> {
        if self.gauge_tracking_secs.is_none() {
            self.gauge_tracking_secs =
                MetrixGaugeTrackingSecs::try_from_env_prefixed(prefix.as_ref())?;
        }
        Ok(())
    }
}

impl Instruments for Metrix {
    // === GLOBAL ===

    fn consumer_batches_in_flight_inc(&self) {
        self.tx
            .observed_one_value_now(Metric::ConsumerInFlightChanged, Increment);
    }
    fn consumer_batches_in_flight_dec(&self) {
        self.tx
            .observed_one_value_now(Metric::ConsumerInFlightChanged, Decrement);
    }
    fn consumer_batches_in_flight_dec_by(&self, by: usize) {
        self.tx
            .observed_one_value_now(Metric::ConsumerInFlightChanged, DecrementBy(by as u32));
    }

    // === STREAM ===
    fn stream_connect_attempt_success(&self, time: Duration) {
        self.tx.observed_one_value_now(
            Metric::StreamConnectAttemptSuccessTime,
            (time, TimeUnit::Milliseconds),
        );
    }
    fn stream_connect_attempt_failed(&self, time: Duration) {
        self.tx.observed_one_value_now(
            Metric::StreamConnectAttemptFailedTime,
            (time, TimeUnit::Milliseconds),
        );
    }
    fn stream_connected(&self, time: Duration) {
        self.tx
            .observed_one_value_now(Metric::StreamConnectedTime, (time, TimeUnit::Milliseconds));
    }
    fn stream_not_connected(&self, time: Duration) {
        self.tx.observed_one_value_now(
            Metric::StreamNotConnectedTime,
            (time, TimeUnit::Milliseconds),
        );
    }

    fn stream_chunk_received(&self, n_bytes: usize) {
        self.tx
            .observed_one_value_now(Metric::StreamChunkReceivedBytes, n_bytes);
    }
    fn stream_frame_received(&self, n_bytes: usize) {
        self.tx
            .observed_one_value_now(Metric::StreamFrameReceivedBytes, n_bytes);
    }
    fn stream_tick_emitted(&self) {
        self.tx.observed_one_now(Metric::StreamTickEmitted);
    }

    // === CONTROLLER ===
    fn controller_batch_received(&self, frame_received_at: Instant, events_bytes: usize) {
        self.tx
            .observed_one_value_now(Metric::ControllerBatchReceivedBytes, events_bytes);
        self.tx.observed_one_value_now(
            Metric::ControllerBatchReceivedLag,
            (frame_received_at.elapsed(), TimeUnit::Microseconds),
        );
    }
    fn controller_info_received(&self, frame_received_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::ControllerInfoReceivedLag,
            (frame_received_at.elapsed(), TimeUnit::Microseconds),
        );
    }
    fn controller_keep_alive_received(&self, frame_received_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::ControllerKeepAliveReceivedLag,
            (frame_received_at.elapsed(), TimeUnit::Microseconds),
        );
    }

    fn controller_partition_activated(&self) {
        self.tx
            .observed_one_value_now(Metric::ControllerPartitionActivated, Increment);
    }
    fn controller_partition_deactivated(&self, active_for: Duration) {
        self.tx.observed_one_value_now(
            Metric::ControllerPartitionDeactivatedAfter,
            (active_for, TimeUnit::Milliseconds),
        );
    }

    fn controller_no_frames_warning(&self, no_frames_for: Duration) {
        self.tx.observed_one_value_now(
            Metric::ControllerNoFramesForWarning,
            (no_frames_for, TimeUnit::Milliseconds),
        );
    }
    fn controller_no_events_warning(&self, no_events_for: Duration) {
        self.tx.observed_one_value_now(
            Metric::ControllerNoEventsForWarning,
            (no_events_for, TimeUnit::Milliseconds),
        );
    }

    // === DISPATCHER ===

    // === HANDLERS ===

    fn handler_batch_processed_1(&self, n_events: Option<usize>, time: Duration) {
        if let Some(n_events) = n_events {
            self.tx
                .observed_one_value_now(Metric::HandlerProcessedEvents, n_events);
        }
        self.tx.observed_one_value_now(
            Metric::HandlerBatchProcessedTime,
            (time, TimeUnit::Microseconds),
        );
    }
    fn handler_batch_processed_2(&self, frame_received_at: Instant, n_bytes: usize) {
        self.tx
            .observed_one_value_now(Metric::HandlerBatchProcessedBytes, n_bytes)
            .observed_one_value_now(
                Metric::HandlerBatchLag,
                (frame_received_at.elapsed(), TimeUnit::Microseconds),
            );
    }
    fn handler_deserialization(&self, n_bytes: usize, time: Duration) {
        self.tx
            .observed_one_value_now(Metric::HandlerBatchDeserializationBytes, n_bytes)
            .observed_one_value_now(
                Metric::HandlerBatchDeserializationTime,
                (time, TimeUnit::Microseconds),
            );
    }

    // === COMMITTER ===

    fn committer_cursor_received(&self, frame_received_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::CommitterCursorsReceivedLag,
            (frame_received_at.elapsed(), TimeUnit::Milliseconds),
        );
    }

    fn committer_cursors_committed(&self, n_cursors: usize, time: Duration) {
        self.tx
            .observed_one_value_now(Metric::CommitterCursorsCommittedCount, n_cursors)
            .observed_one_value_now(
                Metric::CommitterCursorsCommittedTime,
                (time, TimeUnit::Milliseconds),
            );
    }

    fn committer_cursors_not_committed(&self, n_cursors: usize, time: Duration) {
        self.tx.observed_one_now(Metric::CommitterCommitFailed);
        self.tx
            .observed_one_value_now(Metric::CommitterCursorsNotCommittedCount, n_cursors)
            .observed_one_value_now(
                Metric::CommitterCursorsNotCommittedTime,
                (time, TimeUnit::Milliseconds),
            );
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Metric {
    ConsumerInFlightChanged,
    StreamTickEmitted,
    StreamConnectedTime,
    StreamNotConnectedTime,
    StreamConnectAttemptSuccessTime,
    StreamConnectAttemptFailedTime,
    StreamChunkReceivedBytes,
    StreamFrameReceivedBytes,
    ControllerBatchReceivedBytes,
    ControllerBatchReceivedLag,
    ControllerInfoReceivedLag,
    ControllerKeepAliveReceivedLag,
    ControllerPartitionActivated,
    ControllerPartitionDeactivatedAfter,
    ControllerNoFramesForWarning,
    ControllerNoEventsForWarning,
    HandlerBatchLag,
    HandlerBatchProcessedBytes,
    HandlerBatchProcessedTime,
    HandlerBatchDeserializationBytes,
    HandlerBatchDeserializationTime,
    HandlerProcessedEvents,
    CommitterCursorsReceivedLag,
    CommitterCursorsCommittedTime,
    CommitterCursorsCommittedCount,
    CommitterCommitFailed,
    CommitterCursorsNotCommittedTime,
    CommitterCursorsNotCommittedCount,
}

mod instr {
    use metrix::instruments::*;
    use metrix::processor::TelemetryProcessor;
    use metrix::TelemetryTransmitter;
    use metrix::TimeUnit;

    use super::{create_gauge, create_staircase_timer};
    use super::{Metric, MetrixConfig};

    pub fn create(
        config: &MetrixConfig,
    ) -> (TelemetryTransmitter<Metric>, TelemetryProcessor<Metric>) {
        let (tx, rx) = TelemetryProcessor::new_pair_without_name();

        let rx = rx.cockpit(
            Cockpit::without_name()
                .panel(
                    Panel::named(AcceptAllLabels, "batches")
                        .gauge(
                            create_gauge("in_flight", config)
                                .deltas_only(Metric::ConsumerInFlightChanged),
                        )
                        .meter(
                            Meter::new_with_defaults("per_second")
                                .for_label(Metric::HandlerBatchProcessedTime),
                        )
                        .histogram(
                            Histogram::new_with_defaults("processing_time_us")
                                .display_time_unit(TimeUnit::Microseconds)
                                .for_label(Metric::HandlerBatchProcessedTime),
                        )
                        .handler(
                            ValueMeter::new_with_defaults("bytes_per_second")
                                .for_label(Metric::HandlerBatchProcessedBytes),
                        )
                        .histogram(
                            Histogram::new_with_defaults("bytes_per_batch")
                                .for_label(Metric::HandlerBatchProcessedBytes),
                        ),
                )
                .panel(
                    Panel::named(
                        (
                            Metric::ControllerPartitionActivated,
                            Metric::ControllerPartitionDeactivatedAfter,
                        ),
                        "partitions",
                    )
                    .gauge(create_gauge("active", config).inc_dec_on(
                        Metric::ControllerPartitionActivated,
                        Metric::ControllerPartitionDeactivatedAfter,
                    )),
                )
                .panel(
                    Panel::named(Metric::ControllerNoFramesForWarning, "frames").handler(
                        create_staircase_timer("no_frames_warning", &config)
                            .for_label(Metric::ControllerNoFramesForWarning),
                    ),
                )
                .panel(
                    Panel::named(AcceptAllLabels, "events")
                        .handler(
                            ValueMeter::new_with_defaults("per_second")
                                .for_label(Metric::HandlerProcessedEvents),
                        )
                        .histogram(
                            Histogram::new_with_defaults("per_batch")
                                .for_label(Metric::HandlerProcessedEvents),
                        )
                        .histogram(
                            Histogram::new_with_defaults("batch_deserialization_us")
                                .display_time_unit(TimeUnit::Microseconds)
                                .for_label(Metric::HandlerBatchDeserializationTime),
                        )
                        .handler(
                            ValueMeter::new_with_defaults("batch_deserialization_bytes_per_second")
                                .for_label(Metric::HandlerBatchDeserializationBytes),
                        )
                        .handler(
                            create_staircase_timer("no_events_warning", &config)
                                .for_label(Metric::ControllerNoEventsForWarning),
                        ),
                )
                .panel(
                    Panel::named(AcceptAllLabels, "batch_lag")
                        .histogram(
                            Histogram::new_with_defaults("controller")
                                .display_time_unit(TimeUnit::Microseconds)
                                .accept((
                                    Metric::ControllerBatchReceivedLag,
                                    Metric::ControllerKeepAliveReceivedLag,
                                    Metric::ControllerInfoReceivedLag,
                                )),
                        )
                        .histogram(
                            Histogram::new_with_defaults("handlers")
                                .display_time_unit(TimeUnit::Microseconds)
                                .accept(Metric::HandlerBatchLag),
                        )
                        .histogram(
                            Histogram::new_with_defaults("committer")
                                .display_time_unit(TimeUnit::Microseconds)
                                .accept(Metric::CommitterCursorsReceivedLag),
                        ),
                ),
        );

        (tx, rx)
    }
}

fn create_gauge(name: &str, config: &MetrixConfig) -> Gauge {
    let tracking_seconds = config.gauge_tracking_secs.unwrap_or_default();
    Gauge::new(name)
        .tracking(tracking_seconds.into_inner() as usize)
        .group_values(true)
}

fn create_staircase_timer(
    name: &str,
    config: &MetrixConfig,
) -> metrix::instruments::StaircaseTimer {
    let switch_off_after = config.gauge_tracking_secs.unwrap_or_default();
    metrix::instruments::StaircaseTimer::new(name).switch_off_after(switch_off_after.into())
}
