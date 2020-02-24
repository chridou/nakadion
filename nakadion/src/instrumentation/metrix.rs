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
    #[doc="The maximum retry delay between failed attempts to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct MetrixTrackingSecs(usize, env="METRIX_TRACKING_SECS");
}
impl Default for MetrixTrackingSecs {
    fn default() -> Self {
        60.into()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MetrixConfig {
    tracking_secs: Option<MetrixTrackingSecs>,
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
            Metric::ControllerInfoReceivedElapsed,
            (frame_received_at.elapsed(), TimeUnit::Microseconds),
        );
    }
    fn controller_keep_alive_received(&self, frame_received_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::ControllerKeepAliveReceivedElapsed,
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
            .observed_one_value_now(Metric::HandlerBatchProcessedBytes, n_bytes);
    }
    fn handler_deserialization_time(&self, n_bytes: usize, time: Duration) {
        self.tx
            .observed_one_value_now(Metric::HandlerBatchDeserializationBytes, n_bytes)
            .observed_one_value_now(
                Metric::HandlerBatchDeserializationTime,
                (time, TimeUnit::Microseconds),
            );
    }

    // === HANDLERS ===

    // === COMMITTER ===

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
    ControllerInfoReceivedElapsed,
    ControllerKeepAliveReceivedElapsed,
    ControllerPartitionActivated,
    ControllerPartitionDeactivatedAfter,
    HandlerBatchProcessedBytes,
    HandlerBatchDeserializationBytes,
    HandlerBatchDeserializationTime,
    HandlerProcessedEvents,
    HandlerBatchProcessedTime,
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

    use super::create_gauge;
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
                        ),
                ),
        );

        (tx, rx)
    }
}

fn create_gauge(name: &str, config: &MetrixConfig) -> Gauge {
    let tracking_seconds = config.tracking_secs.unwrap_or_default();
    Gauge::new(name)
        .tracking(tracking_seconds.into())
        .group_values(true)
}
