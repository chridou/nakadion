use std::time::{Duration, Instant};

use metrix::instruments::Gauge;
use metrix::{
    processor::ProcessorMount, AggregatesProcessors, Decrement, DecrementBy, Increment,
    TelemetryTransmitter, TimeUnit, TransmitsTelemetryData,
};

use serde::{Deserialize, Serialize};

use super::Instruments;

/// Instrumentation with Metrix
pub struct Metrix {
    consumer_tx: TelemetryTransmitter<ConsumerMetric>,
    stream_tx: TelemetryTransmitter<StreamMetric>,
    controller_tx: TelemetryTransmitter<ControllerMetric>,
    handler_tx: TelemetryTransmitter<HandlerMetric>,
    committer_tx: TelemetryTransmitter<CommitterMetric>,
}

impl Metrix {
    /// Initializes the metrics.
    ///
    /// Adds them directly into the given `processor` without creating an additional group.
    pub fn new<A: AggregatesProcessors>(config: &MetrixConfig, processor: &mut A) -> Self {
        let (consumer_tx, global_proc) = consumer::create(&config);
        processor.add_processor(global_proc);
        let (stream_tx, stream_proc) = stream::create(&config);
        processor.add_processor(stream_proc);
        let (controller_tx, controller_proc) = controller::create(&config);
        processor.add_processor(controller_proc);
        let (handler_tx, worker_proc) = handlers::create(&config);
        processor.add_processor(worker_proc);
        let (committer_tx, committer_proc) = committer::create(&config);
        processor.add_processor(committer_proc);
        Metrix {
            consumer_tx,
            stream_tx,
            controller_tx,
            handler_tx,
            committer_tx,
        }
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
        self.consumer_tx
            .observed_one_value_now(ConsumerMetric::ConsumerInFlightChanged, Increment);
    }
    fn consumer_batches_in_flight_dec(&self) {
        self.consumer_tx
            .observed_one_value_now(ConsumerMetric::ConsumerInFlightChanged, Decrement);
    }
    fn consumer_batches_in_flight_dec_by(&self, by: usize) {
        self.consumer_tx.observed_one_value_now(
            ConsumerMetric::ConsumerInFlightChanged,
            DecrementBy(by as u32),
        );
    }

    // === STREAM ===
    fn stream_connect_attempt_success(&self, time: Duration) {
        self.stream_tx.observed_one_value_now(
            StreamMetric::StreamConnectAttemptSuccessTime,
            (time, TimeUnit::Milliseconds),
        );
    }
    fn stream_connect_attempt_failed(&self, time: Duration) {
        self.stream_tx.observed_one_value_now(
            StreamMetric::StreamConnectAttemptFailedTime,
            (time, TimeUnit::Milliseconds),
        );
    }
    fn stream_connected(&self, time: Duration) {
        self.stream_tx.observed_one_value_now(
            StreamMetric::StreamConnectedTime,
            (time, TimeUnit::Milliseconds),
        );
    }
    fn stream_not_connected(&self, time: Duration) {
        self.stream_tx.observed_one_value_now(
            StreamMetric::StreamNotConnectedTime,
            (time, TimeUnit::Milliseconds),
        );
        self.consumer_tx
            .observed_one_now(ConsumerMetric::StreamConnectFailed);
    }

    fn stream_chunk_received(&self, n_bytes: usize) {
        self.stream_tx
            .observed_one_value_now(StreamMetric::StreamChunkReceivedBytes, n_bytes);
    }
    fn stream_frame_received(&self, n_bytes: usize) {
        self.stream_tx
            .observed_one_value_now(StreamMetric::StreamFrameReceivedBytes, n_bytes);
    }
    fn stream_tick_emitted(&self) {
        self.consumer_tx
            .observed_one_now(ConsumerMetric::StreamTickEmitted);
    }

    // === CONTROLLER ===
    fn controller_batch_received(&self, frame_received_at: Instant, events_bytes: usize) {
        self.consumer_tx
            .observed_one_value_now(ConsumerMetric::ControllerBatchReceivedBytes, events_bytes);
        self.controller_tx.observed_one_value_now(
            ControllerMetric::ControllerBatchReceivedElapsed,
            (frame_received_at.elapsed(), TimeUnit::Microseconds),
        );
    }
    fn controller_info_received(&self, frame_received_at: Instant) {
        self.controller_tx.observed_one_value_now(
            ControllerMetric::ControllerInfoReceivedElapsed,
            (frame_received_at.elapsed(), TimeUnit::Microseconds),
        );
    }
    fn controller_keep_alive_received(&self, frame_received_at: Instant) {
        self.controller_tx.observed_one_value_now(
            ControllerMetric::ControllerKeepAliveReceivedElapsed,
            (frame_received_at.elapsed(), TimeUnit::Microseconds),
        );
    }

    // === DISPATCHER ===

    // === HANDLERS ===

    fn handler_batch_processed_1(&self, n_events: Option<usize>, time: Duration) {
        if let Some(n_events) = n_events {
            self.consumer_tx
                .observed_one_value_now(ConsumerMetric::HandlerProcessedEvents, n_events);
        }
        self.consumer_tx.observed_one_value_now(
            ConsumerMetric::HandlerBatchProcessedTime,
            (time, TimeUnit::Microseconds),
        );
    }
    fn handler_batch_processed_2(&self, frame_received_at: Instant, n_bytes: usize) {
        self.handler_tx
            .observed_one_value_now(HandlerMetric::HandlerBatchProcessedBytes, n_bytes);
    }
    fn handler_deserialization_time(&self, n_bytes: usize, time: Duration) {
        self.handler_tx
            .observed_one_value_now(HandlerMetric::HandlerBatchDeserializationBytes, n_bytes)
            .observed_one_value_now(
                HandlerMetric::HandlerBatchDeserializationTime,
                (time, TimeUnit::Microseconds),
            );
    }

    // === HANDLERS ===

    // === COMMITTER ===

    fn committer_cursors_committed(&self, n_cursors: usize, time: Duration) {
        self.committer_tx
            .observed_one_value_now(CommitterMetric::CommitterCursorsCommittedCount, n_cursors)
            .observed_one_value_now(
                CommitterMetric::CommitterCursorsCommittedTime,
                (time, TimeUnit::Milliseconds),
            );
    }
    fn committer_cursors_not_committed(&self, n_cursors: usize, time: Duration) {
        self.consumer_tx
            .observed_one_now(ConsumerMetric::CommitterCommitFailed);
        self.committer_tx
            .observed_one_value_now(
                CommitterMetric::CommitterCursorsNotCommittedCount,
                n_cursors,
            )
            .observed_one_value_now(
                CommitterMetric::CommitterCursorsNotCommittedTime,
                (time, TimeUnit::Milliseconds),
            );
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ConsumerMetric {
    ConsumerInFlightChanged,
    HandlerProcessedEvents,
    HandlerBatchProcessedTime,
    ControllerBatchReceivedBytes,
    StreamConnectFailed,
    CommitterCommitFailed,
    StreamTickEmitted,
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum StreamMetric {
    StreamConnectedTime,
    StreamNotConnectedTime,
    StreamConnectAttemptSuccessTime,
    StreamConnectAttemptFailedTime,
    StreamChunkReceivedBytes,
    StreamFrameReceivedBytes,
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ControllerMetric {
    ControllerBatchReceivedElapsed,
    ControllerInfoReceivedElapsed,
    ControllerKeepAliveReceivedElapsed,
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum HandlerMetric {
    HandlerBatchProcessedBytes,
    HandlerBatchDeserializationBytes,
    HandlerBatchDeserializationTime,
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum CommitterMetric {
    CommitterCursorsCommittedTime,
    CommitterCursorsCommittedCount,
    CommitterCursorsNotCommittedTime,
    CommitterCursorsNotCommittedCount,
}

mod consumer {
    use metrix::instruments::*;
    use metrix::processor::TelemetryProcessor;
    use metrix::TelemetryTransmitter;

    use super::create_gauge;
    use super::{ConsumerMetric, MetrixConfig};

    pub fn create(
        config: &MetrixConfig,
    ) -> (
        TelemetryTransmitter<ConsumerMetric>,
        TelemetryProcessor<ConsumerMetric>,
    ) {
        let (tx, rx) = TelemetryProcessor::new_pair_without_name();

        let rx = rx.cockpit(
            Cockpit::without_name().panel(
                Panel::new(AcceptAllLabels).gauge(
                    create_gauge("in_flight_batches", config)
                        .deltas_only(ConsumerMetric::ConsumerInFlightChanged),
                ),
            ),
        );

        (tx, rx)
    }
}

mod stream {
    use metrix::instruments::*;
    use metrix::processor::TelemetryProcessor;
    use metrix::TelemetryTransmitter;

    use super::create_gauge;
    use super::{MetrixConfig, StreamMetric};

    pub fn create(
        config: &MetrixConfig,
    ) -> (
        TelemetryTransmitter<StreamMetric>,
        TelemetryProcessor<StreamMetric>,
    ) {
        let (tx, rx) = TelemetryProcessor::new_pair("stream");

        (tx, rx)
    }
}

mod controller {
    use metrix::instruments::*;
    use metrix::processor::TelemetryProcessor;
    use metrix::TelemetryTransmitter;

    use super::create_gauge;
    use super::{ControllerMetric, MetrixConfig};

    pub fn create(
        config: &MetrixConfig,
    ) -> (
        TelemetryTransmitter<ControllerMetric>,
        TelemetryProcessor<ControllerMetric>,
    ) {
        let (tx, rx) = TelemetryProcessor::new_pair("consumer");

        (tx, rx)
    }
}

mod handlers {
    use metrix::instruments::*;
    use metrix::processor::TelemetryProcessor;
    use metrix::TelemetryTransmitter;

    use super::create_gauge;
    use super::{HandlerMetric, MetrixConfig};

    pub fn create(
        config: &MetrixConfig,
    ) -> (
        TelemetryTransmitter<HandlerMetric>,
        TelemetryProcessor<HandlerMetric>,
    ) {
        let (tx, rx) = TelemetryProcessor::new_pair("handlers");

        (tx, rx)
    }
}

mod committer {
    use metrix::instruments::*;
    use metrix::processor::TelemetryProcessor;
    use metrix::TelemetryTransmitter;

    use super::create_gauge;
    use super::{CommitterMetric, MetrixConfig};

    pub fn create(
        config: &MetrixConfig,
    ) -> (
        TelemetryTransmitter<CommitterMetric>,
        TelemetryProcessor<CommitterMetric>,
    ) {
        let (tx, rx) = TelemetryProcessor::new_pair("stream");

        (tx, rx)
    }
}

fn create_gauge(name: &str, config: &MetrixConfig) -> Gauge {
    let tracking_seconds = config.tracking_secs.unwrap_or_default();
    Gauge::new(name)
        .tracking(tracking_seconds.into())
        .group_values(true)
}
