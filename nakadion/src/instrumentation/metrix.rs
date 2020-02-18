use std::time::{Duration, Instant};

use metrix::instruments::Gauge;
use metrix::{processor::ProcessorMount, AggregatesProcessors, TelemetryTransmitter};

use serde::{Deserialize, Serialize};

use super::Instruments;

/// Instrumentation with Metrix
pub struct Metrix {
    consumer_tx: TelemetryTransmitter<ConsumerMetric>,
    stream_tx: TelemetryTransmitter<StreamMetric>,
    controller_tx: TelemetryTransmitter<ControllerMetric>,
    worker_tx: TelemetryTransmitter<WorkerMetric>,
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
        let (worker_tx, worker_proc) = workers::create(&config);
        processor.add_processor(worker_proc);
        let (committer_tx, committer_proc) = committer::create(&config);
        processor.add_processor(committer_proc);
        Metrix {
            consumer_tx,
            stream_tx,
            controller_tx,
            worker_tx,
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

    fn consumer_batches_in_flight_inc(&self) {}
    fn consumer_batches_in_flight_dec(&self) {}
    fn consumer_batches_in_flight_dec_by(&self, by: usize) {}

    // === STREAM ===
    fn stream_connect_attempt_success(&self, time: Duration) {}
    fn stream_connect_attempt_failed(&self, time: Duration) {}
    fn stream_connected(&self, time: Duration) {}
    fn stream_not_connected(&self, time: Duration) {}

    fn stream_chunk_received(&self, n_bytes: usize) {}
    fn stream_frame_received(&self, n_bytes: usize) {}
    fn stream_tick_emitted(&self) {}

    // === CONTROLLER ===
    fn controller_batch_received(&self, frame_received_at: Instant, events_bytes: usize) {}
    fn controller_info_received(&self, frame_received_at: Instant) {}
    fn controller_keep_alive_received(&self, frame_received_at: Instant) {}

    // === DISPATCHER ===

    // === WORKERS ===

    fn worker_batch_processed(&self, n_bytes: usize, n_events: Option<usize>, time: Duration) {}
    fn worker_deserialization_time(&self, n_bytes: usize, time: Duration) {}

    // === HANDLERS ===

    // === COMMITTER ===

    fn committer_cursors_committed(&self, n_cursors: usize, time: Duration) {}
    fn committer_cursors_not_committed(&self, n_cursors: usize, time: Duration) {}
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ConsumerMetric {
    InFlightChanged,
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum StreamMetric {}
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ControllerMetric {}
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum WorkerMetric {}
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum CommitterMetric {}

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
                        .deltas_only(ConsumerMetric::InFlightChanged),
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

mod workers {
    use metrix::instruments::*;
    use metrix::processor::TelemetryProcessor;
    use metrix::TelemetryTransmitter;

    use super::create_gauge;
    use super::{MetrixConfig, WorkerMetric};

    pub fn create(
        config: &MetrixConfig,
    ) -> (
        TelemetryTransmitter<WorkerMetric>,
        TelemetryProcessor<WorkerMetric>,
    ) {
        let (tx, rx) = TelemetryProcessor::new_pair("workers");

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
