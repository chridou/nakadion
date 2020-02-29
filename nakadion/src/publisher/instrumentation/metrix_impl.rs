use std::time::Duration;

use metrix::{
    processor::ProcessorMount, AggregatesProcessors, TelemetryTransmitter, TimeUnit,
    TransmitsTelemetryData,
};

use super::Instruments;
use crate::nakadi_types::publishing::BatchStats;

/// Instrumentation with Metrix
#[derive(Clone)]
pub struct Metrix {
    tx: TelemetryTransmitter<Metric>,
}

impl Metrix {
    /// Initializes the metrics.
    ///
    /// Adds them directly into the given `processor` without creating an additional group.
    pub fn new<A: AggregatesProcessors>(processor: &mut A) -> Self {
        let (tx, global_proc) = instr::create();
        processor.add_processor(global_proc);
        Metrix { tx }
    }

    /// Creates new Metrix instrumentation and returns a mount that can be plugged with metrix
    /// and the instrumentation which can be plugged into the `Publisher`
    pub fn new_mountable(name: Option<&str>) -> (Metrix, ProcessorMount) {
        let mut mount = if let Some(name) = name {
            ProcessorMount::new(name)
        } else {
            ProcessorMount::default()
        };

        let me = Self::new(&mut mount);

        (me, mount)
    }
}

impl Instruments for Metrix {
    fn published(&self, elapsed: Duration) {
        self.tx
            .observed_one_value_now(Metric::PublishedWithTime, (elapsed, TimeUnit::Milliseconds));
    }
    fn publish_failed(&self, elapsed: Duration) {
        self.tx.observed_one_value_now(
            Metric::PublishFailedWithTime,
            (elapsed, TimeUnit::Milliseconds),
        );
    }
    fn batch_stats(&self, stats: BatchStats) {
        let BatchStats {
            n_items,
            n_submitted,
            n_failed,
            n_aborted,
            n_not_submitted,
        } = stats;
        self.tx
            .observed_one_value_now(Metric::BatchStatsNItems, n_items as u64)
            .observed_one_value_now(Metric::BatchStatsNSubmitted, n_submitted as u64)
            .observed_one_value_now(Metric::BatchStatsNFailed, n_failed as u64)
            .observed_one_value_now(Metric::BatchStatsNAborted, n_aborted as u64)
            .observed_one_value_now(Metric::BatchStatsNNotSubmitted, n_not_submitted as u64);
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Metric {
    PublishedWithTime,
    PublishFailedWithTime,
    BatchStatsNItems,
    BatchStatsNSubmitted,
    BatchStatsNFailed,
    BatchStatsNAborted,
    BatchStatsNNotSubmitted,
}

mod instr {
    use metrix::instruments::*;
    use metrix::processor::TelemetryProcessor;
    use metrix::TelemetryTransmitter;
    use metrix::TimeUnit;

    use super::Metric;

    pub fn create() -> (TelemetryTransmitter<Metric>, TelemetryProcessor<Metric>) {
        let (tx, rx) = TelemetryProcessor::new_pair_without_name();

        let rx = rx.cockpit(
            Cockpit::without_name()
                .panel(
                    Panel::named(Metric::PublishedWithTime, "batches_ok")
                        .meter(Meter::new_with_defaults("per_second"))
                        .histogram(
                            Histogram::new_with_defaults("time_ms")
                                .display_time_unit(TimeUnit::Milliseconds),
                        ),
                )
                .panel(
                    Panel::named(Metric::PublishFailedWithTime, "batches_not_ok")
                        .meter(Meter::new_with_defaults("per_second"))
                        .histogram(
                            Histogram::new_with_defaults("time_ms")
                                .display_time_unit(TimeUnit::Milliseconds),
                        ),
                )
                .panel(
                    Panel::named(
                        (Metric::PublishedWithTime, Metric::PublishFailedWithTime),
                        "all_batches",
                    )
                    .meter(Meter::new_with_defaults("per_second"))
                    .histogram(
                        Histogram::new_with_defaults("time_ms")
                            .display_time_unit(TimeUnit::Milliseconds),
                    ),
                )
                .panel(
                    Panel::named(
                        (
                            Metric::BatchStatsNItems,
                            Metric::BatchStatsNFailed,
                            Metric::BatchStatsNAborted,
                            Metric::BatchStatsNSubmitted,
                            Metric::BatchStatsNNotSubmitted,
                        ),
                        "batch_items",
                    )
                    .panel(
                        Panel::named(Metric::BatchStatsNItems, "total")
                            .instrument(ValueMeter::new_with_defaults("per_second"))
                            .histogram(Histogram::new_with_defaults("distribution")),
                    )
                    .panel(
                        Panel::named(Metric::BatchStatsNSubmitted, "submitted")
                            .instrument(ValueMeter::new_with_defaults("per_second"))
                            .histogram(Histogram::new_with_defaults("distribution")),
                    )
                    .panel(
                        Panel::named(Metric::BatchStatsNFailed, "failed")
                            .instrument(ValueMeter::new_with_defaults("per_second"))
                            .histogram(Histogram::new_with_defaults("distribution")),
                    )
                    .panel(
                        Panel::named(Metric::BatchStatsNAborted, "aborted")
                            .instrument(ValueMeter::new_with_defaults("per_second"))
                            .histogram(Histogram::new_with_defaults("distribution")),
                    )
                    .panel(
                        Panel::named(Metric::BatchStatsNNotSubmitted, "not_submitted")
                            .instrument(ValueMeter::new_with_defaults("per_second"))
                            .histogram(Histogram::new_with_defaults("distribution")),
                    ),
                ),
        );

        (tx, rx)
    }
}
