use std::time::{Duration, Instant};

use metrix::instruments::Gauge;
use metrix::{
    processor::ProcessorMount, AggregatesProcessors, Decrement, DecrementBy, Increment,
    TelemetryTransmitter, TimeUnit, TransmitsTelemetryData,
};

use serde::{Deserialize, Serialize};

use crate::components::{
    committer::CommitError,
    connector::ConnectError,
    streams::{BatchLineError, BatchLineErrorKind},
};

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

new_type! {
    #[doc="The time an alert will stay on after trigger.\n\nDefault is 60s.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct AlertDurationSecs(u32, env="METRIX_ALERT_DURATION_SECS");
}
impl Default for AlertDurationSecs {
    fn default() -> Self {
        60.into()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MetrixConfig {
    /// Enables tracking for gauges for the given amount of seconds
    pub gauge_tracking_secs: Option<MetrixGaugeTrackingSecs>,
    /// Enables tracking for gauges for the given amount of seconds
    pub alert_duration_secs: Option<AlertDurationSecs>,
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
        if self.alert_duration_secs.is_none() {
            self.alert_duration_secs = AlertDurationSecs::try_from_env_prefixed(prefix.as_ref())?;
        }
        Ok(())
    }
}

impl Instruments for Metrix {
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
    fn stream_not_connected(&self, time: Duration, _err: &ConnectError) {
        self.tx.observed_one_value_now(
            Metric::StreamNotConnectedTime,
            (time, TimeUnit::Milliseconds),
        );
    }

    fn stream_chunk_received(&self, n_bytes: usize) {
        self.tx
            .observed_one_value_now(Metric::StreamChunkReceivedBytes, n_bytes);
    }
    fn stream_frame_completed(&self, n_bytes: usize, time: Duration) {
        self.tx
            .observed_one_value_now(Metric::StreamFrameCompletedBytes, n_bytes)
            .observed_one_value_now(
                Metric::StreamFrameCompletedTime,
                (time, TimeUnit::Microseconds),
            );
    }
    fn stream_tick_emitted(&self) {
        self.tx.observed_one_now(Metric::StreamTickEmitted);
    }

    fn info_frame_received(&self, frame_received_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::StreamInfoFrameReceivedLag,
            (frame_received_at.elapsed(), TimeUnit::Microseconds),
        );
    }
    fn keep_alive_frame_received(&self, frame_received_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::StreamKeepAliveFrameReceivedLag,
            (frame_received_at.elapsed(), TimeUnit::Microseconds),
        );
    }
    fn batch_frame_received(&self, frame_received_at: Instant, events_bytes: usize) {
        self.tx
            .observed_one_value_now(Metric::StreamBatchFrameReceivedBytes, events_bytes);
        self.tx.observed_one_value_now(
            Metric::StreamBatchFrameReceivedLag,
            (frame_received_at.elapsed(), TimeUnit::Microseconds),
        );
    }

    fn no_frames_warning(&self, no_frames_for: Duration) {
        self.tx.observed_one_value_now(
            Metric::NoFramesForWarning,
            (no_frames_for, TimeUnit::Milliseconds),
        );
    }
    fn no_events_warning(&self, no_events_for: Duration) {
        self.tx.observed_one_value_now(
            Metric::NoEventsForWarning,
            (no_events_for, TimeUnit::Milliseconds),
        );
    }

    fn stream_error(&self, err: &BatchLineError) {
        match err.kind() {
            BatchLineErrorKind::Io => {
                self.tx.observed_one_now(Metric::StreamErrorIo);
            }
            BatchLineErrorKind::Parser => {
                self.tx.observed_one_now(Metric::StreamErrorParse);
            }
        }
    }

    fn batches_in_flight_inc(&self) {
        self.tx
            .observed_one_value_now(Metric::BatchesInFlightChanged, Increment);
    }
    fn batches_in_flight_dec(&self) {
        self.tx
            .observed_one_value_now(Metric::BatchesInFlightChanged, Decrement);
    }
    fn batches_in_flight_dec_by(&self, by: usize) {
        self.tx
            .observed_one_value_now(Metric::BatchesInFlightChanged, DecrementBy(by as u32));
    }

    fn event_type_partition_activated(&self) {
        self.tx
            .observed_one_value_now(Metric::EventTypePartitionActivated, Increment);
    }

    fn event_type_partition_deactivated(&self, active_for: Duration) {
        self.tx.observed_one_value_now(
            Metric::EventTypePartitionDeactivatedAfter,
            (active_for, TimeUnit::Milliseconds),
        );
    }

    fn batch_processing_started(&self, frame_received_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::BatchProcessingStartedLag,
            (frame_received_at.elapsed(), TimeUnit::Microseconds),
        );
    }

    fn batch_processed(&self, n_bytes: usize, time: Duration) {
        self.tx
            .observed_one_value_now(Metric::BatchProcessedTime, (time, TimeUnit::Microseconds))
            .observed_one_value_now(Metric::BatchProcessedBytes, n_bytes);
    }
    fn batch_processed_n_events(&self, n_events: usize) {
        self.tx
            .observed_one_value_now(Metric::BatchProcessedNEvents, n_events);
    }

    fn batch_deserialized(&self, n_bytes: usize, time: Duration) {
        self.tx
            .observed_one_value_now(Metric::BatchDeserializationBytes, n_bytes)
            .observed_one_value_now(
                Metric::BatchDeserializationTime,
                (time, TimeUnit::Microseconds),
            );
    }

    fn cursor_to_commit_received(&self, frame_received_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::CommitterCursorsReceivedLag,
            (frame_received_at.elapsed(), TimeUnit::Milliseconds),
        );
    }

    fn cursors_committed(&self, n_cursors: usize, time: Duration) {
        self.tx
            .observed_one_value_now(Metric::CommitterCursorsCommittedCount, n_cursors)
            .observed_one_value_now(
                Metric::CommitterCursorsCommittedTime,
                (time, TimeUnit::Milliseconds),
            );
    }

    fn cursors_not_committed(&self, n_cursors: usize, time: Duration, _err: &CommitError) {
        self.tx.observed_one_now(Metric::CommitterCommitFailed);
        self.tx
            .observed_one_value_now(Metric::CommitterCursorsNotCommittedCount, n_cursors)
            .observed_one_value_now(
                Metric::CommitterCursorsNotCommittedTime,
                (time, TimeUnit::Milliseconds),
            );
    }

    fn commit_cursors_attempt_failed(&self, n_cursors: usize, time: Duration) {
        self.tx
            .observed_one_value_now(Metric::CommitterAttemptFailedCount, n_cursors)
            .observed_one_value_now(
                Metric::CommitterAttemptFailedTime,
                (time, TimeUnit::Milliseconds),
            );
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Metric {
    StreamConnectedTime,
    StreamNotConnectedTime,
    StreamConnectAttemptSuccessTime,
    StreamConnectAttemptFailedTime,
    StreamChunkReceivedBytes,
    StreamFrameCompletedBytes,
    StreamFrameCompletedTime,
    StreamTickEmitted,
    StreamInfoFrameReceivedLag,
    StreamKeepAliveFrameReceivedLag,
    StreamBatchFrameReceivedBytes,
    StreamBatchFrameReceivedLag,
    NoFramesForWarning,
    NoEventsForWarning,
    StreamErrorIo,
    StreamErrorParse,
    BatchesInFlightChanged,
    EventTypePartitionActivated,
    EventTypePartitionDeactivatedAfter,
    BatchProcessingStartedLag,
    BatchProcessedBytes,
    BatchProcessedTime,
    BatchDeserializationBytes,
    BatchDeserializationTime,
    BatchProcessedNEvents,
    CommitterCursorsReceivedLag,
    CommitterCursorsCommittedTime,
    CommitterCursorsCommittedCount,
    CommitterCommitFailed,
    CommitterCursorsNotCommittedTime,
    CommitterCursorsNotCommittedCount,
    CommitterAttemptFailedTime,
    CommitterAttemptFailedCount,
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
        let (tx, mut rx) = TelemetryProcessor::new_pair_without_name();

        let mut cockpit = Cockpit::without_name();

        create_alerts(&mut cockpit, config);
        create_connector_metrics(&mut cockpit, config);
        create_stream_metrics(&mut cockpit, config);
        create_lag_metrics(&mut cockpit, config);
        create_batch_metrics(&mut cockpit, config);
        create_events_metrics(&mut cockpit, config);
        create_committer_metrics(&mut cockpit, config);

        rx.add_cockpit(cockpit);

        (tx, rx)
    }

    fn create_alerts(cockpit: &mut Cockpit<Metric>, config: &MetrixConfig) {
        use Metric::*;
        let panel = Panel::named(
            (
                StreamErrorIo,
                StreamErrorParse,
                NoEventsForWarning,
                NoFramesForWarning,
                CommitterCommitFailed,
            ),
            "alerts",
        )
        .handler(create_staircase_timer("stream_io_error", config).for_label(StreamErrorIo))
        .handler(create_staircase_timer("stream_parse_error", config).for_label(StreamErrorParse))
        .handler(create_staircase_timer("no_events", config).for_label(NoEventsForWarning))
        .handler(create_staircase_timer("no_frames", config).for_label(NoFramesForWarning))
        .handler(create_staircase_timer("commit_failed", config).for_label(CommitterCommitFailed));

        cockpit.add_panel(panel);
    }

    fn create_connector_metrics(cockpit: &mut Cockpit<Metric>, _config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "connector");

        cockpit.add_panel(panel);
    }

    fn create_stream_metrics(cockpit: &mut Cockpit<Metric>, _config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "stream")
            .panel(
                Panel::named(AcceptAllLabels, "chunks")
                    .meter(
                        Meter::new_with_defaults("per_second")
                            .for_label(Metric::StreamChunkReceivedBytes),
                    )
                    .handler(
                        ValueMeter::new_with_defaults("bytes_per_second")
                            .for_label(Metric::StreamChunkReceivedBytes),
                    )
                    .histogram(
                        Histogram::new_with_defaults("size_distribution")
                            .accept(Metric::StreamChunkReceivedBytes),
                    ),
            )
            .panel(
                Panel::named(AcceptAllLabels, "frames")
                    .meter(
                        Meter::new_with_defaults("per_second")
                            .for_label(Metric::StreamFrameCompletedBytes),
                    )
                    .handler(
                        ValueMeter::new_with_defaults("bytes_per_second")
                            .for_label(Metric::StreamFrameCompletedBytes),
                    )
                    .histogram(
                        Histogram::new_with_defaults("size_distribution")
                            .accept(Metric::StreamFrameCompletedBytes),
                    )
                    .histogram(
                        Histogram::new_with_defaults("completion_time_us")
                            .display_time_unit(TimeUnit::Microseconds)
                            .accept(Metric::StreamFrameCompletedTime),
                    )
                    .meter(
                        Meter::new_with_defaults("info_frames_per_second")
                            .for_label(Metric::StreamInfoFrameReceivedLag),
                    )
                    .meter(
                        Meter::new_with_defaults("keep_alive_frames_per_second")
                            .for_label(Metric::StreamKeepAliveFrameReceivedLag),
                    )
                    .meter(
                        Meter::new_with_defaults("batch_frames_per_second")
                            .for_label(Metric::StreamBatchFrameReceivedLag),
                    ),
            );

        cockpit.add_panel(panel);
    }

    fn create_lag_metrics(cockpit: &mut Cockpit<Metric>, _config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "frame_lag")
            .histogram(
                Histogram::new_with_defaults("stream_us")
                    .display_time_unit(TimeUnit::Microseconds)
                    .accept((
                        Metric::StreamBatchFrameReceivedLag,
                        Metric::StreamKeepAliveFrameReceivedLag,
                        Metric::StreamInfoFrameReceivedLag,
                    )),
            )
            .histogram(
                Histogram::new_with_defaults("batch_handlers_us")
                    .display_time_unit(TimeUnit::Microseconds)
                    .accept(Metric::BatchProcessingStartedLag),
            )
            .histogram(
                Histogram::new_with_defaults("committer_us")
                    .display_time_unit(TimeUnit::Microseconds)
                    .accept(Metric::CommitterCursorsReceivedLag),
            );

        cockpit.add_panel(panel);
    }

    fn create_batch_metrics(cockpit: &mut Cockpit<Metric>, config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "batches")
            .gauge(create_gauge("in_flight", config).deltas_only(Metric::BatchesInFlightChanged))
            .gauge(create_gauge("in_processing", config).inc_dec_on(
                Metric::BatchProcessingStartedLag,
                Metric::BatchProcessedBytes,
            ))
            .meter(
                Meter::new_with_defaults("per_second").for_label(Metric::BatchProcessingStartedLag),
            )
            .histogram(
                Histogram::new_with_defaults("processing_time_us")
                    .display_time_unit(TimeUnit::Microseconds)
                    .for_label(Metric::BatchProcessedTime),
            )
            .handler(
                ValueMeter::new_with_defaults("bytes_per_second")
                    .for_label(Metric::BatchProcessedBytes),
            )
            .histogram(
                Histogram::new_with_defaults("bytes_per_batch")
                    .for_label(Metric::BatchProcessedBytes),
            );

        cockpit.add_panel(panel);
    }

    fn create_events_metrics(cockpit: &mut Cockpit<Metric>, _config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "events")
            .handler(
                ValueMeter::new_with_defaults("per_second")
                    .for_label(Metric::BatchProcessedNEvents),
            )
            .histogram(
                Histogram::new_with_defaults("per_batch").for_label(Metric::BatchProcessedNEvents),
            )
            .histogram(
                Histogram::new_with_defaults("deserialization_time_us")
                    .display_time_unit(TimeUnit::Microseconds)
                    .for_label(Metric::BatchDeserializationTime),
            )
            .handler(
                ValueMeter::new_with_defaults("deserialization_bytes_per_second")
                    .for_label(Metric::BatchDeserializationBytes),
            );

        cockpit.add_panel(panel);
    }

    fn create_committer_metrics(cockpit: &mut Cockpit<Metric>, _config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "committer")
            .panel(
                Panel::named(AcceptAllLabels, "cursors")
                    .meter(
                        Meter::new_with_defaults("received_per_second")
                            .for_label(Metric::CommitterCursorsReceivedLag),
                    )
                    .handler(
                        ValueMeter::new_with_defaults("committed_per_second")
                            .for_label(Metric::CommitterCursorsCommittedCount),
                    )
                    .handler(
                        ValueMeter::new_with_defaults("not_committed_per_second")
                            .for_label(Metric::CommitterCursorsNotCommittedCount),
                    )
                    .handler(
                        ValueMeter::new_with_defaults("attempt_failed_per_second")
                            .for_label(Metric::CommitterCursorsNotCommittedCount),
                    ),
            )
            .panel(
                Panel::named(Metric::CommitterCursorsCommittedTime, "committed")
                    .meter(
                        Meter::new_with_defaults("per_second")
                            .for_label(Metric::CommitterCursorsCommittedTime),
                    )
                    .histogram(
                        Histogram::new_with_defaults("latency_ms")
                            .display_time_unit(TimeUnit::Milliseconds)
                            .for_label(Metric::CommitterCursorsCommittedTime),
                    ),
            )
            .panel(
                Panel::named(Metric::CommitterCursorsNotCommittedTime, "not_committed")
                    .meter(
                        Meter::new_with_defaults("per_second")
                            .for_label(Metric::CommitterCursorsNotCommittedTime),
                    )
                    .histogram(
                        Histogram::new_with_defaults("latency_ms")
                            .display_time_unit(TimeUnit::Milliseconds)
                            .for_label(Metric::CommitterCursorsNotCommittedTime),
                    ),
            )
            .panel(
                Panel::named((Metric::CommitterAttemptFailedCount, Metric::CommitterAttemptFailedTime), "failed_attempts")
                    .meter(
                        Meter::new_with_defaults("per_second")
                            .for_label(Metric::CommitterAttemptFailedCount),
                    )
                    .histogram(
                        Histogram::new_with_defaults("latency_ms")
                            .display_time_unit(TimeUnit::Milliseconds)
                            .for_label(Metric::CommitterAttemptFailedTime),
                    ),
            );

        cockpit.add_panel(panel);
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
    let switch_off_after = config.alert_duration_secs.unwrap_or_default();
    metrix::instruments::StaircaseTimer::new(name).switch_off_after(switch_off_after.into())
}
