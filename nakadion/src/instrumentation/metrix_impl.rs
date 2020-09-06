use std::time::{Duration, Instant};

use metrix::{
    processor::ProcessorMount, AggregatesProcessors, Decrement, DecrementBy, Increment,
    IncrementBy, TelemetryTransmitter, TimeUnit, TransmitsTelemetryData,
};

use serde::{Deserialize, Serialize};

use crate::components::{
    committer::CommitError,
    connector::ConnectError,
    streams::{EventStreamBatchStats, EventStreamError, EventStreamErrorKind},
};
use crate::internals::background_committer::CommitTrigger;

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
    #[doc="The time an alert will stay on after trigger.\n\nDefault is 61s.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct AlertDurationSecs(u32, env="METRIX_ALERT_DURATION_SECS");
}
impl Default for AlertDurationSecs {
    fn default() -> Self {
        61.into()
    }
}

new_type! {
    #[doc="The time after which a histogram is reset.\n\nDefault is 60s.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct HistogramInactivityResetSecs(u32, env="METRIX_HISTOGRAM_INACTIVITY_RESET_SECS");
}
impl Default for HistogramInactivityResetSecs {
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
    /// Enables tracking for gauges for the given amount of seconds
    pub histogram_inactivity_reset_secs: Option<HistogramInactivityResetSecs>,
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
        if self.histogram_inactivity_reset_secs.is_none() {
            self.histogram_inactivity_reset_secs =
                HistogramInactivityResetSecs::try_from_env_prefixed(prefix.as_ref())?;
        }
        Ok(())
    }
}

impl Instruments for Metrix {
    fn consumer_started(&self) {
        self.tx.observed_one_now(Metric::ConsumerStarted);
    }

    fn consumer_stopped(&self, ran_for: Duration) {
        self.tx.observed_one_value_now(
            Metric::ConsumerStoppedWithTime,
            (ran_for, TimeUnit::Milliseconds),
        );
    }

    fn streaming_ended(&self, streamed_for: Duration) {
        self.tx.observed_one_value_now(
            Metric::StreamingEndedWithTime,
            (streamed_for, TimeUnit::Milliseconds),
        );
    }

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

    fn info_frame_received(&self, _frame_started_at: Instant, frame_completed_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::StreamInfoFrameReceivedLag,
            (frame_completed_at.elapsed(), TimeUnit::Microseconds),
        );
    }
    fn keep_alive_frame_received(&self, _frame_started_at: Instant, frame_completed_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::StreamKeepAliveFrameReceivedLag,
            (frame_completed_at.elapsed(), TimeUnit::Microseconds),
        );
    }
    fn batch_frame_received(
        &self,
        _frame_started_at: Instant,
        frame_completed_at: Instant,
        events_bytes: usize,
    ) {
        self.tx
            .observed_one_value_now(Metric::StreamBatchFrameReceivedBytes, events_bytes);
        self.tx.observed_one_value_now(
            Metric::StreamBatchFrameReceivedLag,
            (frame_completed_at.elapsed(), TimeUnit::Microseconds),
        );
    }

    fn batch_frame_gap(&self, gap: Duration) {
        self.tx
            .observed_one_value_now(Metric::StreamBatchFrameGap, (gap, TimeUnit::Microseconds));
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

    fn stream_dead(&self, after: Duration) {
        self.tx
            .observed_one_value_now(Metric::StreamDeadAfter, (after, TimeUnit::Milliseconds));
    }

    fn stream_error(&self, err: &EventStreamError) {
        match err.kind() {
            EventStreamErrorKind::Io => {
                self.tx.observed_one_now(Metric::StreamErrorIo);
            }
            EventStreamErrorKind::Parser => {
                self.tx.observed_one_now(Metric::StreamErrorParse);
            }
        }
    }

    fn stream_unconsumed_events(&self, n_unconsumed: usize) {
        self.tx
            .observed_one_value_now(Metric::StreamUnconsumedEvents, n_unconsumed);
    }

    fn batches_in_flight_incoming(&self, stats: &EventStreamBatchStats) {
        self.tx
            .observed_one_value_now(Metric::BatchesInFlightChanged, Increment)
            .observed_one_value_now(
                Metric::EventsInFlightChanged,
                IncrementBy(stats.n_events as u32),
            )
            .observed_one_value_now(
                Metric::BytesInFlightChanged,
                IncrementBy(stats.n_bytes as u32),
            );
    }
    fn batches_in_flight_processed(&self, stats: &EventStreamBatchStats) {
        self.tx
            .observed_one_value_now(Metric::BatchesInFlightChanged, Decrement)
            .observed_one_value_now(
                Metric::EventsInFlightChanged,
                DecrementBy(stats.n_events as u32),
            )
            .observed_one_value_now(
                Metric::BytesInFlightChanged,
                DecrementBy(stats.n_bytes as u32),
            );
    }

    fn batches_in_flight_reset(&self) {
        self.tx
            .observed_one_value_now(Metric::BatchesInFlightChanged, 0)
            .observed_one_value_now(Metric::EventsInFlightChanged, 0)
            .observed_one_value_now(Metric::BytesInFlightChanged, 0);
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

    fn batch_processing_started(&self, _frame_started_at: Instant, frame_completed_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::BatchProcessingStartedLag,
            (frame_completed_at.elapsed(), TimeUnit::Microseconds),
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

    fn cursor_to_commit_received(&self, _frame_started_at: Instant, frame_completed_at: Instant) {
        self.tx.observed_one_value_now(
            Metric::CommitterCursorsReceivedLag,
            (frame_completed_at.elapsed(), TimeUnit::Milliseconds),
        );
    }

    fn cursors_commit_triggered(&self, trigger: CommitTrigger) {
        match trigger {
            CommitTrigger::Deadline {
                n_cursors,
                n_events,
            } => {
                self.tx
                    .observed_one_value_now(Metric::CommitterTriggerDeadlineCursorsCount, n_cursors)
                    .observed_one_value_now(Metric::CommitterTriggerDeadlineEventsCount, n_events);
            }
            CommitTrigger::Events {
                n_cursors,
                n_events,
            } => {
                self.tx
                    .observed_one_value_now(Metric::CommitterTriggerEventsCursorsCount, n_cursors)
                    .observed_one_value_now(Metric::CommitterTriggerEventsEventsCount, n_events);
            }
            CommitTrigger::Cursors {
                n_cursors,
                n_events,
            } => {
                self.tx
                    .observed_one_value_now(Metric::CommitterTriggerCursorsCursorsCount, n_cursors)
                    .observed_one_value_now(Metric::CommitterTriggerCursorsEventsCount, n_events);
            }
        }
    }

    fn cursor_ages_on_commit_attempt(
        &self,
        first_cursor_age: Duration,
        last_cursor_age: Duration,
        first_cursor_age_warning: bool,
    ) {
        self.tx
            .observed_one_value_now(
                Metric::CommitterFirstCursorAgeOnCommitAttempt,
                (first_cursor_age, TimeUnit::Milliseconds),
            )
            .observed_one_value_now(
                Metric::CommitterLastCursorAgeOnCommitAttempt,
                (last_cursor_age, TimeUnit::Milliseconds),
            );

        if first_cursor_age_warning {
            self.tx
                .observed_one_now(Metric::CommitterFirstCursorAgeOnCommitAttemptAgeWarning);
        }
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
    ConsumerStarted,
    ConsumerStoppedWithTime,
    StreamingEndedWithTime,
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
    StreamBatchFrameGap,
    StreamDeadAfter,
    StreamUnconsumedEvents,
    NoFramesForWarning,
    NoEventsForWarning,
    StreamErrorIo,
    StreamErrorParse,
    BatchesInFlightChanged,
    EventsInFlightChanged,
    BytesInFlightChanged,
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
    CommitterTriggerDeadlineCursorsCount,
    CommitterTriggerEventsCursorsCount,
    CommitterTriggerCursorsCursorsCount,
    CommitterTriggerDeadlineEventsCount,
    CommitterTriggerEventsEventsCount,
    CommitterTriggerCursorsEventsCount,
    CommitterFirstCursorAgeOnCommitAttempt,
    CommitterFirstCursorAgeOnCommitAttemptAgeWarning,
    CommitterLastCursorAgeOnCommitAttempt,
}

mod instr {
    use metrix::instruments::*;
    use metrix::processor::TelemetryProcessor;
    use metrix::TelemetryTransmitter;
    use metrix::TimeUnit;

    use super::{Metric, MetrixConfig};

    pub fn create(
        config: &MetrixConfig,
    ) -> (TelemetryTransmitter<Metric>, TelemetryProcessor<Metric>) {
        let (tx, mut rx) = TelemetryProcessor::new_pair_without_name();

        let mut cockpit = Cockpit::without_name();

        create_notifications(&mut cockpit, config);
        create_stream_metrics(&mut cockpit, config);
        create_lag_metrics(&mut cockpit, config);
        create_batch_metrics(&mut cockpit, config);
        create_events_metrics(&mut cockpit, config);
        create_committer_metrics(&mut cockpit, config);
        create_event_type_partition_metrics(&mut cockpit, config);

        rx.add_cockpit(cockpit);

        (tx, rx)
    }

    fn create_notifications(cockpit: &mut Cockpit<Metric>, config: &MetrixConfig) {
        use Metric::*;
        let panel = Panel::named(AcceptAllLabels, "notifications")
            .handler(
                create_staircase_timer("consumer_started", config)
                    .for_label(Metric::ConsumerStarted),
            )
            .handler(
                create_staircase_timer("consumer_stopped", config)
                    .for_label(Metric::ConsumerStoppedWithTime),
            )
            .handler(
                create_staircase_timer("streaming_ended", config)
                    .for_label(Metric::StreamingEndedWithTime),
            )
            .handler(create_staircase_timer("stream_io_error", config).for_label(StreamErrorIo))
            .handler(
                create_staircase_timer("stream_parse_error", config).for_label(StreamErrorParse),
            )
            .handler(create_staircase_timer("no_events", config).for_label(NoEventsForWarning))
            .handler(create_staircase_timer("no_frames", config).for_label(NoFramesForWarning))
            .handler(
                create_staircase_timer("commit_cursor_age_warning", config)
                    .for_label(CommitterFirstCursorAgeOnCommitAttemptAgeWarning),
            )
            .handler(
                create_staircase_timer("commit_failed", config).for_label(CommitterCommitFailed),
            )
            .handler(create_staircase_timer("stream_dead", config).for_label(StreamDeadAfter))
            .handler(
                create_staircase_timer("connect_attempt_failed", config)
                    .for_label(StreamConnectAttemptFailedTime),
            );

        cockpit.add_panel(panel);
    }

    fn create_stream_metrics(cockpit: &mut Cockpit<Metric>, config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "stream")
            .panel(create_connector_metrics(config))
            .panel(Panel::named(AcceptAllLabels, "ticks").meter(
                Meter::new_with_defaults("emitted_per_second").for_label(Metric::StreamTickEmitted),
            ))
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
                        create_histogram("size_distribution", config)
                            .accept(Metric::StreamChunkReceivedBytes),
                    ),
            )
            .panel(
                Panel::named(AcceptAllLabels, "frames")
                    .gauge(
                        create_gauge("in_flight_bytes", config)
                            .for_label(Metric::BytesInFlightChanged),
                    )
                    .meter(
                        Meter::new_with_defaults("per_second")
                            .for_label(Metric::StreamFrameCompletedBytes),
                    )
                    .handler(
                        ValueMeter::new_with_defaults("bytes_per_second")
                            .for_label(Metric::StreamFrameCompletedBytes),
                    )
                    .histogram(
                        create_histogram("size_distribution", config)
                            .accept(Metric::StreamFrameCompletedBytes),
                    )
                    .histogram(
                        create_histogram("completion_time_us", config)
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
                    )
                    .histogram(
                        create_histogram("batch_frame_gap_us", config)
                            .display_time_unit(TimeUnit::Microseconds)
                            .accept(Metric::StreamBatchFrameGap),
                    ),
            )
            .panel(
                Panel::named(AcceptAllLabels, "unconsumed_events").gauge(
                    create_gauge("stream", config).for_label(Metric::StreamUnconsumedEvents),
                ),
            );

        cockpit.add_panel(panel);
    }

    fn create_connector_metrics(config: &MetrixConfig) -> Panel<Metric> {
        let panel = Panel::named(AcceptAllLabels, "connector")
            .panel(
                Panel::named(AcceptAllLabels, "attempts")
                    .meter(
                        Meter::new_with_defaults("success_per_second")
                            .for_label(Metric::StreamConnectAttemptSuccessTime),
                    )
                    .histogram(
                        Histogram::new_with_defaults("success_time_ms")
                            .display_time_unit(TimeUnit::Milliseconds)
                            .accept(Metric::StreamConnectAttemptSuccessTime),
                    )
                    .meter(
                        Meter::new_with_defaults("failed_per_second")
                            .for_label(Metric::StreamConnectAttemptFailedTime),
                    )
                    .histogram(
                        create_histogram("failed_time_ms", config)
                            .display_time_unit(TimeUnit::Milliseconds)
                            .accept(Metric::StreamConnectAttemptFailedTime),
                    ),
            )
            .panel(
                Panel::named(AcceptAllLabels, "connected")
                    .meter(
                        Meter::new_with_defaults("success_per_second")
                            .for_label(Metric::StreamConnectedTime),
                    )
                    .histogram(
                        Histogram::new_with_defaults("success_time_ms")
                            .display_time_unit(TimeUnit::Milliseconds)
                            .accept(Metric::StreamConnectedTime),
                    ),
            )
            .panel(
                Panel::named(AcceptAllLabels, "not_connected")
                    .meter(
                        Meter::new_with_defaults("success_per_second")
                            .for_label(Metric::StreamNotConnectedTime),
                    )
                    .histogram(
                        create_histogram("success_time_ms", config)
                            .display_time_unit(TimeUnit::Milliseconds)
                            .accept(Metric::StreamNotConnectedTime),
                    ),
            );

        panel
    }

    fn create_lag_metrics(cockpit: &mut Cockpit<Metric>, config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "frame_lag")
            .histogram(
                create_histogram("stream_us", config)
                    .display_time_unit(TimeUnit::Microseconds)
                    .accept((
                        Metric::StreamBatchFrameReceivedLag,
                        Metric::StreamKeepAliveFrameReceivedLag,
                        Metric::StreamInfoFrameReceivedLag,
                    )),
            )
            .histogram(
                create_histogram("batch_handlers_us", config)
                    .display_time_unit(TimeUnit::Microseconds)
                    .accept(Metric::BatchProcessingStartedLag),
            )
            .histogram(
                create_histogram("committer_us", config)
                    .display_time_unit(TimeUnit::Microseconds)
                    .accept(Metric::CommitterCursorsReceivedLag),
            );

        cockpit.add_panel(panel);
    }

    fn create_batch_metrics(cockpit: &mut Cockpit<Metric>, config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "batches")
            .gauge(create_gauge("in_flight", config).for_label(Metric::BatchesInFlightChanged))
            .gauge(create_gauge("in_processing", config).inc_dec_on(
                Metric::BatchProcessingStartedLag,
                Metric::BatchProcessedBytes,
            ))
            .meter(
                Meter::new_with_defaults("per_second").for_label(Metric::BatchProcessingStartedLag),
            )
            .histogram(
                create_histogram("processing_time_us", config)
                    .display_time_unit(TimeUnit::Microseconds)
                    .for_label(Metric::BatchProcessedTime),
            )
            .handler(
                ValueMeter::new_with_defaults("bytes_per_second")
                    .for_label(Metric::BatchProcessedBytes),
            )
            .histogram(
                create_histogram("bytes_per_batch", config).for_label(Metric::BatchProcessedBytes),
            );

        cockpit.add_panel(panel);
    }

    fn create_events_metrics(cockpit: &mut Cockpit<Metric>, config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "events")
            .gauge(create_gauge("in_flight", config).for_label(Metric::EventsInFlightChanged))
            .handler(
                ValueMeter::new_with_defaults("per_second")
                    .for_label(Metric::BatchProcessedNEvents),
            )
            .histogram(
                create_histogram("per_batch", config).for_label(Metric::BatchProcessedNEvents),
            )
            .histogram(
                create_histogram("deserialization_time_us", config)
                    .display_time_unit(TimeUnit::Microseconds)
                    .for_label(Metric::BatchDeserializationTime),
            )
            .handler(
                ValueMeter::new_with_defaults("deserialization_bytes_per_second")
                    .for_label(Metric::BatchDeserializationBytes),
            );

        cockpit.add_panel(panel);
    }

    fn create_committer_metrics(cockpit: &mut Cockpit<Metric>, config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "committer")
            .panel(
                Panel::named(AcceptAllLabels, "triggers")
                    .panel(
                        Panel::named(
                            (
                                Metric::CommitterTriggerDeadlineCursorsCount,
                                Metric::CommitterTriggerDeadlineEventsCount,
                            ),
                            "deadline",
                        )
                        .meter(
                            Meter::new_with_defaults("occurrences_per_second")
                                .for_label(Metric::CommitterTriggerDeadlineCursorsCount),
                        )
                        .handler(
                            ValueMeter::new_with_defaults("cursors_per_second")
                                .for_label(Metric::CommitterTriggerDeadlineCursorsCount),
                        )
                        .histogram(
                            create_histogram("cursors_distribution", config)
                                .for_label(Metric::CommitterTriggerDeadlineCursorsCount),
                        )
                        .handler(
                            ValueMeter::new_with_defaults("events_per_second")
                                .for_label(Metric::CommitterTriggerDeadlineEventsCount),
                        )
                        .histogram(
                            create_histogram("events_distribution", config)
                                .for_label(Metric::CommitterTriggerDeadlineEventsCount),
                        ),
                    )
                    .panel(
                        Panel::named(
                            (
                                Metric::CommitterTriggerCursorsCursorsCount,
                                Metric::CommitterTriggerCursorsEventsCount,
                            ),
                            "cursors",
                        )
                        .meter(
                            Meter::new_with_defaults("occurrences_per_second")
                                .for_label(Metric::CommitterTriggerCursorsCursorsCount),
                        )
                        .handler(
                            ValueMeter::new_with_defaults("cursors_per_second")
                                .for_label(Metric::CommitterTriggerCursorsCursorsCount),
                        )
                        .histogram(
                            create_histogram("cursors_distribution", config)
                                .for_label(Metric::CommitterTriggerCursorsCursorsCount),
                        )
                        .handler(
                            ValueMeter::new_with_defaults("events_per_second")
                                .for_label(Metric::CommitterTriggerCursorsEventsCount),
                        )
                        .histogram(
                            create_histogram("events_distribution", config)
                                .for_label(Metric::CommitterTriggerCursorsEventsCount),
                        ),
                    )
                    .panel(
                        Panel::named(
                            (
                                Metric::CommitterTriggerEventsCursorsCount,
                                Metric::CommitterTriggerEventsEventsCount,
                            ),
                            "events",
                        )
                        .meter(
                            Meter::new_with_defaults("occurrences_per_second")
                                .for_label(Metric::CommitterTriggerEventsCursorsCount),
                        )
                        .handler(
                            ValueMeter::new_with_defaults("cursors_per_second")
                                .for_label(Metric::CommitterTriggerEventsCursorsCount),
                        )
                        .histogram(
                            create_histogram("cursors_distribution", config)
                                .for_label(Metric::CommitterTriggerEventsCursorsCount),
                        )
                        .handler(
                            ValueMeter::new_with_defaults("events_per_second")
                                .for_label(Metric::CommitterTriggerEventsEventsCount),
                        )
                        .histogram(
                            create_histogram("events_distribution", config)
                                .for_label(Metric::CommitterTriggerEventsEventsCount),
                        ),
                    ),
            )
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
                Panel::named(
                    (
                        Metric::CommitterFirstCursorAgeOnCommitAttempt,
                        Metric::CommitterLastCursorAgeOnCommitAttempt,
                    ),
                    "cursor_ages_on_commit_attempt",
                )
                .histogram(
                    create_histogram("first_ms", config)
                        .display_time_unit(TimeUnit::Milliseconds)
                        .for_label(Metric::CommitterFirstCursorAgeOnCommitAttempt),
                )
                .histogram(
                    create_histogram("last_ms", config)
                        .display_time_unit(TimeUnit::Milliseconds)
                        .for_label(Metric::CommitterLastCursorAgeOnCommitAttempt),
                ),
            )
            .panel(
                Panel::named(Metric::CommitterCursorsCommittedTime, "committed")
                    .meter(
                        Meter::new_with_defaults("per_second")
                            .for_label(Metric::CommitterCursorsCommittedTime),
                    )
                    .histogram(
                        create_histogram("latency_ms", config)
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
                        create_histogram("latency_ms", config)
                            .display_time_unit(TimeUnit::Milliseconds)
                            .for_label(Metric::CommitterCursorsNotCommittedTime),
                    ),
            )
            .panel(
                Panel::named(
                    (
                        Metric::CommitterAttemptFailedCount,
                        Metric::CommitterAttemptFailedTime,
                    ),
                    "failed_attempts",
                )
                .meter(
                    Meter::new_with_defaults("per_second")
                        .for_label(Metric::CommitterAttemptFailedCount),
                )
                .histogram(
                    create_histogram("latency_ms", config)
                        .display_time_unit(TimeUnit::Milliseconds)
                        .for_label(Metric::CommitterAttemptFailedTime),
                ),
            );

        cockpit.add_panel(panel);
    }

    fn create_event_type_partition_metrics(cockpit: &mut Cockpit<Metric>, config: &MetrixConfig) {
        let panel = Panel::named(AcceptAllLabels, "event_type_partitions").gauge(
            create_gauge("active", config).inc_dec_on(
                Metric::EventTypePartitionActivated,
                Metric::EventTypePartitionDeactivatedAfter,
            ),
        );

        cockpit.add_panel(panel);
    }

    fn create_gauge(name: &str, config: &MetrixConfig) -> Gauge {
        let tracking_seconds = config.gauge_tracking_secs.unwrap_or_default();
        Gauge::new(name)
            .tracking(tracking_seconds.into_inner() as usize)
            .group_values(true)
    }

    fn create_staircase_timer(name: &str, config: &MetrixConfig) -> StaircaseTimer {
        let switch_off_after = config.alert_duration_secs.unwrap_or_default();
        StaircaseTimer::new(name).switch_off_after(switch_off_after.into())
    }

    fn create_histogram(name: &str, config: &MetrixConfig) -> Histogram {
        let inactivity_dur = config
            .histogram_inactivity_reset_secs
            .unwrap_or_default()
            .into_duration();
        Histogram::new(name)
            .inactivity_limit(inactivity_dur)
            .reset_after_inactivity(true)
            .show_activity_state(false)
    }
}
