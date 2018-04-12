//! Metrics collected by `Nakadion`
//!
//! A general interface for colleczing metrics is provided
//! by Nakadion. nakadion will call the appropriate methods
//! on the trait `MetricsCollector` when certain things
//! happen within Nakadion.
//!
//! The trait `MetricsCollector` basically allows to attach
//! any form of metrics collection as required by
//! the application using Nakadion.
//!
//! When the feature `metrix` is enabled an implementation
//! for `MetricsCollector` using [metrix](https://crates.io/crates/metrix)
//! is provided and as are constructor
//! functions for Nakadion.
use std::time::{Duration, Instant};

#[cfg(feature = "metrix")]
pub use self::metrix::MetrixCollector;

/// An interface for a `Nakadion` that `Nakadion` can use to notify
/// on changing values and states.
pub trait MetricsCollector {
    /// A connect attempt for streaming has been made.
    fn streaming_connect_attempt(&self);

    /// A connect attempt for streaming failed.
    fn streaming_connect_attempt_failed(&self);

    /// A connect attempt the consumer requested succeeded.
    ///
    /// # Parameters
    ///
    /// * attempt_started: The timestampt when the attempt
    /// to establish a connection was started
    fn consumer_connected(&self, attempt_started: Instant);

    /// The instant of when the connection that just shut
    /// down was initiated. Used to determine for how long Nakadion
    /// was connected.
    fn consumer_connection_lifetime(&self, connected_since: Instant);
    /// A line with the given number of bytes was reveived.
    fn consumer_line_received(&self, bytes: usize);
    /// A line with an info field was received. The info
    /// fieldhad bytes bytes..
    fn consumer_info_line_received(&self, bytes: usize);
    /// A keep alive line with the given number of bytes was reveived.
    fn consumer_keep_alive_line_received(&self, bytes: usize);
    /// A line of events with the given number of bytes was reveived.
    fn consumer_batch_line_received(&self, bytes: usize);

    /// The number of workers currently processing partitions.
    fn dispatcher_current_workers(&self, num_workers: usize);

    /// A worker was started
    fn worker_worker_started(&self);
    /// A worker was stopped
    fn worker_worker_stopped(&self);
    /// Events with a comined legth of `bytes` bytes have been
    /// received.
    fn worker_batch_size_bytes(&self, bytes: usize);
    /// A batch has been processed where processing was started at 'started`.
    fn worker_batch_processed(&self, started: Instant);
    /// The worker processed `n` events of the same batch.
    fn worker_events_in_same_batch_processed(&self, n: usize);

    /// Time elapsed from receiving the cursor from `Nakadi` until
    /// it was send for being committed. This is most probably right
    /// after events have been processed?
    fn committer_cursor_received(&self, cursor_received_at_timestamp: Instant);
    /// A commit attempt has been made. It was started at `commit_attempt_started`.
    /// No difference is made between success and failure.
    fn committer_cursor_commit_attempt(&self, commit_attempt_started: Instant);
    /// A cursor has been committed and the instant when the commit attempt was started
    /// is given.
    fn committer_cursor_committed(&self, commit_attempt_started: Instant);
    /// A cursor has not been committed and the instant when the commit attempt was started
    /// is given.
    fn committer_cursor_commit_failed(&self, commit_attempt_started: Instant);
    /// The number of batches that have been committed with the last cursor.
    fn committer_batches_committed(&self, n: usize);
    /// The number of events that have been committed with the last cursor.
    fn committer_events_committed(&self, n: usize);
    /// How old is this cursor first(oldest) that is committed with the current cursor?
    /// `received_at` is the timestamp when `Nakadion` received the batch
    fn committer_first_cursor_age_on_commit(&self, age: Duration);
    /// How old is this cursor that is currently committed?
    /// This is the cursor that also commits the previously buffered cursors.
    /// `received_at` is the timestamp when `Nakadion` received the batch
    fn committer_last_cursor_age_on_commit(&self, age: Duration);
    /// How much time has elapsed from receiving intial cursor to be committed
    /// until that cursor finally got committed?
    fn committer_cursor_buffer_time(&self, time_buffered: Duration);
    /// The time left when committing the cursor until the stream would have become
    /// invalid.
    fn committer_time_left_on_commit_until_invalid(&self, time_left: Duration);
    /// A panic occured somewhere.
    fn other_panicked(&self);
    fn other_dispatcher_gone(&self);
    fn other_worker_gone(&self);
    fn other_committer_gone(&self);
}

/// Using this disables metrics collection.
#[derive(Clone)]
pub struct DevNullMetricsCollector;

impl MetricsCollector for DevNullMetricsCollector {
    fn streaming_connect_attempt(&self) {}
    fn streaming_connect_attempt_failed(&self) {}

    fn consumer_connected(&self, _attempt_started: Instant) {}
    fn consumer_connection_lifetime(&self, _connected_since: Instant) {}
    fn consumer_line_received(&self, _bytes: usize) {}
    fn consumer_info_line_received(&self, _bytes: usize) {}
    fn consumer_keep_alive_line_received(&self, _bytes: usize) {}
    fn consumer_batch_line_received(&self, _bytes: usize) {}

    fn dispatcher_current_workers(&self, _num_workers: usize) {}

    fn worker_worker_started(&self) {}
    fn worker_worker_stopped(&self) {}
    fn worker_batch_size_bytes(&self, _bytes: usize) {}
    fn worker_batch_processed(&self, _started: Instant) {}
    fn worker_events_in_same_batch_processed(&self, _n: usize) {}

    fn committer_cursor_received(&self, _cursor_received_at_timestamp: Instant) {}
    fn committer_cursor_committed(&self, _commit_attempt_started: Instant) {}
    fn committer_batches_committed(&self, _n: usize) {}
    fn committer_events_committed(&self, _n: usize) {}
    fn committer_cursor_commit_attempt(&self, _commit_attempt_started: Instant) {}
    fn committer_cursor_commit_failed(&self, _commit_attempt_started: Instant) {}
    fn committer_first_cursor_age_on_commit(&self, _age: Duration) {}
    fn committer_last_cursor_age_on_commit(&self, _age: Duration) {}
    fn committer_cursor_buffer_time(&self, _time_buffered: Duration) {}
    fn committer_time_left_on_commit_until_invalid(&self, _time_left: Duration) {}

    fn other_panicked(&self) {}
    fn other_dispatcher_gone(&self) {}
    fn other_worker_gone(&self) {}
    fn other_committer_gone(&self) {}
}

#[cfg(feature = "metrix")]
mod metrix {
    use std::time::{Duration, Instant};

    use metrix::TelemetryTransmitterSync;
    use metrix::TransmitsTelemetryData;
    use metrix::cockpit::*;
    use metrix::instruments::other_instruments::*;
    use metrix::instruments::switches::*;
    use metrix::instruments::*;
    use metrix::processor::*;

    #[derive(Clone, PartialEq, Eq)]
    enum OtherMetrics {
        Panicked,
        DispatcherGone,
        WorkerGone,
        CommitterGone,
    }

    #[derive(Clone, PartialEq, Eq)]
    enum ConnectorMetrics {
        ConnectAttempt,
        ConnectAttemptFailed,
    }

    #[derive(Clone, PartialEq, Eq)]
    enum ConsumerMetrics {
        Connected,
        ConnectionLifetime,
        LineReceived,
        KeepAliveLineReceived,
        InfoLineReceived,
        BatchLineReceived,
    }

    #[derive(Clone, PartialEq, Eq)]
    enum DispatcherMetrics {
        NumWorkers,
    }

    #[derive(Clone, PartialEq, Eq)]
    enum WorkerMetrics {
        WorkerStarted,
        WorkerStopped,
        BatchSizeInBytes,
        BatchProcessed,
        EventsProcessed,
    }

    #[derive(Clone, PartialEq, Eq)]
    enum CursorMetrics {
        CursorReceived,
        CursorCommitted,
        BatchesCommitted,
        EventsCommitted,
        CursorCommitAttempt,
        CursorCommitAttemptFailed,
        FirstCursorAgeOnCommit,
        LastCursorAgeOnCommit,
        CursorBufferTime,
        TimeLeftUntilInvalid,
    }

    /// A `MetricsCollector` that works with the [`metrix`](https://crates.io/crates/metrix)
    ///  library
    #[derive(Clone)]
    pub struct MetrixCollector {
        connector: TelemetryTransmitterSync<ConnectorMetrics>,
        consumer: TelemetryTransmitterSync<ConsumerMetrics>,
        dispatcher: TelemetryTransmitterSync<DispatcherMetrics>,
        worker: TelemetryTransmitterSync<WorkerMetrics>,
        cursor: TelemetryTransmitterSync<CursorMetrics>,
        other: TelemetryTransmitterSync<OtherMetrics>,
    }

    impl MetrixCollector {
        /// Creates a new collector that
        /// is attached to `add_metrics_to`.
        pub fn new<T>(add_metrics_to: &mut T) -> MetrixCollector
        where
            T: AggregatesProcessors,
        {
            let (connector_tx, connector_rx) = create_connector_metrics();
            let (consumer_tx, consumer_rx) = create_consumer_metrics();
            let (dispatcher_tx, dispatcher_rx) = create_dispatcher_metrics();
            let (worker_tx, worker_rx) = create_worker_metrics();
            let (cursor_tx, cursor_rx) = create_cursor_metrics();
            let (other_tx, other_rx) = create_other_metrics();

            add_metrics_to.add_processor(connector_rx);
            add_metrics_to.add_processor(consumer_rx);
            add_metrics_to.add_processor(dispatcher_rx);
            add_metrics_to.add_processor(worker_rx);
            add_metrics_to.add_processor(cursor_rx);
            add_metrics_to.add_processor(other_rx);

            MetrixCollector {
                connector: connector_tx,
                consumer: consumer_tx,
                dispatcher: dispatcher_tx,
                worker: worker_tx,
                cursor: cursor_tx,
                other: other_tx,
            }
        }
    }

    impl super::MetricsCollector for MetrixCollector {
        fn streaming_connect_attempt(&self) {
            self.connector
                .observed_one_now(ConnectorMetrics::ConnectAttempt);
        }
        fn streaming_connect_attempt_failed(&self) {
            self.connector
                .observed_one_now(ConnectorMetrics::ConnectAttemptFailed);
        }

        fn consumer_connected(&self, attempt_started: Instant) {
            self.consumer
                .measure_time(ConsumerMetrics::Connected, attempt_started);
        }
        fn consumer_connection_lifetime(&self, connected_since: Instant) {
            self.consumer
                .measure_time(ConsumerMetrics::ConnectionLifetime, connected_since);
        }
        fn consumer_line_received(&self, bytes: usize) {
            self.consumer
                .observed_one_value_now(ConsumerMetrics::LineReceived, bytes as u64);
        }
        fn consumer_info_line_received(&self, bytes: usize) {
            self.consumer
                .observed_one_value_now(ConsumerMetrics::InfoLineReceived, bytes as u64);
        }
        fn consumer_keep_alive_line_received(&self, bytes: usize) {
            self.consumer
                .observed_one_value_now(ConsumerMetrics::KeepAliveLineReceived, bytes as u64);
        }
        fn consumer_batch_line_received(&self, bytes: usize) {
            self.consumer
                .observed_one_value_now(ConsumerMetrics::BatchLineReceived, bytes as u64);
        }

        fn dispatcher_current_workers(&self, num_workers: usize) {
            self.dispatcher
                .observed_one_value_now(DispatcherMetrics::NumWorkers, num_workers as u64);
        }

        fn worker_worker_started(&self) {
            self.worker.observed_one_now(WorkerMetrics::WorkerStarted)
        }
        fn worker_worker_stopped(&self) {
            self.worker.observed_one_now(WorkerMetrics::WorkerStopped)
        }
        fn worker_batch_size_bytes(&self, bytes: usize) {
            self.worker
                .observed_one_value_now(WorkerMetrics::BatchSizeInBytes, bytes as u64);
        }
        fn worker_batch_processed(&self, started: Instant) {
            self.worker
                .measure_time(WorkerMetrics::BatchProcessed, started);
        }
        fn worker_events_in_same_batch_processed(&self, n: usize) {
            self.worker
                .observed_one_value_now(WorkerMetrics::EventsProcessed, n as u64);
        }

        fn committer_cursor_received(&self, cursor_received_at_timestamp: Instant) {
            self.cursor
                .measure_time(CursorMetrics::CursorReceived, cursor_received_at_timestamp);
        }
        fn committer_cursor_committed(&self, commit_attempt_started: Instant) {
            self.cursor
                .measure_time(CursorMetrics::CursorCommitted, commit_attempt_started);
        }
        fn committer_batches_committed(&self, n: usize) {
            if n > 0 {
                self.cursor
                    .observed_now(CursorMetrics::BatchesCommitted, n as u64);
            }
        }
        fn committer_events_committed(&self, n: usize) {
            if n > 0 {
                self.cursor
                    .observed_now(CursorMetrics::EventsCommitted, n as u64);
            }
        }
        fn committer_cursor_commit_attempt(&self, commit_attempt_started: Instant) {
            self.cursor
                .measure_time(CursorMetrics::CursorCommitAttempt, commit_attempt_started);
        }
        fn committer_cursor_commit_failed(&self, commit_attempt_started: Instant) {
            self.cursor.measure_time(
                CursorMetrics::CursorCommitAttemptFailed,
                commit_attempt_started,
            );
        }
        fn committer_first_cursor_age_on_commit(&self, age: Duration) {
            self.cursor
                .observed_one_duration_now(CursorMetrics::FirstCursorAgeOnCommit, age);
        }
        fn committer_last_cursor_age_on_commit(&self, age: Duration) {
            self.cursor
                .observed_one_duration_now(CursorMetrics::LastCursorAgeOnCommit, age);
        }
        fn committer_cursor_buffer_time(&self, time_buffered: Duration) {
            self.cursor
                .observed_one_duration_now(CursorMetrics::CursorBufferTime, time_buffered);
        }
        fn committer_time_left_on_commit_until_invalid(&self, time_left: Duration) {
            self.cursor
                .observed_one_duration_now(CursorMetrics::TimeLeftUntilInvalid, time_left);
        }

        fn other_panicked(&self) {
            self.other.observed_one_now(OtherMetrics::Panicked)
        }
        fn other_dispatcher_gone(&self) {
            self.other.observed_one_now(OtherMetrics::DispatcherGone)
        }
        fn other_worker_gone(&self) {
            self.other.observed_one_now(OtherMetrics::WorkerGone)
        }
        fn other_committer_gone(&self) {
            self.other.observed_one_now(OtherMetrics::CommitterGone)
        }
    }

    fn create_other_metrics() -> (
        TelemetryTransmitterSync<OtherMetrics>,
        TelemetryProcessor<OtherMetrics>,
    ) {
        let mut cockpit: Cockpit<OtherMetrics> = Cockpit::without_name(None);

        let mut panel = Panel::with_name(OtherMetrics::Panicked, "panicked");
        let switch = StaircaseTimer::new_with_defaults("occurred");
        panel.add_instrument(switch);

        let mut panel = Panel::with_name(OtherMetrics::DispatcherGone, "dispatcher_gone");
        let switch = StaircaseTimer::new_with_defaults("occurred");
        panel.add_instrument(switch);

        let mut panel = Panel::with_name(OtherMetrics::WorkerGone, "worker_gone");
        let switch = StaircaseTimer::new_with_defaults("occurred");
        panel.add_instrument(switch);

        let mut panel = Panel::with_name(OtherMetrics::CommitterGone, "committer_gone");
        let switch = StaircaseTimer::new_with_defaults("occurred");
        panel.add_instrument(switch);

        cockpit.add_panel(panel);

        let (tx, rx) = TelemetryProcessor::new_pair("other");

        tx.add_cockpit(cockpit);

        (tx.synced(), rx)
    }

    fn create_connector_metrics() -> (
        TelemetryTransmitterSync<ConnectorMetrics>,
        TelemetryProcessor<ConnectorMetrics>,
    ) {
        let mut cockpit: Cockpit<ConnectorMetrics> = Cockpit::without_name(None);

        let connect_attempts_panel =
            Panel::with_name(ConnectorMetrics::ConnectAttempt, "connect_attempts");
        add_counting_instruments_to_cockpit(connect_attempts_panel, &mut cockpit);
        let connect_attempts_failed_panel = Panel::with_name(
            ConnectorMetrics::ConnectAttemptFailed,
            "connect_attempts_failed",
        );
        add_counting_instruments_to_cockpit(connect_attempts_failed_panel, &mut cockpit);

        let (tx, rx) = TelemetryProcessor::new_pair("connector");

        tx.add_cockpit(cockpit);

        (tx.synced(), rx)
    }

    fn create_consumer_metrics() -> (
        TelemetryTransmitterSync<ConsumerMetrics>,
        TelemetryProcessor<ConsumerMetrics>,
    ) {
        let mut cockpit: Cockpit<ConsumerMetrics> = Cockpit::without_name(None);

        let connected_panel = Panel::with_name(ConsumerMetrics::Connected, "connected");
        add_counting_and_time_ms_instruments_to_cockpit(connected_panel, &mut cockpit);

        let connection_lifetimes_panel =
            Panel::with_name(ConsumerMetrics::ConnectionLifetime, "connection_lifetimes");
        add_ms_histogram_instruments_to_cockpit(connection_lifetimes_panel, &mut cockpit);

        let line_received_panel = Panel::with_name(ConsumerMetrics::LineReceived, "all_lines");
        add_line_instruments_to_cockpit(line_received_panel, &mut cockpit);

        let info_line_received_panel =
            Panel::with_name(ConsumerMetrics::InfoLineReceived, "info_lines");
        add_line_instruments_to_cockpit(info_line_received_panel, &mut cockpit);

        let keep_alive_line_received_panel =
            Panel::with_name(ConsumerMetrics::KeepAliveLineReceived, "keep_alive_lines");
        add_line_instruments_to_cockpit(keep_alive_line_received_panel, &mut cockpit);

        let mut batch_line_received_panel =
            Panel::with_name(ConsumerMetrics::BatchLineReceived, "batch_lines");
        let last_batch_line_received_tracker =
            LastOccurrenceTracker::new_with_defaults("last_received_seconds_ago");
        batch_line_received_panel.add_instrument(last_batch_line_received_tracker);
        add_line_instruments_to_cockpit(batch_line_received_panel, &mut cockpit);

        let mut alerts_panel = Panel::with_name(ConsumerMetrics::BatchLineReceived, "alerts");
        let mut no_batches_for_one_minute_alert =
            NonOccurrenceIndicator::new_with_defaults("no_batches_for_one_minute");
        no_batches_for_one_minute_alert.set_if_not_happened_within(Duration::from_secs(60));
        alerts_panel.add_instrument(no_batches_for_one_minute_alert);

        let mut no_batches_for_two_minutes_alert =
            NonOccurrenceIndicator::new_with_defaults("no_batches_for_two_minutes");
        no_batches_for_two_minutes_alert.set_if_not_happened_within(Duration::from_secs(2 * 60));
        alerts_panel.add_instrument(no_batches_for_two_minutes_alert);

        let mut no_batches_for_five_minutes_alert =
            NonOccurrenceIndicator::new_with_defaults("no_batches_for_five_minutes");
        no_batches_for_five_minutes_alert.set_if_not_happened_within(Duration::from_secs(5 * 60));
        alerts_panel.add_instrument(no_batches_for_five_minutes_alert);

        let mut no_batches_for_ten_minutes_alert =
            NonOccurrenceIndicator::new_with_defaults("no_batches_for_ten_minutes");
        no_batches_for_ten_minutes_alert.set_if_not_happened_within(Duration::from_secs(10 * 60));
        alerts_panel.add_instrument(no_batches_for_ten_minutes_alert);

        let mut no_batches_for_fifteen_minutes_alert =
            NonOccurrenceIndicator::new_with_defaults("no_batches_for_fifteen_minutes");
        no_batches_for_fifteen_minutes_alert
            .set_if_not_happened_within(Duration::from_secs(15 * 60));
        alerts_panel.add_instrument(no_batches_for_fifteen_minutes_alert);

        cockpit.add_panel(alerts_panel);

        let (tx, rx) = TelemetryProcessor::new_pair("consumer");

        tx.add_cockpit(cockpit);

        (tx.synced(), rx)
    }

    fn create_dispatcher_metrics() -> (
        TelemetryTransmitterSync<DispatcherMetrics>,
        TelemetryProcessor<DispatcherMetrics>,
    ) {
        let mut cockpit: Cockpit<DispatcherMetrics> = Cockpit::without_name(None);

        let mut num_workers_panel = Panel::new(DispatcherMetrics::NumWorkers);
        num_workers_panel.set_gauge(Gauge::new_with_defaults("num_workers"));
        cockpit.add_panel(num_workers_panel);

        let (tx, rx) = TelemetryProcessor::new_pair("dispatcher");

        tx.add_cockpit(cockpit);

        (tx.synced(), rx)
    }

    fn create_worker_metrics() -> (
        TelemetryTransmitterSync<WorkerMetrics>,
        TelemetryProcessor<WorkerMetrics>,
    ) {
        let mut cockpit: Cockpit<WorkerMetrics> = Cockpit::without_name(None);

        let mut event_bytes_panel =
            Panel::with_name(WorkerMetrics::BatchSizeInBytes, "incoming_batches");
        event_bytes_panel.add_instrument(ValueMeter::new_with_defaults("bytes_per_second"));
        event_bytes_panel.set_histogram(Histogram::new_with_defaults("bytes_distribution"));
        cockpit.add_panel(event_bytes_panel);

        let batches_processed_panel =
            Panel::with_name(WorkerMetrics::BatchProcessed, "batches_processed");
        add_counting_and_time_us_instruments_to_cockpit(batches_processed_panel, &mut cockpit);

        let mut events_processed_panel =
            Panel::with_name(WorkerMetrics::EventsProcessed, "events_processed");
        events_processed_panel.add_instrument(ValueMeter::new_with_defaults("per_second"));
        events_processed_panel.set_histogram(Histogram::new_with_defaults("batch_size"));
        cockpit.add_panel(events_processed_panel);

        let mut worker_started_panel = Panel::new(WorkerMetrics::WorkerStarted);
        let mut tracker = LastOccurrenceTracker::new_with_defaults("worker_started");
        tracker.set_title("Worker started");
        tracker.set_description(
            "A worker for a partition has been started within the last minute started",
        );
        worker_started_panel.add_instrument(tracker);
        cockpit.add_panel(worker_started_panel);

        let mut worker_started_panel = Panel::new(WorkerMetrics::WorkerStopped);
        let mut tracker = LastOccurrenceTracker::new_with_defaults("worker_stopped");
        tracker.set_title("Worker stopped");
        tracker.set_description(
            "A worker for a partition has been stopped within the last minute started",
        );
        worker_started_panel.add_instrument(tracker);
        cockpit.add_panel(worker_started_panel);

        let (tx, rx) = TelemetryProcessor::new_pair("workers");

        tx.add_cockpit(cockpit);

        (tx.synced(), rx)
    }

    fn create_cursor_metrics() -> (
        TelemetryTransmitterSync<CursorMetrics>,
        TelemetryProcessor<CursorMetrics>,
    ) {
        let mut cockpit: Cockpit<CursorMetrics> = Cockpit::without_name(None);

        let mut cursors_received_panel =
            Panel::with_name(CursorMetrics::CursorReceived, "cursors_received");
        cursors_received_panel.set_value_scaling(ValueScaling::NanosToMicros);
        cursors_received_panel.set_counter(Counter::new_with_defaults("count"));
        cursors_received_panel.set_meter(Meter::new_with_defaults("per_second"));
        cursors_received_panel.set_histogram(Histogram::new_with_defaults("elapsed_us"));
        cockpit.add_panel(cursors_received_panel);

        let cursors_committed_panel =
            Panel::with_name(CursorMetrics::CursorCommitted, "cursors_committed");
        add_counting_and_time_us_instruments_to_cockpit(cursors_committed_panel, &mut cockpit);

        let batches_committed_panel =
            Panel::with_name(CursorMetrics::BatchesCommitted, "batches_committed");
        add_counting_instruments_to_cockpit(batches_committed_panel, &mut cockpit);

        let events_committed_panel =
            Panel::with_name(CursorMetrics::EventsCommitted, "events_committed");
        add_counting_instruments_to_cockpit(events_committed_panel, &mut cockpit);

        let commit_attempts_panel =
            Panel::with_name(CursorMetrics::CursorCommitAttempt, "commit_attempts");
        add_counting_instruments_to_cockpit(commit_attempts_panel, &mut cockpit);

        let commit_attempts_failed_panel = Panel::with_name(
            CursorMetrics::CursorCommitAttemptFailed,
            "commit_attempts_failed",
        );
        add_counting_instruments_to_cockpit(commit_attempts_failed_panel, &mut cockpit);

        let mut first_cursor_age_on_commit_panel = Panel::with_name(
            CursorMetrics::FirstCursorAgeOnCommit,
            "first_cursor_age_on_commit",
        );
        first_cursor_age_on_commit_panel.set_description(
            "The age of the first \
             cursor(of maybe many subsequent cursors) to be \
             committed when it was committed.",
        );
        add_us_histogram_instruments_to_cockpit(first_cursor_age_on_commit_panel, &mut cockpit);

        let mut last_cursor_age_on_commit_panel = Panel::with_name(
            CursorMetrics::LastCursorAgeOnCommit,
            "last_cursor_age_on_commit",
        );
        last_cursor_age_on_commit_panel.set_description(
            "The age of the last \
             cursor(of maybe many subsequent cursors) to be \
             committed when it was committed.",
        );
        add_us_histogram_instruments_to_cockpit(last_cursor_age_on_commit_panel, &mut cockpit);

        let mut cursor_buffer_time_panel =
            Panel::with_name(CursorMetrics::CursorBufferTime, "cursor_buffer_time");
        cursor_buffer_time_panel
            .set_description("The time a cursor has been buffered until it was finally committed.");
        add_us_histogram_instruments_to_cockpit(cursor_buffer_time_panel, &mut cockpit);

        let mut time_left_panel = Panel::with_name(
            CursorMetrics::TimeLeftUntilInvalid,
            "cursor_time_left_until_invalid",
        );
        time_left_panel.set_description(
            "The time left after a commit until the \
             stream would have become invalid(closed by Nakadi).",
        );
        add_us_histogram_instruments_to_cockpit(time_left_panel, &mut cockpit);

        let (tx, rx) = TelemetryProcessor::new_pair("cursors");

        tx.add_cockpit(cockpit);

        (tx.synced(), rx)
    }

    fn add_line_instruments_to_cockpit<L>(mut panel: Panel<L>, cockpit: &mut Cockpit<L>)
    where
        L: Clone + Eq + Send + 'static,
    {
        panel.set_counter(Counter::new_with_defaults("count"));
        panel.set_meter(Meter::new_with_defaults("per_second"));
        panel.add_instrument(ValueMeter::new_with_defaults("bytes_per_second"));
        panel.set_histogram(Histogram::new_with_defaults("bytes_distribution"));
        cockpit.add_panel(panel);
    }

    fn add_counting_instruments_to_cockpit<L>(mut panel: Panel<L>, cockpit: &mut Cockpit<L>)
    where
        L: Clone + Eq + Send + 'static,
    {
        panel.set_counter(Counter::new_with_defaults("count"));
        panel.set_meter(Meter::new_with_defaults("per_second"));
        cockpit.add_panel(panel);
    }

    fn add_counting_and_time_us_instruments_to_cockpit<L>(
        mut panel: Panel<L>,
        cockpit: &mut Cockpit<L>,
    ) where
        L: Clone + Eq + Send + 'static,
    {
        panel.set_value_scaling(ValueScaling::NanosToMicros);
        panel.set_counter(Counter::new_with_defaults("count"));
        panel.set_meter(Meter::new_with_defaults("per_second"));
        panel.set_histogram(Histogram::new_with_defaults("time_us"));
        cockpit.add_panel(panel);
    }

    fn add_counting_and_time_ms_instruments_to_cockpit<L>(
        mut panel: Panel<L>,
        cockpit: &mut Cockpit<L>,
    ) where
        L: Clone + Eq + Send + 'static,
    {
        panel.set_value_scaling(ValueScaling::NanosToMillis);
        panel.set_counter(Counter::new_with_defaults("count"));
        panel.set_meter(Meter::new_with_defaults("per_second"));
        panel.set_histogram(Histogram::new_with_defaults("time_ms"));
        cockpit.add_panel(panel);
    }

    fn add_us_histogram_instruments_to_cockpit<L>(mut panel: Panel<L>, cockpit: &mut Cockpit<L>)
    where
        L: Clone + Eq + Send + 'static,
    {
        panel.set_value_scaling(ValueScaling::NanosToMicros);
        panel.set_counter(Counter::new_with_defaults("count"));
        panel.set_histogram(Histogram::new_with_defaults("microseconds"));
        cockpit.add_panel(panel);
    }

    fn add_ms_histogram_instruments_to_cockpit<L>(mut panel: Panel<L>, cockpit: &mut Cockpit<L>)
    where
        L: Clone + Eq + Send + 'static,
    {
        panel.set_value_scaling(ValueScaling::NanosToMillis);
        panel.set_counter(Counter::new_with_defaults("count"));
        panel.set_histogram(Histogram::new_with_defaults("milliseconds"));
        cockpit.add_panel(panel);
    }
}
