use std::time::Instant;

pub trait MetricsCollector {
    fn streaming_connect_attempt(&self);
    fn streaming_connect_attempt_failed(&self);

    fn consumer_connected(&self, attempt_started: Instant);
    fn consumer_line_received(&self, bytes: usize);
    fn consumer_info_line_received(&self, bytes: usize);
    fn consumer_keep_alive_line_received(&self, bytes: usize);
    fn consumer_batch_line_received(&self, bytes: usize);

    fn worker_events_received(&self, bytes: usize);
    fn worker_batch_processed(&self, started: Instant);
    fn worker_events_processed(&self, n: usize);

    /// How old is this cursor when the committer received it?
    fn committer_cursor_received(&self, cursor_received_at_timestamp: Instant);
    fn committer_cursor_committed(&self, commit_attempt_started: Instant);
    fn committer_batches_committed(&self, n: usize);
    fn committer_events_committed(&self, n: usize);
    fn committer_cursor_commit_attempt(&self, commit_attempt_started: Instant);
    fn committer_cursor_commit_failed(&self, commit_attempt_started: Instant);
    /// How old is this cursor that is currently committed?
    fn committer_cursor_age_at_commit(&self, received_at_timestamp: Instant);
    /// How match time has elapsed from the first cursor to be committed
    /// until the batch finally got committed?
    fn committer_time_elapsed_until_commit(&self, first_cursor_age: Instant);
    fn committer_time_left_on_commit(&self, committed_at: Instant, deadline: Instant);
}

#[derive(Clone)]
pub struct DevNullMetricsCollector;

impl MetricsCollector for DevNullMetricsCollector {
    fn streaming_connect_attempt(&self) {}
    fn streaming_connect_attempt_failed(&self) {}

    fn consumer_connected(&self, _attempt_started: Instant) {}
    fn consumer_line_received(&self, _bytes: usize) {}
    fn consumer_info_line_received(&self, _bytes: usize) {}
    fn consumer_keep_alive_line_received(&self, _bytes: usize) {}
    fn consumer_batch_line_received(&self, _bytes: usize) {}

    fn worker_events_received(&self, _bytes: usize) {}
    fn worker_batch_processed(&self, _started: Instant) {}
    fn worker_events_processed(&self, _n: usize) {}

    fn committer_cursor_received(&self, _cursor_received_at_timestamp: Instant) {}
    fn committer_cursor_committed(&self, _commit_attempt_started: Instant) {}
    fn committer_batches_committed(&self, _n: usize) {}
    fn committer_events_committed(&self, _n: usize) {}
    fn committer_cursor_commit_attempt(&self, _commit_attempt_started: Instant) {}
    fn committer_cursor_commit_failed(&self, _commit_attempt_started: Instant) {}
    fn committer_cursor_age_at_commit(&self, _received_at_timestamp: Instant) {}
    fn committer_time_elapsed_until_commit(&self, _first_cursor_age: Instant) {}
    fn committer_time_left_on_commit(&self, _committed_at: Instant, _deadline: Instant) {}
}
