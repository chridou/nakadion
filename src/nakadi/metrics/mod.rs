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

#[cfg(feature = "metrix")]
mod metrix;

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
    /// Time elapsed from receiving the batch from `Nakadi`.
    fn consumer_batch_received(&self, batch_received_at_timestamp: Instant);

    /// Time elapsed from receiving the batch from `Nakadi`.
    fn dispatcher_batch_received(&self, batch_received_at_timestamp: Instant);
    /// The number of workers currently processing partitions.
    fn dispatcher_current_workers(&self, num_workers: usize);

    /// A worker was started
    fn worker_worker_started(&self);
    /// A worker was stopped
    fn worker_worker_stopped(&self);
    /// Time elapsed from receiving the batch from `Nakadi`.
    fn worker_batch_received(&self, batch_received_at_timestamp: Instant);
    /// Events with a comined legth of `bytes` bytes have been
    /// received.
    fn worker_batch_size_bytes(&self, bytes: usize);
    /// A batch has been processed where processing was started at 'started`.
    fn worker_batch_processed(&self, started: Instant);
    /// The worker processed `n` events of the same batch.
    fn worker_events_in_same_batch_processed(&self, n: usize);

    /// Time elapsed from receiving the batch from `Nakadi`.
    fn committer_batch_received(&self, batch_received_at_timestamp: Instant);
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
    fn consumer_batch_received(&self, _batch_received_at_timestamp: Instant) {}

    fn dispatcher_batch_received(&self, _batch_received_at_timestamp: Instant) {}
    fn dispatcher_current_workers(&self, _num_workers: usize) {}

    fn worker_batch_received(&self, _batch_received_at_timestamp: Instant) {}
    fn worker_worker_started(&self) {}
    fn worker_worker_stopped(&self) {}
    fn worker_batch_size_bytes(&self, _bytes: usize) {}
    fn worker_batch_processed(&self, _started: Instant) {}
    fn worker_events_in_same_batch_processed(&self, _n: usize) {}

    fn committer_batch_received(&self, _batch_received_at_timestamp: Instant) {}
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
