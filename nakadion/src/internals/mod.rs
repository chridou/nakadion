//! Internals of the `Consumer`
use std::fmt::Arguments;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Weak,
};

use crate::consumer::{Config, ConsumerError, Instrumentation};
use crate::logging::{ContextualLogger, Logger};
use crate::{
    components::streams::EventStreamBatchStats,
    instrumentation::Instruments,
    nakadi_types::subscription::{StreamId, StreamParameters, SubscriptionId},
};

//pub mod committer;
pub mod background_committer;
pub mod controller;
pub mod dispatcher;
pub mod worker;

/// A result which influences the outcome of the consumer
pub(crate) type ConsumptionResult<T> = Result<T, ConsumerError>;

/// A state that stays valid for the whole lifetime of the consumer.
///
/// That means it is valid over multiple streams.
#[derive(Clone)]
pub(crate) struct ConsumerState {
    is_globally_cancelled: Arc<AtomicBool>,
    config: Arc<Config>,
    logger: ContextualLogger,
    instrumentation: Instrumentation,
}

impl ConsumerState {
    pub fn new(config: Config, logger: ContextualLogger) -> Self {
        let subscription_id = config.subscription_id;
        let instrumentation = config.instrumentation.clone();
        Self {
            is_globally_cancelled: Arc::new(AtomicBool::new(false)),
            config: Arc::new(config),
            logger: logger.subscription_id(subscription_id),
            instrumentation,
        }
    }

    /// Creates a new stream state.
    ///
    /// The `StreamState` as the analogy to this one for the lifetime of a stream
    pub fn stream_state(&self, stream_id: StreamId) -> StreamState {
        StreamState::new(
            stream_id,
            Arc::clone(&self.config),
            Arc::downgrade(&self.is_globally_cancelled),
            self.logger.stream_id(stream_id),
            self.instrumentation.clone(),
        )
    }

    /// Triggering this will cause the `Consumer` to abort
    pub fn request_global_cancellation(&self) {
        self.info(format_args!("Consumer cancellation requested"));
        self.is_globally_cancelled.store(true, Ordering::SeqCst)
    }

    pub fn global_cancellation_requested(&self) -> bool {
        self.is_globally_cancelled.load(Ordering::SeqCst)
    }

    pub fn subscription_id(&self) -> SubscriptionId {
        self.config().subscription_id
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    #[allow(dead_code)]
    pub fn stream_parameters(&self) -> &StreamParameters {
        &self.config().connect_config.stream_parameters
    }

    pub fn instrumentation(&self) -> &Instrumentation {
        &self.instrumentation
    }
}

impl Logger for ConsumerState {
    fn debug(&self, args: Arguments) {
        self.logger.debug(args);
    }

    fn info(&self, args: Arguments) {
        self.logger.info(args);
    }
    fn warn(&self, args: Arguments) {
        self.logger.warn(args);
    }

    fn error(&self, args: Arguments) {
        self.logger.error(args);
    }
}

/// The state of a stream.
///
/// This struct is valid for the whole lifetime of a stream.
#[derive(Clone)]
pub(crate) struct StreamState {
    stream_id: StreamId,
    config: Arc<Config>,
    is_cancelled: Arc<AtomicBool>,
    is_globally_cancelled: Weak<AtomicBool>,
    logger: ContextualLogger,
    instrumentation: Instrumentation,
    stats: Arc<StreamStats>,
}

impl StreamState {
    pub fn new(
        stream_id: StreamId,
        config: Arc<Config>,
        is_globally_cancelled: Weak<AtomicBool>,
        logger: ContextualLogger,
        instrumentation: Instrumentation,
    ) -> Self {
        Self {
            stream_id,
            config,
            is_cancelled: Arc::new(AtomicBool::new(false)),
            is_globally_cancelled,
            logger,
            instrumentation,
            stats: Default::default(),
        }
    }

    /// Returns true if cancellation of the stream was requested.
    ///
    /// This holds also true for the whole consumer being aborted.
    pub fn cancellation_requested(&self) -> bool {
        if self.is_cancelled.load(Ordering::SeqCst) {
            return true;
        }

        if let Some(is_globally_cancelled) = self.is_globally_cancelled.upgrade() {
            is_globally_cancelled.load(Ordering::SeqCst)
        } else {
            true
        }
    }

    /// Request the cancellation of the current stream
    ///
    /// This will initiate a reconnect for a new stream
    pub fn request_stream_cancellation(&self) {
        self.is_cancelled.store(true, Ordering::SeqCst)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    #[allow(dead_code)]
    pub fn stream_parameters(&self) -> &StreamParameters {
        &self.config().connect_config.stream_parameters
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn subscription_id(&self) -> SubscriptionId {
        self.config().subscription_id
    }

    pub fn logger(&self) -> &ContextualLogger {
        &self.logger
    }

    pub fn instrumentation(&self) -> &Instrumentation {
        &self.instrumentation
    }

    pub fn dispatched_events_batch(&self, stats: EventStreamBatchStats) {
        self.instrumentation.batches_in_flight_incoming(&stats);
        self.stats.batches_in_flight.fetch_add(1, Ordering::SeqCst);
        self.stats
            .events_in_flight
            .fetch_add(stats.n_events, Ordering::SeqCst);
        self.stats
            .bytes_in_flight
            .fetch_add(stats.n_bytes, Ordering::SeqCst);
        self.stats
            .uncommitted_batches
            .fetch_add(1, Ordering::SeqCst);
        self.stats
            .uncommitted_events
            .fetch_add(stats.n_events, Ordering::SeqCst);
    }

    pub fn processed_events_batch(&self, stats: EventStreamBatchStats) {
        self.instrumentation.batches_in_flight_processed(&stats);
        self.stats.batches_in_flight.fetch_sub(1, Ordering::SeqCst);
        self.stats
            .events_in_flight
            .fetch_sub(stats.n_events, Ordering::SeqCst);
        self.stats
            .bytes_in_flight
            .fetch_sub(stats.n_bytes, Ordering::SeqCst);
    }

    pub fn batches_committed(&self, n_batches: usize, n_events: usize) {
        self.instrumentation.batches_committed(n_batches, n_events);
        self.stats
            .uncommitted_batches
            .fetch_sub(n_batches, Ordering::SeqCst);
        self.stats
            .uncommitted_events
            .fetch_sub(n_events, Ordering::SeqCst);
    }

    pub fn reset_in_flight_stats(&self) {
        self.instrumentation.in_flight_stats_reset();
        self.stats.reset();
    }

    pub fn batches_in_flight(&self) -> usize {
        self.stats.batches_in_flight.load(Ordering::SeqCst)
    }
}

impl Logger for StreamState {
    fn debug(&self, args: Arguments) {
        self.logger.debug(args);
    }

    fn info(&self, args: Arguments) {
        self.logger.info(args);
    }
    fn warn(&self, args: Arguments) {
        self.logger.warn(args);
    }

    fn error(&self, args: Arguments) {
        self.logger.error(args);
    }
}

#[derive(Debug, Default)]
pub struct StreamStats {
    batches_in_flight: AtomicUsize,
    events_in_flight: AtomicUsize,
    bytes_in_flight: AtomicUsize,
    uncommitted_batches: AtomicUsize,
    uncommitted_events: AtomicUsize,
}

impl StreamStats {
    pub fn reset(&self) {
        self.batches_in_flight.store(0, Ordering::SeqCst);
        self.events_in_flight.store(0, Ordering::SeqCst);
        self.bytes_in_flight.store(0, Ordering::SeqCst);
        self.uncommitted_batches.store(0, Ordering::SeqCst);
        self.uncommitted_events.store(0, Ordering::SeqCst);
    }

    pub fn is_a_warning(&self) -> bool {
        self.batches_in_flight.load(Ordering::SeqCst) != 0
            || self.events_in_flight.load(Ordering::SeqCst) != 0
            || self.bytes_in_flight.load(Ordering::SeqCst) != 0
            || self.uncommitted_batches.load(Ordering::SeqCst) != 0
            || self.uncommitted_events.load(Ordering::SeqCst) != 0
    }
}
