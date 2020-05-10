//! Internals of the `Consumer`
use std::fmt::Arguments;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Weak,
};

use crate::consumer::{Config, ConsumerError, Instrumentation};
use crate::logging::{ContextualLogger, Logger};
use crate::nakadi_types::subscription::{StreamId, StreamParameters, SubscriptionId};

//pub mod committer;
pub mod controller;
pub mod dispatcher;
pub mod worker;

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
/// This struct is valid for the whole lifetime of  stream.
#[derive(Clone)]
pub(crate) struct StreamState {
    stream_id: StreamId,
    config: Arc<Config>,
    is_cancelled: Arc<AtomicBool>,
    is_globally_cancelled: Weak<AtomicBool>,
    logger: ContextualLogger,
    instrumentation: Instrumentation,
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

/// A result that contains data that is (mostly) present in both the success and the error case
pub(crate) type EnrichedResult<T> = Result<EnrichedOk<T>, EnrichedErr>;

/// Additional data returned with a success
pub(crate) struct EnrichedOk<T> {
    /// The number of batches that have been processed.
    ///
    /// This is used to correct the "in-flight" metrics since they are
    /// "delta based".
    pub processed_batches: usize,
    pub payload: T,
}

impl<T> EnrichedOk<T> {
    pub fn new(payload: T, processed_batches: usize) -> Self {
        Self {
            payload,
            processed_batches,
        }
    }

    pub fn map<F, O>(self, f: F) -> EnrichedOk<O>
    where
        F: FnOnce(T) -> O,
    {
        EnrichedOk {
            payload: f(self.payload),
            processed_batches: self.processed_batches,
        }
    }
}

/// An Error with additional data
pub(crate) struct EnrichedErr {
    /// The number of batches that have been processed.
    ///
    /// This is used to correct the "in-flight" metrics since they are
    /// "delta based".
    ///
    /// This can not be added on "hard errors" like spawn failures etc. where
    /// we loose state.
    pub processed_batches: Option<usize>,
    pub err: ConsumerError,
}

impl EnrichedErr {
    pub fn new<E: Into<ConsumerError>>(err: E, processed_batches: usize) -> Self {
        Self {
            err: err.into(),
            processed_batches: Some(processed_batches),
        }
    }

    /// Convenience ctor for cases where we lost state
    pub fn no_data<E: Into<ConsumerError>>(err: E) -> Self {
        Self {
            err: err.into(),
            processed_batches: None,
        }
    }
}
