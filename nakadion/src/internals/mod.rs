use std::fmt::Arguments;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use crate::consumer::Config;
use crate::logging::{Logger, Logs};
use crate::nakadi_types::model::subscription::{StreamId, StreamParameters, SubscriptionId};

pub mod committer;
pub mod controller;
pub mod dispatcher;
pub mod worker;

#[derive(Clone)]
pub(crate) struct ConsumerState {
    is_globally_cancelled: Arc<AtomicBool>,
    config: Arc<Config>,
    logger: Logger,
}

impl ConsumerState {
    pub fn new(config: Config, logger: Logger) -> Self {
        let subscription_id = config.subscription_id;
        Self {
            is_globally_cancelled: Arc::new(AtomicBool::new(false)),
            config: Arc::new(config),
            logger: logger.with_subscription_id(subscription_id),
        }
    }

    pub fn stream_state(&self, stream_id: StreamId) -> StreamState {
        StreamState::new(
            stream_id,
            Arc::clone(&self.config),
            Arc::downgrade(&self.is_globally_cancelled),
            self.logger.with_stream_id(stream_id),
        )
    }

    pub fn request_global_cancellation(&self) {
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

    pub fn stream_parameters(&self) -> &StreamParameters {
        &self.config().stream_parameters
    }

    pub fn logger(&self) -> &Logger {
        &self.logger
    }
}

impl Logs for ConsumerState {
    #[cfg(feature = "debug-mode")]
    fn debug(&self, args: Arguments) {
        self.logger.debug(args);
    }

    #[cfg(not(feature = "debug-mode"))]
    fn debug(&self, args: Arguments) {}

    #[cfg(feature = "debug-mode")]
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

#[derive(Clone)]
pub(crate) struct StreamState {
    stream_id: StreamId,
    config: Arc<Config>,
    is_cancelled: Arc<AtomicBool>,
    is_globally_cancelled: Weak<AtomicBool>,
    logger: Logger,
}

impl StreamState {
    pub fn new(
        stream_id: StreamId,
        config: Arc<Config>,
        is_globally_cancelled: Weak<AtomicBool>,
        logger: Logger,
    ) -> Self {
        Self {
            stream_id,
            config,
            is_cancelled: Arc::new(AtomicBool::new(false)),
            is_globally_cancelled,
            logger,
        }
    }

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

    pub fn request_stream_cancellation(&self) {
        self.is_cancelled.store(true, Ordering::SeqCst)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn stream_parameters(&self) -> &StreamParameters {
        &self.config().stream_parameters
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn subscription_id(&self) -> SubscriptionId {
        self.config().subscription_id
    }

    pub fn logger(&self) -> &Logger {
        &self.logger
    }
}

impl Logs for StreamState {
    #[cfg(feature = "debug-mode")]
    fn debug(&self, args: Arguments) {
        self.logger.debug(args);
    }

    #[cfg(not(feature = "debug-mode"))]
    fn debug(&self, args: Arguments) {}

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
