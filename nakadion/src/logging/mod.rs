use std::fmt::Arguments;
use std::sync::Arc;

use crate::nakadi_types::model::{
    event_type::EventTypeName,
    partition::PartitionId,
    subscription::{StreamId, SubscriptionId},
};

pub(crate) trait Logs {
    fn debug(&self, args: Arguments);
    fn info(&self, args: Arguments);
    fn warn(&self, args: Arguments);
    fn error(&self, args: Arguments);
}

#[derive(Clone)]
pub(crate) struct Logger {
    context: Arc<LoggingContext>,
    logging_adapter: Arc<dyn LoggingAdapter>,
}

impl Logger {
    pub fn new(logging_adapter: Arc<dyn LoggingAdapter>) -> Self {
        Logger {
            context: Arc::new(LoggingContext::default()),
            logging_adapter,
        }
    }

    pub fn with_subscription_id(&self, subscription_id: SubscriptionId) -> Self {
        let mut context = (*self.context).clone();
        context.subscription_id = Some(subscription_id);
        let logging_adapter = Arc::clone(&self.logging_adapter);

        Logger {
            context: Arc::new(context),
            logging_adapter,
        }
    }

    pub fn with_stream_id(&self, stream_id: StreamId) -> Self {
        let mut context = (*self.context).clone();
        context.stream_id = Some(stream_id);
        let logging_adapter = Arc::clone(&self.logging_adapter);

        Logger {
            context: Arc::new(context),
            logging_adapter,
        }
    }

    pub fn with_partition_id(&self, partition_id: PartitionId) -> Self {
        let mut context = (*self.context).clone();
        context.partition_id = Some(partition_id);
        let logging_adapter = Arc::clone(&self.logging_adapter);

        Logger {
            context: Arc::new(context),
            logging_adapter,
        }
    }

    pub fn with_event_type(&self, event_type: EventTypeName) -> Self {
        let mut context = (*self.context).clone();
        context.event_type = Some(event_type);
        let logging_adapter = Arc::clone(&self.logging_adapter);

        Logger {
            context: Arc::new(context),
            logging_adapter,
        }
    }
}

impl Logs for Logger {
    fn debug(&self, args: Arguments) {
        self.logging_adapter.debug(&self.context, args);
    }
    fn info(&self, args: Arguments) {
        self.logging_adapter.info(&self.context, args);
    }
    fn warn(&self, args: Arguments) {
        self.logging_adapter.warn(&self.context, args);
    }

    fn error(&self, args: Arguments) {
        self.logging_adapter.error(&self.context, args);
    }
}

#[derive(Default, Debug, Clone)]
pub struct LoggingContext {
    subscription_id: Option<SubscriptionId>,
    stream_id: Option<StreamId>,
    event_type: Option<EventTypeName>,
    partition_id: Option<PartitionId>,
}

pub trait LoggingAdapter: Send + Sync + 'static {
    fn debug(&self, context: &LoggingContext, args: Arguments);
    fn info(&self, context: &LoggingContext, args: Arguments);
    fn warn(&self, context: &LoggingContext, args: Arguments);
    fn error(&self, context: &LoggingContext, args: Arguments);
}

#[derive(Clone, Copy)]
pub struct StdLogger;

impl StdLogger {
    pub fn new() -> Self {
        StdLogger
    }
}

impl LoggingAdapter for StdLogger {
    fn debug(&self, context: &LoggingContext, args: Arguments) {
        println!("[DEBUG] {}", args);
    }
    fn info(&self, context: &LoggingContext, args: Arguments) {
        println!("[INFO] {}", args);
    }

    fn warn(&self, context: &LoggingContext, args: Arguments) {
        println!("[WARN] {}", args);
    }
    fn error(&self, context: &LoggingContext, args: Arguments) {
        println!("[ERROR] {}", args);
    }
}

#[derive(Clone, Copy)]
pub struct DevNullLogger;

impl LoggingAdapter for DevNullLogger {
    fn debug(&self, _context: &LoggingContext, _args: Arguments) {}
    fn info(&self, _context: &LoggingContext, _args: Arguments) {}
    fn warn(&self, _context: &LoggingContext, _args: Arguments) {}
    fn error(&self, _context: &LoggingContext, _args: Arguments) {}
}

#[cfg(feature = "slog")]
pub mod slog_adapter {
    use std::fmt::Arguments;

    use super::*;
    use slog::{debug, error, info, warn, Logger};

    #[derive(Clone)]
    pub struct SlogLogger {
        logger: Logger,
    }

    impl SlogLogger {
        pub fn new(logger: Logger) -> Self {
            SlogLogger { logger }
        }
    }

    impl LoggingAdapter for SlogLogger {
        fn debug(&self, context: &LoggingContext, args: Arguments) {
            debug!(&self.logger, "{}", args)
        }

        fn info(&self, context: &LoggingContext, args: Arguments) {
            info!(&self.logger, "{}", args)
        }

        fn warn(&self, context: &LoggingContext, args: Arguments) {
            warn!(&self.logger, "{}", args)
        }

        fn error(&self, context: &LoggingContext, args: Arguments) {
            error!(&self.logger, "{}", args)
        }
    }
}

#[cfg(feature = "log")]
pub mod log_adapter {
    use std::fmt::Arguments;

    use super::*;
    use log::{debug, error, info, warn};

    #[derive(Clone)]
    pub struct LogLogger;

    impl LogLogger {
        pub fn new() -> Self {
            LogLogger
        }
    }

    impl LoggingAdapter for LogLogger {
        fn debug(&self, context: &LoggingContext, args: Arguments) {
            debug!("{}", args)
        }

        fn info(&self, context: &LoggingContext, args: Arguments) {
            info!("{}", args)
        }

        fn warn(&self, context: &LoggingContext, args: Arguments) {
            warn!("{}", args)
        }

        fn error(&self, context: &LoggingContext, args: Arguments) {
            error!("{}", args)
        }
    }
}
