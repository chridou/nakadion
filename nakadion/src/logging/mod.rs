use std::fmt::Arguments;
use std::sync::Arc;

use crate::nakadi_types::model::{
    event_type::EventTypeName,
    partition::PartitionId,
    subscription::{StreamId, SubscriptionId},
};

//pub use logging_internal::*;

#[derive(Clone)]
pub struct Logger {
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

    pub fn debug(&self, args: Arguments) {
        self.logging_adapter.debug(&self.context, args);
    }
    pub fn info(&self, args: Arguments) {
        self.logging_adapter.info(&self.context, args);
    }
    pub fn warn(&self, args: Arguments) {
        self.logging_adapter.warn(&self.context, args);
    }

    pub fn error(&self, args: Arguments) {
        self.logging_adapter.error(&self.context, args);
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

pub struct PrintLogger;

impl LoggingAdapter for PrintLogger {
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

/*
#[cfg(all(not(feature = "log"), not(feature = "slog")))]
mod logging_internal {
    use super::*;
    use std::fmt::Arguments;

    struct LogsImpl;

    impl Logs for LogsImpl {}

    pub fn init_logger() {}
}

#[cfg(all(feature = "log", feature = "slog"))]
mod logging_internal {
    use std::fmt::Arguments;

    use super::*;

    struct LogsImpl;

    impl Logs for LogsImpl {}

    pub fn init_logger() {}
}

#[cfg(all(feature = "log", not(feature = "slog")))]
mod logging_internal {
    use std::fmt::Arguments;

    use super::*;

    pub fn init_logger() {}
}

#[cfg(all(not(feature = "log"), feature = "slog"))]
mod logging_internal {
    use std::fmt::Arguments;

    use super::*;
    use slog::Logger;

    pub fn init_logger() {}

    use crate::nakadi_types::model::{
        event_type::EventTypeName,
        partition::PartitionId,
        subscription::{StreamId, SubscriptionId},
    };
}
*/
