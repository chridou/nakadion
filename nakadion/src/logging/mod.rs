use std::fmt;
use std::fmt::Arguments;
use std::sync::Arc;

use crate::nakadi_types::{
    event_type::EventTypeName,
    partition::PartitionId,
    subscription::{StreamId, SubscriptionId},
};

pub trait Logs {
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
    #[cfg(feature = "debug-mode")]
    fn debug(&self, args: Arguments) {
        self.logging_adapter.debug(&self.context, args);
    }

    #[cfg(not(feature = "debug-mode"))]
    fn debug(&self, _args: Arguments) {}

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

/// An adapter for pluggable logging.
///
/// Implementors can be used by the `Consumer`
pub trait LoggingAdapter: Send + Sync + 'static {
    fn debug(&self, context: &LoggingContext, args: Arguments);
    fn info(&self, context: &LoggingContext, args: Arguments);
    fn warn(&self, context: &LoggingContext, args: Arguments);
    fn error(&self, context: &LoggingContext, args: Arguments);
}

/// Logs to stdout
///
/// This does not use the tokio version. It blocks the current thread.
#[derive(Clone)]
pub struct StdOutLogger(LogConfig);

impl StdOutLogger {
    pub fn new(config: LogConfig) -> Self {
        Self(config)
    }
}

impl Default for StdOutLogger {
    fn default() -> Self {
        Self::new(LogConfig::default())
    }
}

impl LoggingAdapter for StdOutLogger {
    fn debug(&self, context: &LoggingContext, args: Arguments) {
        println!("[DEBUG]{}{}", context.create_display(&self.0), args);
    }
    fn info(&self, context: &LoggingContext, args: Arguments) {
        println!("[INFO]{}{}", context.create_display(&self.0), args);
    }

    fn warn(&self, context: &LoggingContext, args: Arguments) {
        println!("[WARN]{}{}", context.create_display(&self.0), args);
    }
    fn error(&self, context: &LoggingContext, args: Arguments) {
        println!("[ERROR]{}{}", context.create_display(&self.0), args);
    }
}

/// Logs to stderr
///
/// This does not use the tokio version. It blocks the current thread.
#[derive(Clone)]
pub struct StdErrLogger(LogConfig);

impl StdErrLogger {
    pub fn new(config: LogConfig) -> Self {
        Self(config)
    }
}

impl Default for StdErrLogger {
    fn default() -> Self {
        Self::new(LogConfig::default())
    }
}

impl LoggingAdapter for StdErrLogger {
    fn debug(&self, context: &LoggingContext, args: Arguments) {
        eprintln!("[DEBUG]{}{}", context.create_display(&self.0), args);
    }

    fn info(&self, context: &LoggingContext, args: Arguments) {
        eprintln!("[INFO]{}{}", context.create_display(&self.0), args);
    }

    fn warn(&self, context: &LoggingContext, args: Arguments) {
        eprintln!("[WARN]{}{}", context.create_display(&self.0), args);
    }
    fn error(&self, context: &LoggingContext, args: Arguments) {
        eprintln!("[ERROR]{}{}", context.create_display(&self.0), args);
    }
}

/// Does no logging at all
#[derive(Clone, Copy)]
pub struct DevNullLogger;

impl LoggingAdapter for DevNullLogger {
    fn debug(&self, _context: &LoggingContext, _args: Arguments) {}
    fn info(&self, _context: &LoggingContext, _args: Arguments) {}
    fn warn(&self, _context: &LoggingContext, _args: Arguments) {}
    fn error(&self, _context: &LoggingContext, _args: Arguments) {}
}

/// Contextual data passed to a logger to be displayed along with a log message
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct LoggingContext {
    subscription_id: Option<SubscriptionId>,
    stream_id: Option<StreamId>,
    event_type: Option<EventTypeName>,
    partition_id: Option<PartitionId>,
}

/// Configures which contextual data should be made available with a log message
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LogConfig {
    pub show_subscription_id: bool,
    pub show_stream_id: bool,
    pub show_event_type: bool,
    pub show_partition_id: bool,
}

impl LogConfig {
    /// Only display the stream id and the partition id
    pub fn short() -> Self {
        Self {
            show_subscription_id: false,
            show_stream_id: true,
            show_event_type: false,
            show_partition_id: true,
        }
    }

    /// Only display the stream id, the event type and the partition id
    pub fn medium() -> Self {
        Self {
            show_subscription_id: false,
            show_stream_id: true,
            show_event_type: true,
            show_partition_id: true,
        }
    }

    /// Only display the subscription id, the stream id, the event type and the partition id
    pub fn long() -> Self {
        Self {
            show_subscription_id: true,
            show_stream_id: true,
            show_event_type: true,
            show_partition_id: true,
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self::short()
    }
}

impl LoggingContext {
    /// Creates a `Display` based on the given `LogConfig`
    pub fn create_display<'a>(&'a self, config: &'a LogConfig) -> ContextDisplay<'a> {
        ContextDisplay {
            context: self,
            config,
        }
    }
}

pub struct ContextDisplay<'a> {
    config: &'a LogConfig,
    context: &'a LoggingContext,
}

impl<'a> ContextDisplay<'a> {
    fn format(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let mut n = self.item_count();
        if n == 0 {
            return Ok(());
        }

        write!(f, "[")?;
        if self.config.show_subscription_id {
            if let Some(subscription_id) = self.context.subscription_id {
                write!(f, "SUB:{}", subscription_id)?;
                add_delimiter(&mut n, f)?;
            }
        }
        if self.config.show_stream_id {
            if let Some(stream_id) = self.context.stream_id {
                write!(f, "STR:{}", stream_id)?;
                add_delimiter(&mut n, f)?;
            }
        }
        if self.config.show_event_type {
            if let Some(ref event_type) = self.context.event_type {
                write!(f, "E:{}", event_type)?;
                add_delimiter(&mut n, f)?;
            }
        }
        if self.config.show_partition_id {
            if let Some(ref partition_id) = self.context.partition_id {
                write!(f, "P:{}", partition_id)?;
                add_delimiter(&mut n, f)?;
            }
        }
        Ok(())
    }

    fn item_count(&self) -> usize {
        let mut n = 0;
        if self.config.show_subscription_id && self.context.subscription_id.is_some() {
            n += 1;
        }

        if self.config.show_stream_id && self.context.stream_id.is_some() {
            n += 1;
        }
        if self.config.show_event_type && self.context.event_type.is_some() {
            n += 1;
        }
        if self.config.show_partition_id && self.context.partition_id.is_some() {
            n += 1;
        }
        n
    }
}

fn add_delimiter(n: &mut usize, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    (*n) -= 1;
    if *n == 0 {
        write!(f, "] ")?;
    } else {
        write!(f, ";")?;
    }
    Ok(())
}

impl<'a> fmt::Display for ContextDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.format(f)
    }
}

#[cfg(feature = "slog")]
pub mod slog_adapter {
    use std::fmt;

    use super::*;
    use slog::{debug, error, info, o, warn, Logger};

    /// A logger based on `slog`
    #[derive(Clone)]
    pub struct SlogLogger {
        logger: Logger,
        config: LogConfig,
    }

    impl SlogLogger {
        pub fn new(logger: Logger) -> Self {
            SlogLogger {
                logger,
                config: LogConfig::short(),
            }
        }

        pub fn new_with_config(logger: Logger, config: LogConfig) -> Self {
            SlogLogger { logger, config }
        }
    }

    impl LoggingAdapter for SlogLogger {
        fn debug(&self, context: &LoggingContext, args: Arguments) {
            let ctx_display = context.create_display(&self.config);
            let kvs = o!("subscription" => value(context.subscription_id.as_ref()),
            "stream" => value(context.stream_id.as_ref()),
            "event_type" => value(context.event_type.as_ref()),
            "partition" => value(context.partition_id.as_ref()));

            debug!(&self.logger, "{}{}", ctx_display, args; kvs)
        }

        fn info(&self, context: &LoggingContext, args: Arguments) {
            let ctx_display = context.create_display(&self.config);
            let kvs = o!("subscription" => value(context.subscription_id.as_ref()),
            "stream" => value(context.stream_id.as_ref()),
            "event_type" => value(context.event_type.as_ref()),
            "partition" => value(context.partition_id.as_ref()));
            info!(&self.logger, "{}{}", ctx_display, args; kvs)
        }

        fn warn(&self, context: &LoggingContext, args: Arguments) {
            let ctx_display = context.create_display(&self.config);
            let kvs = o!("subscription" => value(context.subscription_id.as_ref()),
            "stream" => value(context.stream_id.as_ref()),
            "event_type" => value(context.event_type.as_ref()),
            "partition" => value(context.partition_id.as_ref()));
            warn!(&self.logger, "{}{}", ctx_display, args; kvs)
        }

        fn error(&self, context: &LoggingContext, args: Arguments) {
            let ctx_display = context.create_display(&self.config);
            let kvs = o!("subscription" => value(context.subscription_id.as_ref()),
            "stream" => value(context.stream_id.as_ref()),
            "event_type" => value(context.event_type.as_ref()),
            "partition" => value(context.partition_id.as_ref()));
            error!(&self.logger, "{}{}", ctx_display, args; kvs)
        }
    }

    fn value<V: fmt::Display>(value: Option<&V>) -> String {
        value
            .map(|s| s.to_string())
            .unwrap_or_else(|| "none".to_owned())
    }
}

#[cfg(feature = "log")]
pub mod log_adapter {
    use std::fmt::Arguments;

    use super::*;
    use log::{debug, error, info, warn};

    /// A logger based on `log`
    #[derive(Clone)]
    pub struct LogLogger(LogConfig);

    impl LogLogger {
        pub fn new(config: LogConfig) -> Self {
            Self(config)
        }
    }

    impl Default for LogLogger {
        fn default() -> Self {
            Self::new(LogConfig::default())
        }
    }

    impl LoggingAdapter for LogLogger {
        fn debug(&self, context: &LoggingContext, args: Arguments) {
            debug!("[DEBUG]{}{}", context.create_display(&self.0), args);
        }
        fn info(&self, context: &LoggingContext, args: Arguments) {
            info!("[INFO]{}{}", context.create_display(&self.0), args);
        }
        fn warn(&self, context: &LoggingContext, args: Arguments) {
            warn!("[WARN]{}{}", context.create_display(&self.0), args);
        }
        fn error(&self, context: &LoggingContext, args: Arguments) {
            error!("[ERROR]{}{}", context.create_display(&self.0), args);
        }
    }
}
