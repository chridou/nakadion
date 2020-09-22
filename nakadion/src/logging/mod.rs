use std::fmt::{self, Arguments};
use std::str::FromStr;
use std::sync::Arc;

use chrono::Utc;

use serde::{Deserialize, Serialize};

use crate::nakadi_types::{
    event_type::EventTypeName,
    partition::PartitionId,
    subscription::{StreamId, SubscriptionId},
    Error,
};

/// Logs the given `Arguments` at different log levels
pub trait Logger: Send + Sync + 'static {
    fn debug(&self, args: Arguments);
    fn info(&self, args: Arguments);
    fn warn(&self, args: Arguments);
    fn error(&self, args: Arguments);
}

impl<T> Logger for T
where
    T: LoggingAdapter,
{
    fn debug(&self, args: Arguments) {
        LoggingAdapter::debug(self, &LoggingContext::default(), args)
    }

    fn info(&self, args: Arguments) {
        LoggingAdapter::info(self, &LoggingContext::default(), args)
    }
    fn warn(&self, args: Arguments) {
        LoggingAdapter::warn(self, &LoggingContext::default(), args)
    }
    fn error(&self, args: Arguments) {
        LoggingAdapter::error(self, &LoggingContext::default(), args)
    }
}

#[derive(Clone)]
pub(crate) struct ContextualLogger {
    context: Arc<LoggingContext>,
    logging_adapter: Arc<dyn LoggingAdapter>,
}

impl ContextualLogger {
    pub fn new(logging_adapter: Arc<dyn LoggingAdapter>) -> Self {
        ContextualLogger {
            context: Arc::new(LoggingContext::default()),
            logging_adapter,
        }
    }

    pub fn subscription_id(&self, subscription_id: SubscriptionId) -> Self {
        let mut context = (*self.context).clone();
        context.subscription_id = Some(subscription_id);
        let logging_adapter = Arc::clone(&self.logging_adapter);

        ContextualLogger {
            context: Arc::new(context),
            logging_adapter,
        }
    }

    pub fn stream_id(&self, stream_id: StreamId) -> Self {
        let mut context = (*self.context).clone();
        context.stream_id = Some(stream_id);
        let logging_adapter = Arc::clone(&self.logging_adapter);

        ContextualLogger {
            context: Arc::new(context),
            logging_adapter,
        }
    }

    pub fn partition_id(&self, partition_id: PartitionId) -> Self {
        let mut context = (*self.context).clone();
        context.partition_id = Some(partition_id);
        let logging_adapter = Arc::clone(&self.logging_adapter);

        ContextualLogger {
            context: Arc::new(context),
            logging_adapter,
        }
    }

    pub fn event_type(&self, event_type: EventTypeName) -> Self {
        let mut context = (*self.context).clone();
        context.event_type = Some(event_type);
        let logging_adapter = Arc::clone(&self.logging_adapter);

        ContextualLogger {
            context: Arc::new(context),
            logging_adapter,
        }
    }
}

impl Logger for ContextualLogger {
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
pub struct StdOutLoggingAdapter(LogConfig);

impl StdOutLoggingAdapter {
    pub fn new(config: LogConfig) -> Self {
        Self(config)
    }
}

impl Default for StdOutLoggingAdapter {
    fn default() -> Self {
        Self::new(LogConfig::default())
    }
}

impl LoggingAdapter for StdOutLoggingAdapter {
    fn debug(&self, context: &LoggingContext, args: Arguments) {
        if self.0.debug_enabled {
            if !self.0.log_debug_as_info {
                println!(
                    "[{}][DEBUG]{} {}",
                    Utc::now(),
                    context.create_display(&self.0),
                    args
                );
            } else {
                println!(
                    "[{}][INFO][DBG]{} {}",
                    Utc::now(),
                    context.create_display(&self.0),
                    args
                );
            }
        }
    }

    fn info(&self, context: &LoggingContext, args: Arguments) {
        println!(
            "[{}][INFO]{} {}",
            Utc::now(),
            context.create_display(&self.0),
            args
        );
    }

    fn warn(&self, context: &LoggingContext, args: Arguments) {
        println!(
            "[{}][WARN]{} {}",
            Utc::now(),
            context.create_display(&self.0),
            args
        );
    }
    fn error(&self, context: &LoggingContext, args: Arguments) {
        println!(
            "[{}][ERROR]{} {}",
            Utc::now(),
            context.create_display(&self.0),
            args
        );
    }
}

/// Logs to stderr
///
/// This does not use the tokio version. It blocks the current thread.
#[derive(Clone)]
pub struct StdErrLoggingAdapter(LogConfig);

impl StdErrLoggingAdapter {
    pub fn new(config: LogConfig) -> Self {
        Self(config)
    }
}

impl Default for StdErrLoggingAdapter {
    fn default() -> Self {
        Self::new(LogConfig::default())
    }
}

impl LoggingAdapter for StdErrLoggingAdapter {
    fn debug(&self, context: &LoggingContext, args: Arguments) {
        if self.0.debug_enabled {
            if !self.0.log_debug_as_info {
                eprintln!(
                    "[{}][DEBUG]{} {}",
                    Utc::now(),
                    context.create_display(&self.0),
                    args
                );
            } else {
                eprintln!(
                    "[{}][INFO][DBG]{} {}",
                    Utc::now(),
                    context.create_display(&self.0),
                    args
                );
            }
        }
    }

    fn info(&self, context: &LoggingContext, args: Arguments) {
        eprintln!(
            "[{}][INFO]{} {}",
            Utc::now(),
            context.create_display(&self.0),
            args
        );
    }

    fn warn(&self, context: &LoggingContext, args: Arguments) {
        eprintln!(
            "[{}][WARN]{} {}",
            Utc::now(),
            context.create_display(&self.0),
            args
        );
    }
    fn error(&self, context: &LoggingContext, args: Arguments) {
        eprintln!(
            "[{}][ERROR]{} {}",
            Utc::now(),
            context.create_display(&self.0),
            args
        );
    }
}

/// Does no logging at all
#[derive(Clone, Copy)]
pub struct DevNullLoggingAdapter;

impl LoggingAdapter for DevNullLoggingAdapter {
    fn debug(&self, _context: &LoggingContext, _args: Arguments) {}
    fn info(&self, _context: &LoggingContext, _args: Arguments) {}
    fn warn(&self, _context: &LoggingContext, _args: Arguments) {}
    fn error(&self, _context: &LoggingContext, _args: Arguments) {}
}

/// Contextual data passed to a logger to be displayed along with a log message
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct LoggingContext {
    pub subscription_id: Option<SubscriptionId>,
    pub stream_id: Option<StreamId>,
    pub event_type: Option<EventTypeName>,
    pub partition_id: Option<PartitionId>,
}

/// Configures which contextual data should be made available with a log message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LogConfig {
    /// Add the `SubscriptionId` to the log message
    pub show_subscription_id: bool,
    /// Add the `StreamId` to the log message
    pub show_stream_id: bool,
    /// Add the `EventType` to the log message
    pub show_event_type: bool,
    /// Add the `PartitionId` to the log message
    pub show_partition_id: bool,
    /// Enable logging ad debug level
    pub debug_enabled: bool,
    /// If debug logging is enabled, log debug messages as info.
    ///
    /// This can be helpful if debug logging is disabled at compile time (e.g. slog)
    pub log_debug_as_info: bool,
}

impl LogConfig {
    env_ctors!(no_fill);
    fn fill_from_env_prefixed_internal<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), Error> {
        let level = LogDetailLevel::try_from_env_prefixed(prefix.as_ref())?.unwrap_or_default();

        match level {
            LogDetailLevel::Minimal => *self = Self::minimal(),
            LogDetailLevel::Low => *self = Self::low(),
            LogDetailLevel::Medium => *self = Self::medium(),
            LogDetailLevel::High => *self = Self::high(),
            LogDetailLevel::Debug => *self = Self::debug(),
        }

        if let Some(debug_enabled) = DebugLoggingEnabled::try_from_env_prefixed(prefix.as_ref())? {
            self.debug_enabled = debug_enabled.into();
        }

        if let Some(log_debug_as_info) = LogDebugAsInfo::try_from_env_prefixed(prefix.as_ref())? {
            self.log_debug_as_info = log_debug_as_info.into();
        }

        Ok(())
    }

    /// Only display the stream id
    pub fn minimal() -> Self {
        Self {
            show_subscription_id: false,
            show_stream_id: true,
            show_event_type: false,
            show_partition_id: false,
            debug_enabled: false,
            log_debug_as_info: false,
        }
    }

    /// Only display the stream id and the partition id
    pub fn low() -> Self {
        Self {
            show_subscription_id: false,
            show_stream_id: true,
            show_event_type: false,
            show_partition_id: true,
            debug_enabled: false,
            log_debug_as_info: false,
        }
    }

    /// Only display the stream id, the event type and the partition id
    pub fn medium() -> Self {
        Self {
            show_subscription_id: false,
            show_stream_id: true,
            show_event_type: true,
            show_partition_id: true,
            debug_enabled: false,
            log_debug_as_info: false,
        }
    }

    /// Only display the subscription id, the stream id, the event type and the partition id
    pub fn high() -> Self {
        Self {
            show_subscription_id: true,
            show_stream_id: true,
            show_event_type: true,
            show_partition_id: true,
            debug_enabled: false,
            log_debug_as_info: false,
        }
    }

    /// Display everything and enable debug logging
    pub fn debug() -> Self {
        Self {
            show_subscription_id: true,
            show_stream_id: true,
            show_event_type: true,
            show_partition_id: true,
            debug_enabled: true,
            log_debug_as_info: false,
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self::medium()
    }
}

new_type! {
    #[doc="If `true` debug logging is enabled.\n\n\
    The default is `false`\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct DebugLoggingEnabled(bool, env="DEBUG_LOGGING_ENABLED");
}
impl Default for DebugLoggingEnabled {
    fn default() -> Self {
        false.into()
    }
}

new_type! {
    #[doc="If `true` log debug messages as info messages.\n\n\
    The default is `false`\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct LogDebugAsInfo(bool, env="LOG_DEBUG_AS_INFO");
}
impl Default for LogDebugAsInfo {
    fn default() -> Self {
        false.into()
    }
}

/// Defines the level of context being logged
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum LogDetailLevel {
    Minimal,
    Low,
    Medium,
    High,
    Debug,
}

impl LogDetailLevel {
    env_funs!("LOG_DETAIL_LEVEL");
}

impl Default for LogDetailLevel {
    fn default() -> Self {
        LogDetailLevel::Medium
    }
}

impl FromStr for LogDetailLevel {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "minimal" => Ok(LogDetailLevel::Minimal),
            "low" => Ok(LogDetailLevel::Low),
            "medium" => Ok(LogDetailLevel::Medium),
            "high" => Ok(LogDetailLevel::High),
            "debug" => Ok(LogDetailLevel::Debug),
            s => Err(Error::new(format!("{} is not a valid LogDetailLevel", s))),
        }
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
        write!(f, "]")?;
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
    pub struct SlogLoggingAdapter {
        logger: Logger,
        config: LogConfig,
    }

    impl SlogLoggingAdapter {
        pub fn new(logger: Logger) -> Self {
            SlogLoggingAdapter {
                logger,
                config: LogConfig::low(),
            }
        }

        pub fn new_with_config(logger: Logger, config: LogConfig) -> Self {
            SlogLoggingAdapter { logger, config }
        }
    }

    impl LoggingAdapter for SlogLoggingAdapter {
        fn debug(&self, context: &LoggingContext, args: Arguments) {
            if self.config.debug_enabled {
                let ctx_display = context.create_display(&self.config);
                let kvs = o!("subscription" => value(context.subscription_id.as_ref()),
            "stream" => value(context.stream_id.as_ref()),
            "event_type" => value(context.event_type.as_ref()),
            "partition" => value(context.partition_id.as_ref()));

                if !self.config.log_debug_as_info {
                    debug!(&self.logger, "{} {}", ctx_display, args; kvs)
                } else {
                    info!(&self.logger, "[DBG]{} {}", ctx_display, args; kvs)
                }
            }
        }

        fn info(&self, context: &LoggingContext, args: Arguments) {
            let ctx_display = context.create_display(&self.config);
            let kvs = o!("subscription" => value(context.subscription_id.as_ref()),
            "stream" => value(context.stream_id.as_ref()),
            "event_type" => value(context.event_type.as_ref()),
            "partition" => value(context.partition_id.as_ref()));
            info!(&self.logger, "{} {}", ctx_display, args; kvs)
        }

        fn warn(&self, context: &LoggingContext, args: Arguments) {
            let ctx_display = context.create_display(&self.config);
            let kvs = o!("subscription" => value(context.subscription_id.as_ref()),
            "stream" => value(context.stream_id.as_ref()),
            "event_type" => value(context.event_type.as_ref()),
            "partition" => value(context.partition_id.as_ref()));
            warn!(&self.logger, "{} {}", ctx_display, args; kvs)
        }

        fn error(&self, context: &LoggingContext, args: Arguments) {
            let ctx_display = context.create_display(&self.config);
            let kvs = o!("subscription" => value(context.subscription_id.as_ref()),
            "stream" => value(context.stream_id.as_ref()),
            "event_type" => value(context.event_type.as_ref()),
            "partition" => value(context.partition_id.as_ref()));
            error!(&self.logger, "{} {}", ctx_display, args; kvs)
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
    pub struct LogLoggingAdapter(LogConfig);

    impl LogLoggingAdapter {
        pub fn new(config: LogConfig) -> Self {
            Self(config)
        }
    }

    impl Default for LogLoggingAdapter {
        fn default() -> Self {
            Self::new(LogConfig::default())
        }
    }

    impl LoggingAdapter for LogLoggingAdapter {
        fn debug(&self, context: &LoggingContext, args: Arguments) {
            if self.0.debug_enabled {
                if !self.0.log_debug_as_info {
                    debug!("{} {}", context.create_display(&self.0), args);
                } else {
                    info!("[DBG]{} {}", context.create_display(&self.0), args);
                }
            }
        }
        fn info(&self, context: &LoggingContext, args: Arguments) {
            info!("{} {}", context.create_display(&self.0), args);
        }
        fn warn(&self, context: &LoggingContext, args: Arguments) {
            warn!("{} {}", context.create_display(&self.0), args);
        }
        fn error(&self, context: &LoggingContext, args: Arguments) {
            error!("{} {}", context.create_display(&self.0), args);
        }
    }
}
