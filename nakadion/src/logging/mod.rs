use std::fmt;
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
    #[cfg(feature = "debug-mode")]
    fn debug(&self, args: Arguments) {
        self.logging_adapter.debug(&self.context, args);
    }

    #[cfg(not(feature = "debug-mode"))]
    fn debug(&self, args: Arguments) {}

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

pub trait LoggingAdapter: Send + Sync + 'static {
    fn debug(&self, context: &LoggingContext, args: Arguments);
    fn info(&self, context: &LoggingContext, args: Arguments);
    fn warn(&self, context: &LoggingContext, args: Arguments);
    fn error(&self, context: &LoggingContext, args: Arguments);
}

#[derive(Clone, Copy)]
pub struct StdOutLogger(bool);

impl StdOutLogger {
    pub fn new(long_display: bool) -> Self {
        Self(long_display)
    }

    pub fn long_display() -> Self {
        Self(true)
    }

    pub fn short_display() -> Self {
        Self(false)
    }
}

impl Default for StdOutLogger {
    fn default() -> Self {
        Self::short_display()
    }
}

impl LoggingAdapter for StdOutLogger {
    fn debug(&self, context: &LoggingContext, args: Arguments) {
        println!("[DEBUG]{} {}", context.select_display(self.0), args);
    }
    fn info(&self, context: &LoggingContext, args: Arguments) {
        println!("[INFO]{} {}", context.select_display(self.0), args);
    }

    fn warn(&self, context: &LoggingContext, args: Arguments) {
        println!("[WARN]{} {}", context.select_display(self.0), args);
    }
    fn error(&self, context: &LoggingContext, args: Arguments) {
        println!("[ERROR]{} {}", context.select_display(self.0), args);
    }
}

#[derive(Clone, Copy)]
pub struct StdErrLogger(bool);

impl StdErrLogger {
    pub fn new(long_display: bool) -> Self {
        Self(long_display)
    }

    pub fn long_display() -> Self {
        Self(true)
    }

    pub fn short_display() -> Self {
        Self(false)
    }
}

impl Default for StdErrLogger {
    fn default() -> Self {
        Self::short_display()
    }
}
impl LoggingAdapter for StdErrLogger {
    fn debug(&self, context: &LoggingContext, args: Arguments) {
        eprintln!("[DEBUG]{} {}", context.select_display(self.0), args);
    }

    fn info(&self, context: &LoggingContext, args: Arguments) {
        eprintln!("[INFO]{} {}", context.select_display(self.0), args);
    }

    fn warn(&self, context: &LoggingContext, args: Arguments) {
        eprintln!("[WARN]{} {}", context.select_display(self.0), args);
    }
    fn error(&self, context: &LoggingContext, args: Arguments) {
        eprintln!("[ERROR]{} {}", context.select_display(self.0), args);
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

#[derive(Default, Debug, Clone)]
pub struct LoggingContext {
    subscription_id: Option<SubscriptionId>,
    stream_id: Option<StreamId>,
    event_type: Option<EventTypeName>,
    partition_id: Option<PartitionId>,
}

impl LoggingContext {
    fn select_display(&self, long: bool) -> ContextDisplay {
        ContextDisplay {
            context: self,
            long,
        }
    }

    fn format(&self, f: &mut fmt::Formatter, long: bool) -> Result<(), fmt::Error> {
        let mut n = self.item_count(long);
        if n == 0 {
            return Ok(());
        }

        write!(f, "[")?;
        if long {
            if let Some(subscription_id) = self.subscription_id {
                write!(f, "SUB:{}", subscription_id)?;
                add_delemiter(&mut n, f)?;
            }
        }
        if let Some(stream_id) = self.stream_id {
            write!(f, "STR:{}", stream_id)?;
            add_delemiter(&mut n, f)?;
        }
        if long {
            if let Some(ref event_type) = self.event_type {
                write!(f, "E:{}", event_type)?;
                add_delemiter(&mut n, f)?;
            }
        }
        if let Some(ref partition_id) = self.partition_id {
            write!(f, "P:{}", partition_id)?;
            add_delemiter(&mut n, f)?;
        }

        Ok(())
    }

    fn item_count(&self, long: bool) -> usize {
        let mut n = 0;
        if long && self.subscription_id.is_some() {
            n += 1;
        }

        if self.stream_id.is_some() {
            n += 1;
        }
        if long && self.event_type.is_some() {
            n += 1;
        }
        if self.partition_id.is_some() {
            n += 1;
        }
        n
    }
}

fn add_delemiter(n: &mut usize, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    (*n) -= 1;
    if *n == 0 {
        write!(f, "]")?;
    } else {
        write!(f, ";")?;
    }
    Ok(())
}

struct ContextDisplay<'a> {
    long: bool,
    context: &'a LoggingContext,
}

impl<'a> fmt::Display for ContextDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.context.format(f, self.long)
    }
}

#[cfg(feature = "slog")]
pub mod slog_adapter {
    use std::fmt::Arguments;

    use super::*;
    use slog::{debug, error, info, kv, warn, Logger, OwnedKVList};

    #[derive(Clone)]
    pub struct SlogLogger {
        logger: Logger,
        long_display: Option<bool>,
    }

    impl SlogLogger {
        pub fn new(logger: Logger) -> Self {
            SlogLogger {
                logger,
                long_display: None,
            }
        }

        pub fn long_display(mut self) -> Self {
            self.long_display = Some(true);
            self
        }

        pub fn short_display(mut self) -> Self {
            self.long_display = Some(false);
            self
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
    pub struct LogLogger(bool);

    impl LogLogger {
        pub fn new(long_display: bool) -> Self {
            Self(long_display)
        }
        pub fn long_display() -> Self {
            Self(true)
        }
        pub fn short_display() -> Self {
            Self(false)
        }
    }

    impl LoggingAdapter for LogLogger {
        fn debug(&self, context: &LoggingContext, args: Arguments) {
            debug!("[DEBUG]{} {}", context.select_display(self.0), args);
        }
        fn info(&self, context: &LoggingContext, args: Arguments) {
            info!("[INFO]{} {}", context.select_display(self.0), args);
        }
        fn warn(&self, context: &LoggingContext, args: Arguments) {
            warn!("[WARN]{} {}", context.select_display(self.0), args);
        }
        fn error(&self, context: &LoggingContext, args: Arguments) {
            error!("[ERROR]{} {}", context.select_display(self.0), args);
        }
    }
}
