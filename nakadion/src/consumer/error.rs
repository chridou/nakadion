use std::error::Error as StdError;
use std::fmt;

use crate::components::connector::{ConnectError, ConnectErrorKind};
use crate::nakadi_types::Error;

#[derive(Debug)]
pub enum ConsumerAbort {
    UserInitiated,
    Error(ConsumerError),
}

impl ConsumerAbort {
    pub fn user_initiated() -> Self {
        Self::UserInitiated
    }

    pub fn error<E: Into<ConsumerError>>(err: E) -> Self {
        Self::Error(err.into())
    }

    pub fn is_error(&self) -> bool {
        match self {
            ConsumerAbort::UserInitiated => false,
            _ => true,
        }
    }

    pub fn is_user_abort(&self) -> bool {
        match self {
            ConsumerAbort::UserInitiated => true,
            _ => false,
        }
    }

    pub fn try_into_error(self) -> Result<ConsumerError, Self> {
        match self {
            ConsumerAbort::UserInitiated => Err(self),
            ConsumerAbort::Error(error) => Ok(error),
        }
    }

    pub fn maybe_as_consumer_error(&self) -> Option<&ConsumerError> {
        match self {
            ConsumerAbort::UserInitiated => None,
            ConsumerAbort::Error(ref error) => Some(error),
        }
    }
}

impl fmt::Display for ConsumerAbort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsumerAbort::UserInitiated => write!(f, "user initiated")?,
            ConsumerAbort::Error(ref error) => write!(f, "{}", error)?,
        }
        Ok(())
    }
}

impl StdError for ConsumerAbort {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ConsumerAbort::UserInitiated => None,
            ConsumerAbort::Error(ref error) => error.source(),
        }
    }
}

impl<T> From<T> for ConsumerAbort
where
    T: Into<ConsumerError>,
{
    fn from(err: T) -> Self {
        Self::Error(err.into())
    }
}

impl From<ConnectError> for ConsumerAbort {
    fn from(err: ConnectError) -> Self {
        match err.kind() {
            ConnectErrorKind::Aborted => ConsumerAbort::user_initiated(),
            ConnectErrorKind::SubscriptionNotFound => ConsumerAbort::error(
                ConsumerError::new(ConsumerErrorKind::SubscriptionNotFound).with_source(err),
            ),
            ConnectErrorKind::AccessDenied => ConsumerAbort::error(
                ConsumerError::new(ConsumerErrorKind::AccessDenied).with_source(err),
            ),
            ConnectErrorKind::Unprocessable => ConsumerAbort::error(
                ConsumerError::new(ConsumerErrorKind::ConnectStream).with_source(err),
            ),
            ConnectErrorKind::BadRequest => ConsumerAbort::error(
                ConsumerError::new(ConsumerErrorKind::ConnectStream).with_source(err),
            ),
            ConnectErrorKind::Io => ConsumerAbort::error(
                ConsumerError::new(ConsumerErrorKind::ConnectStream).with_source(err),
            ),
            ConnectErrorKind::Other => ConsumerAbort::error(
                ConsumerError::new(ConsumerErrorKind::ConnectStream).with_source(err),
            ),
            ConnectErrorKind::Conflict => ConsumerAbort::error(
                ConsumerError::new(ConsumerErrorKind::ConnectStream).with_source(err),
            ),
            ConnectErrorKind::NakadiError => {
                ConsumerAbort::error(ConsumerError::new(ConsumerErrorKind::Other).with_source(err))
            }
        }
    }
}

/// Always leads to Nakadion shutting down
#[derive(Debug)]
pub struct ConsumerError {
    message: Option<String>,
    kind: ConsumerErrorKind,
    source: Option<Box<dyn StdError + Send + 'static>>,
}

impl ConsumerError {
    pub fn new(kind: ConsumerErrorKind) -> Self {
        Self {
            message: None,
            kind,
            source: None,
        }
    }

    pub fn internal() -> Self {
        Self::new(ConsumerErrorKind::Internal)
    }

    pub fn other() -> Self {
        Self::new(ConsumerErrorKind::Other)
    }

    pub fn connect_stream() -> Self {
        Self::new(ConsumerErrorKind::ConnectStream)
    }

    pub fn new_with_message<M: fmt::Display>(kind: ConsumerErrorKind, message: M) -> Self {
        Self {
            message: Some(message.to_string()),
            kind,
            source: None,
        }
    }

    pub fn with_message<T: fmt::Display>(mut self, message: T) -> Self {
        self.message = Some(message.to_string());
        self
    }

    pub fn with_source<E: StdError + Send + 'static>(mut self, source: E) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    pub fn with_kind(mut self, kind: ConsumerErrorKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn kind(&self) -> ConsumerErrorKind {
        self.kind
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_ref().map(|m| &**m)
    }

    pub(crate) fn enriched(self, batches_processed: usize) -> crate::internals::EnrichedErr {
        crate::internals::EnrichedErr::new(self, batches_processed)
    }
}

impl StdError for ConsumerError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|e| &**e as &(dyn StdError + 'static))
    }
}

impl fmt::Display for ConsumerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(message) = self.message() {
            write!(f, "{}", message)?;
        } else if let Some(source) = self.source() {
            write!(f, "{}", source)?;
        } else {
            write!(f, "{}", self.kind)?;
        }
        Ok(())
    }
}

impl From<ConsumerErrorKind> for ConsumerError {
    fn from(kind: ConsumerErrorKind) -> Self {
        Self::new(kind)
    }
}

impl From<nakadi_types::Error> for ConsumerError {
    fn from(err: nakadi_types::Error) -> Self {
        Self {
            message: Some(err.into_inner()),
            kind: ConsumerErrorKind::Other,
            source: None,
        }
    }
}

impl From<tokio::task::JoinError> for ConsumerError {
    fn from(err: tokio::task::JoinError) -> Self {
        Self {
            message: None,
            kind: ConsumerErrorKind::Internal,
            source: Some(Box::new(err)),
        }
    }
}

impl From<ConsumerError> for Error {
    fn from(err: ConsumerError) -> Self {
        Self::from_error(err)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConsumerErrorKind {
    SubscriptionNotFound,
    ConnectStream,
    AccessDenied,
    Internal,
    HandlerAbort,
    HandlerFactory,
    InvalidBatch,
    Other,
}

impl fmt::Display for ConsumerErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsumerErrorKind::SubscriptionNotFound => write!(f, "subscription not found")?,
            ConsumerErrorKind::ConnectStream => write!(f, "connect to stream failed")?,
            ConsumerErrorKind::Internal => write!(f, "internal")?,
            ConsumerErrorKind::HandlerAbort => write!(f, "handler initiated")?,
            ConsumerErrorKind::HandlerFactory => write!(f, "handler factory")?,
            ConsumerErrorKind::InvalidBatch => write!(f, "invalid batch")?,
            ConsumerErrorKind::Other => write!(f, "other")?,
            _ => write!(f, "not categorized")?,
        }
        Ok(())
    }
}
