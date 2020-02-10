use std::error::Error;
use std::fmt;

/// Always leads to Nakadion shutting down
#[derive(Debug)]
pub struct ConsumerError {
    message: Option<String>,
    kind: ConsumerErrorKind,
    source: Option<Box<dyn Error + Send + 'static>>,
}

impl ConsumerError {
    pub fn new(kind: ConsumerErrorKind) -> Self {
        Self {
            message: None,
            kind,
            source: None,
        }
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

    pub fn with_source<E: Error + Send + 'static>(mut self, source: E) -> Self {
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
}

impl Error for ConsumerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|e| &**e as &(dyn Error + 'static))
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

impl From<nakadi_types::GenericError> for ConsumerError {
    fn from(err: nakadi_types::GenericError) -> Self {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConsumerErrorKind {
    SubscriptionNotFound,
    Internal,
    HandlerAbort,
    UserAbort,
    HandlerFactory,
    InvalidBatch,
    Other,
}

impl fmt::Display for ConsumerErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsumerErrorKind::SubscriptionNotFound => write!(f, "subscription not found")?,
            ConsumerErrorKind::Internal => write!(f, "internal")?,
            ConsumerErrorKind::UserAbort => write!(f, "user initiated")?,
            ConsumerErrorKind::HandlerAbort => write!(f, "handler initiated")?,
            ConsumerErrorKind::HandlerFactory => write!(f, "handler factory")?,
            ConsumerErrorKind::InvalidBatch => write!(f, "invalid batch")?,
            ConsumerErrorKind::Other => write!(f, "other")?,
            _ => write!(f, "uncategorized")?,
        }
        Ok(())
    }
}
