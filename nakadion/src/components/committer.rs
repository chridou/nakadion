use std::error::Error as StdError;
use std::fmt;
use std::time::Instant;

use futures::future::{BoxFuture, FutureExt};
use http::StatusCode;
use tokio::time::timeout;

use crate::api::{NakadiApiError, SubscriptionApi};
use crate::consumer::CommitAttemptTimeoutMillis;
use crate::instrumentation::{Instrumentation, Instruments};
use crate::nakadi_types::{
    model::subscription::{CursorCommitResults, StreamId, SubscriptionCursor, SubscriptionId},
    Error, FlowId,
};

/// Gives a `Connects`
pub trait ProvidesCommitter {
    fn committer(
        &self,
        subscription_id: SubscriptionId,
        stream_id: StreamId,
    ) -> Box<dyn Commits + Send + Sync + 'static>;
}

impl<T> ProvidesCommitter for T
where
    T: SubscriptionApi + Sync + Send + 'static + Clone,
{
    fn committer(
        &self,
        subscription_id: SubscriptionId,
        stream_id: StreamId,
    ) -> Box<dyn Commits + Send + Sync + 'static> {
        Box::new(Committer::new(self.clone(), subscription_id, stream_id))
    }
}

pub type CommitFuture<'a> = BoxFuture<'a, Result<CursorCommitResults, CommitError>>;

/// Can connect to a Nakadi Stream
pub trait Commits {
    /// Commit cursor s to Nakadi.
    fn commit<'a>(&'a self, cursors: &'a [SubscriptionCursor]) -> CommitFuture<'a>;

    /// Return instrumentation.
    ///
    /// Returns `Instrumentation::default()` by default.
    fn instrumentation(&self) -> Instrumentation {
        Instrumentation::default()
    }

    fn set_flow_id(&mut self, flow_id: FlowId);

    fn set_timeout(&mut self, timeout: CommitAttemptTimeoutMillis);

    fn set_instrumentation(&mut self, instrumentation: Instrumentation);

    fn subscription_id(&self) -> SubscriptionId;

    fn stream_id(&self) -> StreamId;
}

pub struct Committer<C> {
    client: C,
    flow_id: Option<FlowId>,
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    instrumentation: Instrumentation,
    timeout: CommitAttemptTimeoutMillis,
}

impl<C> Committer<C>
where
    C: SubscriptionApi + Send + Sync + 'static,
{
    pub fn new(client: C, subscription_id: SubscriptionId, stream_id: StreamId) -> Self {
        Self {
            client,
            flow_id: None,
            subscription_id,
            stream_id,
            instrumentation: Instrumentation::default(),
            timeout: CommitAttemptTimeoutMillis::default(),
        }
    }
}

impl<C> Commits for Committer<C>
where
    C: SubscriptionApi + Send + Sync + 'static,
{
    fn commit<'a>(&'a self, cursors: &'a [SubscriptionCursor]) -> CommitFuture<'a> {
        async move {
            let started = Instant::now();
            match timeout(
                self.timeout.into_duration(),
                self.client.commit_cursors(
                    self.subscription_id,
                    self.stream_id,
                    cursors,
                    self.flow_id.clone().unwrap_or_else(FlowId::random),
                ),
            )
            .await
            {
                Ok(Ok(results)) => {
                    self.instrumentation()
                        .committer_cursors_committed(cursors.len(), started.elapsed());
                    Ok(results)
                }
                Ok(Err(err)) => {
                    self.instrumentation()
                        .committer_cursors_not_committed(cursors.len(), started.elapsed());
                    Err(err.into())
                }
                Err(err) => {
                    self.instrumentation()
                        .committer_cursors_not_committed(cursors.len(), started.elapsed());
                    Err(CommitError::io()
                        .context(format!(
                            "Commit attempt timed out after {:?}",
                            started.elapsed()
                        ))
                        .caused_by(err))
                }
            }
        }
        .boxed()
    }

    fn instrumentation(&self) -> Instrumentation {
        self.instrumentation.clone()
    }

    fn set_flow_id(&mut self, flow_id: FlowId) {
        self.flow_id = Some(flow_id);
    }

    fn set_timeout(&mut self, timeout: CommitAttemptTimeoutMillis) {
        self.timeout = timeout;
    }

    fn set_instrumentation(&mut self, instrumentation: Instrumentation) {
        self.instrumentation = instrumentation;
    }

    fn subscription_id(&self) -> SubscriptionId {
        self.subscription_id
    }

    fn stream_id(&self) -> StreamId {
        self.stream_id
    }
}

/// Error returned on failed commit attempts for a stream
#[derive(Debug)]
pub struct CommitError {
    context: Option<String>,
    kind: CommitErrorKind,
    source: Option<Box<dyn StdError + Send + 'static>>,
}

impl CommitError {
    pub fn new(kind: CommitErrorKind) -> Self {
        Self {
            context: None,
            kind,
            source: None,
        }
    }

    pub fn not_found() -> Self {
        Self::new(CommitErrorKind::SubscriptionNotFound)
    }
    pub fn access_denied() -> Self {
        Self::new(CommitErrorKind::AccessDenied)
    }
    pub fn unprocessable() -> Self {
        Self::new(CommitErrorKind::Unprocessable)
    }
    pub fn bad_request() -> Self {
        Self::new(CommitErrorKind::BadRequest)
    }
    pub fn io() -> Self {
        Self::new(CommitErrorKind::Io)
    }
    pub fn other() -> Self {
        Self::new(CommitErrorKind::Other)
    }

    pub fn context<T: Into<String>>(mut self, context: T) -> Self {
        self.context = Some(context.into());
        self
    }

    pub fn caused_by<E: StdError + Send + 'static>(mut self, source: E) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    pub fn kind(&self) -> CommitErrorKind {
        self.kind
    }

    pub fn is_recoverable(&self) -> bool {
        match self.kind {
            CommitErrorKind::SubscriptionNotFound => false,
            CommitErrorKind::AccessDenied => false,
            CommitErrorKind::Unprocessable => false,
            CommitErrorKind::BadRequest => false,
            CommitErrorKind::Io => false,
            CommitErrorKind::Other => false,
        }
    }
}

impl StdError for CommitError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|e| &**e as &(dyn StdError + 'static))
    }
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref context) = self.context {
            write!(f, "{}", context)?;
        } else {
            write!(f, "could not connect to stream: {}", self.kind)?;
        }
        Ok(())
    }
}

impl From<CommitError> for Error {
    fn from(err: CommitError) -> Self {
        Error::from_error(err)
    }
}

impl From<NakadiApiError> for CommitError {
    fn from(api_error: NakadiApiError) -> Self {
        if let Some(status) = api_error.status() {
            match status {
                StatusCode::NOT_FOUND => CommitError::not_found().caused_by(api_error),
                StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => {
                    CommitError::access_denied().caused_by(api_error)
                }
                StatusCode::BAD_REQUEST => CommitError::bad_request().caused_by(api_error),
                StatusCode::UNPROCESSABLE_ENTITY => {
                    CommitError::unprocessable().caused_by(api_error)
                }
                _ => CommitError::other().caused_by(api_error),
            }
        } else if api_error.is_io_error() {
            CommitError::bad_request().caused_by(api_error)
        } else {
            CommitError::other().caused_by(api_error)
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CommitErrorKind {
    SubscriptionNotFound,
    AccessDenied,
    Unprocessable,
    BadRequest,
    Io,
    Other,
}

impl fmt::Display for CommitErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitErrorKind::SubscriptionNotFound => write!(f, "subscription not found")?,
            CommitErrorKind::AccessDenied => write!(f, "access denied")?,
            CommitErrorKind::Unprocessable => write!(f, "unprocessable")?,
            CommitErrorKind::BadRequest => write!(f, "bad request")?,
            CommitErrorKind::Io => write!(f, "io")?,
            CommitErrorKind::Other => write!(f, "other")?,
        }
        Ok(())
    }
}
