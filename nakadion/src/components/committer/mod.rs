//! Component to commit cursors
use std::error::Error as StdError;
use std::fmt;
use std::time::Instant;

use backoff::{backoff::Backoff, ExponentialBackoff};
use futures::future::BoxFuture;
use http::StatusCode;
use tokio::{
    sync::mpsc::UnboundedSender,
    time::{delay_for, timeout},
};

use crate::api::{NakadiApiError, SubscriptionCommitApi};
use crate::instrumentation::{Instrumentation, Instruments};
use crate::logging::{DevNullLoggingAdapter, Logger};
use crate::nakadi_types::subscription::EventTypePartition;
use crate::nakadi_types::{
    subscription::{CursorCommitResults, StreamId, SubscriptionCursor, SubscriptionId},
    Error, FlowId,
};

mod background;
mod config;

pub use config::*;

#[derive(Debug)]
pub struct CommitData {
    pub cursor: SubscriptionCursor,
    /// Timestamp when the cursor/frame was received used to
    /// calculate the effective time for when to commit
    pub frame_started_at: Instant,
    /// Timestamp used for measurement purposes
    /// to track internal timings.
    pub frame_completed_at: Instant,
    pub n_events: Option<usize>,
}

impl CommitData {
    fn etp(&self) -> EventTypePartition {
        EventTypePartition::new(
            self.cursor.event_type.clone(),
            self.cursor.cursor.partition.clone(),
        )
    }
}

/// The reason why data was committed if working in background mode
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CommitTrigger {
    /// The deadline to commit was reached
    Deadline,
    /// Enough events were received
    Events,
    /// Enough cursors were received
    Cursors,
}

/// Handle to send commit messages to the background task.
///
/// The background task will stop, once the last handle is dropped.
#[derive(Clone)]
pub struct CommitHandle {
    sender: UnboundedSender<CommitData>,
}

impl CommitHandle {
    /// Commit the given cursor with additional information.
    ///
    /// Fails if the data could not be send which means
    /// that the backend is gone. The appropriate action is then
    /// to stop streaming.
    pub fn commit(
        &self,
        cursor: SubscriptionCursor,
        frame_started_at: Instant,
        frame_completed_at: Instant,
        n_events: Option<usize>,
    ) -> Result<(), CommitData> {
        self.commit_data(CommitData {
            cursor,
            frame_started_at,
            frame_completed_at,
            n_events,
        })
    }

    /// Commit the given cursor with  additional information packed in the struct
    /// `CommitData`.
    ///
    /// Fails if the data could not be send which means
    /// that the backend is gone. The appropriate action is then
    /// to stop streaming.
    pub fn commit_data(&self, data: CommitData) -> Result<(), CommitData> {
        if let Err(err) = self.sender.send(data) {
            Err(err.0)
        } else {
            Ok(())
        }
    }
}

/// Commits cursors for a stream
///
/// `Committer` is bound to a given subscription and stream.
/// It has to be configured with its mutating methods.
pub struct Committer<C> {
    client: C,
    flow_id: Option<FlowId>,
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    pub(crate) instrumentation: Instrumentation,
    config: CommitConfig,
    pub(crate) logger: Box<dyn Logger>,
}

impl<C> Committer<C>
where
    C: SubscriptionCommitApi + Send + Sync + 'static,
{
    /// Create a new instance bound to the given subscription and stream
    pub fn new(client: C, subscription_id: SubscriptionId, stream_id: StreamId) -> Self {
        Self {
            client,
            flow_id: None,
            subscription_id,
            stream_id,
            instrumentation: Instrumentation::default(),
            config: CommitConfig::default(),
            logger: Box::new(DevNullLoggingAdapter),
        }
    }

    pub fn run(self) -> (CommitHandle, BoxFuture<'static, Result<(), Error>>) {
        background::start(self)
    }

    pub fn set_logger<L: Logger>(&mut self, logger: L) {
        self.logger = Box::new(logger);
    }

    pub fn logger<L: Logger>(mut self, logger: L) -> Self {
        self.set_logger(logger);
        self
    }

    pub fn instrumentation(&self) -> Instrumentation {
        self.instrumentation.clone()
    }

    pub fn set_flow_id(&mut self, flow_id: FlowId) {
        self.flow_id = Some(flow_id);
    }

    pub fn set_config(&mut self, config: CommitConfig) {
        self.config = config
    }

    pub fn set_instrumentation(&mut self, instrumentation: Instrumentation) {
        self.instrumentation = instrumentation;
    }

    pub fn subscription_id(&self) -> SubscriptionId {
        self.subscription_id
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn config(&self) -> &CommitConfig {
        &self.config
    }

    pub async fn commit(
        &self,
        cursors: &[SubscriptionCursor],
    ) -> Result<CursorCommitResults, CommitError> {
        let started = Instant::now();
        match self.retry_attempts(cursors).await {
            Ok(results) => {
                self.instrumentation()
                    .cursors_committed(cursors.len(), started.elapsed());
                Ok(results)
            }
            Err(err) => {
                self.instrumentation().cursors_not_committed(
                    cursors.len(),
                    started.elapsed(),
                    &err,
                );
                Err(err)
            }
        }
    }

    async fn retry_attempts(
        &self,
        cursors: &[SubscriptionCursor],
    ) -> Result<CursorCommitResults, CommitError> {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(self.config.timeout_millis.unwrap_or_default().into());
        backoff.max_interval = self
            .config
            .max_retry_interval_millis
            .unwrap_or_default()
            .into();
        backoff.multiplier = self
            .config
            .retry_interval_multiplier
            .unwrap_or_default()
            .into();
        backoff.initial_interval = self
            .config
            .initial_retry_interval_millis
            .unwrap_or_default()
            .into();
        let retry_on_auth_errors: bool = self.config.retry_on_auth_error.unwrap_or_default().into();
        loop {
            match self.single_attempt_with_timeout(cursors).await {
                Ok(commit_result) => return Ok(commit_result),
                Err(err) => {
                    let retry = match err.kind() {
                        CommitErrorKind::SubscriptionNotFound => false,
                        CommitErrorKind::AccessDenied => retry_on_auth_errors,
                        CommitErrorKind::Unprocessable => false,
                        CommitErrorKind::BadRequest => false,
                        CommitErrorKind::ServerError => true,
                        CommitErrorKind::Io => true,
                        CommitErrorKind::Other => false,
                    };
                    if retry {
                        if let Some(delay) = backoff.next_backoff() {
                            self.logger.warn(format_args!(
                                "commit attempt failed (retry in {:?}): {}",
                                delay, err
                            ));
                            delay_for(delay).await;
                            continue;
                        } else {
                            return Err(err);
                        }
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }

    async fn single_attempt_with_timeout(
        &self,
        cursors: &[SubscriptionCursor],
    ) -> Result<CursorCommitResults, CommitError> {
        let started = Instant::now();
        match timeout(
            self.config
                .attempt_timeout_millis
                .unwrap_or_default()
                .into(),
            self.client.commit_cursors(
                self.subscription_id,
                self.stream_id,
                cursors,
                self.flow_id.clone().unwrap_or_else(FlowId::random),
            ),
        )
        .await
        {
            Ok(Ok(results)) => Ok(results),
            Ok(Err(err)) => {
                self.instrumentation
                    .commit_cursors_attempt_failed(cursors.len(), started.elapsed());
                Err(err.into())
            }
            Err(err) => {
                self.instrumentation
                    .commit_cursors_attempt_failed(cursors.len(), started.elapsed());
                Err(CommitError::io()
                    .context(format!(
                        "Commit attempt timed out after {:?}",
                        started.elapsed()
                    ))
                    .caused_by(err))
            }
        }
    }
}

/// Error returned on failed commit attempts for a stream
#[derive(Debug)]
pub struct CommitError {
    context: Option<String>,
    kind: CommitErrorKind,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
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
    pub fn server() -> Self {
        Self::new(CommitErrorKind::ServerError)
    }
    pub fn other() -> Self {
        Self::new(CommitErrorKind::Other)
    }

    pub fn context<T: Into<String>>(mut self, context: T) -> Self {
        self.context = Some(context.into());
        self
    }

    pub fn caused_by<E: StdError + Send + Sync + 'static>(mut self, source: E) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    pub fn kind(&self) -> CommitErrorKind {
        self.kind
    }

    pub fn is_recoverable(&self) -> bool {
        match self.kind {
            CommitErrorKind::SubscriptionNotFound => false,
            CommitErrorKind::AccessDenied => true,
            CommitErrorKind::ServerError => true,
            CommitErrorKind::Unprocessable => false,
            CommitErrorKind::BadRequest => false,
            CommitErrorKind::Io => true,
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
        match (self.context.as_ref(), self.source.as_ref()) {
            (None, None) => write!(f, "{}", self.kind),
            (None, Some(source)) => write!(f, "{} - Caused by: {}", self.kind, source),
            (Some(context), None) => write!(f, "{} - {}", self.kind, context),
            (Some(context), Some(source)) => {
                write!(f, "{} - {} - Caused by: {}", self.kind, context, source)
            }
        }
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
                _ => {
                    if status.is_server_error() {
                        CommitError::server().caused_by(api_error)
                    } else {
                        CommitError::other().caused_by(api_error)
                    }
                }
            }
        } else if api_error.is_io_error() {
            CommitError::io().caused_by(api_error)
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
    ServerError,
    Other,
}

impl fmt::Display for CommitErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitErrorKind::SubscriptionNotFound => write!(f, "subscription not found")?,
            CommitErrorKind::AccessDenied => write!(f, "access denied")?,
            CommitErrorKind::Unprocessable => write!(f, "unprocessable")?,
            CommitErrorKind::BadRequest => write!(f, "bad request")?,
            CommitErrorKind::ServerError => write!(f, "server error")?,
            CommitErrorKind::Io => write!(f, "io")?,
            CommitErrorKind::Other => write!(f, "other")?,
        }
        Ok(())
    }
}
