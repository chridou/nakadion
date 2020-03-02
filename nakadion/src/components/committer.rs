//! Component to commit cursors
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use backoff::{backoff::Backoff, ExponentialBackoff};
use futures::future::FutureExt;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::time::{delay_for, timeout};

use crate::api::{NakadiApiError, SubscriptionCommitApi};
use crate::instrumentation::{Instrumentation, Instruments};
use crate::nakadi_types::{
    subscription::{CursorCommitResults, StreamId, SubscriptionCursor, SubscriptionId},
    Error, FlowId,
};

new_type! {
    #[doc="The time a publish attempt for an events batch may take.\n\n\
    Default is 1000 ms\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct CommitAttemptTimeoutMillis(u64, env="COMMIT_ATTEMPT_TIMEOUT_MILLIS");
}

impl Default for CommitAttemptTimeoutMillis {
    fn default() -> Self {
        Self(1_000)
    }
}

new_type! {
    #[doc="The a publishing the events batch including retries may take.\n\n\
    Default is 5000 ms.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct CommitTimeoutMillis(u64, env="COMMIT_TIMEOUT_MILLIS");
}
impl Default for CommitTimeoutMillis {
    fn default() -> Self {
        Self(5_000)
    }
}

new_type! {
    #[doc="The initial delay between retry attempts.\n\n\
    Default is 100 ms.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct CommitInitialRetryIntervalMillis(u64, env="COMMIT_RETRY_INITIAL_INTERVAL_MILLIS");
}
impl Default for CommitInitialRetryIntervalMillis {
    fn default() -> Self {
        Self(100)
    }
}
new_type! {
    #[doc="The multiplier for the delay increase between retries.\n\n\
    Default is 1.5 (+50%).\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
    pub copy struct CommitRetryIntervalMultiplier(f64, env="COMMIT_RETRY_INTERVAL_MULTIPLIER");
}
impl Default for CommitRetryIntervalMultiplier {
    fn default() -> Self {
        Self(1.5)
    }
}
new_type! {
    #[doc="The maximum interval between retries.\n\n\
    Default is 1000 ms.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct CommitMaxRetryIntervalMillis(u64, env="COMMIT_MAX_RETRY_INTERVAL_MILLIS");
}
impl Default for CommitMaxRetryIntervalMillis {
    fn default() -> Self {
        Self(1000)
    }
}
new_type! {
    #[doc="If true, retries are done on auth errors.\n\n\
    Default is false.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct CommitRetryOnAuthError(bool, env="COMMIT_RETRY_ON_AUTH_ERROR");
}
impl Default for CommitRetryOnAuthError {
    fn default() -> Self {
        Self(false)
    }
}

/// Configuration for a publisher
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct CommitterConfig {
    /// Timeout for a complete commit including potential retries
    #[serde(default)]
    pub timeout_millis: Option<CommitTimeoutMillis>,
    /// Timeout for a single commit request with Nakadi
    #[serde(default)]
    pub attempt_timeout_millis: Option<CommitAttemptTimeoutMillis>,
    /// Interval length before the first retry attempt
    #[serde(default)]
    pub initial_retry_interval_millis: Option<CommitInitialRetryIntervalMillis>,
    /// Multiplier for the length of of the next retry interval
    #[serde(default)]
    pub retry_interval_multiplier: Option<CommitRetryIntervalMultiplier>,
    /// Maximum length of an interval before a retry
    #[serde(default)]
    pub max_retry_interval_millis: Option<CommitMaxRetryIntervalMillis>,
    /// Retry on authentication/authorization errors if `true`
    #[serde(default)]
    pub retry_on_auth_error: Option<CommitRetryOnAuthError>,
}

impl CommitterConfig {
    pub fn from_env() -> Result<Self, Error> {
        let mut me = Self::default();
        me.update_from_env()?;
        Ok(me)
    }

    pub fn from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Self, Error> {
        let mut me = Self::default();
        me.update_from_env_prefixed(prefix)?;
        Ok(me)
    }

    pub fn update_from_env(&mut self) -> Result<(), Error> {
        self.update_from_env_prefixed(crate::helpers::NAKADION_PREFIX)
    }

    pub fn update_from_env_prefixed<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), Error> {
        if self.timeout_millis.is_none() {
            self.timeout_millis(CommitTimeoutMillis::from_env_prefixed(prefix)?);
        }
        if self.attempt_timeout_millis.is_none() {
            self.attempt_timeout_millis(CommitAttemptTimeoutMillis::from_env_prefixed(prefix)?);
        }
        if self.initial_retry_interval_millis.is_none() {
            self.initial_retry_interval_millis(
                CommitInitialRetryIntervalMillis::from_env_prefixed(prefix)?,
            );
        }
        if self.retry_interval_multiplier.is_none() {
            self.retry_interval_multiplier(CommitRetryIntervalMultiplier::from_env_prefixed(
                prefix,
            )?);
        }
        if self.max_retry_interval_millis.is_none() {
            self.max_retry_interval_millis(CommitMaxRetryIntervalMillis::from_env_prefixed(
                prefix,
            )?);
        }
        if self.retry_on_auth_error.is_none() {
            self.retry_on_auth_error(CommitRetryOnAuthError::from_env_prefixed(prefix)?);
        }

        Ok(())
    }

    pub fn timeout_millis<T: Into<CommitTimeoutMillis>>(mut self, v: T) -> Self {
        self.timeout_millis = Some(v.into());
        self
    }
    pub fn attempt_timeout_millis<T: Into<CommitAttemptTimeoutMillis>>(mut self, v: T) -> Self {
        self.attempt_timeout_millis = Some(v.into());
        self
    }
    pub fn initial_retry_interval_millis<T: Into<CommitInitialRetryIntervalMillis>>(
        mut self,
        v: T,
    ) -> Self {
        self.initial_retry_interval_millis = Some(v.into());
        self
    }
    pub fn retry_interval_multiplier<T: Into<CommitRetryIntervalMultiplier>>(
        mut self,
        v: T,
    ) -> Self {
        self.retry_interval_multiplier = Some(v.into());
        self
    }
    pub fn max_retry_interval_millis<T: Into<CommitMaxRetryIntervalMillis>>(
        mut self,
        v: T,
    ) -> Self {
        self.max_retry_interval_millis = Some(v.into());
        self
    }
    pub fn retry_on_auth_error<T: Into<CommitRetryOnAuthError>>(mut self, v: T) -> Self {
        self.retry_on_auth_error = Some(v.into());
        self
    }
}

/// Commits cursors for a stream
///
/// `Committer` is bound to a given subscription and stream.
/// It has to be configured with its mutating methods.
///
/// ## `on_retry`
///
/// A closure to be called before a retry. The error which caused the retry and
/// the time until the retry will be made is passed. This closure overrides the current one
/// and will be used for all subsequent clones of this instance. This allows
/// users to give context on the call site.
pub struct Committer<C> {
    client: C,
    flow_id: Option<FlowId>,
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    instrumentation: Instrumentation,
    config: CommitterConfig,
    on_retry_callback: Arc<dyn Fn(&CommitError, Duration) + Send + Sync + 'static>,
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
            timeout_millis: CommitAttemptTimeoutMillis::default(),
            config: CommitterConfig::default(),
        }
    }

    async fn commit(
        &self,
        cursors: &[SubscriptionCursor],
    ) -> Result<CursorCommitResults, CommitError> {
        let started = Instant::now();
        match self.retry_attempts(cursors).await {
            Ok(results) => {
                self.instrumentation()
                    .committer_cursors_committed(cursors.len(), started.elapsed());
                Ok(results)
            }
            Err(err) => {
                self.instrumentation()
                    .committer_cursors_not_committed(cursors.len(), started.elapsed());
                Err(err.into())
            }
        }
    }

    pub fn set_on_retry<F: Fn(&CommitError, Duration) + Send + Sync + 'static>(
        &mut self,
        on_retry: F,
    ) {
        self.on_retry_callback = Arc::new(on_retry);
    }

    pub fn on_retry<F: Fn(&CommitError, Duration) + Send + Sync + 'static>(
        mut self,
        on_retry: F,
    ) -> Self {
        self.set_on_retry(on_retry);
        self
    }

    fn instrumentation(&self) -> Instrumentation {
        self.instrumentation.clone()
    }

    fn set_flow_id(&mut self, flow_id: FlowId) {
        self.flow_id = Some(flow_id);
    }

    fn set_config(&mut self, config: CommitterConfig) {
        self.config = config
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

    fn config(&self) -> &CommitterConfig {
        &self.config
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
                            (self.on_retry_callback)(&err, delay);
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
            Ok(Err(err)) => Err(err.into()),
            Err(err) => Err(CommitError::io()
                .context(format!(
                    "Commit attempt timed out after {:?}",
                    started.elapsed()
                ))
                .caused_by(err)),
        }
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
