//! Component to commit cursors
use std::error::Error as StdError;
use std::fmt;
use std::str::FromStr;
use std::time::Instant;

use backoff::{backoff::Backoff, ExponentialBackoff};
use futures::future::BoxFuture;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc::UnboundedSender,
    time::{delay_for, timeout},
};

use crate::api::{NakadiApiError, SubscriptionCommitApi};
use crate::instrumentation::{Instrumentation, Instruments};
use crate::logging::{DevNullLoggingAdapter, Logger};
use crate::nakadi_types::subscription::{EventTypePartition, StreamCommitTimeoutSecs};
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

/// Defines how to commit cursors
///
/// This value should always be set when creating a `Consumer` because otherwise
/// a it will be guessed by `Nakadion` which might not result in best performance.
///
/// # FromStr
///
/// ```rust
/// use nakadion::components::committer::CommitStrategy;
///
/// let strategy = "immediately".parse::<CommitStrategy>().unwrap();
/// assert_eq!(strategy, CommitStrategy::Immediately);
///
/// let strategy = "latest_possible".parse::<CommitStrategy>().unwrap();
/// assert_eq!(strategy, CommitStrategy::LatestPossible);
///
/// let strategy = "after seconds:1 cursors:2 events:3".parse::<CommitStrategy>().unwrap();
/// assert_eq!(
///     strategy,
///     CommitStrategy::After {
///         seconds: Some(1),
///         cursors: Some(2),
///         events: Some(3),
///     }
/// );
///
/// let strategy = "after seconds:1 events:3".parse::<CommitStrategy>().unwrap();
/// assert_eq!(
///     strategy,
///     CommitStrategy::After {
///         seconds: Some(1),
///         cursors: None,
///         events: Some(3),
///     }
/// );
///
/// assert!("after".parse::<CommitStrategy>().is_err());
/// ```
///
/// JSON is also valid:
///
/// ```rust
/// use nakadion::components::committer::CommitStrategy;
///
/// let strategy = r#""immediately""#.parse::<CommitStrategy>().unwrap();
/// assert_eq!(strategy, CommitStrategy::Immediately);
///
/// let strategy = r#""latest_possible""#.parse::<CommitStrategy>().unwrap();
/// assert_eq!(strategy, CommitStrategy::LatestPossible);
///
/// let strategy = r#"{"after":{"seconds":1, "cursors":3}}"#.parse::<CommitStrategy>().unwrap();
/// assert_eq!(
///     strategy,
///     CommitStrategy::After {
///         seconds: Some(1),
///         cursors: Some(3),
///         events: None,
///     }
/// );
/// ```
///
/// # Environment variables
///
/// Fetching values from the environment uses `FromStr` for parsing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitStrategy {
    /// Commit cursors immediately
    Immediately,
    /// Commit cursors as late as possible.
    ///
    /// This strategy is determined by the commit timeout defined
    /// via `StreamParameters`
    LatestPossible,
    /// Commit after on of the criteria was met:
    ///
    /// * `seconds`: After `seconds` seconds
    /// * `cursors`: After `cursors` cursors have been received
    /// * `events`: After `events` have been received. This requires the `BatchHandler` to give
    /// a hint on the amount of events processed.
    After {
        #[serde(skip_serializing_if = "Option::is_none")]
        seconds: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cursors: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        events: Option<u32>,
    },
}

impl CommitStrategy {
    pub fn after_seconds(seconds: u32) -> Self {
        Self::After {
            seconds: Some(seconds),
            cursors: None,
            events: None,
        }
    }

    env_funs!("COMMIT_STRATEGY");

    pub fn validate(&self) -> Result<(), Error> {
        match self {
            CommitStrategy::After {
                seconds: None,
                cursors: None,
                events: None,
            } => Err(Error::new(
                "'CommitStrategy::After' with all fields set to `None` is not valid",
            )),
            CommitStrategy::After {
                seconds,
                cursors,
                events,
            } => {
                if let Some(seconds) = seconds {
                    if *seconds == 0 {
                        return Err(Error::new("'CommitStrategy::After::seconds' must not be 0"));
                    }
                } else if let Some(cursors) = cursors {
                    if *cursors == 0 {
                        return Err(Error::new("'CommitStrategy::After::cursors' must not be 0"));
                    }
                } else if let Some(events) = events {
                    if *events == 0 {
                        return Err(Error::new("'CommitStrategy::After::events' must not be 0"));
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl Default for CommitStrategy {
    fn default() -> Self {
        Self::Immediately
    }
}

impl FromStr for CommitStrategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn parse_after(s: &str) -> Result<CommitStrategy, Error> {
            let parts: Vec<_> = s.split(' ').filter(|s| !s.is_empty()).collect();

            let mut seconds: Option<u32> = None;
            let mut cursors: Option<u32> = None;
            let mut events: Option<u32> = None;

            if parts.is_empty() {
                return Err(Error::new("invalid"));
            }

            if parts[0] != "after" {
                return Err(Error::new("must start with 'after'"));
            }

            for p in parts.into_iter().skip(1) {
                let parts: Vec<_> = p.split(':').collect();
                if parts.len() != 2 {
                    return Err(Error::new(format!("not valid: {}", p)));
                }
                let v: u32 = parts[1]
                    .parse()
                    .map_err(|err| Error::new(format!("{} not an u32: {}", parts[0], err)))?;

                match parts[0] {
                    "seconds" => seconds = Some(v),
                    "cursors" => cursors = Some(v),
                    "events" => events = Some(v),
                    _ => {
                        return Err(Error::new(format!(
                            "not a part of CommitStrategy: {}",
                            parts[0]
                        )))
                    }
                }
            }

            Ok(CommitStrategy::After {
                seconds,
                events,
                cursors,
            })
        }
        let s = s.trim();

        if s.starts_with('{') || s.starts_with('\"') {
            return Ok(serde_json::from_str(s)?);
        }

        let strategy = match s {
            "immediately" => CommitStrategy::Immediately,
            "latest_possible" => CommitStrategy::LatestPossible,
            _ => parse_after(s).map_err(|err| {
                Error::new(format!(
                    "could not parse CommitStrategy from {}: {}",
                    s, err
                ))
            })?,
        };
        strategy.validate()?;
        Ok(strategy)
    }
}

impl fmt::Display for CommitStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitStrategy::Immediately => write!(f, "immediately")?,
            CommitStrategy::LatestPossible => write!(f, "latest_possible")?,
            CommitStrategy::After {
                seconds: None,
                cursors: None,
                events: None,
            } => {
                write!(f, "after")?;
            }
            CommitStrategy::After {
                seconds,
                cursors,
                events,
            } => {
                write!(f, "after ")?;
                if let Some(seconds) = seconds {
                    write!(f, "seconds:{}", seconds)?;
                    if cursors.is_some() || events.is_some() {
                        write!(f, " ")?;
                    }
                }

                if let Some(cursors) = cursors {
                    write!(f, "cursors:{}", cursors)?;
                    if events.is_some() {
                        write!(f, " ")?;
                    }
                }
                if let Some(events) = events {
                    write!(f, "events:{}", events)?;
                }
            }
        }

        Ok(())
    }
}

/// Configuration for a publisher
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct CommitConfig {
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
    #[serde(default)]
    pub commit_strategy: Option<CommitStrategy>,
    /// Maximum amount of seconds that Nakadi will be waiting for commit after sending a batch to a client.
    #[serde(default)]
    pub stream_commit_timeout_secs: Option<StreamCommitTimeoutSecs>,
}

impl CommitConfig {
    env_ctors!();
    fn fill_from_env_prefixed_internal<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), Error> {
        if self.timeout_millis.is_none() {
            self.timeout_millis = CommitTimeoutMillis::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.attempt_timeout_millis.is_none() {
            self.attempt_timeout_millis =
                CommitAttemptTimeoutMillis::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.initial_retry_interval_millis.is_none() {
            self.initial_retry_interval_millis =
                CommitInitialRetryIntervalMillis::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.retry_interval_multiplier.is_none() {
            self.retry_interval_multiplier =
                CommitRetryIntervalMultiplier::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.max_retry_interval_millis.is_none() {
            self.max_retry_interval_millis =
                CommitMaxRetryIntervalMillis::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.retry_on_auth_error.is_none() {
            self.retry_on_auth_error =
                CommitRetryOnAuthError::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.commit_strategy.is_none() {
            self.commit_strategy = CommitStrategy::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.stream_commit_timeout_secs.is_none() {
            self.stream_commit_timeout_secs =
                StreamCommitTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        Ok(())
    }

    pub fn apply_defaults(&mut self) {
        if self.timeout_millis.is_none() {
            self.timeout_millis = Some(CommitTimeoutMillis::default());
        }
        if self.attempt_timeout_millis.is_none() {
            self.attempt_timeout_millis = Some(CommitAttemptTimeoutMillis::default());
        }
        if self.initial_retry_interval_millis.is_none() {
            self.initial_retry_interval_millis = Some(CommitInitialRetryIntervalMillis::default());
        }
        if self.retry_interval_multiplier.is_none() {
            self.retry_interval_multiplier = Some(CommitRetryIntervalMultiplier::default());
        }
        if self.max_retry_interval_millis.is_none() {
            self.max_retry_interval_millis = Some(CommitMaxRetryIntervalMillis::default());
        }
        if self.retry_on_auth_error.is_none() {
            self.retry_on_auth_error = Some(CommitRetryOnAuthError::default());
        }
        if self.commit_strategy.is_none() {
            self.commit_strategy = Some(CommitStrategy::default());
        }
        if self.stream_commit_timeout_secs.is_none() {
            self.stream_commit_timeout_secs = Some(StreamCommitTimeoutSecs::default());
        }
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
    pub fn commit_strategy<T: Into<CommitStrategy>>(mut self, v: T) -> Self {
        self.commit_strategy = Some(v.into());
        self
    }
    pub fn stream_commit_timeout_secs<T: Into<StreamCommitTimeoutSecs>>(mut self, v: T) -> Self {
        self.stream_commit_timeout_secs = Some(v.into());
        self
    }
}

#[derive(Debug)]
pub struct CommitData {
    pub cursor: SubscriptionCursor,
    pub cursor_received_at: Instant,
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

#[derive(Clone)]
pub struct CommitHandle {
    sender: UnboundedSender<CommitterMessage>,
}

impl CommitHandle {
    pub fn commit(
        &self,
        cursor: SubscriptionCursor,
        cursor_received_at: Instant,
        n_events: Option<usize>,
    ) -> Result<(), CommitData> {
        self.commit_data(CommitData {
            cursor,
            cursor_received_at,
            n_events,
        })
    }

    pub fn commit_data(&self, data: CommitData) -> Result<(), CommitData> {
        if let Err(err) = self.sender.send(CommitterMessage::Data(data)) {
            match err.0 {
                CommitterMessage::Data(data) => Err(data),
                _ => panic!("WRONG MESSAGE SENT IN COMMITTER[commit_data] - THIS IS A BUG"),
            }
        } else {
            Ok(())
        }
    }

    pub fn stop(&self) {
        let _ = self.sender.send(CommitterMessage::Stop);
    }
}

enum CommitterMessage {
    Data(CommitData),
    Stop,
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
                    .committer_cursors_committed(cursors.len(), started.elapsed());
                Ok(results)
            }
            Err(err) => {
                self.instrumentation()
                    .committer_cursors_not_committed(cursors.len(), started.elapsed());
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
                                "commit attempt failed (retry in {:?}: {}",
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

mod background {
    use std::collections::{hash_map::Entry, HashMap};
    use std::time::{Duration, Instant};

    use futures::future::{BoxFuture, FutureExt};
    use tokio::{
        spawn,
        sync::mpsc::{error::TryRecvError, unbounded_channel, UnboundedReceiver},
        time::delay_for,
    };

    use crate::api::SubscriptionCommitApi;
    use crate::nakadi_types::{
        subscription::{EventTypePartition, StreamCommitTimeoutSecs, SubscriptionCursor},
        Error, FlowId,
    };

    use super::*;

    pub fn start<C>(
        committer: Committer<C>,
    ) -> (CommitHandle, BoxFuture<'static, Result<(), Error>>)
    where
        C: SubscriptionCommitApi + Send + Sync + 'static,
    {
        let (tx, to_commit) = unbounded_channel();

        let join_handle = spawn(run_committer(to_commit, committer));

        let f = async move { join_handle.await.map_err(Error::new)? }.boxed();

        (CommitHandle { sender: tx }, f)
    }

    async fn run_committer<C>(
        mut to_commit: UnboundedReceiver<CommitterMessage>,
        mut committer: Committer<C>,
    ) -> Result<(), Error>
    where
        C: SubscriptionCommitApi + Send + Sync + 'static,
    {
        committer.logger.debug(format_args!("Committer starting"));

        let config = committer.config.clone();

        let mut pending = PendingCursors::new(
            config.commit_strategy.unwrap_or_default(),
            config
                .clone()
                .stream_commit_timeout_secs
                .unwrap_or_default(),
        );
        let delay_on_no_cursor = Duration::from_millis(50);

        let mut next_commit_earliest_at = Instant::now();
        loop {
            let now = Instant::now();
            let cursor_received = match to_commit.try_recv() {
                Ok(CommitterMessage::Data(next)) => {
                    pending.add(next, now);
                    true
                }
                Ok(CommitterMessage::Stop) => {
                    break;
                }
                Err(TryRecvError::Empty) => false,
                Err(TryRecvError::Closed) => {
                    committer
                        .logger
                        .debug(format_args!("Exiting committer. Channel closed."));
                    break;
                }
            };

            if next_commit_earliest_at > now {
                continue;
            }

            if !pending.commit_required(now) {
                if !cursor_received {
                    delay_for(delay_on_no_cursor).await;
                }
                continue;
            }

            let cursors: Vec<_> = pending.cursors().collect();
            committer.set_flow_id(FlowId::random());
            match committer.commit(&cursors).await {
                Ok(_) => {
                    pending.reset();
                }
                Err(err) => {
                    if err.is_recoverable() {
                        committer.logger.warn(format_args!(
                            "Failed to commit cursors (recoverable): {}",
                            err
                        ));
                        next_commit_earliest_at = Instant::now() + Duration::from_millis(500)
                    } else {
                        committer.logger.error(format_args!(
                            "Failed to commit cursors (unrecoverable): {}",
                            err
                        ));
                        return Err(Error::from_error(err));
                    }
                }
            };
        }

        if !pending.is_empty() {
            // try to commit the rest
            let cursors: Vec<_> = pending.into_cursors().collect();
            let n_to_commit = cursors.len();

            committer.set_flow_id(FlowId::random());
            match committer.commit(&cursors).await {
                Ok(_) => {
                    committer
                        .logger
                        .debug(format_args!("Committed {} final cursors.", n_to_commit));
                }
                Err(err) => {
                    committer.logger.warn(format_args!(
                        "Failed to commit {} final cursors: {}",
                        n_to_commit, err
                    ));
                }
            };
        }

        committer.logger.debug(format_args!("Committer stopped"));

        Ok(())
    }

    struct PendingCursors {
        stream_commit_timeout: Duration,
        current_deadline: Option<Instant>,
        collected_events: usize,
        collected_cursors: usize,
        commit_strategy: CommitStrategy,
        pending: HashMap<EventTypePartition, SubscriptionCursor>,
    }

    impl PendingCursors {
        pub fn new(
            commit_strategy: CommitStrategy,
            stream_commit_timeout_secs: StreamCommitTimeoutSecs,
        ) -> Self {
            let stream_commit_timeout = safe_commit_timeout(stream_commit_timeout_secs.into());
            Self {
                stream_commit_timeout,
                current_deadline: None,
                collected_events: 0,
                collected_cursors: 0,
                commit_strategy,
                pending: HashMap::new(),
            }
        }

        pub fn add(&mut self, data: CommitData, now: Instant) {
            let key = data.etp();

            self.collected_cursors += 1;
            if let Some(n_events) = data.n_events {
                self.collected_events += n_events
            }

            let deadline = match self.commit_strategy {
                CommitStrategy::Immediately => calc_effective_deadline(
                    self.current_deadline,
                    Some(Duration::from_secs(0)),
                    self.stream_commit_timeout,
                    data.cursor_received_at,
                    now,
                ),
                CommitStrategy::LatestPossible => calc_effective_deadline(
                    self.current_deadline,
                    None,
                    self.stream_commit_timeout,
                    data.cursor_received_at,
                    now,
                ),
                CommitStrategy::After {
                    seconds: Some(seconds),
                    ..
                } => calc_effective_deadline(
                    self.current_deadline,
                    Some(Duration::from_secs(u64::from(seconds))),
                    self.stream_commit_timeout,
                    data.cursor_received_at,
                    now,
                ),
                CommitStrategy::After { seconds: None, .. } => calc_effective_deadline(
                    self.current_deadline,
                    None,
                    self.stream_commit_timeout,
                    data.cursor_received_at,
                    now,
                ),
            };

            self.current_deadline = Some(deadline);

            match self.pending.entry(key) {
                Entry::Vacant(e) => {
                    e.insert(data.cursor);
                }
                Entry::Occupied(mut e) => *e.get_mut() = data.cursor,
            }
        }

        pub fn commit_required(&self, now: Instant) -> bool {
            if self.pending.is_empty() {
                return false;
            }

            if let Some(deadline) = self.current_deadline {
                if deadline <= now {
                    return true;
                }
            }

            match self.commit_strategy {
                CommitStrategy::Immediately => true,
                CommitStrategy::LatestPossible => false,
                CommitStrategy::After {
                    cursors, events, ..
                } => {
                    if let Some(cursors) = cursors {
                        if self.collected_cursors >= cursors as usize {
                            return true;
                        }
                    }
                    if let Some(events) = events {
                        if self.collected_events >= events as usize {
                            return true;
                        }
                    }
                    false
                }
            }
        }

        pub fn reset(&mut self) {
            self.current_deadline = None;
            self.collected_events = 0;
            self.collected_cursors = 0;
            self.pending.clear();
        }

        pub fn cursors<'a>(&'a self) -> impl Iterator<Item = SubscriptionCursor> + 'a {
            self.pending.values().cloned()
        }

        pub fn into_cursors(self) -> impl Iterator<Item = SubscriptionCursor> {
            self.pending.into_iter().map(|(_, v)| v)
        }

        pub fn is_empty(&self) -> bool {
            self.pending.is_empty()
        }
    }

    fn calc_effective_deadline(
        current_deadline: Option<Instant>,
        commit_after: Option<Duration>,
        stream_commit_timeout: Duration,
        cursor_received_at: Instant,
        now: Instant,
    ) -> Instant {
        let deadline_for_cursor = if let Some(commit_after) = commit_after {
            cursor_received_at + std::cmp::min(commit_after, stream_commit_timeout)
        } else {
            cursor_received_at + stream_commit_timeout
        };
        let deadline_for_cursor = if now >= deadline_for_cursor {
            now
        } else {
            deadline_for_cursor
        };
        if let Some(current_deadline) = current_deadline {
            std::cmp::min(deadline_for_cursor, current_deadline)
        } else {
            deadline_for_cursor
        }
    }

    fn safe_commit_timeout(secs: u32) -> Duration {
        if secs > 1 {
            Duration::from_secs(u64::from(secs - 1))
        } else {
            Duration::from_millis(100)
        }
    }
}
