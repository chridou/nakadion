use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::nakadi_types::subscription::StreamCommitTimeoutSecs;
use crate::{consumer::StreamParameters, nakadi_types::Error};

new_type! {
    #[doc="The time a publish attempt for an events batch may take.\n\n\
    Default is 2500 ms\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct CommitAttemptTimeoutMillis(u64, env="COMMIT_ATTEMPT_TIMEOUT_MILLIS");
}

impl Default for CommitAttemptTimeoutMillis {
    fn default() -> Self {
        Self(2_500)
    }
}

new_type! {
    #[doc="The publishing an events batch including retries may take.\n\n\
    Default is 10 seconds.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct CommitTimeoutMillis(u64, env="COMMIT_TIMEOUT_MILLIS");
}
impl Default for CommitTimeoutMillis {
    fn default() -> Self {
        Self(10_000)
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
    env_funs!("COMMIT_STRATEGY");

    pub fn after_seconds(seconds: u32) -> Self {
        Self::After {
            seconds: Some(seconds),
            cursors: None,
            events: None,
        }
    }

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

    pub fn derive_from_stream_parameters(stream_parameters: &StreamParameters) -> Self {
        let max_uncommitted_events = stream_parameters.effective_max_uncommitted_events();
        let after_events = ((max_uncommitted_events as f32 * 0.75) as u32).max(10);
        let batch_limit: u32 = stream_parameters.batch_limit.unwrap_or_default().into();
        let after_batches = (after_events / batch_limit).max(1);
        CommitStrategy::After {
            seconds: Some(1),
            cursors: Some(after_batches),
            events: Some(after_events),
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

/// Configuration for a `Committer`
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
