use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::api::NakadionEssentials;
use crate::handler::{BatchHandler, BatchHandlerFactory};
use crate::logging::LoggingAdapter;
use crate::nakadi_types::model::subscription::{StreamParameters, SubscriptionId};
use crate::Error;

use super::instrumentation::Instrumentation;
use super::{Config, Consumer, Inner};

/// Defines how to dispatch batches to handlers.
///
/// # FromStr
///
/// ```rust
/// use nakadion::consumer::DispatchStrategy;
///
/// let strategy = "all_sequential".parse::<DispatchStrategy>().unwrap();
/// assert_eq!(strategy, DispatchStrategy::AllSequential);
/// ```
///
/// # Environment variables
///
/// Fetching values from the environment uses `FromStr` for parsing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
#[serde(tag = "strategy")]
pub enum DispatchStrategy {
    /// Dispatch all batches to a single worker(handler)
    ///
    /// This means batches are processed sequentially.
    AllSequential,
}

impl DispatchStrategy {
    env_funs!("DISPATCH_STRATEGY");
}

impl Default for DispatchStrategy {
    fn default() -> Self {
        DispatchStrategy::AllSequential
    }
}

impl fmt::Display for DispatchStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DispatchStrategy::AllSequential => write!(f, "all_sequential")?,
        }

        Ok(())
    }
}

impl FromStr for DispatchStrategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "all_sequential" => Ok(DispatchStrategy::AllSequential),
            _ => Err(Error::new(format!("not a valid dispatch strategy: {}", s))),
        }
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
/// use nakadion::consumer::CommitStrategy;
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
/// # Environment variables
///
/// Fetching values from the environment uses `FromStr` for parsing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "strategy")]
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

new_type! {
    #[doc="The internal tick interval.\n\nThe applied value is always between [100..5_000] ms. \
    When set outside of its bounds it will be adjusted to fit within the bounds.\n\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct TickIntervalMillis(u64, env="TICK_INTERVAL_MILLIS");
}
impl TickIntervalMillis {
    pub fn into_duration(self) -> Duration {
        Duration::from_millis(self.0)
    }

    /// Only 100ms up to 5_000ms allowed. We simply adjust the
    /// values because there is no reason to crash if these have been set
    /// to an out of range value.
    pub fn adjust(self) -> TickIntervalMillis {
        std::cmp::min(5_000, std::cmp::max(100, self.0)).into()
    }
}
impl Default for TickIntervalMillis {
    fn default() -> Self {
        1000.into()
    }
}
impl From<TickIntervalMillis> for Duration {
    fn from(v: TickIntervalMillis) -> Self {
        v.into_duration()
    }
}

new_type! {
    #[doc="The time after which a stream or partition is considered inactive.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct InactivityTimeoutSecs(u64, env="INACTIVITY_TIMEOUT_SECS");
}
impl InactivityTimeoutSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl From<InactivityTimeoutSecs> for Duration {
    fn from(v: InactivityTimeoutSecs) -> Self {
        v.into_duration()
    }
}

new_type! {
    #[doc="The time after which a stream is considered stuck and has to be aborted.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct StreamDeadTimeoutSecs(u64, env="STREAM_DEAD_TIMEOUT_SECS");
}
impl StreamDeadTimeoutSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl From<StreamDeadTimeoutSecs> for Duration {
    fn from(v: StreamDeadTimeoutSecs) -> Self {
        v.into_duration()
    }
}
new_type! {
    #[doc="Emits a warning when no lines were received from Nakadi.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct WarnStreamStalledSecs(u64, env="WARN_STREAM_STALLED_SECS");
}
impl WarnStreamStalledSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl From<WarnStreamStalledSecs> for Duration {
    fn from(v: WarnStreamStalledSecs) -> Self {
        v.into_duration()
    }
}

new_type! {
    #[doc="If `true` abort the consumer when an auth error occurs while connecting to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct AbortConnectOnAuthError(bool, env="ABORT_CONNECT_ON_AUTH_ERROR");
}
impl Default for AbortConnectOnAuthError {
    fn default() -> Self {
        false.into()
    }
}
new_type! {
    #[doc="If `true` abort the consumer when a subscription does not exist when connection to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct AbortConnectOnSubscriptionNotFound(bool, env="ABORT_CONNECT_ON_SUBSCRIPTION_NOT_FOUND");
}
impl Default for AbortConnectOnSubscriptionNotFound {
    fn default() -> Self {
        true.into()
    }
}

new_type! {
    #[doc="The maximum retry delay between failed attempts to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ConnectStreamRetryMaxDelaySecs(u64, env="CONNECT_STREAM_RETRY_MAX_DELAY_SECS");
}
impl Default for ConnectStreamRetryMaxDelaySecs {
    fn default() -> Self {
        300.into()
    }
}
impl ConnectStreamRetryMaxDelaySecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl From<ConnectStreamRetryMaxDelaySecs> for Duration {
    fn from(v: ConnectStreamRetryMaxDelaySecs) -> Self {
        v.into_duration()
    }
}
new_type! {
    #[doc="The timeout for a request made to Nakadi to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ConnectStreamTimeoutSecs(u64, env="CONNECT_STREAM_TIMEOUT_SECS");
}
impl ConnectStreamTimeoutSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl Default for ConnectStreamTimeoutSecs {
    fn default() -> Self {
        10.into()
    }
}
impl From<ConnectStreamTimeoutSecs> for Duration {
    fn from(v: ConnectStreamTimeoutSecs) -> Self {
        v.into_duration()
    }
}

new_type! {
    #[doc="The timeout for a request made to Nakadi to commit cursors.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct CommitAttemptTimeoutMillis(u64, env="COMMIT_ATTEMPT_TIMEOUT_MILLIS");
}
impl CommitAttemptTimeoutMillis {
    pub fn into_duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
}
impl Default for CommitAttemptTimeoutMillis {
    fn default() -> Self {
        1000.into()
    }
}
impl From<CommitAttemptTimeoutMillis> for Duration {
    fn from(v: CommitAttemptTimeoutMillis) -> Self {
        v.into_duration()
    }
}

new_type! {
    #[doc="The delay between failed attempts to commit cursors.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct CommitRetryDelayMillis(u64, env="COMMIT_RETRY_DELAY_MILLIS");
}
impl CommitRetryDelayMillis {
    pub fn into_duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
}
impl Default for CommitRetryDelayMillis {
    fn default() -> Self {
        500.into()
    }
}
impl From<CommitRetryDelayMillis> for Duration {
    fn from(v: CommitRetryDelayMillis) -> Self {
        v.into_duration()
    }
}

/// Creates a `Consumer`
///
/// This struct configures and creates a consumer. Before a consumer is build
/// the given values will be validated and defaults will be applied.
///
/// The `Builder` can be created and updated from the environment. When updated
/// from the environment only those values will be updated which were not set
/// before.
///
/// ## De-/Serialization
///
/// The `Builder` supports serialization but the instrumentation will never be
/// part of any serialization and therefore default to `None`
///
/// # Environment
///
///
/// When initialized/updated from the environment the following environment variable
/// are used which by are by default prefixed with "NAKADION_" or a custom prefix "<prefix>_":
///
/// For Nakadion itself:
///
/// * "SUBSCRIPTION_ID"
/// * "TICK_INTERVAL_MILLIS"
/// * "INACTIVITY_TIMEOUT_SECS"
/// * "STREAM_DEAD_TIMEOUT_SECS"
/// * "WARN_STREAM_STALLED_SECS"
/// * "DISPATCH_STRATEGY"
/// * "COMMIT_STRATEGY"
/// * "ABORT_CONNECT_ON_AUTH_ERROR"
/// * "ABORT_CONNECT_ON_SUBSCRIPTION_NOT_FOUND"
/// * "CONNECT_STREAM_RETRY_MAX_DELAY_SECS"
/// * "CONNECT_STREAM_TIMEOUT_SECS"
/// * "COMMIT_ATTEMPT_TIMEOUT_MILLIS"
/// * "COMMIT_RETRY_DELAY_MILLIS"
///
/// For `stream_parameters`:
///
/// * "MAX_UNCOMMITTED_EVENTS"
/// * "BATCH_LIMIT"
/// * "STREAM_LIMIT"
/// * "BATCH_FLUSH_TIMEOUT_SECS"
/// * "BATCH_TIMESPAN_SECS"
/// * "STREAM_TIMEOUT_SECS"
/// * "COMMIT_TIMEOUT_SECS"
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Builder {
    /// The `SubscriptionId` of the subscription to be consumed.
    ///
    /// This value **must** be set.
    pub subscription_id: Option<SubscriptionId>,
    /// Parameters that configure the stream to be consumed.
    pub stream_parameters: Option<StreamParameters>,
    /// The instrumentation to be used to generate metrics
    #[serde(skip)]
    pub instrumentation: Option<Instrumentation>,
    /// The internal tick interval.
    ///
    /// This triggers internal notification used to montitor the state
    /// of the currently consumed stream.
    pub tick_interval_millis: Option<TickIntervalMillis>,
    /// The time after which a stream or partition is considered inactive.
    pub inactivity_timeout_secs: Option<InactivityTimeoutSecs>,
    /// The time after which a stream is considered stuck and has to be aborted.
    pub stream_dead_timeout_secs: Option<StreamDeadTimeoutSecs>,
    /// Emits a warning when no lines were received from Nakadi for the given time.
    pub warn_stream_stalled_secs: Option<WarnStreamStalledSecs>,
    /// Defines how batches are internally dispatched.
    ///
    /// This e.g. configures parallelism.
    pub dispatch_strategy: Option<DispatchStrategy>,
    /// Defines when to commit cursors.
    ///
    /// It is recommended to set this value instead of letting Nakadion
    /// determine defaults.
    pub commit_strategy: Option<CommitStrategy>,
    /// If `true` abort the consumer when an auth error occurs while connecting to a stream.
    pub abort_connect_on_auth_error: Option<AbortConnectOnAuthError>,
    /// If `true` abort the consumer when a subscription does not exist when connection to a stream.
    pub abort_connect_on_subscription_not_found: Option<AbortConnectOnSubscriptionNotFound>,
    /// The maximum retry delay between failed attempts to connect to a stream.
    pub connect_stream_retry_max_delay_secs: Option<ConnectStreamRetryMaxDelaySecs>,
    /// The timeout for a request made to Nakadi to connect to a stream.
    pub connect_stream_timeout_secs: Option<ConnectStreamTimeoutSecs>,
    /// The timeout for a request made to Nakadi to commit cursors.
    pub commit_attempt_timeout_millis: Option<CommitAttemptTimeoutMillis>,
    /// The delay between failed attempts to commit cursors.
    pub commit_retry_delay_millis: Option<CommitRetryDelayMillis>,
}

impl Builder {
    /// Creates a new `Builder` from the environment where all the env vars
    /// are prefixed with `NAKADION_`.
    pub fn try_from_env() -> Result<Self, Error> {
        Self::try_from_env_prefixed(crate::helpers::NAKADION_PREFIX)
    }

    /// Creates a new `Builder` from the environment where all the env vars
    /// are prefixed with `<prefix>_`.
    pub fn try_from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Self, Error> {
        let mut me = Self::default();
        me.fill_from_env_prefixed(prefix)?;
        Ok(me)
    }

    /// Sets all values that have not been set so far from the environment.
    ///
    /// All the env vars are prefixed with `NAKADION_`.
    pub fn fill_from_env(&mut self) -> Result<(), Error> {
        self.fill_from_env_prefixed(crate::helpers::NAKADION_PREFIX)
    }

    /// Sets all values that have not been set so far from the environment.
    ///
    /// All the env vars are prefixed with `<prefix>_`.
    pub fn fill_from_env_prefixed<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), Error> {
        if self.subscription_id.is_none() {
            self.subscription_id = SubscriptionId::try_from_env_prefixed(prefix.as_ref())?;
        }

        if let Some(ref mut stream_parameters) = self.stream_parameters {
            stream_parameters.fill_from_env_prefixed(prefix.as_ref())?;
        } else {
            self.stream_parameters = Some(StreamParameters::from_env_prefixed(prefix.as_ref())?);
        }

        if self.instrumentation.is_none() {
            self.instrumentation = Default::default();
        }

        if self.tick_interval_millis.is_none() {
            self.tick_interval_millis = TickIntervalMillis::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.inactivity_timeout_secs.is_none() {
            self.inactivity_timeout_secs =
                InactivityTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.stream_dead_timeout_secs.is_none() {
            self.stream_dead_timeout_secs =
                StreamDeadTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.abort_connect_on_auth_error.is_none() {
            self.abort_connect_on_auth_error =
                AbortConnectOnAuthError::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.abort_connect_on_subscription_not_found.is_none() {
            self.abort_connect_on_subscription_not_found =
                AbortConnectOnSubscriptionNotFound::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.connect_stream_retry_max_delay_secs.is_none() {
            self.connect_stream_retry_max_delay_secs =
                ConnectStreamRetryMaxDelaySecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.connect_stream_timeout_secs.is_none() {
            self.connect_stream_timeout_secs =
                ConnectStreamTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        Ok(())
    }

    /// The `SubscriptionId` of the subscription to be consumed.
    ///
    /// This value **must** be set.
    pub fn subscription_id(mut self, subscription_id: SubscriptionId) -> Self {
        self.subscription_id = Some(subscription_id);
        self
    }

    /// Parameters that configure the stream to be consumed.
    pub fn stream_parameters(mut self, params: StreamParameters) -> Self {
        self.stream_parameters = Some(params);
        self
    }

    /// The instrumentation to be used to generate metrics
    pub fn instrumentation(mut self, instr: Instrumentation) -> Self {
        self.instrumentation = Some(instr);
        self
    }

    /// The internal tick interval.
    ///
    /// This triggers internal notification used to montitor the state
    /// of the currently consumed stream.
    pub fn tick_interval_millis<T: Into<TickIntervalMillis>>(mut self, tick_interval: T) -> Self {
        self.tick_interval_millis = Some(tick_interval.into());
        self
    }

    /// The time after which a stream or partition is considered inactive.
    pub fn inactivity_timeout_secs<T: Into<InactivityTimeoutSecs>>(
        mut self,
        inactivity_timeout: T,
    ) -> Self {
        self.inactivity_timeout_secs = Some(inactivity_timeout.into());
        self
    }

    /// The time after which a stream is considered stuck and has to be aborted.
    pub fn stream_dead_timeout_secs<T: Into<StreamDeadTimeoutSecs>>(
        mut self,
        stream_dead_timeout: T,
    ) -> Self {
        self.stream_dead_timeout_secs = Some(stream_dead_timeout.into());
        self
    }

    /// Emits a warning when no lines were received from Nakadi for the given time.
    pub fn warn_stream_stalled_secs<T: Into<WarnStreamStalledSecs>>(
        mut self,
        warn_stream_stalled_secs: T,
    ) -> Self {
        self.warn_stream_stalled_secs = Some(warn_stream_stalled_secs.into());
        self
    }

    /// Defines how batches are internally dispatched.
    ///
    /// This e.g. configures parallelism.
    pub fn dispatch_strategy(mut self, dispatch_strategy: DispatchStrategy) -> Self {
        self.dispatch_strategy = Some(dispatch_strategy);
        self
    }

    /// Defines when to commit cursors.
    ///
    /// It is recommended to set this value instead of letting Nakadion
    /// determine defaults.
    pub fn commit_strategy(mut self, commit_strategy: CommitStrategy) -> Self {
        self.commit_strategy = Some(commit_strategy);
        self
    }

    /// If `true` abort the consumer when an auth error occurs while connecting to a stream.
    pub fn abort_connect_on_auth_error<T: Into<AbortConnectOnAuthError>>(
        mut self,
        abort_connect_on_auth_error: T,
    ) -> Self {
        self.abort_connect_on_auth_error = Some(abort_connect_on_auth_error.into());
        self
    }

    /// If `true` abort the consumer when a subscription does not exist when connection to a stream.
    pub fn abort_connect_on_subscription_not_found<T: Into<AbortConnectOnSubscriptionNotFound>>(
        mut self,
        abort_connect_on_subscription_not_found: T,
    ) -> Self {
        self.abort_connect_on_subscription_not_found =
            Some(abort_connect_on_subscription_not_found.into());
        self
    }

    /// The maximum retry delay between failed attempts to connect to a stream.
    pub fn connect_stream_retry_max_delay_secs<T: Into<ConnectStreamRetryMaxDelaySecs>>(
        mut self,
        connect_stream_retry_max_delay_secs: T,
    ) -> Self {
        self.connect_stream_retry_max_delay_secs = Some(connect_stream_retry_max_delay_secs.into());
        self
    }

    /// The timeout for a request made to Nakadi to connect to a stream.
    pub fn connect_stream_timeout_secs<T: Into<ConnectStreamTimeoutSecs>>(
        mut self,
        connect_stream_timeout_secs: T,
    ) -> Self {
        self.connect_stream_timeout_secs = Some(connect_stream_timeout_secs.into());
        self
    }

    /// The timeout for a request made to Nakadi to commit cursors.
    pub fn commit_attempt_timeout_millis<T: Into<CommitAttemptTimeoutMillis>>(
        mut self,
        commit_attempt_timeout_millis: T,
    ) -> Self {
        self.commit_attempt_timeout_millis = Some(commit_attempt_timeout_millis.into());
        self
    }

    /// The delay between failed attempts to commit cursors.
    pub fn commit_retry_delay_millis<T: Into<CommitRetryDelayMillis>>(
        mut self,
        commit_retry_delay_millis: T,
    ) -> Self {
        self.commit_retry_delay_millis = Some(commit_retry_delay_millis.into());
        self
    }

    /// Modify the current `StreamParameters` with a closure.
    ///
    /// If these have not been set `StreamParameters::default()` will
    /// be passed into the closure.
    pub fn configure_stream_parameters<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(StreamParameters) -> StreamParameters,
    {
        let stream_parameters = self.stream_parameters.unwrap_or_default();
        self.stream_parameters = Some(f(stream_parameters));
        self
    }

    /// Modify the current `StreamParameters` with a closure.
    ///
    /// If these have not been set `StreamParameters::default()` will
    /// be passed into the closure.
    ///
    /// If the closure fails, the whole `Builder` will fail.
    pub fn try_configure_stream_parameters<F>(mut self, mut f: F) -> Result<Self, Error>
    where
        F: FnMut(StreamParameters) -> Result<StreamParameters, Error>,
    {
        let stream_parameters = self.stream_parameters.unwrap_or_default();
        self.stream_parameters = Some(f(stream_parameters)?);
        Ok(self)
    }

    /// Applies the defaults to all values that have not been set so far.
    ///
    /// Remember that there is no default for a `SubscriptionId` which must be set otherwise.
    pub fn apply_defaults(&mut self) {
        let stream_parameters = self
            .stream_parameters
            .clone()
            .unwrap_or_else(StreamParameters::default);

        let instrumentation = self
            .instrumentation
            .clone()
            .unwrap_or_else(Instrumentation::default);

        let tick_interval = self.tick_interval_millis.unwrap_or_default().adjust();

        let inactivity_timeout = self.inactivity_timeout_secs;

        let stream_dead_timeout = self.stream_dead_timeout_secs;
        let warn_stream_stalled = self.warn_stream_stalled_secs;

        let dispatch_strategy = self.dispatch_strategy.clone().unwrap_or_default();

        let commit_strategy = if let Some(commit_strategy) = self.commit_strategy {
            commit_strategy
        } else {
            self.guess_commit_strategy(&stream_parameters)
        };

        let abort_connect_on_auth_error = self.abort_connect_on_auth_error.unwrap_or_default();

        let abort_connect_on_subscription_not_found = self
            .abort_connect_on_subscription_not_found
            .unwrap_or_default();

        let connect_stream_retry_max_delay =
            self.connect_stream_retry_max_delay_secs.unwrap_or_default();
        let connect_stream_timeout = self.connect_stream_timeout_secs.unwrap_or_default();

        let commit_attempt_timeout = self.commit_attempt_timeout_millis.unwrap_or_default();
        let commit_retry_delay = self.commit_retry_delay_millis.unwrap_or_default();

        self.stream_parameters = Some(stream_parameters);
        self.instrumentation = Some(instrumentation);
        self.tick_interval_millis = Some(tick_interval);
        self.inactivity_timeout_secs = inactivity_timeout;
        self.stream_dead_timeout_secs = stream_dead_timeout;
        self.warn_stream_stalled_secs = warn_stream_stalled;
        self.dispatch_strategy = Some(dispatch_strategy);
        self.commit_strategy = Some(commit_strategy);
        self.abort_connect_on_auth_error = Some(abort_connect_on_auth_error);
        self.abort_connect_on_subscription_not_found =
            Some(abort_connect_on_subscription_not_found);
        self.connect_stream_retry_max_delay_secs = Some(connect_stream_retry_max_delay);
        self.connect_stream_timeout_secs = Some(connect_stream_timeout);
        self.commit_attempt_timeout_millis = Some(commit_attempt_timeout);
        self.commit_retry_delay_millis = Some(commit_retry_delay);
    }

    /// Create a `Consumer`
    pub fn build_with<C, HF, L>(
        &self,
        api_client: C,
        handler_factory: HF,
        logs: L,
    ) -> Result<Consumer, Error>
    where
        C: NakadionEssentials + Send + Sync + 'static + Clone,
        HF: BatchHandlerFactory,
        HF::Handler: BatchHandler,
        L: LoggingAdapter,
    {
        let config = self.config()?;

        let inner = Inner {
            config,
            api_client,
            handler_factory: Arc::new(handler_factory),
            logging_adapter: Arc::new(logs),
        };

        Ok(Consumer {
            inner: Arc::new(inner),
        })
    }

    fn config(&self) -> Result<Config, Error> {
        let subscription_id = if let Some(subscription_id) = self.subscription_id {
            subscription_id
        } else {
            return Err(Error::new("`subscription_id` is missing"));
        };

        let stream_parameters = self
            .stream_parameters
            .clone()
            .unwrap_or_else(StreamParameters::default);

        let instrumentation = self
            .instrumentation
            .clone()
            .unwrap_or_else(Instrumentation::default);

        let tick_interval = self.tick_interval_millis.unwrap_or_default().adjust();

        let inactivity_timeout = self.inactivity_timeout_secs;

        let stream_dead_timeout = self.stream_dead_timeout_secs;
        let warn_stream_stalled = self.warn_stream_stalled_secs;

        let dispatch_strategy = self.dispatch_strategy.clone().unwrap_or_default();

        let commit_strategy = if let Some(commit_strategy) = self.commit_strategy {
            commit_strategy
        } else {
            self.guess_commit_strategy(&stream_parameters)
        };

        commit_strategy.validate()?;

        let abort_connect_on_auth_error = self.abort_connect_on_auth_error.unwrap_or_default();

        let abort_connect_on_subscription_not_found = self
            .abort_connect_on_subscription_not_found
            .unwrap_or_default();

        let connect_stream_retry_max_delay =
            self.connect_stream_retry_max_delay_secs.unwrap_or_default();
        let connect_stream_timeout = self.connect_stream_timeout_secs.unwrap_or_default();

        let commit_attempt_timeout = self.commit_attempt_timeout_millis.unwrap_or_default();
        let commit_retry_delay = self.commit_retry_delay_millis.unwrap_or_default();

        let config = Config {
            subscription_id,
            stream_parameters,
            instrumentation,
            tick_interval,
            inactivity_timeout,
            stream_dead_timeout,
            warn_stream_stalled,
            dispatch_strategy,
            commit_strategy,
            abort_connect_on_auth_error,
            abort_connect_on_subscription_not_found,
            connect_stream_retry_max_delay,
            connect_stream_timeout,
            commit_attempt_timeout,
            commit_retry_delay,
        };

        Ok(config)
    }

    fn guess_commit_strategy(&self, stream_parameters: &StreamParameters) -> CommitStrategy {
        let timeout = stream_parameters.effective_commit_timeout_secs();
        let commit_after = timeout / 6;
        let commit_after = std::cmp::max(1, commit_after);
        let max_uncommitted_events = stream_parameters.effective_max_uncommitted_events();
        let effective_events_limit = max_uncommitted_events / 2;
        let effective_events_limit = std::cmp::max(1, effective_events_limit);
        let batch_limit = stream_parameters.effective_batch_limit();
        let effective_batches_limit = std::cmp::max((max_uncommitted_events / batch_limit) / 2, 1);
        let effective_batches_limit = std::cmp::max(1, effective_batches_limit);
        CommitStrategy::After {
            seconds: Some(commit_after),
            cursors: Some(effective_batches_limit),
            events: Some(effective_events_limit),
        }
    }
}
