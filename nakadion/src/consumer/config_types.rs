use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::api::NakadionEssentials;
use crate::event_handler::{BatchHandler, BatchHandlerFactory};
use crate::logging::LoggingAdapter;
use crate::nakadi_types::model::subscription::{StreamParameters, SubscriptionId};
use crate::Error;

use super::instrumentation::Instrumentation;
use super::{Config, Consumer, Inner};

/// Defines how to dispatch batches to handlers.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DispatchStrategy {
    /// Dispatch all batches to a single worker(handler)
    ///
    /// This means batches are processed sequentially.
    SingleWorker,
}

impl DispatchStrategy {
    env_funs!("DISPATCH_STRATEGY");
}

impl Default for DispatchStrategy {
    fn default() -> Self {
        DispatchStrategy::SingleWorker
    }
}

impl fmt::Display for DispatchStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DispatchStrategy::SingleWorker => write!(f, "single_worker")?,
        }

        Ok(())
    }
}

impl FromStr for DispatchStrategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "single_worker" => Ok(DispatchStrategy::SingleWorker),
            _ => Err(Error::new(format!(
                "'{}' is not a valid DispatchStrategy.",
                s
            ))),
        }
    }
}

/// Defines how to commit cursors
///
/// This value should always be set when creating a `Consumer` because otherwise
/// a it will be guessed by `Nakadion` which might not result in best performance.
#[derive(Debug, Clone, Copy)]
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
        seconds: Option<u32>,
        cursors: Option<u32>,
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
            _ => Ok(()),
        }
    }
}

impl FromStr for CommitStrategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "immediately" => Ok(CommitStrategy::Immediately),
            "latest_possible" => Ok(CommitStrategy::LatestPossible),
            _ => Err(Error::new(format!(
                "'{}' is not a valid CommitStrategy or `Latest` which currently can not be parsed.",
                s
            ))),
        }
    }
}

new_type! {
    #[doc="The internal tick interval.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct TickIntervalSecs(u64, env="TICK_INTERVAL_SECS");
}
impl TickIntervalSecs {
    pub fn duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl Default for TickIntervalSecs {
    fn default() -> Self {
        1.into()
    }
}

new_type! {
    #[doc="The time after which a stream or partition is considered inactive.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct InactivityTimeoutSecs(u64, env="INACTIVITY_TIMEOUT_SECS");
}
impl InactivityTimeoutSecs {
    pub fn duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
new_type! {
    #[doc="The time after which a stream is considered stuck and has to be aborted.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct StreamDeadTimeoutSecs(u64, env="STREAM_DEAD_TIMEOUT_SECS");
}
impl StreamDeadTimeoutSecs {
    pub fn duration(self) -> Duration {
        Duration::from_secs(self.0)
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
        60.into()
    }
}
impl ConnectStreamRetryMaxDelaySecs {
    pub fn duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
new_type! {
    #[doc="The timeout for a request made to Nakadi to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ConnectStreamTimeoutSecs(u64, env="CONNECT_STREAM_TIMEOUT_SECS");
}
impl ConnectStreamTimeoutSecs {
    pub fn duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl Default for ConnectStreamTimeoutSecs {
    fn default() -> Self {
        10.into()
    }
}
new_type! {
    #[doc="The timeout for a request made to Nakadi to commit cursors.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct CommitTimeoutMillis(u64, env="COMMIT_TIMEOUT_MILLIS");
}
impl CommitTimeoutMillis {
    pub fn duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
}
impl Default for CommitTimeoutMillis {
    fn default() -> Self {
        1000.into()
    }
}
new_type! {
    #[doc="The delay between failed attempts to commit cursors.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct CommitRetryDelayMillis(u64, env="COMMIT_RETRY_DELAY_MILLIS");
}
impl CommitRetryDelayMillis {
    pub fn duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
}
impl Default for CommitRetryDelayMillis {
    fn default() -> Self {
        500.into()
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
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct Builder {
    /// The `SubscriptionId` of the subscription to be consumed.
    ///
    /// This value **must** be set.
    pub subscription_id: Option<SubscriptionId>,
    /// Parameters that configure the stream to be consumed.
    pub stream_parameters: Option<StreamParameters>,
    /// The instrumentation to be used to generate metrics
    pub instrumentation: Option<Instrumentation>,
    /// The internal tick interval.
    ///
    /// This triggers internal notification used to montitor the state
    /// of the currently consumed stream.
    pub tick_interval_secs: Option<TickIntervalSecs>,
    /// The time after which a stream or partition is considered inactive.
    pub inactivity_timeout_secs: Option<InactivityTimeoutSecs>,
    /// The time after which a stream is considered stuck and has to be aborted.
    pub stream_dead_timeout_secs: Option<StreamDeadTimeoutSecs>,
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
    pub commit_timeout_millis: Option<CommitTimeoutMillis>,
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

        if self.tick_interval_secs.is_none() {
            self.tick_interval_secs = TickIntervalSecs::try_from_env_prefixed(prefix.as_ref())?;
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
    pub fn tick_interval_secs<T: Into<TickIntervalSecs>>(mut self, tick_interval: T) -> Self {
        self.tick_interval_secs = Some(tick_interval.into());
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
    pub fn commit_timeout_millis<T: Into<CommitTimeoutMillis>>(
        mut self,
        commit_timeout_millis: T,
    ) -> Self {
        self.commit_timeout_millis = Some(commit_timeout_millis.into());
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
    pub fn update_stream_parameters<F>(mut self, mut f: F) -> Self
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
    pub fn try_update_stream_parameters<F>(mut self, mut f: F) -> Result<Self, Error>
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

        let tick_interval = self.tick_interval_secs.unwrap_or_default();

        let inactivity_timeout = self.inactivity_timeout_secs;

        let stream_dead_timeout = self.stream_dead_timeout_secs;

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

        let commit_timeout = self.commit_timeout_millis.unwrap_or_default();
        let commit_retry_delay = self.commit_retry_delay_millis.unwrap_or_default();

        self.stream_parameters = Some(stream_parameters);
        self.instrumentation = Some(instrumentation);
        self.tick_interval_secs = Some(tick_interval);
        self.inactivity_timeout_secs = inactivity_timeout;
        self.stream_dead_timeout_secs = stream_dead_timeout;
        self.dispatch_strategy = Some(dispatch_strategy);
        self.commit_strategy = Some(commit_strategy);
        self.abort_connect_on_auth_error = Some(abort_connect_on_auth_error);
        self.abort_connect_on_subscription_not_found =
            Some(abort_connect_on_subscription_not_found);
        self.connect_stream_retry_max_delay_secs = Some(connect_stream_retry_max_delay);
        self.connect_stream_timeout_secs = Some(connect_stream_timeout);
        self.commit_timeout_millis = Some(commit_timeout);
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

        let tick_interval = self.tick_interval_secs.unwrap_or_default();

        let inactivity_timeout = self.inactivity_timeout_secs;

        let stream_dead_timeout = self.stream_dead_timeout_secs;

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

        let commit_timeout = self.commit_timeout_millis.unwrap_or_default();
        let commit_retry_delay = self.commit_retry_delay_millis.unwrap_or_default();

        let config = Config {
            subscription_id,
            stream_parameters,
            instrumentation,
            tick_interval,
            inactivity_timeout,
            stream_dead_timeout,
            dispatch_strategy,
            commit_strategy,
            abort_connect_on_auth_error,
            abort_connect_on_subscription_not_found,
            connect_stream_retry_max_delay,
            connect_stream_timeout,
            commit_timeout,
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