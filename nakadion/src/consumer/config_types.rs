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

#[derive(Debug, Clone)]
pub enum DispatchStrategy {
    SingleWorker,
    EventType,
    EventTypePartition,
}

impl Default for DispatchStrategy {
    fn default() -> Self {
        DispatchStrategy::SingleWorker
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CommitStrategy {
    Immediately,
    LatestPossible,
    After {
        seconds: Option<u32>,
        batches: Option<u32>,
        events: Option<u32>,
    },
}

new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct TickIntervalSecs(u64, env="TICK_INTERVAL_SECS");
}
impl TickIntervalSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct InactivityTimeoutSecs(u64, env="INACTIVITY_TIMEOUT_SECS");
}
impl InactivityTimeoutSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct StreamDeadTimeoutSecs(u64, env="STREAM_DEAD_TIMEOUT_SECS");
}
impl StreamDeadTimeoutSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct AbortConnectOnAuthError(bool, env="ABORT_CONNECT_ON_AUTH_ERROR");
}
new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct AbortConnectOnSubscriptionNotFound(bool, env="ABORT_CONNECT_ON_SUBSCRIPTION_NOT_FOUND");
}
new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ConnectRetryDelaySecs(u64, env="CONNECT_RETRY_DELAY_SECS");
}
impl ConnectRetryDelaySecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}

#[derive(Default, Clone)]
#[non_exhaustive]
pub struct Builder {
    pub subscription_id: Option<SubscriptionId>,
    pub stream_parameters: Option<StreamParameters>,
    pub instrumentation: Option<Instrumentation>,
    pub tick_interval: Option<TickIntervalSecs>,
    pub inactivity_timeout: Option<InactivityTimeoutSecs>,
    pub stream_dead_timeout: Option<StreamDeadTimeoutSecs>,
    pub dispatch_strategy: Option<DispatchStrategy>,
    pub commit_strategy: Option<CommitStrategy>,
    pub abort_connect_on_auth_error: Option<AbortConnectOnAuthError>,
    pub abort_connect_on_subscription_not_found: Option<AbortConnectOnSubscriptionNotFound>,
    pub connect_retry_delay: Option<ConnectRetryDelaySecs>,
}

impl Builder {
    pub fn try_from_env() -> Result<Self, Error> {
        Self::try_from_env_prefixed(crate::helpers::NAKADION_PREFIX)
    }

    pub fn try_from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Self, Error> {
        let mut me = Self::default();
        me.fill_from_env_prefixed(prefix)?;
        Ok(me)
    }

    pub fn fill_from_env(&mut self) -> Result<(), Error> {
        self.fill_from_env_prefixed(crate::helpers::NAKADION_PREFIX)
    }

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

        if self.tick_interval.is_none() {
            self.tick_interval = TickIntervalSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.inactivity_timeout.is_none() {
            self.inactivity_timeout =
                InactivityTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.stream_dead_timeout.is_none() {
            self.stream_dead_timeout =
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

        if self.connect_retry_delay.is_none() {
            self.connect_retry_delay =
                ConnectRetryDelaySecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        Ok(())
    }

    pub fn subscription_id(mut self, subscription_id: SubscriptionId) -> Self {
        self.subscription_id = Some(subscription_id);
        self
    }

    pub fn stream_parameters(mut self, params: StreamParameters) -> Self {
        self.stream_parameters = Some(params);
        self
    }

    pub fn instrumentation(mut self, instr: Instrumentation) -> Self {
        self.instrumentation = Some(instr);
        self
    }

    pub fn tick_interval(mut self, tick_interval: TickIntervalSecs) -> Self {
        self.tick_interval = Some(tick_interval);
        self
    }

    pub fn inactivity_timeout(mut self, inactivity_timeout: InactivityTimeoutSecs) -> Self {
        self.inactivity_timeout = Some(inactivity_timeout);
        self
    }

    pub fn stream_dead_timeout(mut self, stream_dead_timeout: StreamDeadTimeoutSecs) -> Self {
        self.stream_dead_timeout = Some(stream_dead_timeout);
        self
    }

    pub fn dispatch_strategy(mut self, dispatch_strategy: DispatchStrategy) -> Self {
        self.dispatch_strategy = Some(dispatch_strategy);
        self
    }

    pub fn commit_strategy(mut self, commit_strategy: CommitStrategy) -> Self {
        self.commit_strategy = Some(commit_strategy);
        self
    }

    pub fn abort_connect_on_auth_error(
        mut self,
        abort_connect_on_auth_error: AbortConnectOnAuthError,
    ) -> Self {
        self.abort_connect_on_auth_error = Some(abort_connect_on_auth_error);
        self
    }

    pub fn abort_connect_on_subscription_not_found(
        mut self,
        abort_connect_on_subscription_not_found: AbortConnectOnSubscriptionNotFound,
    ) -> Self {
        self.abort_connect_on_subscription_not_found =
            Some(abort_connect_on_subscription_not_found);
        self
    }

    pub fn connect_retry_delay(mut self, connect_retry_delay: ConnectRetryDelaySecs) -> Self {
        self.connect_retry_delay = Some(connect_retry_delay);
        self
    }

    pub fn finish_with<C, HF, L>(
        self,
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

    fn config(self) -> Result<Config, Error> {
        let subscription_id = if let Some(subscription_id) = self.subscription_id {
            subscription_id
        } else {
            return Err(Error::new("`subscription_id` is missing"));
        };

        let stream_parameters = self
            .stream_parameters
            .unwrap_or_else(StreamParameters::default);

        let instrumentation = self
            .instrumentation
            .unwrap_or_else(Instrumentation::default);

        let tick_interval = self.tick_interval.unwrap_or_else(|| 1.into());

        let inactivity_timeout = self.inactivity_timeout;

        let stream_dead_timeout = self.stream_dead_timeout;

        let dispatch_strategy = self.dispatch_strategy.unwrap_or_default();

        let commit_strategy = if let Some(commit_strategy) = self.commit_strategy {
            commit_strategy
        } else {
            let timeout = stream_parameters.effective_commit_timeout_secs();
            let timeout = timeout / 2;
            let timeout = if timeout == 0 { 1 } else { timeout };
            let max_uncommitted_events = stream_parameters.effective_max_uncommitted_events();
            let max_uncommitted_events = max_uncommitted_events / 2;
            let max_uncommitted_events = if max_uncommitted_events == 0 {
                1
            } else {
                max_uncommitted_events
            };
            CommitStrategy::After {
                seconds: Some(timeout),
                batches: None,
                events: Some(max_uncommitted_events),
            }
        };

        let abort_connect_on_auth_error = self
            .abort_connect_on_auth_error
            .unwrap_or_else(|| false.into());

        let abort_connect_on_subscription_not_found = self
            .abort_connect_on_subscription_not_found
            .unwrap_or_else(|| true.into());

        let connect_retry_delay = self.connect_retry_delay.unwrap_or_else(|| 1.into());

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
            connect_retry_delay,
        };

        Ok(config)
    }
}
