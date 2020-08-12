use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::components::StreamingEssentials;
use crate::handler::BatchHandlerFactory;
use crate::logging::LoggingAdapter;
use crate::nakadi_types::subscription::{StreamParameters, SubscriptionId};
use crate::Error;

use super::{Config, Consumer, Inner};
use crate::helpers::mandatory;
pub use crate::instrumentation::{Instrumentation, MetricsDetailLevel};
#[cfg(feature = "metrix")]
pub use crate::instrumentation::{Metrix, MetrixConfig};
#[cfg(feature = "metrix")]
pub use metrix::{processor::ProcessorMount, AggregatesProcessors};

mod new_types;
pub use new_types::*;
pub mod complex_types;
use crate::{
    components::{
        committer::{CommitConfig, CommitStrategy},
        connector::ConnectConfig,
    },
    internals::controller::types::LifecycleListeners,
};
pub use complex_types::*;

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
/// * "STREAM_DEAD_POLICY"
/// * "WARN_STREAM_STALLED_SECS"
/// * "dispatch_mode"
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
/// * "STREAM_MAX_UNCOMMITTED_EVENTS"
/// * "STREAM_BATCH_LIMIT"
/// * "STREAM_LIMIT"
/// * "STREAM_BATCH_FLUSH_TIMEOUT_SECS"
/// * "STREAM_BATCH_TIMESPAN_SECS"
/// * "STREAM_TIMEOUT_SECS"
/// * "STREAM_COMMIT_TIMEOUT_SECS"
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Builder {
    /// The `SubscriptionId` of the subscription to be consumed.
    ///
    /// This value **must** be set.
    pub subscription_id: Option<SubscriptionId>,
    /// The instrumentation to be used to generate metrics
    #[serde(skip)]
    pub instrumentation: Option<Instrumentation>,
    /// The internal tick interval.
    ///
    /// This triggers internal notification used to monitor the state
    /// of the currently consumed stream.
    pub tick_interval_millis: Option<TickIntervalMillis>,
    /// The time after which a handler is considered inactive.
    ///
    /// If no batches have been processed for the given amount of time, the handler
    /// will be notified by calling its `on_inactive` method.
    ///
    /// The default is never.
    pub handler_inactivity_timeout_secs: Option<HandlerInactivityTimeoutSecs>,
    /// The time after which a partition is considered inactive.
    ///
    /// If no batches have been processed for the given amount of time, the partition
    /// will be considered inactive.
    ///
    /// The default is 90 seconds.
    pub partition_inactivity_timeout_secs: Option<PartitionInactivityTimeoutSecs>,
    /// The time after which a stream is considered stuck and has to be aborted.
    pub stream_dead_policy: Option<StreamDeadPolicy>,
    /// Emits a warning when no frames (lines) were received from Nakadi for the given time.
    pub warn_no_frames_secs: Option<WarnNoFramesSecs>,
    /// Emits a warning when no events were received from Nakadi for the given time.
    pub warn_no_events_secs: Option<WarnNoEventsSecs>,
    /// Defines how batches are internally dispatched.
    ///
    /// This e.g. configures parallelism.
    pub dispatch_mode: Option<DispatchMode>,

    /// Configure how partition events are logged.
    pub log_partition_events_mode: Option<LogPartitionEventsMode>,

    /// Defines when to commit cursors.
    ///
    /// It is recommended to set this value instead of letting Nakadion
    /// determine defaults.
    pub commit_config: CommitConfig,
    pub connect_config: ConnectConfig,
}

impl Builder {
    env_ctors!();
    fn fill_from_env_prefixed_internal<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), Error> {
        if self.subscription_id.is_none() {
            self.subscription_id = SubscriptionId::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.instrumentation.is_none() {
            self.instrumentation = Default::default();
        }

        if self.tick_interval_millis.is_none() {
            self.tick_interval_millis = TickIntervalMillis::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.handler_inactivity_timeout_secs.is_none() {
            self.handler_inactivity_timeout_secs =
                HandlerInactivityTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.partition_inactivity_timeout_secs.is_none() {
            self.partition_inactivity_timeout_secs =
                PartitionInactivityTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.stream_dead_policy.is_none() {
            self.stream_dead_policy = StreamDeadPolicy::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.warn_no_frames_secs.is_none() {
            self.warn_no_frames_secs = WarnNoFramesSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.warn_no_events_secs.is_none() {
            self.warn_no_events_secs = WarnNoEventsSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.dispatch_mode.is_none() {
            self.dispatch_mode = DispatchMode::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.log_partition_events_mode.is_none() {
            self.log_partition_events_mode =
                LogPartitionEventsMode::try_from_env_prefixed(prefix.as_ref())?;
        }

        self.commit_config.fill_from_env_prefixed(prefix.as_ref())?;
        self.connect_config
            .fill_from_env_prefixed(prefix.as_ref())?;

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
    ///
    /// This are within the `connect_config` so that these will be created
    /// with defaults if not already there
    pub fn stream_parameters(mut self, params: StreamParameters) -> Self {
        self.connect_config.stream_parameters = params;
        self
    }

    /// The instrumentation to be used to generate metrics
    pub fn instrumentation(mut self, instr: Instrumentation) -> Self {
        self.instrumentation = Some(instr);
        self
    }

    /// The internal tick interval.
    ///
    /// This triggers internal notification used to monitor the state
    /// of the currently consumed stream.
    pub fn tick_interval_millis<T: Into<TickIntervalMillis>>(mut self, tick_interval: T) -> Self {
        self.tick_interval_millis = Some(tick_interval.into());
        self
    }

    /// The time after which a handler is considered inactive.
    ///
    /// If no batches have been processed for the given amount of time, the handler
    /// will be notified by calling its `on_inactive` method.
    ///
    /// The default is never.
    pub fn handler_inactivity_timeout_secs<T: Into<HandlerInactivityTimeoutSecs>>(
        mut self,
        handler_inactivity_timeout_secs: T,
    ) -> Self {
        self.handler_inactivity_timeout_secs = Some(handler_inactivity_timeout_secs.into());
        self
    }

    /// The time after which a partition is considered inactive.
    ///
    /// If no batches have been processed for the given amount of time, the partition
    /// will be considered inactive.
    ///
    /// The default is 90 seconds.
    pub fn partition_inactivity_timeout_secs<T: Into<PartitionInactivityTimeoutSecs>>(
        mut self,
        partition_inactivity_timeout_secs: T,
    ) -> Self {
        self.partition_inactivity_timeout_secs = Some(partition_inactivity_timeout_secs.into());
        self
    }

    /// Define when a stream is considered stuck/dead and has to be aborted.
    pub fn stream_dead_policy<T: Into<StreamDeadPolicy>>(mut self, stream_dead_policy: T) -> Self {
        self.stream_dead_policy = Some(stream_dead_policy.into());
        self
    }

    /// Emits a warning when no frames (lines) were received from Nakadi for the given time.
    pub fn warn_no_frames_secs<T: Into<WarnNoFramesSecs>>(
        mut self,
        warn_no_frames_secs: T,
    ) -> Self {
        self.warn_no_frames_secs = Some(warn_no_frames_secs.into());
        self
    }

    /// Emits a warning when no events were received from Nakadi for the given time.
    pub fn warn_no_events_secs<T: Into<WarnNoEventsSecs>>(
        mut self,
        warn_no_events_secs: T,
    ) -> Self {
        self.warn_no_events_secs = Some(warn_no_events_secs.into());
        self
    }

    /// Defines how batches are internally dispatched.
    ///
    /// This e.g. configures parallelism.
    pub fn dispatch_mode(mut self, dispatch_mode: DispatchMode) -> Self {
        self.dispatch_mode = Some(dispatch_mode);
        self
    }

    /// Configure how partition events are logged.
    pub fn log_partition_events_mode(
        mut self,
        log_partition_events_mode: LogPartitionEventsMode,
    ) -> Self {
        self.log_partition_events_mode = Some(log_partition_events_mode);
        self
    }

    /// Defines how connect to nakadi to consume events.
    pub fn commit_config(mut self, connect_config: ConnectConfig) -> Self {
        self.connect_config = connect_config;
        self
    }

    /// Defines how connect to Nakadi.
    pub fn connect_config(mut self, connect_config: ConnectConfig) -> Self {
        self.connect_config = connect_config;
        self
    }

    /// Modify the current `ConnectConfig` with a closure.
    ///
    /// If these have not been set `ConnectConfig::default()` will
    /// be passed into the closure.
    pub fn configure_connector<F>(self, mut f: F) -> Self
    where
        F: FnMut(ConnectConfig) -> ConnectConfig,
    {
        self.try_configure_connector(|config| Ok(f(config)))
            .unwrap()
    }

    /// Modify the current `ConnectConfig` with a closure.
    ///
    /// If these have not been set `ConnectConfig::default()` will
    /// be passed into the closure.
    ///
    /// If the closure fails, the whole `Builder` will fail.
    pub fn try_configure_connector<F>(mut self, mut f: F) -> Result<Self, Error>
    where
        F: FnMut(ConnectConfig) -> Result<ConnectConfig, Error>,
    {
        let connect_config = self.connect_config;
        self.connect_config = f(connect_config)?;
        Ok(self)
    }
    /// Modify the current `CommitConfig` with a closure.
    ///
    /// If these have not been set `CommitConfig::default()` will
    /// be passed into the closure.
    pub fn configure_committer<F>(self, mut f: F) -> Self
    where
        F: FnMut(CommitConfig) -> CommitConfig,
    {
        self.try_configure_committer(|config| Ok(f(config)))
            .unwrap()
    }

    /// Modify the current `CommitConfig` with a closure.
    ///
    /// If these have not been set `CommitConfig::default()` will
    /// be passed into the closure.
    ///
    /// If the closure fails, the whole `Builder` will fail.
    pub fn try_configure_committer<F>(mut self, mut f: F) -> Result<Self, Error>
    where
        F: FnMut(CommitConfig) -> Result<CommitConfig, Error>,
    {
        let commit_config = self.commit_config;
        self.commit_config = f(commit_config)?;
        Ok(self)
    }

    /// Modify the current `StreamParameters` with a closure.
    ///
    /// If these have not been set `StreamParameters::default()` will
    /// be passed into the closure.
    pub fn configure_stream_parameters<F>(self, mut f: F) -> Self
    where
        F: FnMut(StreamParameters) -> StreamParameters,
    {
        self.try_configure_stream_parameters(|params| Ok(f(params)))
            .unwrap()
    }

    /// Modify the current `StreamParameters` with a closure.
    ///
    /// If these have not been set `StreamParameters::default()` will
    /// be passed into the closure.
    ///
    /// If the closure fails, the whole `Builder` will fail.
    pub fn try_configure_stream_parameters<F>(mut self, f: F) -> Result<Self, Error>
    where
        F: FnMut(StreamParameters) -> Result<StreamParameters, Error>,
    {
        self.connect_config = self.connect_config.try_configure_stream_parameters(f)?;
        Ok(self)
    }

    /// Use the given Metrix instrumentation as the instrumentation
    #[cfg(feature = "metrix")]
    pub fn metrix(mut self, metrix: Metrix, detail: MetricsDetailLevel) -> Self {
        self.instrumentation = Some(Instrumentation::metrix(metrix, detail));
        self
    }

    /// Create new Metrix instrumentation and
    /// put it into the given processor with the optional name.
    #[cfg(feature = "metrix")]
    pub fn metrix_mounted<A: AggregatesProcessors>(
        mut self,
        config: &MetrixConfig,
        detail: MetricsDetailLevel,
        processor: &mut A,
    ) -> Self {
        let instr = Instrumentation::metrix_mounted(config, detail, processor);
        self.instrumentation = Some(instr);
        self
    }

    /// Applies the defaults to all values that have not been set so far.
    pub fn apply_defaults(&mut self) {
        let instrumentation = self
            .instrumentation
            .clone()
            .unwrap_or_else(Instrumentation::default);
        let tick_interval = self.tick_interval_millis.unwrap_or_default().adjust();
        let handler_inactivity_timeout = self.handler_inactivity_timeout_secs;
        let partition_inactivity_timeout =
            Some(self.partition_inactivity_timeout_secs.unwrap_or_default());
        let stream_dead_policy = self.stream_dead_policy.unwrap_or_default();
        let warn_no_frames_secs = Some(self.warn_no_frames_secs.unwrap_or_default());
        let warn_no_events_secs = Some(self.warn_no_events_secs.unwrap_or_default());
        let dispatch_mode = Some(self.dispatch_mode.unwrap_or_default());
        let log_partition_events_mode = Some(self.log_partition_events_mode.unwrap_or_default());

        self.connect_config.apply_defaults();

        if self.commit_config.commit_strategy.is_none() {
            self.commit_config.commit_strategy =
                Some(CommitStrategy::derive_from_stream_parameters(
                    &self.connect_config.stream_parameters,
                ))
        }

        set_stream_commit_timeout(&mut self.commit_config, &self.connect_config);
        self.commit_config.apply_defaults();

        self.instrumentation = Some(instrumentation);
        self.tick_interval_millis = Some(tick_interval);
        self.handler_inactivity_timeout_secs = handler_inactivity_timeout;
        self.partition_inactivity_timeout_secs = partition_inactivity_timeout;
        self.stream_dead_policy = Some(stream_dead_policy);
        self.warn_no_frames_secs = warn_no_frames_secs;
        self.warn_no_events_secs = warn_no_events_secs;
        self.dispatch_mode = dispatch_mode;
        self.log_partition_events_mode = log_partition_events_mode;
    }

    /// Create a `Consumer`
    pub fn build_with<C, HF, L>(
        &self,
        api_client: C,
        handler_factory: HF,
        logs: L,
    ) -> Result<Consumer, Error>
    where
        C: StreamingEssentials + Send + Sync + 'static + Clone,
        HF: BatchHandlerFactory,
        L: LoggingAdapter,
    {
        let config = self.config()?;

        let inner = Inner {
            config,
            api_client,
            handler_factory: Arc::new(handler_factory),
            logging_adapter: Arc::new(logs),
            lifecycle_listeners: LifecycleListeners::default(),
        };

        Ok(Consumer {
            inner: Arc::new(inner),
        })
    }

    fn config(&self) -> Result<Config, Error> {
        let subscription_id = mandatory(self.subscription_id, "subscription_id")?;

        let instrumentation = self
            .instrumentation
            .clone()
            .unwrap_or_else(Instrumentation::default);

        let tick_interval = self.tick_interval_millis.unwrap_or_default().adjust();

        let handler_inactivity_timeout = self.handler_inactivity_timeout_secs.unwrap_or_default();
        let partition_inactivity_timeout =
            self.partition_inactivity_timeout_secs.unwrap_or_default();

        let stream_dead_policy = self.stream_dead_policy.unwrap_or_default();
        stream_dead_policy.validate()?;
        let warn_no_frames = self.warn_no_frames_secs.unwrap_or_default();
        let warn_no_events = self.warn_no_events_secs.unwrap_or_default();

        let dispatch_mode = self.dispatch_mode.clone().unwrap_or_default();
        let log_partition_events_mode = self.log_partition_events_mode.unwrap_or_default();

        let mut connect_config = self.connect_config.clone();
        connect_config.apply_defaults();

        let mut commit_config = self.commit_config.clone();

        if let Some(commit_strategy) = commit_config.commit_strategy {
            commit_strategy.validate()?;
        } else {
            commit_config.commit_strategy = Some(CommitStrategy::derive_from_stream_parameters(
                &connect_config.stream_parameters,
            ));
        }
        set_stream_commit_timeout(&mut commit_config, &connect_config);
        commit_config.apply_defaults();

        let config = Config {
            subscription_id,
            instrumentation,
            tick_interval,
            handler_inactivity_timeout,
            partition_inactivity_timeout,
            stream_dead_policy,
            warn_no_frames,
            warn_no_events,
            dispatch_mode,
            log_partition_events_mode,
            commit_config,
            connect_config,
        };

        Ok(config)
    }
}

fn set_stream_commit_timeout(commit_config: &mut CommitConfig, connect_config: &ConnectConfig) {
    if commit_config.stream_commit_timeout_secs.is_none() {
        let timeout = connect_config
            .stream_parameters
            .commit_timeout_secs
            .unwrap_or_default();
        commit_config.stream_commit_timeout_secs = Some(timeout);
    }
}
