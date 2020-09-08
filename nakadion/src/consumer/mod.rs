//! A consumer for a subscription
//!
//! Start here if you want to consume a stream. You will need
//! a `BatchHandlerFactory` to consume a stream.
//!
//! The consumer instantiates handlers to process events and manages
//! cursor commits.
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{
    task::{Context, Poll},
    time::Instant,
};

use futures::future::{BoxFuture, FutureExt};

use crate::components::StreamingEssentials;
use crate::handler::BatchHandlerFactory;
use crate::instrumentation::Instruments;
use crate::internals::{
    controller::{
        types::{ControllerParams, LifecycleListeners},
        Controller,
    },
    ConsumerState,
};
use crate::logging::{ContextualLogger, Logger};
pub use crate::nakadi_types::{
    subscription::{
        StreamBatchFlushTimeoutSecs, StreamBatchLimit, StreamBatchTimespanSecs, StreamId,
        StreamLimit, StreamMaxUncommittedEvents, StreamParameters, StreamTimeoutSecs,
        SubscriptionId,
    },
    Error,
};

pub use crate::instrumentation::{Instrumentation, MetricsDetailLevel};
#[cfg(feature = "metrix")]
pub use crate::instrumentation::{Metrix, MetrixConfig};

#[cfg(feature = "log")]
pub use crate::logging::log_adapter::LogLoggingAdapter;
#[cfg(feature = "slog")]
pub use crate::logging::slog_adapter::SlogLoggingAdapter;

pub use crate::components::{
    committer::{
        CommitAttemptTimeoutMillis, CommitConfig, CommitInitialRetryIntervalMillis,
        CommitMaxRetryIntervalMillis, CommitRetryIntervalMultiplier, CommitRetryOnAuthError,
        CommitStrategy, CommitTimeoutMillis,
    },
    connector::{
        ConnectAbortOnAuthError, ConnectAbortOnSubscriptionNotFound, ConnectAttemptTimeoutSecs,
        ConnectConfig, ConnectMaxRetryDelaySecs, ConnectTimeout, ConnectTimeoutSecs,
    },
};
pub use crate::logging::{
    DebugLoggingEnabled, DevNullLoggingAdapter, LogConfig, LogDetailLevel, LoggingAdapter,
    StdErrLoggingAdapter, StdOutLoggingAdapter,
};
pub use config_types::*;
pub use error::*;

mod config_types;
mod error;

/// Consumes an event stream
///
/// A consumer can be started to to consume a stream of events.
/// To start it will consume itself and be returned once streaming has
/// stopped so that it can be started again.
///
/// A consumer can be stopped internally and externally.
///
/// The consumer will only return if stopped via a `ConsumerHandle` or if
/// an error occurs internally. Note that stopping the `Consumer` from within a
/// handler is also considered an error case as is failing to connect to for a stream.
/// In the error case of not being able to connect to a stream the behaviour can be
/// configured via the `ConnectConfig` (e.g. it can be configured to retry indefinitely)
pub struct Consumer {
    inner: Arc<dyn ConsumerInternal + Send + Sync + 'static>,
}

impl Consumer {
    /// Get an uninitialized `Builder`.
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Create a `Builder` initialized with values from the environment
    /// whereas the environment variables will be prefixed with `NAKADION_`.
    pub fn builder_from_env() -> Result<Builder, Error> {
        Builder::from_env()
    }

    /// Create a `Builder` initialized with values from the environment
    /// whereas the environment variables will be prefixed with `<prefix>_`.
    pub fn builder_from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Builder, Error> {
        Builder::from_env_prefixed(prefix)
    }

    /// Create a `Builder` initialized with values from the environment
    /// whereas the environment variables will not be prefixed at all.
    pub fn builder_from_env_type_names() -> Result<Builder, Error> {
        Builder::from_env_type_names()
    }

    /// Start consuming events.
    ///
    /// A 'Consuming` and a `ConsumerHandle` will be returned. The `Consuming`
    /// will complete with a `ConsumptionOutcome` once consumption has stopped.
    /// `Consuming` can be dropped if there is no interest in waiting the consumer
    /// to finish.
    ///
    /// The `ConsumerHandle` can be used to check whether
    /// the `Consumer` is still running and to stop it.
    pub fn start(self) -> (ConsumerHandle, Consuming) {
        let subscription_id = self.inner.config().subscription_id;
        let started_at = Instant::now();

        let logger =
            ContextualLogger::new(self.inner.logging_adapter()).subscription_id(subscription_id);

        let consumer_state = ConsumerState::new(self.inner.config().clone(), logger);
        consumer_state.instrumentation().consumer_started();
        consumer_state
            .instrumentation()
            .stream_parameters(consumer_state.stream_parameters());

        consumer_state.info(format_args!(
            "Connecting to subscription with id {}",
            subscription_id
        ));

        let handle = ConsumerHandle {
            consumer_state: consumer_state.clone(),
        };

        let kept_inner = Arc::clone(&self.inner);
        let join = tokio::spawn({
            let consumer_state = consumer_state.clone();
            async move {
                let stop_reason = self.inner.start(consumer_state).await;

                ConsumptionOutcome {
                    stop_reason,
                    consumer: self,
                }
            }
        });

        let f = async move {
            consumer_state
                .instrumentation()
                .consumer_stopped(started_at.elapsed());
            match join.await {
                Ok(outcome) => {
                    consumer_state.info(format_args!(
                        "Consumer for subscription {} stopped",
                        subscription_id
                    ));

                    outcome
                }
                Err(join_error) => {
                    consumer_state.error(format_args!(
                        "Consumer for subscription {} stopped with a join error!!!",
                        subscription_id
                    ));

                    ConsumptionOutcome {
                        stop_reason: join_error.into(),
                        consumer: Consumer { inner: kept_inner },
                    }
                }
            }
        };

        (handle, Consuming::new(f))
    }

    pub fn add_lifecycle_listener<T: LifecycleListener>(&self, listener: T) {
        self.inner.add_lifecycle_listener(Box::new(listener))
    }
}

impl fmt::Debug for Consumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Consumer({:?})", self.inner)?;
        Ok(())
    }
}

/// Returned once a `Consumer` has stopped. It contains the
/// original consumer and a `ConsumerAbort`
/// which gives more insight on why the consumer was stopped.
pub struct ConsumptionOutcome {
    stop_reason: ConsumerAbort,
    consumer: Consumer,
}

impl ConsumptionOutcome {
    /// `true` if the consumption was aborted by the user via the handle.
    pub fn is_user_aborted(&self) -> bool {
        self.stop_reason.is_user_abort()
    }

    /// `true` if the consumption was afrom internally.
    pub fn is_error(&self) -> bool {
        self.stop_reason.is_error()
    }

    /// Turn the outcome into the contained `Consumer`
    pub fn into_consumer(self) -> Consumer {
        self.consumer
    }

    /// If there was an error return the error as `OK` otherwise
    /// return `self` as an error.
    pub fn try_into_err(self) -> Result<ConsumerError, Self> {
        let consumer = self.consumer;
        match self.stop_reason.try_into_error() {
            Ok(err) => Ok(err),
            Err(stop_reason) => Err(ConsumptionOutcome {
                stop_reason,
                consumer,
            }),
        }
    }

    /// Turns this into the reason loosing the consumer
    pub fn into_reason(self) -> ConsumerAbort {
        self.stop_reason
    }

    pub fn as_reason(&self) -> &ConsumerAbort {
        &self.stop_reason
    }

    /// Split this outcome into the `Consumer` and the reason.
    pub fn spilt(self) -> (Consumer, ConsumerAbort) {
        (self.consumer, self.stop_reason)
    }

    /// If there was an error return a reference to it.
    pub fn error(&self) -> Option<&ConsumerError> {
        self.stop_reason.maybe_as_consumer_error()
    }

    /// Turn this outcome into a `Result`.
    ///
    /// If there was an error the
    /// `Err` case will contain the error. Otherwise the `OK` case will
    /// contain the `Consumer´. If there was an error the `Consumer`
    /// will be lost.
    pub fn into_result(self) -> Result<Consumer, ConsumerError> {
        match self.stop_reason {
            ConsumerAbort::UserInitiated => Ok(self.consumer),
            ConsumerAbort::Error(error) => Err(error),
        }
    }
}

/// A Future that completes once the consumer stopped events consumption
///
/// This `Future` is just a proxy and does not drive the consumer. It can be
/// dropped without causing the consumer to stop.
pub struct Consuming {
    inner: Pin<Box<dyn Future<Output = ConsumptionOutcome> + Send>>,
}

impl Consuming {
    fn new<F>(f: F) -> Self
    where
        F: Future<Output = ConsumptionOutcome> + Send + 'static,
    {
        Self { inner: Box::pin(f) }
    }
}

impl Future for Consuming {
    type Output = ConsumptionOutcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

/// A handle for controlling a `Consumer` from externally.
pub struct ConsumerHandle {
    consumer_state: ConsumerState,
}

impl ConsumerHandle {
    /// Stops the `Consumer`.
    ///
    /// The returned `ConsumptionOutcome` will contain
    /// `ConsumerAbort::UserInitiated` as the reason stopping
    /// the `Consumer`
    pub fn stop(&self) {
        self.consumer_state.request_global_cancellation()
    }

    /// Returns true if the consumer is stopped or
    /// stopping the consumer was requested.
    pub fn stop_requested(&self) -> bool {
        self.consumer_state.global_cancellation_requested()
    }
}

pub trait LifecycleListener: Send + 'static {
    fn on_consumer_started(&self, _subscription_id: SubscriptionId) {}
    fn on_consumer_stopped(&self, _subscription_id: SubscriptionId) {}
    fn on_stream_connected(&self, _subscription_id: SubscriptionId, _stream_id: StreamId) {}
    fn on_stream_ended(&self, _subscription_id: SubscriptionId, _stream_id: StreamId) {}
}

trait ConsumerInternal: fmt::Debug {
    fn start(&self, consumer_state: ConsumerState) -> BoxFuture<'static, ConsumerAbort>;

    fn config(&self) -> &Config;

    fn logging_adapter(&self) -> Arc<dyn LoggingAdapter>;

    fn add_lifecycle_listener(&self, listener: Box<dyn LifecycleListener>);
}

struct Inner<C> {
    config: Config,
    api_client: C,
    handler_factory: Arc<dyn BatchHandlerFactory>,
    logging_adapter: Arc<dyn LoggingAdapter>,
    lifecycle_listeners: LifecycleListeners,
}

impl<C> fmt::Debug for Inner<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[]")?;
        Ok(())
    }
}

impl<C> ConsumerInternal for Inner<C>
where
    C: StreamingEssentials + Clone,
{
    fn start(&self, consumer_state: ConsumerState) -> BoxFuture<'static, ConsumerAbort> {
        let controller_params = ControllerParams {
            api_client: self.api_client.clone(),
            consumer_state,
            handler_factory: Arc::clone(&self.handler_factory),
            lifecycle_listeners: self.lifecycle_listeners.clone(),
        };

        let controller = Controller::new(controller_params);
        controller.start().boxed()
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn logging_adapter(&self) -> Arc<dyn LoggingAdapter> {
        Arc::clone(&self.logging_adapter)
    }

    fn add_lifecycle_listener(&self, listener: Box<dyn LifecycleListener>) {
        self.lifecycle_listeners.add_listener(listener)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub subscription_id: SubscriptionId,
    pub instrumentation: Instrumentation,
    pub tick_interval: TickIntervalMillis,
    pub handler_inactivity_timeout: HandlerInactivityTimeoutSecs,
    pub partition_inactivity_timeout: PartitionInactivityTimeoutSecs,
    pub stream_dead_policy: StreamDeadPolicy,
    pub warn_no_frames: WarnNoFramesSecs,
    pub warn_no_events: WarnNoEventsSecs,
    pub dispatch_mode: DispatchMode,
    pub log_partition_events_mode: LogPartitionEventsMode,
    pub connect_config: ConnectConfig,
    pub commit_config: CommitConfig,
}
