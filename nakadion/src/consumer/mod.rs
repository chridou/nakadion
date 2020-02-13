use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::{BoxFuture, FutureExt};

use crate::api::NakadionEssentials;
use crate::event_handler::{BatchHandler, BatchHandlerFactory};
use crate::internals::{
    controller::{Controller, ControllerParams},
    ConsumerState,
};
use crate::logging::Logs;
pub use crate::nakadi_types::{
    model::subscription::{
        BatchFlushTimeoutSecs, BatchLimit, BatchTimespanSecs, MaxUncommittedEvents, StreamLimit,
        StreamParameters, StreamTimeoutSecs, SubscriptionId,
    },
    Error,
};

mod config_types;
mod error;
mod instrumentation;

#[cfg(feature = "log")]
pub use crate::logging::log_adapter::LogLogger;
#[cfg(feature = "slog")]
pub use crate::logging::slog_adapter::SlogLogger;

use crate::logging::Logger;
pub use crate::logging::{DevNullLogger, LoggingAdapter, StdErrLogger, StdOutLogger};
pub use config_types::{
    AbortConnectOnAuthError, AbortConnectOnSubscriptionNotFound, Builder, CommitRetryDelayMillis,
    CommitStrategy, CommitTimeoutMillis, ConnectStreamRetryMaxDelaySecs, ConnectStreamTimeoutSecs,
    DispatchStrategy, InactivityTimeoutSecs, StreamDeadTimeoutSecs, TickIntervalSecs,
};
pub use error::*;
pub use instrumentation::*;

#[derive(Clone)]
pub struct Consumer {
    inner: Arc<dyn ConsumerInternal + Send + Sync + 'static>,
}

impl Consumer {
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub fn builder_from_env() -> Result<Builder, Error> {
        Builder::try_from_env()
    }

    pub fn builder_from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Builder, Error> {
        Builder::try_from_env_prefixed(prefix)
    }

    pub fn start(self) -> (ConsumerHandle, ConsumerTask) {
        let subscription_id = self.inner.config().subscription_id;

        let logger =
            Logger::new(self.inner.logging_adapter()).with_subscription_id(subscription_id);

        let consumer_state = ConsumerState::new(self.inner.config().clone(), logger);

        consumer_state.info(format_args!(
            "Connecting to subscription with id {}",
            subscription_id
        ));

        let handle = ConsumerHandle {
            consumer_state: consumer_state.clone(),
        };

        let f = self.inner.start(consumer_state).map(move |r| {
            let mut outcome = ConsumptionOutcome {
                aborted: None,
                consumer: self,
            };

            if let Err(err) = r {
                outcome.aborted = Some(err);
            }

            outcome
        });
        let join = ConsumerTask { inner: f.boxed() };
        (handle, join)
    }
}

pub struct ConsumptionOutcome {
    aborted: Option<ConsumerError>,
    consumer: Consumer,
}

impl ConsumptionOutcome {
    pub fn is_aborted(&self) -> bool {
        self.aborted.is_some()
    }

    pub fn into_consumer(self) -> Consumer {
        self.consumer
    }

    pub fn try_into_err(self) -> Result<ConsumerError, Self> {
        if self.aborted.is_some() {
            Ok(self.aborted.unwrap())
        } else {
            Err(self)
        }
    }

    pub fn spilt(self) -> (Consumer, Option<ConsumerError>) {
        (self.consumer, self.aborted)
    }

    pub fn error(&self) -> Option<&ConsumerError> {
        self.aborted.as_ref()
    }

    pub fn into_result(self) -> Result<Consumer, ConsumerError> {
        if let Some(aborted) = self.aborted {
            Err(aborted)
        } else {
            Ok(self.consumer)
        }
    }
}

pub struct ConsumerTask {
    inner: Pin<Box<dyn Future<Output = ConsumptionOutcome> + Send>>,
}

impl ConsumerTask {
    pub fn new<F>(f: F) -> Self
    where
        F: Future<Output = ConsumptionOutcome> + Send + 'static,
    {
        Self { inner: Box::pin(f) }
    }
}

impl Future for ConsumerTask {
    type Output = ConsumptionOutcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

pub struct ConsumerHandle {
    consumer_state: ConsumerState,
}

impl ConsumerHandle {
    pub fn stop(&self) {
        self.consumer_state.request_global_cancellation()
    }
}

trait ConsumerInternal {
    fn start(&self, consumer_state: ConsumerState)
        -> BoxFuture<'static, Result<(), ConsumerError>>;

    fn config(&self) -> &Config;

    fn logging_adapter(&self) -> Arc<dyn LoggingAdapter>;
}

struct Inner<C, H> {
    config: Config,
    api_client: C,
    handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
    logging_adapter: Arc<dyn LoggingAdapter>,
}

impl<C, H> ConsumerInternal for Inner<C, H>
where
    C: NakadionEssentials + Send + Sync + 'static + Clone,
    H: BatchHandler,
{
    fn start(
        &self,
        consumer_state: ConsumerState,
    ) -> BoxFuture<'static, Result<(), ConsumerError>> {
        let controller_params = ControllerParams {
            api_client: self.api_client.clone(),
            consumer_state,
            handler_factory: Arc::clone(&self.handler_factory),
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
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub subscription_id: SubscriptionId,
    pub stream_parameters: StreamParameters,
    pub instrumentation: Instrumentation,
    pub tick_interval: TickIntervalSecs,
    pub inactivity_timeout: Option<InactivityTimeoutSecs>,
    pub stream_dead_timeout: Option<StreamDeadTimeoutSecs>,
    pub dispatch_strategy: DispatchStrategy,
    pub commit_strategy: CommitStrategy,
    pub abort_connect_on_auth_error: AbortConnectOnAuthError,
    pub abort_connect_on_subscription_not_found: AbortConnectOnSubscriptionNotFound,
    pub connect_stream_retry_max_delay: ConnectStreamRetryMaxDelaySecs,
    pub connect_stream_timeout: ConnectStreamTimeoutSecs,
    pub commit_timeout: CommitTimeoutMillis,
    pub commit_retry_delay: CommitRetryDelayMillis,
}
