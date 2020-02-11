use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
pub use std::time::Duration;

use futures::future::{BoxFuture, FutureExt};

use crate::api::NakadionEssentials;
use crate::event_handler::{BatchHandler, BatchHandlerFactory};
use crate::internals::{
    controller::{Controller, ControllerParams},
    ConsumerState,
};
pub use crate::nakadi_types::{
    model::subscription::{StreamParameters, SubscriptionId},
    Error,
};

mod error;
mod instrumentation;

use crate::logging::Logger;
pub use crate::logging::{DevNullLogger, LoggingAdapter, PrintLogger};
pub use error::*;
pub use instrumentation::*;

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

#[derive(Clone)]
pub struct Consumer {
    inner: Arc<dyn ConsumerInternal + Send + Sync + 'static>,
}

impl Consumer {
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub fn builder_from_env() -> Result<Builder, Error> {
        unimplemented!()
    }

    pub fn start(self) -> (ConsumerHandle, ConsumerTask) {
        let subscription_id = self.inner.config().subscription_id;

        let logger =
            Logger::new(self.inner.logging_adapter()).with_subscription_id(subscription_id);

        logger.info(format_args!(
            "Connecting to subscription with id {}",
            subscription_id
        ));

        let consumer_state = ConsumerState::new(
            subscription_id,
            self.inner.config().stream_parameters.clone(),
            logger,
        );

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

#[derive(Default, Clone)]
#[non_exhaustive]
pub struct Builder {
    pub subscription_id: Option<SubscriptionId>,
    pub stream_parameters: Option<StreamParameters>,
    pub instrumentation: Instrumentation,
    pub tick_interval: Option<Duration>,
    pub handler_inactivity_after: Option<Duration>,
    pub stream_dead_after: Option<Duration>,
    pub dispatch_strategy: DispatchStrategy,
}

impl Builder {
    pub fn subscription_id(mut self, subscription_id: SubscriptionId) -> Self {
        self.subscription_id = Some(subscription_id);
        self
    }

    pub fn stream_parameters(mut self, params: StreamParameters) -> Self {
        self.stream_parameters = Some(params);
        self
    }

    pub fn instrumentation(mut self, instr: Instrumentation) -> Self {
        self.instrumentation = instr;
        self
    }

    pub fn tick_interval(mut self, tick_interval: Duration) -> Self {
        self.tick_interval = Some(tick_interval);
        self
    }

    pub fn handler_inactivity_after(mut self, handler_inactivity_after: Duration) -> Self {
        self.handler_inactivity_after = Some(handler_inactivity_after);
        self
    }

    pub fn stream_dead_after(mut self, stream_dead_after: Duration) -> Self {
        self.stream_dead_after = Some(stream_dead_after);
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

        let config = Config {
            subscription_id,
            stream_parameters,
            instrumentation: self.instrumentation,
            tick_interval: self.tick_interval.unwrap_or(Duration::from_secs(1)),
            handler_inactivity_after: self
                .handler_inactivity_after
                .unwrap_or(Duration::from_secs(300)),
            stream_dead_after: self.stream_dead_after,
            dispatch_strategy: self.dispatch_strategy,
        };

        Ok(config)
    }
}

#[derive(Clone)]
struct Config {
    pub subscription_id: SubscriptionId,
    pub stream_parameters: StreamParameters,
    pub instrumentation: Instrumentation,
    pub tick_interval: Duration,
    pub handler_inactivity_after: Duration,
    pub stream_dead_after: Option<Duration>,
    pub dispatch_strategy: DispatchStrategy,
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
            consumer_state: consumer_state.clone(),
            dispatch_strategy: self.config.dispatch_strategy.clone(),
            handler_factory: Arc::clone(&self.handler_factory),
            tick_interval: self.config.tick_interval,
        };

        let controller = Controller::new(controller_params);
        controller.start(consumer_state).boxed()
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn logging_adapter(&self) -> Arc<dyn LoggingAdapter> {
        Arc::clone(&self.logging_adapter)
    }
}
