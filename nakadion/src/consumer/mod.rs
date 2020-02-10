pub use std::time::Duration;

use crate::api::NakadionEssentials;
use crate::event_handler::{BatchHandler, BatchHandlerFactory};
pub use crate::nakadi_types::{
    model::subscription::{StreamParameters, SubscriptionId},
    GenericError,
};

mod error;
mod instrumentation;

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
#[non_exhaustive]
pub struct Builder {
    pub subscription_id: Option<SubscriptionId>,
    pub stream_parameters: Option<StreamParameters>,
    pub instrumentation: Instrumentation,
    pub tick_interval: Option<Duration>,
    pub handler_inactivity_after: Option<Duration>,
    pub stream_dead_after: Option<Duration>,
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

    pub fn finish<C, HF>(self, api_client: C, handler_factory: HF) -> Result<Consumer, GenericError>
    where
        C: NakadionEssentials + Send + Sync + 'static + Clone,
        HF: BatchHandlerFactory,
        HF::Handler: BatchHandler,
    {
        self.validate()?;
        unimplemented!()
    }

    fn validate(&self) -> Result<(), GenericError> {
        unimplemented!()
    }
}

pub struct Consumer;

impl Consumer {
    pub fn builder() {}

    pub fn builder_from_env() {}
}
