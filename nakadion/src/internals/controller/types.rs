use std::sync::Arc;

use crate::consumer::Config;
use crate::handler::BatchHandlerFactory;
use crate::internals::ConsumerState;

pub(crate) struct ControllerParams<C> {
    pub api_client: C,
    pub consumer_state: ConsumerState,
    pub handler_factory: Arc<dyn BatchHandlerFactory>,
}

impl<C> ControllerParams<C> {
    pub fn config(&self) -> &Config {
        &self.consumer_state.config()
    }
}

impl<C> Clone for ControllerParams<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            api_client: self.api_client.clone(),
            consumer_state: self.consumer_state.clone(),
            handler_factory: Arc::clone(&self.handler_factory),
        }
    }
}
