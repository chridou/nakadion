use std::sync::{Arc, Mutex};

use crate::consumer::{Config, LifecycleListener};
use crate::handler::BatchHandlerFactory;
use crate::internals::ConsumerState;
use crate::nakadi_types::subscription::{StreamId, SubscriptionId};

#[derive(Default, Clone)]
pub(crate) struct LifecycleListeners {
    listeners: Arc<Mutex<Vec<Box<dyn LifecycleListener>>>>,
}

impl LifecycleListeners {
    pub fn add_listener(&self, listener: Box<dyn LifecycleListener>) {
        self.listeners.lock().unwrap().push(listener);
    }

    pub fn on_consumer_started(&self, subscription_id: SubscriptionId) -> ConsumerStoppedGuard {
        self.listeners
            .lock()
            .unwrap()
            .iter()
            .for_each(|l| l.on_consumer_started(subscription_id));
        ConsumerStoppedGuard {
            subscription_id,
            listeners: Arc::clone(&self.listeners),
        }
    }

    pub fn on_stream_connected(
        &self,
        subscription_id: SubscriptionId,
        stream_id: StreamId,
    ) -> StreamEndGuard {
        self.listeners
            .lock()
            .unwrap()
            .iter()
            .for_each(|l| l.on_stream_connected(subscription_id, stream_id));
        StreamEndGuard {
            subscription_id,
            stream_id,
            listeners: Arc::clone(&self.listeners),
        }
    }
}

pub(crate) struct ConsumerStoppedGuard {
    subscription_id: SubscriptionId,
    listeners: Arc<Mutex<Vec<Box<dyn LifecycleListener>>>>,
}

impl Drop for ConsumerStoppedGuard {
    fn drop(&mut self) {
        self.listeners
            .lock()
            .unwrap()
            .iter()
            .for_each(|l| l.on_consumer_stopped(self.subscription_id));
    }
}

pub(crate) struct StreamEndGuard {
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    listeners: Arc<Mutex<Vec<Box<dyn LifecycleListener>>>>,
}

impl Drop for StreamEndGuard {
    fn drop(&mut self) {
        self.listeners
            .lock()
            .unwrap()
            .iter()
            .for_each(|l| l.on_stream_ended(self.subscription_id, self.stream_id));
    }
}

pub(crate) struct ControllerParams<C> {
    pub api_client: C,
    pub consumer_state: ConsumerState,
    pub handler_factory: Arc<dyn BatchHandlerFactory>,
    pub lifecycle_listeners: LifecycleListeners,
}

impl<C> ControllerParams<C> {
    pub fn config(&self) -> &Config {
        &self.consumer_state.config()
    }
}
