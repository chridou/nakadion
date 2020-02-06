use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use crate::nakadi_types::model::subscription::{StreamId, StreamParameters, SubscriptionId};

pub mod committer;
pub mod controller;
pub mod dispatcher;
pub mod worker;

#[derive(Clone)]
pub struct ConsumerState {
    is_globally_cancelled: Arc<AtomicBool>,
    stream_params: Arc<StreamParameters>,
    subscription_id: SubscriptionId,
}

impl ConsumerState {
    pub fn new(subscription_id: SubscriptionId, stream_params: StreamParameters) -> Self {
        Self {
            is_globally_cancelled: Arc::new(AtomicBool::new(false)),
            stream_params: Arc::new(stream_params),
            subscription_id,
        }
    }

    pub fn stream_state(&self, stream_id: StreamId) -> StreamState {
        StreamState::new(
            stream_id,
            self.subscription_id,
            Arc::clone(&self.stream_params),
            Arc::downgrade(&self.is_globally_cancelled),
        )
    }

    pub fn request_global_cancellation(&self) {
        self.is_globally_cancelled.store(true, Ordering::SeqCst)
    }

    pub fn global_cancellation_requested(&self) -> bool {
        self.is_globally_cancelled.load(Ordering::SeqCst)
    }

    pub fn subscription_id(&self) -> SubscriptionId {
        self.subscription_id
    }

    pub fn stream_params(&self) -> &StreamParameters {
        &self.stream_params
    }
}

#[derive(Clone)]
pub struct StreamState {
    stream_id: StreamId,
    subscription_id: SubscriptionId,
    stream_params: Arc<StreamParameters>,
    is_cancelled: Arc<AtomicBool>,
    is_globally_cancelled: Weak<AtomicBool>,
}

impl StreamState {
    pub fn new(
        stream_id: StreamId,
        subscription_id: SubscriptionId,
        stream_params: Arc<StreamParameters>,
        is_globally_cancelled: Weak<AtomicBool>,
    ) -> Self {
        Self {
            stream_id,
            subscription_id,
            is_cancelled: Arc::new(AtomicBool::new(false)),
            stream_params,
            is_globally_cancelled,
        }
    }

    pub fn cancellation_requested(&self) -> bool {
        if self.is_cancelled.load(Ordering::SeqCst) {
            return true;
        }

        if let Some(is_globally_cancelled) = self.is_globally_cancelled.upgrade() {
            is_globally_cancelled.load(Ordering::SeqCst)
        } else {
            true
        }
    }

    pub fn request_stream_cancellation(&self) {
        self.is_cancelled.store(true, Ordering::SeqCst)
    }

    pub fn request_global_cancellation(&self) {
        if let Some(is_globally_cancelled) = self.is_globally_cancelled.upgrade() {
            is_globally_cancelled.store(true, Ordering::SeqCst)
        }
    }

    pub fn subscription_id(&self) -> SubscriptionId {
        self.subscription_id
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn stream_params(&self) -> &StreamParameters {
        &self.stream_params
    }
}
