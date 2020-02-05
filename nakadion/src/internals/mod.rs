use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use crate::nakadi_types::model::subscription::StreamId;

pub mod committer;
pub mod consumer;
pub mod controller;
pub mod worker;

#[derive(Clone)]
pub struct ConsumerState {
    is_globally_cancelled: Arc<AtomicBool>,
}

impl ConsumerState {
    pub fn new() -> Self {
        Self {
            is_globally_cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn stream_state(&self, stream_id: StreamId) -> StreamState {
        StreamState::new(stream_id, Arc::downgrade(&self.is_globally_cancelled))
    }

    pub fn request_global_cancellation(&self) {
        self.is_globally_cancelled.store(true, Ordering::SeqCst)
    }

    pub fn global_cancellation_requested(&self) -> bool {
        self.is_globally_cancelled.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
pub struct StreamState {
    stream_id: StreamId,
    is_cancelled: Arc<AtomicBool>,
    is_globally_cancelled: Weak<AtomicBool>,
}

impl StreamState {
    pub fn new(stream_id: StreamId, is_globally_cancelled: Weak<AtomicBool>) -> Self {
        Self {
            stream_id,
            is_cancelled: Arc::new(AtomicBool::new(false)),
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
}
