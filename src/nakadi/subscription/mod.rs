use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub mod consumer;
pub mod model;
pub mod connector;
pub mod committer;

#[derive(Clone)]
pub struct AbortHandle {
    state: Arc<AtomicBool>,
}

impl AbortHandle {
    pub fn is_abort_requested(&self) -> bool {
        self.state.load(Ordering::Relaxed)
    }

    pub fn request_abort(&self) {
        warn!("Abort requested");
        self.state.store(true, Ordering::Relaxed)
    }
}

impl Default for AbortHandle {
    fn default() -> AbortHandle {
        AbortHandle {
            state: Arc::new(AtomicBool::new(false)),
        }
    }
}
