use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::thread;
use std::time::Duration;

pub mod consumer;
pub mod model;
pub mod connector;
pub mod committer;

#[derive(Clone)]
pub struct AbortHandle {
    state: Arc<AtomicBool>,
    is_committer_stopped: Arc<AtomicBool>,
    is_processor_stopped: Arc<AtomicBool>,
}

impl AbortHandle {
    pub fn abort_requested(&self) -> bool {
        self.state.load(Ordering::Relaxed)
    }

    pub fn request_abort(&self) {
        warn!("Abort requested");
        self.state.store(true, Ordering::Relaxed)
    }

    pub fn mark_committer_stopped(&self) {
        self.is_committer_stopped.store(true, Ordering::Relaxed)
    }

    pub fn mark_processor_stopped(&self) {
        self.is_processor_stopped.store(true, Ordering::Relaxed)
    }

    pub fn all_stopped(&self) -> bool {
        self.is_committer_stopped.load(Ordering::Relaxed)
            && self.is_processor_stopped.load(Ordering::Relaxed)
    }

    pub fn wait_for_all_stopped(&self) {
        while !self.all_stopped() {
            thread::sleep(Duration::from_millis(100))
        }
    }
}

impl Default for AbortHandle {
    fn default() -> AbortHandle {
        AbortHandle {
            state: Arc::new(AtomicBool::new(false)),
            is_committer_stopped: Arc::new(AtomicBool::new(false)),
            is_processor_stopped: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone)]
pub struct InFlightCounter {
    in_flight: Arc<AtomicIsize>,
}

impl InFlightCounter {
    pub fn inc(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn limit_reached(&self, limit: isize) -> bool {
        self.in_flight.load(Ordering::Relaxed) > limit
    }
}

impl Default for InFlightCounter {
    fn default() -> InFlightCounter {
        InFlightCounter {
            in_flight: Arc::new(AtomicIsize::new(0)),
        }
    }
}
