use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct CancellationTokenSource {
    cancellation_requested: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

impl Drop for CancellationTokenSource {
    fn drop(&mut self) {
        self.request_cancellation()
    }
}

impl CancellationTokenSource {
    pub fn request_cancellation(&self) {
        self.cancellation_requested.store(true, Ordering::Relaxed);
    }

    pub fn is_any_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    pub fn auto_token(&self) -> AutoCancellationToken {
        AutoCancellationToken {
            cancellation_requested: self.cancellation_requested.clone(),
            cancelled: self.cancelled.clone(),
        }
    }

    pub fn manual_token(&self) -> ManualCancellationToken {
        ManualCancellationToken {
            cancellation_requested: self.cancellation_requested.clone(),
            cancelled: self.cancelled.clone(),
        }
    }
}

impl Default for CancellationTokenSource {
    fn default() -> Self {
        CancellationTokenSource {
            cancellation_requested: Arc::new(AtomicBool::new(false)),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub trait CancellationToken {
    fn cancellation_requested(&self) -> bool;
    fn cancelled(&self);
}

pub struct ManualCancellationToken {
    cancellation_requested: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken for ManualCancellationToken {
    fn cancellation_requested(&self) -> bool {
        self.cancellation_requested.load(Ordering::Relaxed)
    }

    fn cancelled(&self) {
        self.cancelled.store(true, Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct AutoCancellationToken {
    cancellation_requested: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

impl Drop for AutoCancellationToken {
    fn drop(&mut self) {
        if ::std::thread::panicking() {
            error!(
                "Abnormal cancellation due to a panic on thread '{}'!",
                ::std::thread::current().name().unwrap_or("<unnamed>")
            );
        }
        self.cancelled()
    }
}

impl CancellationToken for AutoCancellationToken {
    fn cancellation_requested(&self) -> bool {
        self.cancellation_requested.load(Ordering::Relaxed)
    }

    fn cancelled(&self) {
        self.cancelled.store(true, Ordering::Relaxed)
    }
}
