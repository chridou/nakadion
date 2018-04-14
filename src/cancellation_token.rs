use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nakadi::metrics::*;

pub struct CancellationTokenSource {
    cancellation_requested: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    metrics_collector: Arc<MetricsCollector + Sync + Send + 'static>,
}

impl Drop for CancellationTokenSource {
    fn drop(&mut self) {
        self.request_cancellation()
    }
}

impl CancellationTokenSource {
    pub fn new<M>(metrics_collector: M) -> Self
    where
        M: MetricsCollector + Sync + Send + 'static,
    {
        CancellationTokenSource::new_arc(Arc::new(metrics_collector))
    }

    pub fn new_arc<M>(metrics_collector: Arc<M>) -> Self
    where
        M: MetricsCollector + Sync + Send + 'static,
    {
        CancellationTokenSource {
            cancellation_requested: Arc::new(AtomicBool::new(false)),
            cancelled: Arc::new(AtomicBool::new(false)),
            metrics_collector: metrics_collector,
        }
    }

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
            metrics_collector: self.metrics_collector.clone(),
        }
    }
}

impl Default for CancellationTokenSource {
    fn default() -> Self {
        CancellationTokenSource {
            cancellation_requested: Arc::new(AtomicBool::new(false)),
            cancelled: Arc::new(AtomicBool::new(false)),
            metrics_collector: Arc::new(DevNullMetricsCollector),
        }
    }
}

pub trait CancellationToken {
    fn cancellation_requested(&self) -> bool;
    fn cancelled(&self);
}

#[derive(Clone)]
pub struct AutoCancellationToken {
    cancellation_requested: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    metrics_collector: Arc<MetricsCollector + Send + Sync + 'static>,
}

impl Drop for AutoCancellationToken {
    fn drop(&mut self) {
        self.cancelled();
        if ::std::thread::panicking() {
            self.metrics_collector.other_panicked();
            error!(
                "Abnormal cancellation due to a panic on thread '{}'!",
                ::std::thread::current().name().unwrap_or("<unnamed>")
            );
        };
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
