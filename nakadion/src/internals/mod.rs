use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub mod consumer;

#[derive(Clone)]
pub struct KillSwitch(Arc<AtomicBool>);

impl KillSwitch {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn is_pushed(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    pub fn push(&self) {
        self.0.store(true, Ordering::SeqCst)
    }
}
