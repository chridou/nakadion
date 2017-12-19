use nakadi::batch::{Batch, BatchLine};

pub trait BatchProcessor<T: BatchLine> {
    fn more_batches_requested(&self) -> bool;
    fn process_batch(batch: Batch<T>) -> Result<(), BatchError>;
    fn stop(&self);
}

pub enum BatchError {
    SomeError,
}
