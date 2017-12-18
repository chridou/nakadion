use nakadi::batchline::BatchLine;

pub trait BatchProcessor<T: BatchLine> {
    fn more_batches_requested(&self) -> bool;
    fn process_batch(line: T);
    fn stop(&self);
}
