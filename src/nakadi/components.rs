pub trait BatchProcessor {
    fn more_batches_requested(&self) -> bool;
    fn process_batch(line: Vec<u8>);

    fn stop(&self);
}
