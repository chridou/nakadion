use std::thread;
use std::time::{Duration, Instant};

use nakadi::{AbortHandle, BatchHandler, HandlerFactory};
use nakadi::batch::Batch;
use nakadi::connector::{NakadiStreamConnector, StreamConnector};
use nakadi::model::StreamId;

const CONNECT_RETRY_BACKOFF: &'static [u64] =
    &[1, 1, 1, 1, 3, 3, 3, 5, 5, 5, 10, 10, 10, 15, 15, 15];

pub type NakadiConsumer<HF> = Consumer<HF, NakadiStreamConnector>;

/// The consumer connects to the stream and sends batch lines to the processor.
pub struct Consumer<C: StreamConnector, HF: HandlerFactory> {
    connector: C,
    handler_factory: HF,
    abort_handle: AbortHandle,
}

impl<C: StreamConnector, HF: HandlerFactory> Consumer<C, HF> {
    pub fn new(connector: C, handler_factory: HF) -> Consumer<C, HF> {
        Consumer {
            connector,
            handler_factory,
            abort_handle: Default::default(),
        }
    }

    /// Blocks until aborted
    pub fn run(&self) {
        while !self.abort_handle.abort_requested() {}
    }

    fn connect(&self, max_dur: Duration) -> Result<(StreamId, C::LineIterator), String> {
        let deadline = Instant::now() + max_dur;
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.connector.connect() {
                Ok(it) => return Ok(it),
                Err(err) => {
                    let sleep_dur_secs = *CONNECT_RETRY_BACKOFF.get(attempt).unwrap_or(&30);
                    if Instant::now() >= deadline {
                        return Err("Failed to connect to Nakadi after {} attempts.".to_string());
                    } else if self.abort_handle.abort_requested() {
                        return Err(
                            "Failed to connect to Nakadi after {} attempts. Abort requested"
                                .to_string(),
                        );
                    } else {
                        warn!(
                            "Failed to connect(attempt {}) to Nakadi(retry in {} seconds): {}",
                            attempt, sleep_dur_secs, err
                        );
                        thread::sleep(Duration::from_secs(sleep_dur_secs));
                    }
                }
            }
        }
    }
}

pub enum BatchError {
    SomeError,
}
