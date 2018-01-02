use std::thread;
use std::time::Duration;

use nakadi::batch::{Batch, BatchLine};
use nakadi::subscription::connector::{NakadiStreamConnector, StreamConnector};
use nakadi::subscription::model::*;

const CONNECT_RETRY_BACKOFF: &'static [u64] =
    &[1, 1, 1, 1, 3, 3, 3, 5, 5, 5, 10, 10, 10, 15, 15, 15];

pub type NakadiSubscriptionConsumer<HF> = SubscriptionConsumer<HF, NakadiStreamConnector>;

pub struct SubscriptionConsumer<C: StreamConnector, HF: HandlerFactory> {
    connector: C,
    handler_factory: HF,
}

impl<C: StreamConnector, HF: HandlerFactory> SubscriptionConsumer<C, HF> {
    pub fn new(connector: C, handler_factory: HF) -> SubscriptionConsumer<C, HF> {
        SubscriptionConsumer {
            connector,
            handler_factory,
        }
    }

    fn connect(&self) -> (StreamId, C::LineIterator) {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.connector.connect() {
                Ok(it) => return it,
                Err(err) => {
                    let sleep_dur_secs = *CONNECT_RETRY_BACKOFF.get(attempt).unwrap_or(&30);
                    error!(
                        "Failed to connect to Nakadi(retry in {} seconds): {}",
                        sleep_dur_secs, err
                    );
                    thread::sleep(Duration::from_secs(sleep_dur_secs));
                }
            }
        }
    }
}

pub enum BatchError {
    SomeError,
}
