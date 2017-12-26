use std::thread;
use std::time::Duration;

use nakadi::batch::{Batch, BatchLine};
use nakadi::subscription::connector::{StreamConnector, NakadiStreamConnector};
use nakadi::subscription::model::*;

const CONNECT_RETRY_BACKOFF: &'static [u64] = &[1,1,2,3,5,10,20,30] ;

pub type NakadiSubscriptionConsumer<HF> = SubscriptionConsumer<HF, NakadiStreamConnector>;

pub struct SubscriptionConsumer<C, HF> {
    connector: C,
    handler_factory: HF,
}

impl<C: StreamConnector, HF: HandlerFactory> SubscriptionConsumer<C, HF> {
    pub fn new(connector: C, handler_factory: HF) -> SubscriptionConsumer<C, HF> 
    {
        SubscriptionConsumer {
            connector,
            handler_factory,
        }
    }

    fn connect(&self) -> (StreamId,C::LineIterator) {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.connector.connect() {
                Ok(it) => return it,
                Err(err) => {
                    error!("Failed to connect to Nakadi: {}", err);
                    let sleep_dur = *CONNECT_RETRY_BACKOFF.get(attempt).unwrap_or(&30);
                    thread::sleep(Duration::from_secs(sleep_dur));
                }
            }
        }
    }
}


pub enum BatchError {
    SomeError,
}
