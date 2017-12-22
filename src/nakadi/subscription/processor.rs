use nakadi::batch::{Batch, BatchLine};
use nakadi::subscription::stream::{StreamConnector, NakadiStreamConnector};
use nakadi::subscription::model::*;

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
}


pub enum BatchError {
    SomeError,
}
