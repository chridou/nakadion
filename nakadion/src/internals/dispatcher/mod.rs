use std::sync::Arc;

use futures::{Stream, TryFutureExt};

use crate::api::SubscriptionCommitApi;
use crate::consumer::{Config, ConsumerError, DispatchStrategy};
use crate::event_stream::BatchLine;
use crate::handler::{BatchHandler, BatchHandlerFactory};
use crate::internals::StreamState;
use crate::logging::Logs;

mod dispatch_event_type;
mod dispatch_event_type_partition;
mod no_dispatch;

#[derive(Debug)]
pub enum DispatcherMessage {
    Batch(BatchLine),
    Tick,
}

pub(crate) struct Dispatcher;

impl Dispatcher {
    pub fn sleeping<H, C>(
        strategy: DispatchStrategy,
        handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
        api_client: C,
        config: Config,
    ) -> SleepingDispatcher<H, C>
    where
        H: BatchHandler,
        C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
    {
        match strategy {
            DispatchStrategy::AllSequential => SleepingDispatcher::SingleWorker(
                self::no_dispatch::Dispatcher::sleeping(handler_factory, api_client, config),
            ),
            _ => SleepingDispatcher::SingleWorker(self::no_dispatch::Dispatcher::sleeping(
                handler_factory,
                api_client,
                config,
            )),
        }
    }
}

pub(crate) enum SleepingDispatcher<H, C> {
    SingleWorker(no_dispatch::Sleeping<H, C>),
}

impl<H, C> SleepingDispatcher<H, C>
where
    H: BatchHandler,
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub fn start<S>(self, stream_state: StreamState, messages: S) -> ActiveDispatcher<'static, H, C>
    where
        S: Stream<Item = DispatcherMessage> + Send + 'static,
    {
        stream_state.debug(format_args!("Dispatcher starting"));
        match self {
            SleepingDispatcher::SingleWorker(dispatcher) => {
                ActiveDispatcher::SingleWorker(dispatcher.start(stream_state, messages))
            }
        }
    }
}

impl<H, C> SleepingDispatcher<H, C>
where
    H: BatchHandler,
{
    pub fn tick(&mut self) {
        match self {
            SleepingDispatcher::SingleWorker(ref mut dispatcher) => dispatcher.tick(),
        }
    }
}

pub(crate) enum ActiveDispatcher<'a, H, C> {
    SingleWorker(no_dispatch::Active<'a, H, C>),
}

impl<'a, H, C> ActiveDispatcher<'a, H, C>
where
    H: BatchHandler,
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub async fn join(self) -> Result<SleepingDispatcher<H, C>, ConsumerError> {
        match self {
            ActiveDispatcher::SingleWorker(dispatcher) => {
                dispatcher
                    .join()
                    .map_ok(SleepingDispatcher::SingleWorker)
                    .await
            }
            _ => panic!("not supported"),
        }
    }
}
