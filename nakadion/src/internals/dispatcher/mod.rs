use std::sync::Arc;

use futures::{Stream, TryFutureExt};

use crate::api::SubscriptionCommitApi;
use crate::consumer::{Config, DispatchStrategy};
use crate::event_stream::BatchLine;
use crate::handler::BatchHandlerFactory;
use crate::internals::{EnrichedResult, StreamState};
use crate::logging::Logs;

mod dispatch_event_type;
mod dispatch_event_type_partition;
mod no_dispatch;

#[derive(Debug)]
pub enum DispatcherMessage {
    Batch(BatchLine),
    Tick,
    StreamEnded,
}

impl DispatcherMessage {
    pub fn is_batch(&self) -> bool {
        match self {
            DispatcherMessage::Batch(_) => true,
            _ => false,
        }
    }
}

pub(crate) struct Dispatcher;

impl Dispatcher {
    pub fn sleeping<C>(
        strategy: DispatchStrategy,
        handler_factory: Arc<dyn BatchHandlerFactory>,
        api_client: C,
        config: Config,
    ) -> SleepingDispatcher<C>
    where
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

pub(crate) enum SleepingDispatcher<C> {
    SingleWorker(no_dispatch::Sleeping<C>),
}

impl<C> SleepingDispatcher<C>
where
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub fn start<S>(self, stream_state: StreamState, messages: S) -> ActiveDispatcher<'static, C>
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

impl<C> SleepingDispatcher<C> {
    pub fn tick(&mut self) {
        match self {
            SleepingDispatcher::SingleWorker(ref mut dispatcher) => dispatcher.tick(),
        }
    }
}

pub(crate) enum ActiveDispatcher<'a, C> {
    SingleWorker(no_dispatch::Active<'a, C>),
}

impl<'a, C> ActiveDispatcher<'a, C>
where
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub async fn join(self) -> EnrichedResult<SleepingDispatcher<C>> {
        match self {
            ActiveDispatcher::SingleWorker(dispatcher) => {
                dispatcher
                    .join()
                    .map_ok(|enr_dispatcher| enr_dispatcher.map(SleepingDispatcher::SingleWorker))
                    .await
            }
            _ => panic!("not supported"),
        }
    }
}
