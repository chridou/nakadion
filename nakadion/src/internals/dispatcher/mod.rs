use std::sync::Arc;

use futures::{Stream, TryFutureExt};

use crate::api::SubscriptionCommitApi;
use crate::consumer::{Config, DispatchStrategy};
use crate::event_stream::BatchLine;
use crate::handler::BatchHandlerFactory;
use crate::internals::{EnrichedResult, StreamState};
use crate::logging::Logs;

mod dispatch_all_sequential;
mod dispatch_event_type;
mod dispatch_event_type_partition;

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
            DispatchStrategy::AllSequential => SleepingDispatcher::AllSequential(
                self::dispatch_all_sequential::Dispatcher::sleeping(
                    handler_factory,
                    api_client,
                    config,
                ),
            ),
            DispatchStrategy::EventTypeSequential => SleepingDispatcher::EventTypeSequential(
                self::dispatch_event_type::Dispatcher::sleeping(handler_factory, api_client),
            ),
            _ => SleepingDispatcher::AllSequential(
                self::dispatch_all_sequential::Dispatcher::sleeping(
                    handler_factory,
                    api_client,
                    config,
                ),
            ),
        }
    }
}

pub(crate) enum SleepingDispatcher<C> {
    AllSequential(dispatch_all_sequential::Sleeping<C>),
    EventTypeSequential(dispatch_event_type::Sleeping<C>),
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
            SleepingDispatcher::AllSequential(dispatcher) => {
                ActiveDispatcher::AllSequential(dispatcher.start(stream_state, messages))
            }
            SleepingDispatcher::EventTypeSequential(dispatcher) => {
                ActiveDispatcher::EventTypeSequential(dispatcher.start(stream_state, messages))
            }
        }
    }
}

impl<C> SleepingDispatcher<C> {
    pub fn tick(&mut self) {
        match self {
            SleepingDispatcher::AllSequential(ref mut dispatcher) => dispatcher.tick(),
            SleepingDispatcher::EventTypeSequential(ref mut dispatcher) => dispatcher.tick(),
        }
    }
}

pub(crate) enum ActiveDispatcher<'a, C> {
    AllSequential(dispatch_all_sequential::Active<'a, C>),
    EventTypeSequential(dispatch_event_type::Active<'a, C>),
}

impl<'a, C> ActiveDispatcher<'a, C>
where
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub async fn join(self) -> EnrichedResult<SleepingDispatcher<C>> {
        match self {
            ActiveDispatcher::AllSequential(dispatcher) => {
                dispatcher
                    .join()
                    .map_ok(|enr_dispatcher| enr_dispatcher.map(SleepingDispatcher::AllSequential))
                    .await
            }
            ActiveDispatcher::EventTypeSequential(dispatcher) => {
                dispatcher
                    .join()
                    .map_ok(|enr_dispatcher| {
                        enr_dispatcher.map(SleepingDispatcher::EventTypeSequential)
                    })
                    .await
            }
        }
    }
}
