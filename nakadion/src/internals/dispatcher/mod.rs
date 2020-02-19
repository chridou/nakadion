use std::sync::Arc;

use futures::{Stream, TryFutureExt};

use crate::api::SubscriptionCommitApi;
use crate::consumer::{Config, DispatchStrategy};
use crate::event_stream::BatchLine;
use crate::handler::BatchHandlerFactory;
use crate::internals::{EnrichedResult, StreamState};
use crate::logging::Logs;

mod disp_all_seq;
mod disp_et_par;
mod disp_etp_par;

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
            DispatchStrategy::AllSeq => SleepingDispatcher::AllSeq(
                self::disp_all_seq::Dispatcher::sleeping(handler_factory, api_client, config),
            ),
            DispatchStrategy::EventTypePar => SleepingDispatcher::EventTypePar(
                self::disp_et_par::Dispatcher::sleeping(handler_factory, api_client),
            ),
            DispatchStrategy::EventTypePartitionPar => SleepingDispatcher::EventTypePartitionPar(
                self::disp_etp_par::Dispatcher::sleeping(handler_factory, api_client),
            ),
            _ => SleepingDispatcher::EventTypePar(self::disp_et_par::Dispatcher::sleeping(
                handler_factory,
                api_client,
            )),
        }
    }
}

pub(crate) enum SleepingDispatcher<C> {
    AllSeq(disp_all_seq::Sleeping<C>),
    EventTypePar(disp_et_par::Sleeping<C>),
    EventTypePartitionPar(disp_etp_par::Sleeping<C>),
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
            SleepingDispatcher::AllSeq(dispatcher) => {
                ActiveDispatcher::AllSeq(dispatcher.start(stream_state, messages))
            }
            SleepingDispatcher::EventTypePar(dispatcher) => {
                ActiveDispatcher::EventTypePar(dispatcher.start(stream_state, messages))
            }
            SleepingDispatcher::EventTypePartitionPar(dispatcher) => {
                ActiveDispatcher::EventTypePartitionPar(dispatcher.start(stream_state, messages))
            }
        }
    }
}

impl<C> SleepingDispatcher<C> {
    pub fn tick(&mut self) {
        match self {
            SleepingDispatcher::AllSeq(ref mut dispatcher) => dispatcher.tick(),
            SleepingDispatcher::EventTypePar(ref mut dispatcher) => dispatcher.tick(),
            SleepingDispatcher::EventTypePartitionPar(ref mut dispatcher) => dispatcher.tick(),
        }
    }
}

pub(crate) enum ActiveDispatcher<'a, C> {
    AllSeq(disp_all_seq::Active<'a, C>),
    EventTypePar(disp_et_par::Active<'a, C>),
    EventTypePartitionPar(disp_etp_par::Active<'a, C>),
}

impl<'a, C> ActiveDispatcher<'a, C>
where
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub async fn join(self) -> EnrichedResult<SleepingDispatcher<C>> {
        match self {
            ActiveDispatcher::AllSeq(dispatcher) => {
                dispatcher
                    .join()
                    .map_ok(|enr_dispatcher| enr_dispatcher.map(SleepingDispatcher::AllSeq))
                    .await
            }
            ActiveDispatcher::EventTypePar(dispatcher) => {
                dispatcher
                    .join()
                    .map_ok(|enr_dispatcher| enr_dispatcher.map(SleepingDispatcher::EventTypePar))
                    .await
            }
            ActiveDispatcher::EventTypePartitionPar(dispatcher) => {
                dispatcher
                    .join()
                    .map_ok(|enr_dispatcher| {
                        enr_dispatcher.map(SleepingDispatcher::EventTypePartitionPar)
                    })
                    .await
            }
        }
    }
}
