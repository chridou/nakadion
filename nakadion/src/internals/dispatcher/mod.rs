use std::sync::Arc;
use std::time::Instant;

use futures::{Stream, TryFutureExt};

use crate::components::committer::ProvidesCommitter;
use crate::components::streams::BatchLine;
use crate::consumer::{Config, DispatchStrategy};
use crate::handler::BatchHandlerFactory;
use crate::internals::{EnrichedResult, StreamState};
use crate::logging::Logs;

mod all_seq;
mod par;

#[derive(Debug)]
pub enum DispatcherMessage {
    Batch(BatchLine),
    Tick(Instant),
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
        C: ProvidesCommitter + Send + Sync + Clone + 'static,
    {
        match strategy {
            DispatchStrategy::AllSeq => SleepingDispatcher::AllSeq(
                self::all_seq::Dispatcher::sleeping(handler_factory, api_client, config),
            ),
            DispatchStrategy::EventTypePar => SleepingDispatcher::EventTypePar(
                self::par::et_par::Dispatcher::sleeping(handler_factory, api_client),
            ),
            DispatchStrategy::EventTypePartitionPar => SleepingDispatcher::EventTypePartitionPar(
                self::par::etp_par::Dispatcher::sleeping(handler_factory, api_client),
            ),
            _ => SleepingDispatcher::EventTypePar(self::par::et_par::Dispatcher::sleeping(
                handler_factory,
                api_client,
            )),
        }
    }
}

pub(crate) enum SleepingDispatcher<C> {
    AllSeq(all_seq::Sleeping<C>),
    EventTypePar(par::et_par::Sleeping<C>),
    EventTypePartitionPar(par::etp_par::Sleeping<C>),
}

impl<C> SleepingDispatcher<C>
where
    C: ProvidesCommitter + Send + Sync + Clone + 'static,
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
    AllSeq(all_seq::Active<'a, C>),
    EventTypePar(par::et_par::Active<'a, C>),
    EventTypePartitionPar(par::etp_par::Active<'a, C>),
}

impl<'a, C> ActiveDispatcher<'a, C>
where
    C: ProvidesCommitter + Send + Sync + Clone + 'static,
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
