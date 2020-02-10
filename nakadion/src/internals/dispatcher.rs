use std::sync::Arc;

use futures::{Stream, TryFutureExt};

use crate::api::SubscriptionCommitApi;
use crate::consumer::{ConsumerError, DispatchStrategy};
use crate::event_handler::{BatchHandler, BatchHandlerFactory};
use crate::event_stream::BatchLine;
use crate::internals::StreamState;

#[derive(Debug)]
pub enum DispatcherMessage {
    Batch(BatchLine),
    Tick,
}

pub struct Dispatcher;

impl Dispatcher {
    pub fn new<H, C>(
        strategy: DispatchStrategy,
        handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
        api_client: C,
    ) -> SleepingDispatcher<H, C>
    where
        H: BatchHandler,
        C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
    {
        match strategy {
            DispatchStrategy::SingleWorker => SleepingDispatcher::SingleWorker(
                self::dispatch_single::Dispatcher::new(handler_factory, api_client),
            ),
            _ => panic!("not supported"),
        }
    }
}

pub enum SleepingDispatcher<H, C> {
    SingleWorker(dispatch_single::Sleeping<H, C>),
}

impl<H, C> SleepingDispatcher<H, C>
where
    H: BatchHandler,
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub fn start<S>(self, stream_state: StreamState, messages: S) -> ActiveDispatcher<H, C>
    where
        S: Stream<Item = DispatcherMessage> + Send + 'static,
    {
        match self {
            SleepingDispatcher::SingleWorker(dispatcher) => {
                ActiveDispatcher::SingleWorker(dispatcher.start(stream_state, messages))
            }
            _ => panic!("not supported"),
        }
    }

    pub fn tick(&mut self) {
        match self {
            SleepingDispatcher::SingleWorker(ref mut dispatcher) => dispatcher.tick(),
            _ => panic!("not supported"),
        }
    }
}

pub enum ActiveDispatcher<H, C> {
    SingleWorker(dispatch_single::Active<H, C>),
}

impl<H, C> ActiveDispatcher<H, C>
where
    H: BatchHandler,
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub async fn join(self) -> Result<SleepingDispatcher<H, C>, ConsumerError> {
        match self {
            ActiveDispatcher::SingleWorker(dispatcher) => {
                dispatcher
                    .join()
                    .map_ok(|d| SleepingDispatcher::SingleWorker(d))
                    .await
            }
            _ => panic!("not supported"),
        }
    }
}

mod dispatch_single {
    use std::sync::Arc;

    use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};

    use crate::api::SubscriptionCommitApi;
    use crate::consumer::ConsumerError;
    use crate::event_handler::{BatchHandler, BatchHandlerFactory, HandlerAssignment};
    use crate::internals::{committer::*, worker::*, StreamState};

    use super::DispatcherMessage;

    pub struct Dispatcher;

    impl Dispatcher {
        pub fn new<H, C>(
            handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
            api_client: C,
        ) -> Sleeping<H, C>
        where
            H: BatchHandler,
            C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
        {
            let worker = Worker::new(handler_factory, HandlerAssignment::Unspecified);

            Sleeping { worker, api_client }
        }
    }

    pub struct Sleeping<H, C> {
        worker: SleepingWorker<H>,
        api_client: C,
    }

    impl<H, C> Sleeping<H, C>
    where
        H: BatchHandler,
        C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
    {
        pub fn start<S>(self, stream_state: StreamState, messages: S) -> Active<H, C>
        where
            S: Stream<Item = DispatcherMessage> + Send + 'static,
        {
            let Sleeping { worker, api_client } = self;

            let (committer, committer_join_handle) =
                Committer::start(api_client.clone(), stream_state.clone());

            let worker_stream = messages.map(|dm| match dm {
                DispatcherMessage::Batch(batch) => WorkerMessage::Batch(batch),
                DispatcherMessage::Tick => WorkerMessage::Tick,
            });

            let active_worker = worker.start(stream_state.clone(), committer, worker_stream);

            let join = active_worker.join().boxed();

            Active { api_client, join }
        }

        pub fn tick(&mut self) {
            self.worker.tick()
        }
    }

    pub struct Active<H, C> {
        api_client: C,
        join: BoxFuture<'static, Result<SleepingWorker<H>, ConsumerError>>,
    }

    impl<H, C> Active<H, C>
    where
        H: BatchHandler,
        C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
    {
        pub async fn join(self) -> Result<Sleeping<H, C>, ConsumerError> {
            let Active { api_client, join } = self;

            let worker = join.await?;

            Ok(Sleeping { worker, api_client })
        }
    }
}

mod dispatch_event_type {}

mod dispatch_event_type_partition {}
