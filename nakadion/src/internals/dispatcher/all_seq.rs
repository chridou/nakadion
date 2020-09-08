//! Dispatch all events on the same worker sequentially
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use std::sync::Arc;

use crate::api::SubscriptionCommitApi;
use crate::consumer::Config;
use crate::handler::{BatchHandlerFactory, HandlerAssignment};
use crate::internals::{
    background_committer::start_committer, worker::*, ConsumptionResult, StreamState,
};
use crate::logging::Logger;

use super::DispatcherMessage;

pub struct Dispatcher;

impl Dispatcher {
    pub(crate) fn sleeping<C>(
        handler_factory: Arc<dyn BatchHandlerFactory>,
        api_client: C,
        config: Config,
    ) -> Sleeping<C>
    where
        C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
    {
        let worker = Worker::sleeping(
            handler_factory,
            HandlerAssignment::Unspecified,
            config.handler_inactivity_timeout,
        );

        Sleeping { worker, api_client }
    }
}

pub(crate) struct Sleeping<C> {
    worker: SleepingWorker,
    api_client: C,
}

impl<C> Sleeping<C>
where
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub fn start<S>(self, stream_state: StreamState, messages: S) -> Active<'static, C>
    where
        S: Stream<Item = DispatcherMessage> + Send + 'static,
    {
        let Sleeping { worker, api_client } = self;

        let (committer, committer_join_handle) =
            start_committer(api_client.clone(), stream_state.clone());

        let worker_stream = messages.map(move |dm| match dm {
            DispatcherMessage::BatchWithEvents(_, batch) => WorkerMessage::BatchWithEvents(batch),
            DispatcherMessage::Tick(timestamp) => WorkerMessage::Tick(timestamp),
        });

        let active_worker = worker.start(stream_state.clone(), committer, worker_stream);

        let join = {
            let stream_state = stream_state.clone();
            async move {
                let worker_result = active_worker.join().await;
                stream_state.debug(format_args!("Waiting for committer to shut down"));
                if let Err(err) = committer_join_handle.await {
                    stream_state.warn(format_args!("Committer exited with error: {}", err));
                } else {
                    stream_state.debug(format_args!("Committer shut down"));
                };
                worker_result.map(|mut worker| {
                    worker.tick();
                    worker
                })
            }
            .boxed()
        };

        Active {
            api_client,
            join,
            stream_state,
        }
    }
}

impl<C> Sleeping<C> {
    pub fn tick(&mut self) {
        self.worker.tick()
    }
}
pub(crate) struct Active<'a, C> {
    stream_state: StreamState,
    api_client: C,
    join: BoxFuture<'a, ConsumptionResult<SleepingWorker>>,
}

impl<'a, C> Active<'a, C>
where
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub async fn join(self) -> ConsumptionResult<Sleeping<C>> {
        let Active {
            api_client,
            join,
            stream_state,
        } = self;

        stream_state.debug(format_args!("Waiting for worker to fall asleep"));

        let worker = join.await?;

        stream_state.debug(format_args!("Dispatcher going to sleep"));

        Ok(Sleeping { worker, api_client })
    }
}
