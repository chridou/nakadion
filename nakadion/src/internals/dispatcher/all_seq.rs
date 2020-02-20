use std::sync::Arc;

use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};

use crate::api::SubscriptionCommitApi;
use crate::consumer::Config;
use crate::handler::{BatchHandlerFactory, HandlerAssignment};
use crate::internals::{committer::*, worker::*, EnrichedResult, StreamState};
use crate::logging::Logs;

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
            Committer::start(api_client.clone(), stream_state.clone());

        let worker_stream = messages.map(|dm| match dm {
            DispatcherMessage::Batch(batch) => WorkerMessage::Batch(batch),
            DispatcherMessage::Tick(timestamp) => WorkerMessage::Tick(timestamp),
            DispatcherMessage::StreamEnded => WorkerMessage::StreamEnded,
        });

        let active_worker = worker.start(stream_state.clone(), committer, worker_stream);

        let join = {
            let stream_state = stream_state.clone();
            async move {
                let worker_result = active_worker.join().await;
                if let Err(err) = committer_join_handle.await {
                    stream_state.warn(format_args!("Committer exited with error: {}", err));
                };
                worker_result.map(|mut w| {
                    w.payload.tick();
                    w
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
    join: BoxFuture<'a, EnrichedResult<SleepingWorker>>,
}

impl<'a, C> Active<'a, C>
where
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub async fn join(self) -> EnrichedResult<Sleeping<C>> {
        let Active {
            api_client,
            join,
            stream_state,
        } = self;

        stream_state.debug(format_args!("Waiting for worker to fall asleep"));

        let enriched_worker = join.await?;

        stream_state.debug(format_args!("Dispatcher going to sleep"));

        Ok(enriched_worker.map(|worker| Sleeping { worker, api_client }))
    }
}