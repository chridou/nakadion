use std::sync::Arc;

use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};

use crate::api::SubscriptionCommitApi;
use crate::consumer::{Config, ConsumerError};
use crate::handler::{BatchHandler, BatchHandlerFactory, HandlerAssignment};
use crate::internals::{committer::*, worker::*, StreamState};
use crate::logging::Logs;

use super::DispatcherMessage;

pub struct Dispatcher;

impl Dispatcher {
    pub(crate) fn sleeping<H, C>(
        handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
        api_client: C,
        config: Config,
    ) -> Sleeping<H, C>
    where
        H: BatchHandler,
        C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
    {
        let worker = Worker::sleeping(
            handler_factory,
            HandlerAssignment::Unspecified,
            config.inactivity_timeout,
        );

        Sleeping { worker, api_client }
    }
}

pub(crate) struct Sleeping<H, C> {
    worker: SleepingWorker<H>,
    api_client: C,
}

impl<H, C> Sleeping<H, C>
where
    H: BatchHandler,
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub fn start<S>(self, stream_state: StreamState, messages: S) -> Active<'static, H, C>
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

        let join = {
            let stream_state = stream_state.clone();
            async move {
                let worker_result = active_worker.join().await;
                if let Err(err) = committer_join_handle.await {
                    stream_state.warn(format_args!("Committer exited with error: {}", err));
                };
                worker_result.map(|mut w| {
                    w.tick();
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

impl<H, C> Sleeping<H, C>
where
    H: BatchHandler,
{
    pub fn tick(&mut self) {
        self.worker.tick()
    }
}
pub(crate) struct Active<'a, H, C> {
    stream_state: StreamState,
    api_client: C,
    join: BoxFuture<'a, Result<SleepingWorker<H>, ConsumerError>>,
}

impl<'a, H, C> Active<'a, H, C>
where
    H: BatchHandler,
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub async fn join(self) -> Result<Sleeping<H, C>, ConsumerError> {
        let Active {
            api_client,
            join,
            stream_state,
        } = self;

        let worker = join.await?;

        stream_state.debug(format_args!("Dispatcher going to sleep"));

        Ok(Sleeping { worker, api_client })
    }
}
