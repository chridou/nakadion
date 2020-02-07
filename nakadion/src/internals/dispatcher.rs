pub enum DispatchStrategy {
    Single,
    EventType,
    EventTypePartition,
}

pub struct SleepingDispatcher;

pub struct ActiveDispatcher;

mod dispatch_single {
    use std::sync::Arc;

    use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
    use tokio::task::JoinHandle;

    use crate::api::SubscriptionCommitApi;
    use crate::consumer::ConsumerError;
    use crate::event_handler::{BatchHandler, BatchHandlerFactory};
    use crate::event_stream::BatchLine;
    use crate::internals::{committer::*, worker::*, StreamState};

    pub struct SingleWorkerDispatcher;

    impl SingleWorkerDispatcher {
        pub fn new<H, C>(
            handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
            api_client: C,
        ) -> Sleeping<H, C>
        where
            H: BatchHandler,
            C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
        {
            let worker = Worker::new(handler_factory, WorkerAssignment::single());

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
        pub fn start<S>(self, stream_state: StreamState, batch_lines: S) -> Active<H, C>
        where
            S: Stream<Item = BatchLine>,
        {
            let Sleeping { worker, api_client } = self;

            let (committer, committer_join_handle) =
                Committer::start(api_client.clone(), stream_state.clone());

            let active_worker = worker.start(stream_state.clone(), committer);

            let join = async move {
                /*while let Some(next) = batch_lines.next().await {
                    if stream_state.cancellation_requested() {
                        break;
                    }
                    active_worker.process(next);
                }

                let sleeping_worker = match active_worker.join().await {
                    Ok(sleeping_worker) => sleeping_worker,
                    Err(consumer_error) => {
                        stream_state.request_global_cancellation();
                        return Err(consumer_error);
                    }
                };
                let _ = committer_join_handle.await;
                Ok(sleeping_worker)*/
            }
            .boxed();

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

            Ok(Sleeping {
                worker
                api_client,
            })
        }
    }
}

mod dispatch_event_type {}

mod dispatch_event_type_partition {}
