//! Dispatch all events for the same event type on a single worker sequentially
use std::collections::BTreeMap;
use std::sync::Arc;

use futures::{future::BoxFuture, pin_mut, FutureExt, Stream, StreamExt};
use std::future::Future;

use crate::api::SubscriptionCommitApi;
use crate::consumer::ConsumerError;
use crate::handler::{BatchHandlerFactory, HandlerAssignment};
use crate::internals::{
    background_committer::{start_committer, CommitHandle},
    dispatcher::DispatcherMessage,
    worker::*,
    ConsumptionResult, StreamState,
};
use crate::logging::Logger;

use crate::nakadi_types::event_type::EventTypeName;

use super::BufferedWorker;

pub struct Dispatcher;

impl Dispatcher {
    pub(crate) fn sleeping<C>(
        handler_factory: Arc<dyn BatchHandlerFactory>,
        api_client: C,
    ) -> Sleeping<C>
    where
        C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
    {
        Sleeping {
            assignments: BTreeMap::default(),
            api_client,
            handler_factory,
        }
    }
}

pub(crate) struct Sleeping<C> {
    assignments: BTreeMap<String, SleepingWorker>,
    api_client: C,
    handler_factory: Arc<dyn BatchHandlerFactory>,
}

impl<C> Sleeping<C>
where
    C: SubscriptionCommitApi + Send + Sync + Clone + 'static,
{
    pub fn start<S>(self, stream_state: StreamState, messages: S) -> Active<'static, C>
    where
        S: Stream<Item = DispatcherMessage> + Send + 'static,
    {
        let Sleeping {
            assignments,
            api_client,
            handler_factory,
        } = self;

        let (committer, committer_join_handle) =
            start_committer(api_client.clone(), stream_state.clone());

        let join_workers = run(
            assignments,
            messages,
            stream_state.clone(),
            committer,
            Arc::clone(&handler_factory),
        );

        let join = {
            let stream_state = stream_state.clone();
            async move {
                let workers_result = join_workers.await;
                stream_state.debug(format_args!("Waiting for committer to shut down"));
                if let Err(err) = committer_join_handle.await {
                    stream_state.warn(format_args!("Committer exited with error: {}", err));
                };
                workers_result
            }
            .boxed()
        };

        Active {
            api_client,
            join,
            stream_state,
            handler_factory,
        }
    }
}

impl<C> Sleeping<C> {
    pub fn tick(&mut self) {
        self.assignments.values_mut().for_each(|w| w.tick())
    }
}

fn run<S>(
    assignments: BTreeMap<String, SleepingWorker>,
    stream: S,
    stream_state: StreamState,
    committer: CommitHandle,
    handler_factory: Arc<dyn BatchHandlerFactory>,
) -> impl Future<Output = ConsumptionResult<BTreeMap<String, SleepingWorker>>>
where
    S: Stream<Item = DispatcherMessage> + Send + 'static,
{
    let task = async move {
        let mut activated: BTreeMap<_, _> = assignments
            .into_iter()
            .map(|(event_type, sleeping_worker)| {
                (
                    event_type,
                    BufferedWorker::new(sleeping_worker, stream_state.clone(), committer.clone()),
                )
            })
            .collect();

        pin_mut!(stream);
        while let Some(next_message) = stream.next().await {
            if stream_state.cancellation_requested() {
                stream_state.debug(format_args!("[ET_PAR_DISPATCHER] Cancellation requested."));
                break;
            }
            let ((event_type, _partition), batch) = match next_message {
                DispatcherMessage::BatchWithEvents(etp, batch) => (etp.split(), batch),
                DispatcherMessage::Tick(timestamp) => {
                    activated.values().for_each(|w| {
                        w.process(WorkerMessage::Tick(timestamp));
                    });
                    continue;
                }
            };

            let event_type_str = event_type.as_str();
            let worker = if let Some(worker) = activated.get(event_type_str) {
                worker
            } else {
                stream_state.info(format_args!(
                    "Encountered new event type: {}",
                    event_type_str
                ));
                let assignment = HandlerAssignment::EventType(EventTypeName::new(event_type_str));
                let sleeping_worker = Worker::sleeping(
                    Arc::clone(&handler_factory),
                    assignment,
                    stream_state.config().handler_inactivity_timeout,
                );
                let worker =
                    BufferedWorker::new(sleeping_worker, stream_state.clone(), committer.clone());
                activated.insert(event_type_str.to_owned(), worker);
                activated.get(event_type_str).unwrap()
            };

            if !worker.process(WorkerMessage::BatchWithEvents(batch)) {
                stream_state.request_stream_cancellation();
                break;
            }
        }

        stream_state.debug(format_args!(
            "'et_par'-Dispatcher loop exited. Waiting for {} workers to fall asleep.",
            activated.len()
        ));

        let mut consumer_error_ocurred = false;
        let mut sleeping_workers: BTreeMap<String, SleepingWorker> = BTreeMap::default();
        for (event_type, worker) in activated {
            match worker.join().await {
                Ok(sleeping_worker) => {
                    sleeping_workers.insert(event_type, sleeping_worker);
                }
                Err(err) => {
                    consumer_error_ocurred = true;
                    stream_state.error(format_args!(
                        "worker for event type {} joined with an error: {}",
                        event_type, err
                    ));
                }
            }
        }

        stream_state.debug(format_args!("'et_par'-Dispatcher: All workers sleeping.",));

        if consumer_error_ocurred {
            Err(ConsumerError::internal().with_message("At least one worker failed to join."))
        } else {
            Ok(sleeping_workers)
        }
    };

    let join_handle = tokio::spawn(task);

    async { join_handle.await? }
}

pub(crate) struct Active<'a, C> {
    stream_state: StreamState,
    api_client: C,
    handler_factory: Arc<dyn BatchHandlerFactory>,
    join: BoxFuture<'a, ConsumptionResult<BTreeMap<String, SleepingWorker>>>,
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
            handler_factory,
        } = self;

        stream_state.debug(format_args!("Waiting for worker to fall asleep"));

        let assignments = join.await?;

        stream_state.debug(format_args!("Dispatcher going to sleep"));

        Ok(Sleeping {
            assignments,
            api_client,
            handler_factory,
        })
    }
}
