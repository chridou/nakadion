//! Dispatch all events for the same event type and partition on a single worker sequentially
use std::collections::BTreeMap;
use std::sync::Arc;

use futures::{
    future::{BoxFuture, TryFutureExt},
    pin_mut, FutureExt, Stream, StreamExt,
};
use std::future::Future;

use crate::api::SubscriptionCommitApi;
use crate::components::committer::{CommitHandle, Committer};
use crate::consumer::ConsumerError;
use crate::handler::{BatchHandlerFactory, HandlerAssignment};
use crate::internals::{
    dispatcher::DispatcherMessage, worker::*, EnrichedErr, EnrichedOk, EnrichedResult, StreamState,
};
use crate::logging::Logger;

use crate::nakadi_types::subscription::EventTypePartition;

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
    assignments: BTreeMap<EventTypePartition, SleepingWorker>,
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

        let mut committer = Committer::new(
            api_client.clone(),
            stream_state.subscription_id(),
            stream_state.stream_id(),
        );
        committer.set_config(stream_state.config.commit_config.clone());
        committer.set_logger(stream_state.clone());
        committer.set_instrumentation(stream_state.instrumentation().clone());

        let (committer, committer_join_handle) = committer.run();

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
    assignments: BTreeMap<EventTypePartition, SleepingWorker>,
    stream: S,
    stream_state: StreamState,
    committer: CommitHandle,
    handler_factory: Arc<dyn BatchHandlerFactory>,
) -> impl Future<Output = EnrichedResult<BTreeMap<EventTypePartition, SleepingWorker>>>
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
                stream_state.info(format_args!("Cancellation requested"));
                break;
            }
            let (event_type_partition, batch) = match next_message {
                DispatcherMessage::BatchWithEvents(etp, batch) => (etp, batch),
                DispatcherMessage::Tick(timestamp) => {
                    activated.values().for_each(|w| {
                        w.process(WorkerMessage::Tick(timestamp));
                    });
                    continue;
                }
            };

            let worker = if let Some(worker) = activated.get(&event_type_partition) {
                worker
            } else {
                stream_state.info(format_args!(
                    "Discovered new event type partition combination for events \
                    processing: {}",
                    event_type_partition
                ));
                let assignment =
                    HandlerAssignment::EventTypePartition(event_type_partition.clone());
                let sleeping_worker = Worker::sleeping(
                    Arc::clone(&handler_factory),
                    assignment,
                    stream_state.config().handler_inactivity_timeout,
                );
                let worker =
                    BufferedWorker::new(sleeping_worker, stream_state.clone(), committer.clone());
                activated.insert(event_type_partition.clone(), worker);
                activated.get(&event_type_partition).unwrap()
            };

            if !worker.process(WorkerMessage::BatchWithEvents(batch)) {
                stream_state.request_stream_cancellation();
                break;
            }
        }

        stream_state.debug(format_args!(
            "'etp_par'-Dispatcher loop exited. Waiting for {} workers to fall asleep.",
            activated.len()
        ));

        let mut consumer_error_ocurred = false;
        let mut processed_batches_total = 0;
        let mut sleeping_workers: BTreeMap<EventTypePartition, SleepingWorker> =
            BTreeMap::default();

        for (event_type_partition, worker) in activated {
            match worker.join().await {
                Ok(EnrichedOk {
                    processed_batches,
                    payload: sleeping_worker,
                }) => {
                    processed_batches_total += processed_batches;
                    sleeping_workers.insert(event_type_partition, sleeping_worker);
                }
                Err(EnrichedErr {
                    processed_batches,
                    err,
                }) => {
                    consumer_error_ocurred = true;
                    stream_state.error(format_args!(
                        "worker for event type {} joined with an error: {}",
                        event_type_partition, err
                    ));
                    if let Some(processed_batches) = processed_batches {
                        processed_batches_total += processed_batches;
                    }
                }
            }
        }

        stream_state.debug(format_args!("'etp_par'-Dispatcher: All workers sleeping.",));

        if consumer_error_ocurred {
            Err(EnrichedErr::new(
                ConsumerError::internal().with_message("At least one worker failed to join."),
                processed_batches_total,
            ))
        } else {
            Ok(EnrichedOk::new(sleeping_workers, processed_batches_total))
        }
    };

    let join_handle = tokio::spawn(task);

    async { join_handle.map_err(EnrichedErr::no_data).await? }
}

pub(crate) struct Active<'a, C> {
    stream_state: StreamState,
    api_client: C,
    handler_factory: Arc<dyn BatchHandlerFactory>,
    join: BoxFuture<'a, EnrichedResult<BTreeMap<EventTypePartition, SleepingWorker>>>,
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
            handler_factory,
        } = self;

        stream_state.debug(format_args!("Waiting for worker to fall asleep"));

        let assignments_enriched = join.await?;

        stream_state.debug(format_args!("Dispatcher going to sleep"));

        Ok(assignments_enriched.map(|assignments| Sleeping {
            assignments,
            api_client,
            handler_factory,
        }))
    }
}
