use std::collections::BTreeMap;
use std::sync::Arc;

use futures::{future::BoxFuture, pin_mut, FutureExt, Stream, StreamExt};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::api::SubscriptionCommitApi;
use crate::consumer::{Config, ConsumerError};
use crate::handler::{BatchHandlerFactory, HandlerAssignment};
use crate::internals::{
    committer::*, worker::*, EnrichedErr, EnrichedOk, EnrichedResult, StreamState,
};
use crate::logging::Logs;

use crate::nakadi_types::model::event_type::EventTypeName;

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
            Committer::start(api_client.clone(), stream_state.clone());

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
                if let Err(err) = committer_join_handle.await {
                    // TODO: Is this sufficient?
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

async fn run<S>(
    assignments: BTreeMap<String, SleepingWorker>,
    stream: S,
    stream_state: StreamState,
    committer: UnboundedSender<CommitData>,
    handler_factory: Arc<dyn BatchHandlerFactory>,
) -> EnrichedResult<BTreeMap<String, SleepingWorker>>
where
    S: Stream<Item = DispatcherMessage> + Send + 'static,
{
    let task = async move {
        stream_state.debug(format_args!("running dispatch by event type dispatcher"));
        let mut activated: BTreeMap<_, _> = assignments
            .into_iter()
            .map(|(event_type, sleeping_worker)| {
                (
                    event_type,
                    RunningWorker::new(sleeping_worker, stream_state.clone(), committer.clone()),
                )
            })
            .collect();

        pin_mut!(stream);
        while let Some(next_message) = stream.next().await {
            let batch = match next_message {
                DispatcherMessage::Batch(batch) => batch,
                DispatcherMessage::Tick => {
                    activated.values().for_each(|w| {
                        w.process(WorkerMessage::Tick);
                    });
                    continue;
                }
                DispatcherMessage::StreamEnded => {
                    activated.values().for_each(|w| {
                        w.process(WorkerMessage::StreamEnded);
                    });
                    break;
                }
            };

            let event_type_str = batch.event_type_str();
            let worker = if let Some(worker) = activated.get(event_type_str) {
                worker
            } else {
                stream_state.info(format_args!(
                    "Discovered new event type: {}",
                    event_type_str
                ));
                let assignment = HandlerAssignment::EventType(EventTypeName::new(event_type_str));
                let sleeping_worker = Worker::sleeping(
                    Arc::clone(&handler_factory),
                    assignment,
                    stream_state.config().inactivity_timeout,
                );
                let worker =
                    RunningWorker::new(sleeping_worker, stream_state.clone(), committer.clone());
                activated.insert(event_type_str.to_owned(), worker);
                activated.get(event_type_str).unwrap()
            };

            if !worker.process(WorkerMessage::Batch(batch)) {
                stream_state.request_stream_cancellation();
                break;
            }
        }

        let mut consumer_error_ocurred = false;
        let mut processed_batches_total = 0;
        let mut sleeping_workers: BTreeMap<String, SleepingWorker> = BTreeMap::default();
        for (event_type, worker) in activated {
            match worker.join().await {
                Ok(EnrichedOk {
                    processed_batches,
                    payload: sleeping_worker,
                }) => {
                    processed_batches_total += processed_batches;
                    sleeping_workers.insert(event_type, sleeping_worker);
                }
                Err(EnrichedErr {
                    processed_batches,
                    err,
                }) => {
                    consumer_error_ocurred = true;
                    stream_state.error(format_args!(
                        "worker for event type {} joined with an error: {}",
                        event_type, err
                    ));
                    if let Some(processed_batches) = processed_batches {
                        processed_batches_total += processed_batches;
                    }
                }
            }
        }

        if consumer_error_ocurred {
            Err(EnrichedErr::new(
                ConsumerError::internal().with_message("At least one worker failed to join."),
                processed_batches_total,
            ))
        } else {
            Ok(EnrichedOk::new(sleeping_workers, processed_batches_total))
        }
    };

    match tokio::spawn(task).await {
        Ok(r) => r,
        Err(join_err) => Err(EnrichedErr::no_data(join_err)),
    }
}

type WorkerJoin<'a> = BoxFuture<'a, EnrichedResult<SleepingWorker>>;

struct RunningWorker {
    join: WorkerJoin<'static>,
    sender: UnboundedSender<WorkerMessage>,
}

impl RunningWorker {
    fn new(
        sleeping_worker: SleepingWorker,
        stream_state: StreamState,
        committer: UnboundedSender<CommitData>,
    ) -> RunningWorker {
        let (tx, rx) = unbounded_channel::<WorkerMessage>();

        let active_worker = sleeping_worker.start(stream_state, committer, rx);

        let join = async move { active_worker.join().await }.boxed();

        RunningWorker { join, sender: tx }
    }

    pub fn process(&self, msg: WorkerMessage) -> bool {
        if let Err(err) = self.sender.send(msg) {
            false
        } else {
            true
        }
    }

    pub fn join(self) -> WorkerJoin<'static> {
        self.join
    }
}

pub(crate) struct Active<'a, C> {
    stream_state: StreamState,
    api_client: C,
    handler_factory: Arc<dyn BatchHandlerFactory>,
    join: BoxFuture<'a, EnrichedResult<BTreeMap<String, SleepingWorker>>>,
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
