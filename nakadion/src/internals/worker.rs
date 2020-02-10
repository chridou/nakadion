use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use tokio::{self, sync::mpsc::UnboundedSender, task::JoinHandle};

use crate::consumer::ConsumerError;
use crate::event_handler::{BatchHandler, BatchHandlerFactory, HandlerAssignment};
use crate::event_stream::BatchLine;
use crate::internals::{committer::CommitData, StreamState};

use processor::HandlerSlot;

#[derive(Debug)]
pub enum WorkerMessage {
    Batch(BatchLine),
    Tick,
}

pub struct Worker;
impl Worker {
    pub fn new<H: BatchHandler>(
        handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
        assignment: HandlerAssignment,
    ) -> SleepingWorker<H> {
        SleepingWorker {
            handler_slot: HandlerSlot::new(handler_factory, assignment, Duration::from_secs(60)),
        }
    }
}

pub struct SleepingWorker<H> {
    handler_slot: HandlerSlot<H>,
}

impl<H> SleepingWorker<H>
where
    H: BatchHandler,
{
    pub fn start<S>(
        self,
        stream_state: StreamState,
        committer: UnboundedSender<CommitData>,
        batches: S,
    ) -> ActiveWorker<H>
    where
        S: Stream<Item = WorkerMessage> + Send + 'static,
    {
        let SleepingWorker { handler_slot } = self;

        let join_handle = processor::start(batches, handler_slot, stream_state, committer);

        ActiveWorker { join_handle }
    }

    pub fn assignment(&self) -> &HandlerAssignment {
        &self.handler_slot.assignment
    }

    pub fn tick(&mut self) {
        self.handler_slot.tick();
    }
}

pub struct ActiveWorker<H> {
    join_handle: JoinHandle<Result<HandlerSlot<H>, ConsumerError>>,
}

impl<H: BatchHandler> ActiveWorker<H> {
    pub async fn join(self) -> Result<SleepingWorker<H>, ConsumerError> {
        let ActiveWorker { join_handle, .. } = self;

        let handler_slot = join_handle.await??;

        Ok(SleepingWorker { handler_slot })
    }
}

mod processor {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use futures::{pin_mut, Stream, StreamExt};
    use tokio::{self, sync::mpsc::UnboundedSender, task::JoinHandle};

    use crate::nakadi_types::{model::subscription::SubscriptionCursor, GenericError};

    use crate::consumer::{ConsumerError, ConsumerErrorKind};
    use crate::event_handler::{
        BatchHandler, BatchHandlerFactory, BatchMeta, BatchPostAction, HandlerAssignment,
    };
    use crate::event_stream::BatchLine;
    use crate::internals::{committer::CommitData, StreamState};

    use super::WorkerMessage;

    pub fn start<H, S>(
        mut batches: S,
        handler_slot: HandlerSlot<H>,
        stream_state: StreamState,
        committer: UnboundedSender<CommitData>,
    ) -> JoinHandle<Result<HandlerSlot<H>, ConsumerError>>
    where
        H: BatchHandler,
        S: Stream<Item = WorkerMessage> + Send + 'static,
    {
        let processor_loop = async move {
            let mut processing_compound = ProcessingCompound {
                handler_slot,
                stream_state: stream_state.clone(),
                committer,
            };

            pin_mut!(batches);
            while let Some(msg) = batches.next().await {
                if stream_state.cancellation_requested() {
                    break;
                }

                let batch = match msg {
                    WorkerMessage::Tick => {
                        processing_compound.handler_slot.tick();
                        continue;
                    }
                    WorkerMessage::Batch(batch) => batch,
                };

                match processing_compound.process_batch_line(batch).await {
                    Ok(true) => {}
                    Ok(false) => {
                        break;
                    }
                    Err(err) => {
                        stream_state.request_global_cancellation();
                        return Err(err);
                    }
                }
            }
            Ok(processing_compound.handler_slot)
        };

        tokio::spawn(processor_loop)
    }

    pub struct ProcessingCompound<H> {
        handler_slot: HandlerSlot<H>,
        stream_state: StreamState,
        committer: UnboundedSender<CommitData>,
    }

    impl<H> ProcessingCompound<H>
    where
        H: BatchHandler,
    {
        /// If this function returns with an error, treat is as a critical
        /// error that aborts the whole consumption
        ///
        /// Returns `false` if the stream was cancelled
        pub async fn process_batch_line(
            &mut self,
            batch: BatchLine,
        ) -> Result<bool, ConsumerError> {
            let events = if let Some(events) = batch.events_bytes() {
                events
            } else {
                // We do not expect info or keep alive lines here
                return Ok(true);
            };

            let received_at = batch.received_at();
            let batch_id = batch.frame_id();
            let cursor = batch.cursor_deserialized::<SubscriptionCursor>()?;

            let meta = BatchMeta {
                cursor: &cursor,
                received_at,
                batch_id,
            };

            match self.handler_slot.process_batch(events, meta).await? {
                BatchPostAction::Commit { n_events } => {
                    let commit_data = CommitData {
                        cursor,
                        received_at,
                        batch_id,
                        events_hint: n_events,
                    };
                    if let Err(err) = self.try_commit(commit_data) {
                        self.stream_state.request_stream_cancellation();
                    }
                    Ok(true)
                }
                BatchPostAction::DoNotCommit { n_events } => Ok(true),
                BatchPostAction::AbortStream => {
                    self.stream_state.request_stream_cancellation();
                    Ok(false)
                }
                BatchPostAction::ShutDown => {
                    let err = ConsumerError::new(ConsumerErrorKind::UserAbort);
                    Err(err)
                }
            }
        }

        fn try_commit(&mut self, commit_data: CommitData) -> Result<(), GenericError> {
            if let Err(err) = self.committer.send(commit_data) {
                return Err(GenericError::new(format!(
                    "failed to enqueue commit data '{}'",
                    err
                )));
            }

            Ok(())
        }
    }

    pub struct HandlerSlot<H> {
        pub handler: Option<H>,
        pub handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
        pub last_event_processed: Instant,
        pub assignment: HandlerAssignment,
        pub inactivity_after: Duration,
        pub notified_on_inactivity: bool,
    }

    impl<H: BatchHandler> HandlerSlot<H> {
        pub fn new(
            handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
            assignment: HandlerAssignment,
            inactivity_after: Duration,
        ) -> Self {
            Self {
                handler: None,
                handler_factory,
                last_event_processed: Instant::now(),
                assignment,
                inactivity_after,
                notified_on_inactivity: false,
            }
        }

        async fn process_batch<'a>(
            &mut self,
            events: Bytes,
            meta: BatchMeta<'a>,
        ) -> Result<BatchPostAction, ConsumerError> {
            self.last_event_processed = Instant::now();
            self.notified_on_inactivity = false;
            let handler = self.get_handler().await?;
            Ok(handler.handle(events, meta).await)
        }

        async fn get_handler(&mut self) -> Result<&mut H, ConsumerError> {
            if self.handler.is_none() {
                let new_handler = self
                    .handler_factory
                    .handler(&self.assignment)
                    .await
                    .map_err(|err| {
                        ConsumerError::from(err).with_kind(ConsumerErrorKind::HandlerFactory)
                    })?;
                self.handler = Some(new_handler);
            }

            Ok(self.handler.as_mut().unwrap())
        }

        pub fn tick(&mut self) {
            if let Some(mut handler) = self.handler.take() {
                let elapsed = self.last_event_processed.elapsed();
                if elapsed > self.inactivity_after
                    && !self.notified_on_inactivity
                    && handler
                        .on_inactivity_detected(elapsed, self.last_event_processed)
                        .should_stay_alive()
                {
                    self.notified_on_inactivity = true;
                    self.handler = Some(handler);
                }
            }
        }
    }
}
