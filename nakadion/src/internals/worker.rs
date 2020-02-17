use std::sync::Arc;

use futures::Stream;
use tokio::{self, sync::mpsc::UnboundedSender, task::JoinHandle};

use crate::consumer::{ConsumerError, InactivityTimeoutSecs};
use crate::event_stream::BatchLine;
use crate::handler::{BatchHandler, BatchHandlerFactory, HandlerAssignment};
use crate::internals::{committer::CommitData, StreamState};

use processor::HandlerSlot;

#[derive(Debug)]
pub enum WorkerMessage {
    Batch(BatchLine),
    Tick,
    StreamEnded,
}

pub struct Worker;
impl Worker {
    pub(crate) fn sleeping<H: BatchHandler>(
        handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
        assignment: HandlerAssignment,
        inactivity_after: Option<InactivityTimeoutSecs>,
    ) -> SleepingWorker<H> {
        SleepingWorker {
            handler_slot: HandlerSlot::new(handler_factory, assignment, inactivity_after),
        }
    }
}

pub(crate) struct SleepingWorker<H> {
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
        let SleepingWorker { mut handler_slot } = self;

        handler_slot.set_logger(stream_state.logger());

        let join_handle = processor::start(batches, handler_slot, stream_state, committer);

        ActiveWorker { join_handle }
    }

    pub fn tick(&mut self) {
        self.handler_slot.tick();
    }
}

pub(crate) struct ActiveWorker<H> {
    join_handle: JoinHandle<Result<HandlerSlot<H>, ConsumerError>>,
}

impl<H: BatchHandler> ActiveWorker<H> {
    pub async fn join(self) -> Result<SleepingWorker<H>, ConsumerError> {
        let ActiveWorker { join_handle, .. } = self;

        let mut handler_slot = join_handle.await??;

        handler_slot.reset();

        Ok(SleepingWorker { handler_slot })
    }
}

mod processor {
    use std::sync::Arc;
    use std::time::Instant;

    use bytes::Bytes;
    use futures::{pin_mut, Stream, StreamExt};
    use tokio::{self, sync::mpsc::UnboundedSender, task::JoinHandle};

    use crate::nakadi_types::{model::subscription::SubscriptionCursor, Error};

    use crate::consumer::{ConsumerError, ConsumerErrorKind, InactivityTimeoutSecs};
    use crate::event_stream::BatchLine;
    use crate::handler::{
        BatchHandler, BatchHandlerFactory, BatchMeta, BatchPostAction, BatchStats,
        HandlerAssignment,
    };
    use crate::internals::{committer::CommitData, StreamState};
    use crate::logging::{Logger, Logs};

    use super::WorkerMessage;

    pub(crate) fn start<H, S>(
        batches: S,
        mut handler_slot: HandlerSlot<H>,
        stream_state: StreamState,
        committer: UnboundedSender<CommitData>,
    ) -> JoinHandle<Result<HandlerSlot<H>, ConsumerError>>
    where
        H: BatchHandler,
        S: Stream<Item = WorkerMessage> + Send + 'static,
    {
        handler_slot.set_logger(&stream_state.logger());
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
                    WorkerMessage::Batch(batch) => batch,
                    WorkerMessage::Tick => {
                        processing_compound.handler_slot.tick();
                        continue;
                    }
                    WorkerMessage::StreamEnded => break,
                };

                match processing_compound.process_batch_line(batch).await {
                    Ok(true) => {}
                    Ok(false) => {
                        break;
                    }
                    Err(err) => {
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
            let frame_id = batch.frame_id();
            let cursor = batch.cursor_deserialized::<SubscriptionCursor>()?;

            let meta = BatchMeta {
                stream_id: self.stream_state.stream_id(),
                cursor: &cursor,
                received_at,
                frame_id,
            };

            match self.handler_slot.process_batch(events, meta).await? {
                BatchPostAction::Commit(BatchStats {
                    n_events,
                    t_deserialize,
                }) => {
                    let commit_data = CommitData {
                        cursor,
                        received_at,
                        frame_id,
                        n_events,
                    };
                    if let Err(err) = self.try_commit(commit_data) {
                        self.stream_state
                            .error(format_args!("Failed to enqueue commit data: {}", err));
                        self.stream_state.request_stream_cancellation();
                    }
                    Ok(true)
                }
                BatchPostAction::DoNotCommit(BatchStats {
                    n_events,
                    t_deserialize,
                }) => Ok(true),
                BatchPostAction::AbortStream(reason) => {
                    self.stream_state.warn(format_args!(
                        "Stream cancellation requested by handler: {}",
                        reason
                    ));
                    self.stream_state.request_stream_cancellation();
                    Ok(false)
                }
                BatchPostAction::ShutDown(reason) => {
                    self.stream_state.warn(format_args!(
                        "Consumer shut down requested by handler: {}",
                        reason
                    ));
                    let err =
                        ConsumerError::new(ConsumerErrorKind::HandlerAbort).with_message(reason);
                    Err(err)
                }
            }
        }

        fn try_commit(&mut self, commit_data: CommitData) -> Result<(), Error> {
            if let Err(err) = self.committer.send(commit_data) {
                return Err(Error::new(format!(
                    "failed to enqueue commit data '{}'",
                    err
                )));
            }

            Ok(())
        }
    }

    pub(crate) struct HandlerSlot<H> {
        pub handler: Option<H>,
        pub handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
        pub last_event_processed: Instant,
        pub assignment: HandlerAssignment,
        pub inactivity_after: Option<InactivityTimeoutSecs>,
        pub notified_on_inactivity: bool,
        pub logger: Option<Logger>,
    }

    impl<H: BatchHandler> HandlerSlot<H> {
        pub fn new(
            handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
            assignment: HandlerAssignment,
            inactivity_after: Option<InactivityTimeoutSecs>,
        ) -> Self {
            Self {
                handler: None,
                handler_factory,
                last_event_processed: Instant::now(),
                assignment,
                inactivity_after,
                notified_on_inactivity: false,
                logger: None,
            }
        }

        async fn process_batch(
            &mut self,
            events: Bytes,
            meta: BatchMeta<'_>,
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
                self.with_logger(|l| {
                    l.info(format_args!(
                        "Created handler for assignment {}",
                        self.assignment
                    ))
                });
                self.handler = Some(new_handler);
            }

            Ok(self.handler.as_mut().unwrap())
        }

        pub fn tick(&mut self) {
            if let Some(inactivity_after) = self.inactivity_after {
                if let Some(mut handler) = self.handler.take() {
                    let elapsed = self.last_event_processed.elapsed();
                    if elapsed > inactivity_after.into_duration() && !self.notified_on_inactivity {
                        if handler
                            .on_inactivity_detected(elapsed, self.last_event_processed)
                            .should_stay_alive()
                        {
                            self.notified_on_inactivity = true;
                            self.handler = Some(handler);
                            self.with_logger(|l| {
                                l.info(format_args!(
                                    "Keeping inactive handler \
                                for assignment {} alive.",
                                    self.assignment
                                ))
                            });
                        } else {
                            self.with_logger(|l| {
                                l.info(format_args!(
                                    "Killed inactive handler \
                            for assignment {}.",
                                    self.assignment
                                ))
                            });
                        }
                    } else {
                        self.handler = Some(handler);
                    }
                }
            }
        }

        pub fn with_logger<F>(&self, f: F)
        where
            F: Fn(&Logger),
        {
            if let Some(ref logger) = self.logger {
                f(logger)
            }
        }

        pub fn set_logger(&mut self, logger: &Logger) {
            let logger = match self.assignment.event_type_and_partition() {
                (Some(e), Some(p)) => logger
                    .with_event_type(e.clone())
                    .with_partition_id(p.clone()),
                (Some(e), None) => logger.with_event_type(e.clone()),
                (None, Some(p)) => logger.with_partition_id(p.clone()),
                (None, None) => logger.clone(),
            };
            self.logger = Some(logger)
        }

        pub fn reset(&mut self) {
            self.tick();
            self.logger = None
        }
    }
}
