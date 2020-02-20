use std::sync::Arc;
use std::time::Instant;

use futures::{stream, Stream, StreamExt, TryStreamExt};
use tokio::{
    self,
    sync::mpsc::{unbounded_channel, UnboundedSender},
    time::interval_at,
};

use crate::api::{BytesStream, NakadionEssentials, SubscriptionCommitApi};
use crate::components::streams::{
    BatchLine, BatchLineError, BatchLineErrorKind, BatchLineStream, FramedStream,
};
use crate::consumer::{ConsumerError, ConsumerErrorKind, TickIntervalMillis};
use crate::instrumentation::{Instrumentation, Instruments};
use crate::internals::{
    dispatcher::{ActiveDispatcher, Dispatcher, DispatcherMessage, SleepingDispatcher},
    EnrichedErr, EnrichedOk, StreamState,
};
use crate::logging::Logs;

mod connect_stream;
pub(crate) mod types;

use types::*;

#[derive(Clone)]
pub(crate) struct Controller<C> {
    params: ControllerParams<C>,
}

impl<C> Controller<C>
where
    C: NakadionEssentials + Clone,
{
    pub(crate) fn new(params: ControllerParams<C>) -> Self {
        Self { params }
    }

    pub(crate) async fn start(self) -> Result<(), ConsumerError> {
        create_background_task(self.params).await
    }
}

async fn create_background_task<C>(params: ControllerParams<C>) -> Result<(), ConsumerError>
where
    C: NakadionEssentials + Clone,
{
    let consumer_state = params.consumer_state.clone();
    let mut sleeping_dispatcher = Dispatcher::sleeping(
        params.config().dispatch_strategy.clone(),
        Arc::clone(&params.handler_factory),
        params.api_client.clone(),
        params.consumer_state.config().clone(),
    );

    loop {
        sleeping_dispatcher = match stream_lifecycle(params.clone(), sleeping_dispatcher).await {
            Ok(sleeping_dispatcher) => sleeping_dispatcher,
            Err(err) => {
                consumer_state.request_global_cancellation();
                return Err(err);
            }
        }
    }
}

enum BatchLineMessage {
    BatchLine(BatchLine),
    Tick(Instant),
    StreamEnded,
}

async fn stream_lifecycle<C>(
    params: ControllerParams<C>,
    sleeping_dispatcher: SleepingDispatcher<C>,
) -> Result<SleepingDispatcher<C>, ConsumerError>
where
    C: NakadionEssentials + Clone,
{
    let consumer_state = params.consumer_state.clone();
    let sleep_ticker = SleepTicker::start(sleeping_dispatcher, consumer_state.clone());
    let (stream_id, bytes_stream) = match connect_stream::connect_with_retries(
        params.api_client.clone(),
        params.consumer_state.clone(),
    )
    .await
    {
        Ok(stream) => stream.parts(),
        Err(err) => {
            return Err(err);
        }
    };

    let stream_state = consumer_state.stream_state(stream_id);

    stream_state
        .logger()
        .info(format_args!("Connected to stream {}.", stream_id));

    let stream = make_ticked_batch_line_stream(
        bytes_stream,
        stream_state.config().tick_interval,
        stream_state.instrumentation().clone(),
    );

    let (active_dispatcher, stream, batch_lines_sink) =
        match wait_for_first_frame(stream, sleep_ticker, stream_state.clone()).await? {
            WaitForFirstFrameResult::GotTheFrame {
                active_dispatcher,
                stream,
                batch_lines_sink,
            } => (active_dispatcher, stream, batch_lines_sink),
            WaitForFirstFrameResult::Aborted {
                sleeping_dispatcher,
            } => return Ok(sleeping_dispatcher),
        };

    let sleeping_dispatcher =
        consume_stream_to_end(stream, active_dispatcher, batch_lines_sink, stream_state).await?;

    Ok(sleeping_dispatcher)
}

async fn consume_stream_to_end<C, S>(
    stream: S,
    active_dispatcher: ActiveDispatcher<'static, C>,
    batch_lines_sink: UnboundedSender<DispatcherMessage>,
    stream_state: StreamState,
) -> Result<SleepingDispatcher<C>, ConsumerError>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    S: Stream<Item = Result<BatchLineMessage, BatchLineError>> + Send + 'static,
{
    let stream_dead_policy = stream_state.config().stream_dead_policy;
    let warn_stream_stalled = stream_state
        .config()
        .warn_stream_stalled
        .map(|t| t.into_duration());

    let now = Instant::now();
    let stream_started_at = now;
    let mut last_events_received_at = now;
    let mut last_frame_received_at = now;

    let mut batches_sent_to_dispatcher = 0usize;
    let instrumentation = stream_state.instrumentation();

    let mut stream = stream.boxed();
    while let Some(batch_line_message_or_err) = stream.next().await {
        let batch_line_message = match batch_line_message_or_err {
            Ok(msg) => msg,
            Err(batch_line_error) => match batch_line_error.kind() {
                BatchLineErrorKind::Parser => return Err(ConsumerErrorKind::InvalidBatch.into()),
                BatchLineErrorKind::Io => {
                    stream_state.request_stream_cancellation();
                    break;
                }
            },
        };

        let msg_for_dispatcher = match batch_line_message {
            BatchLineMessage::StreamEnded => {
                stream_state.info(format_args!("Stream ended"));
                stream_state.request_stream_cancellation();
                let _ = batch_lines_sink.send(DispatcherMessage::StreamEnded);
                break;
            }
            BatchLineMessage::Tick(timestamp) => {
                if stream_dead_policy
                    .is_stream_dead(last_frame_received_at, last_events_received_at)
                {
                    stream_state.warn(format_args!("The stream is dead boys..."));
                    let _ = batch_lines_sink.send(DispatcherMessage::StreamEnded);
                    break;
                }
                if let Some(warn_stream_stalled) = warn_stream_stalled {
                    let elapsed = last_events_received_at.elapsed();
                    if elapsed >= warn_stream_stalled {
                        stream_state.warn(format_args!(
                            "The stream seems to have stalled (for {:?})",
                            elapsed
                        ));
                    }
                }

                DispatcherMessage::Tick(timestamp)
            }
            BatchLineMessage::BatchLine(batch) => {
                let frame_received_at = batch.received_at();
                let now = Instant::now();
                last_frame_received_at = now;

                if let Some(info_str) = batch.info_str() {
                    instrumentation.controller_info_received(frame_received_at);
                    stream_state.info(format_args!("Received info line: {}", info_str));
                }

                if batch.is_keep_alive_line() {
                    instrumentation.controller_keep_alive_received(frame_received_at);
                    stream_state.debug(format_args!("Keep alive line received."));
                    continue;
                } else {
                    last_events_received_at = Instant::now();
                    let bytes = batch.bytes().len();
                    instrumentation.controller_batch_received(frame_received_at, bytes);
                    DispatcherMessage::Batch(batch)
                }
            }
        };

        let was_batch = msg_for_dispatcher.is_batch();
        if batch_lines_sink.send(msg_for_dispatcher).is_err() {
            stream_state.request_stream_cancellation();
            break;
        } else if was_batch {
            instrumentation.consumer_batches_in_flight_inc();
            batches_sent_to_dispatcher += 1;
        }
    }

    drop(stream);

    stream_state.debug(format_args!(
        "Streaming stopping after {:?}.",
        stream_started_at.elapsed()
    ));

    let (result, unprocessed_batches) = match active_dispatcher.join().await {
        Ok(EnrichedOk {
            processed_batches,
            payload: sleeping_dispatcher,
        }) => (
            Ok(sleeping_dispatcher),
            Some(batches_sent_to_dispatcher - processed_batches),
        ),
        Err(EnrichedErr {
            processed_batches,
            err,
        }) => {
            if let Some(processed_batches) = processed_batches {
                (
                    Err(err),
                    Some(batches_sent_to_dispatcher - processed_batches),
                )
            } else {
                (Err(err), None)
            }
        }
    };

    if let Some(unprocessed_batches) = unprocessed_batches {
        if unprocessed_batches > 0 {
            stream_state.info(format_args!(
                "There were still {} unprocessed batches in flight",
                unprocessed_batches,
            ));
            instrumentation.consumer_batches_in_flight_dec_by(unprocessed_batches);
        }
    }

    stream_state.info(format_args!(
        "Streaming stopped after {:?}",
        stream_started_at.elapsed(),
    ));

    result
}

enum WaitForFirstFrameResult<C, T> {
    GotTheFrame {
        active_dispatcher: ActiveDispatcher<'static, C>,
        stream: T,
        batch_lines_sink: UnboundedSender<DispatcherMessage>,
    },
    Aborted {
        sleeping_dispatcher: SleepingDispatcher<C>,
    },
}

async fn wait_for_first_frame<C, S>(
    stream: S,
    sleep_ticker: SleepTicker<C>,
    stream_state: StreamState,
) -> Result<
    WaitForFirstFrameResult<C, impl Stream<Item = Result<BatchLineMessage, BatchLineError>> + Send>,
    ConsumerError,
>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    S: Stream<Item = Result<BatchLineMessage, BatchLineError>> + Send + 'static,
{
    let now = Instant::now();
    let nothing_received_since = now;
    let stream_dead_policy = stream_state.config().stream_dead_policy;
    let warn_stream_stalled = stream_state
        .config()
        .warn_stream_stalled
        .map(|d| d.into_duration());

    let mut stream = stream.boxed();
    let (active_dispatcher, batch_lines_sink, first_frame) = {
        loop {
            if let Some(next) = stream.next().await {
                match next {
                    Ok(BatchLineMessage::StreamEnded) => {
                        stream_state
                            .info(format_args!("Stream ended before receiving a batch line"));
                        let sleeping_dispatcher = sleep_ticker.join().await?;
                        return Ok(WaitForFirstFrameResult::Aborted {
                            sleeping_dispatcher,
                        });
                    }
                    Ok(BatchLineMessage::BatchLine(first_frame)) => {
                        stream_state.info(format_args!("Received first frame."));
                        let sleeping_dispatcher = sleep_ticker.join().await?;
                        let (batch_lines_sink, batch_lines_receiver) =
                            unbounded_channel::<DispatcherMessage>();
                        let active_dispatcher =
                            sleeping_dispatcher.start(stream_state.clone(), batch_lines_receiver);

                        break (active_dispatcher, batch_lines_sink, first_frame);
                    }
                    Ok(BatchLineMessage::Tick(_timestamp)) => {
                        if stream_dead_policy
                            .is_stream_dead(nothing_received_since, nothing_received_since)
                        {
                            stream_state.warn(format_args!("The stream is dead boys..."));
                            let sleeping_dispatcher = sleep_ticker.join().await?;
                            return Ok(WaitForFirstFrameResult::Aborted {
                                sleeping_dispatcher,
                            });
                        }
                        if let Some(warn_stream_stalled) = warn_stream_stalled {
                            let elapsed = nothing_received_since.elapsed();
                            if elapsed >= warn_stream_stalled {
                                stream_state.warn(format_args!(
                                    "The stream seems to have stalled (for {:?})",
                                    elapsed
                                ));
                            }
                        }
                    }
                    Err(batch_line_error) => match batch_line_error.kind() {
                        BatchLineErrorKind::Parser => {
                            return Err(ConsumerErrorKind::InvalidBatch.into())
                        }
                        BatchLineErrorKind::Io => {
                            let sleeping_dispatcher = sleep_ticker.join().await?;
                            return Ok(WaitForFirstFrameResult::Aborted {
                                sleeping_dispatcher,
                            });
                        }
                    },
                }
            } else {
                let sleeping_dispatcher = sleep_ticker.join().await?;
                return Ok(WaitForFirstFrameResult::Aborted {
                    sleeping_dispatcher,
                });
            }
        }
    };

    let stream = stream::once(async { Ok(BatchLineMessage::BatchLine(first_frame)) }).chain(stream);

    Ok(WaitForFirstFrameResult::GotTheFrame {
        active_dispatcher,
        stream,
        batch_lines_sink,
    })
}

fn make_ticked_batch_line_stream(
    bytes_stream: BytesStream,
    tick_interval: TickIntervalMillis,
    instrumentation: Instrumentation,
) -> impl Stream<Item = Result<BatchLineMessage, BatchLineError>> + Send {
    let frame_stream = FramedStream::new(bytes_stream, instrumentation.clone());

    let batch_stream = BatchLineStream::new(frame_stream, instrumentation.clone())
        .map_ok(BatchLineMessage::BatchLine)
        .chain(stream::once(async { Ok(BatchLineMessage::StreamEnded) }));

    let tick_interval = tick_interval.into_duration();
    let ticker =
        interval_at((Instant::now() + tick_interval).into(), tick_interval).map(move |_| {
            instrumentation.stream_tick_emitted();
            Ok(BatchLineMessage::Tick(Instant::now()))
        });

    stream::select(batch_stream, ticker)
}
