use std::sync::Arc;
use std::time::Instant;

use futures::{pin_mut, stream, Stream, StreamExt, TryStreamExt};
use tokio::{
    self,
    sync::mpsc::{unbounded_channel, UnboundedSender},
    time::interval_at,
};

use crate::api::{BytesStream, NakadionEssentials, SubscriptionCommitApi};
use crate::consumer::{ConsumerError, ConsumerErrorKind, TickIntervalMillis};
use crate::event_stream::{
    BatchLine, BatchLineError, BatchLineErrorKind, BatchLineStream, FramedStream,
};
use crate::handler::BatchHandler;
use crate::internals::dispatcher::{
    ActiveDispatcher, Dispatcher, DispatcherMessage, SleepingDispatcher,
};
use crate::logging::Logs;

use super::StreamState;

mod connect_stream;
pub(crate) mod types;

use types::*;

#[derive(Clone)]
pub(crate) struct Controller<H, C> {
    params: ControllerParams<H, C>,
}

impl<H, C> Controller<H, C>
where
    C: NakadionEssentials + Clone,
    H: BatchHandler,
{
    pub(crate) fn new(params: ControllerParams<H, C>) -> Self {
        Self { params }
    }

    pub(crate) async fn start(self) -> Result<(), ConsumerError> {
        create_background_task(self.params).await
    }
}

async fn create_background_task<H, C>(
    mut params: ControllerParams<H, C>,
) -> Result<(), ConsumerError>
where
    C: NakadionEssentials + Clone,
    H: BatchHandler,
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
    Tick,
    StreamEnded,
}

async fn stream_lifecycle<H, C>(
    params: ControllerParams<H, C>,
    sleeping_dispatcher: SleepingDispatcher<H, C>,
) -> Result<SleepingDispatcher<H, C>, ConsumerError>
where
    C: NakadionEssentials + Clone,
    H: BatchHandler,
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

    let stream = make_ticked_batch_line_stream(bytes_stream, stream_state.config().tick_interval);

    let (active_dispatcher, stream, batch_lines_sink) =
        match wait_for_first_batch_line(stream, sleep_ticker, stream_state.clone()).await? {
            WaitForFirstBatchLineResult::GotTheBatch {
                active_dispatcher,
                stream,
                batch_lines_sink,
            } => (active_dispatcher, stream, batch_lines_sink),
            WaitForFirstBatchLineResult::Aborted {
                sleeping_dispatcher,
            } => return Ok(sleeping_dispatcher),
        };

    let sleeping_dispatcher =
        consume_stream_to_end(stream, active_dispatcher, batch_lines_sink, stream_state).await?;

    Ok(sleeping_dispatcher)
}

async fn consume_stream_to_end<H, C, S>(
    stream: S,
    active_dispatcher: ActiveDispatcher<'static, H, C>,
    batch_lines_sink: UnboundedSender<DispatcherMessage>,
    stream_state: StreamState,
) -> Result<SleepingDispatcher<H, C>, ConsumerError>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    H: BatchHandler,
    S: Stream<Item = Result<BatchLineMessage, BatchLineError>> + Send + 'static,
{
    let now = Instant::now();
    let stream_started_at = now;
    let mut last_batch_received_at = now;

    let stream_dead_timeout = stream_state
        .config()
        .stream_dead_timeout
        .map(|t| t.into_duration());
    let warn_stream_stalled = stream_state
        .config()
        .warn_stream_stalled
        .map(|t| t.into_duration());

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
            BatchLineMessage::Tick => {
                if let Some(stream_dead_timeout) = stream_dead_timeout {
                    let elapsed = last_batch_received_at.elapsed();
                    if elapsed > stream_dead_timeout {
                        stream_state.warn(format_args!(
                            "The stream is dead boys... after {:?}",
                            elapsed
                        ));
                        let _ = batch_lines_sink.send(DispatcherMessage::StreamEnded);
                        break;
                    }
                }
                if let Some(warn_stream_stalled) = warn_stream_stalled {
                    let elapsed = last_batch_received_at.elapsed();
                    if elapsed >= warn_stream_stalled {
                        stream_state.warn(format_args!(
                            "The stream seems to have stalled (for {:?})",
                            elapsed
                        ));
                    }
                }

                DispatcherMessage::Tick
            }
            BatchLineMessage::BatchLine(batch) => {
                last_batch_received_at = Instant::now();

                if let Some(info_str) = batch.info_str() {
                    stream_state.info(format_args!("Received info line: {}", info_str));
                }

                if batch.is_keep_alive_line() {
                    stream_state.debug(format_args!("Keep alive line received."));
                    continue;
                } else {
                    DispatcherMessage::Batch(batch)
                }
            }
        };

        if batch_lines_sink.send(msg_for_dispatcher).is_err() {
            stream_state.request_stream_cancellation();
            break;
        }
    }

    drop(stream);

    stream_state.debug(format_args!(
        "Streaming stopping after {:?}.",
        stream_started_at.elapsed()
    ));

    let sleeping_dispatcher = active_dispatcher.join().await?;

    stream_state.info(format_args!(
        "Streaming stopped after {:?}.",
        stream_started_at.elapsed()
    ));

    Ok(sleeping_dispatcher)
}

enum WaitForFirstBatchLineResult<H, C, T> {
    GotTheBatch {
        active_dispatcher: ActiveDispatcher<'static, H, C>,
        stream: T,
        batch_lines_sink: UnboundedSender<DispatcherMessage>,
    },
    Aborted {
        sleeping_dispatcher: SleepingDispatcher<H, C>,
    },
}

async fn wait_for_first_batch_line<H, C, S>(
    stream: S,
    sleep_ticker: SleepTicker<H, C>,
    stream_state: StreamState,
) -> Result<
    WaitForFirstBatchLineResult<
        H,
        C,
        impl Stream<Item = Result<BatchLineMessage, BatchLineError>> + Send,
    >,
    ConsumerError,
>
where
    H: BatchHandler,
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    S: Stream<Item = Result<BatchLineMessage, BatchLineError>> + Send + 'static,
{
    let no_batch_received_since = Instant::now();
    let stream_dead_timeout = stream_state
        .config()
        .stream_dead_timeout
        .map(|d| d.into_duration());
    let warn_stream_stalled = stream_state
        .config()
        .warn_stream_stalled
        .map(|d| d.into_duration());

    let mut stream = stream.boxed();
    let (active_dispatcher, batch_lines_sink, first_batch_line) = {
        loop {
            if let Some(next) = stream.next().await {
                match next {
                    Ok(BatchLineMessage::StreamEnded) => {
                        stream_state
                            .info(format_args!("Stream ended before receiving a batch line"));
                        let sleeping_dispatcher = sleep_ticker.join().await?;
                        return Ok(WaitForFirstBatchLineResult::Aborted {
                            sleeping_dispatcher,
                        });
                    }
                    Ok(BatchLineMessage::BatchLine(first_batch_line)) => {
                        stream_state.info(format_args!("Received first batch line."));
                        let sleeping_dispatcher = sleep_ticker.join().await?;
                        let (batch_lines_sink, batch_lines_receiver) =
                            unbounded_channel::<DispatcherMessage>();
                        let active_dispatcher =
                            sleeping_dispatcher.start(stream_state.clone(), batch_lines_receiver);

                        break (active_dispatcher, batch_lines_sink, first_batch_line);
                    }
                    Ok(BatchLineMessage::Tick) => {
                        let elapsed = no_batch_received_since.elapsed();
                        if let Some(stream_dead_timeout) = stream_dead_timeout {
                            if elapsed > stream_dead_timeout {
                                stream_state.warn(format_args!(
                                    "The stream is dead boys... after {:?}",
                                    elapsed
                                ));
                                let sleeping_dispatcher = sleep_ticker.join().await?;
                                return Ok(WaitForFirstBatchLineResult::Aborted {
                                    sleeping_dispatcher,
                                });
                            }
                        }
                        if let Some(warn_stream_stalled) = warn_stream_stalled {
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
                            return Ok(WaitForFirstBatchLineResult::Aborted {
                                sleeping_dispatcher,
                            });
                        }
                    },
                }
            } else {
                let sleeping_dispatcher = sleep_ticker.join().await?;
                return Ok(WaitForFirstBatchLineResult::Aborted {
                    sleeping_dispatcher,
                });
            }
        }
    };

    let stream =
        stream::once(async { Ok(BatchLineMessage::BatchLine(first_batch_line)) }).chain(stream);

    Ok(WaitForFirstBatchLineResult::GotTheBatch {
        active_dispatcher,
        stream,
        batch_lines_sink,
    })
}

fn make_ticked_batch_line_stream(
    bytes_stream: BytesStream,
    tick_interval: TickIntervalMillis,
) -> impl Stream<Item = Result<BatchLineMessage, BatchLineError>> + Send {
    let frame_stream = FramedStream::new(bytes_stream);

    let batch_stream = BatchLineStream::new(frame_stream)
        .map_ok(BatchLineMessage::BatchLine)
        .chain(stream::once(async { Ok(BatchLineMessage::StreamEnded) }));

    let tick_interval = tick_interval.into_duration();
    let ticker = interval_at((Instant::now() + tick_interval).into(), tick_interval)
        .map(|_| Ok(BatchLineMessage::Tick));

    stream::select(batch_stream, ticker)
}
