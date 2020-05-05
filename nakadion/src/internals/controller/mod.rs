//! The controller controls the life cycle of the `Consumer` over multiple streams
use std::sync::Arc;
use std::time::Instant;

use futures::{stream, Stream, StreamExt, TryStreamExt};
use tokio::{
    self,
    sync::mpsc::{unbounded_channel, UnboundedSender},
    time::interval_at,
};

use crate::api::{BytesStream, SubscriptionCommitApi};
use crate::components::{
    streams::{BatchLine, BatchLineError, BatchLineErrorKind, BatchLineStream, FramedStream},
    StreamingEssentials,
};
use crate::consumer::{ConsumerAbort, ConsumerError, ConsumerErrorKind, TickIntervalMillis};
use crate::instrumentation::{Instrumentation, Instruments};
use crate::internals::{
    dispatcher::{ActiveDispatcher, Dispatcher, DispatcherMessage, SleepingDispatcher},
    EnrichedErr, EnrichedOk, StreamState,
};
use crate::logging::Logger;

mod connect_stream;
mod partition_tracker;
pub(crate) mod types;

use types::*;

/// The controller controls the life cycle of the `Consumer` over multiple streams
///
/// * It connects to Nakadi
/// * It builds up the components needed for consumption after a connect (Dispatcher, Workers, Committer)
/// in a transitive way
/// * It waits for a tear down of stream components when a stream is not consumed anymore
/// * It reconnects to a stream or aborts the consumer
///
/// **Any occurrence of a `ConsumerError` triggers the `Controller` to abort the `Consumer` with an error.**
#[derive(Clone)]
pub(crate) struct Controller<C> {
    params: ControllerParams<C>,
}

impl<C> Controller<C>
where
    C: StreamingEssentials + Clone,
{
    pub(crate) fn new(params: ControllerParams<C>) -> Self {
        Self { params }
    }

    pub(crate) async fn start(self) -> ConsumerAbort {
        create_background_task(self.params).await
    }
}

/// Create a task to be spawned on the runtime to drive the `Controller` over multiple streams
async fn create_background_task<C>(params: ControllerParams<C>) -> ConsumerAbort
where
    C: StreamingEssentials + Clone,
{
    let consumer_state = params.consumer_state.clone();
    let mut sleeping_dispatcher = Dispatcher::sleeping(
        params.config().dispatch_mode,
        Arc::clone(&params.handler_factory),
        params.api_client.clone(),
        params.consumer_state.config().clone(),
    );

    // Each iteration is the life cycle of a stream
    loop {
        if consumer_state.global_cancellation_requested() {
            return ConsumerAbort::UserInitiated;
        }
        sleeping_dispatcher = match stream_life_cycle(params.clone(), sleeping_dispatcher).await {
            Ok(sleeping_dispatcher) => sleeping_dispatcher,
            Err(err) => {
                consumer_state.request_global_cancellation();
                return err;
            }
        }
    }
}

enum BatchLineMessage {
    /// An evaluated frame from Nakadi. It can can contain any kind of line (events, keep alive, info parts)
    BatchLine(BatchLine),
    Tick(Instant),
    StreamEnded,
}

/// Consume a stream and return the (now inactive) sleeping dispatcher one finished
///
/// The error case does not return the dispatcher since we are about aborting the consumer.
async fn stream_life_cycle<C>(
    params: ControllerParams<C>,
    sleeping_dispatcher: SleepingDispatcher<C>,
) -> Result<SleepingDispatcher<C>, ConsumerAbort>
where
    C: StreamingEssentials + Clone,
{
    let consumer_state = params.consumer_state.clone();

    // We need stimuli even if we do not get anything from Nakadi
    // We also us the ticker to carry the sleeping dispatcher to the point
    // where we know that we will process batches
    let sleep_ticker = SleepTicker::start(sleeping_dispatcher, consumer_state.clone());

    // To be able to continue we need an established connection to a stream
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

    // From now on we are in the context of a stream so we
    // continue with a stream state for the current stream
    let stream_state = consumer_state.stream_state(stream_id);

    stream_state
        .logger()
        .info(format_args!("Connected to stream {}.", stream_id));

    // We inject ticks into the stream from Nakadi to be able to
    // act even though the stream might not deliver any data
    let stream = make_ticked_batch_line_stream(
        bytes_stream,
        stream_state.config().tick_interval,
        stream_state.instrumentation().clone(),
    );

    // Only if we receive a frame we start the rest
    // of the infrastructure
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

    // Once we received a frame we start consuming. The dispatcher
    // will run until it falls asleep because the stream ended or
    // was aborted
    let sleeping_dispatcher =
        consume_stream_to_end(stream, active_dispatcher, batch_lines_sink, stream_state).await?;

    Ok(sleeping_dispatcher)
}

/// Wakes up the infrastructure and then consumes the stream until it ends
/// or the consumption is aborted.
///
/// An error returned here means that we abort the `Consumer`
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
    // We might want to abort if we do not receive data from Nakadi in time
    // This is basically to prevents us from being locked in a dead stream.
    let stream_dead_policy = stream_state.config().stream_dead_policy;
    // From time to time, if we do not receive data from Nakadi we want to emit warnings
    let warn_no_frames = stream_state.config().warn_no_frames.into_duration();
    let warn_no_events = stream_state.config().warn_no_events.into_duration();

    let now = Instant::now();
    let stream_started_at = now;
    let mut last_events_received_at = now;
    let mut last_frame_received_at = now;

    // If streaming ends, we use this to correct the in flight metrics
    let mut batches_sent_to_dispatcher = 0usize;
    let instrumentation = stream_state.instrumentation();

    let mut stream = stream.boxed();

    // Track the activity of partitions on event types to collect metrics
    // on active (used) partitions
    let mut partition_tracker = partition_tracker::PartitionTracker::new(
        stream_state.instrumentation().clone(),
        stream_state
            .config()
            .partition_inactivity_timeout
            .into_duration(),
        stream_state.clone(),
    );

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
                stream_state.info(format_args!("Got a tick"));
                if stream_dead_policy
                    .is_stream_dead(last_frame_received_at, last_events_received_at)
                {
                    stream_state.warn(format_args!("The stream is dead boys..."));
                    let _ = batch_lines_sink.send(DispatcherMessage::StreamEnded);
                    break;
                }

                let elapsed = last_frame_received_at.elapsed();
                if elapsed >= warn_no_frames {
                    stream_state.warn(format_args!("No events frames for {:?}.", elapsed));
                    stream_state
                        .instrumentation()
                        .controller_no_frames_warning(elapsed);
                }

                let elapsed = last_events_received_at.elapsed();
                if elapsed >= warn_no_events {
                    stream_state.warn(format_args!("No events received for {:?}.", elapsed));
                    stream_state
                        .instrumentation()
                        .controller_no_events_warning(elapsed);
                }

                partition_tracker.check_for_inactivity(Instant::now());
                DispatcherMessage::Tick(timestamp)
            }
            BatchLineMessage::BatchLine(batch) => {
                let frame_received_at = batch.received_at();
                let now = Instant::now();
                last_frame_received_at = now;

                let event_type_partition = batch.to_event_type_partition();
                partition_tracker.activity(&event_type_partition);

                if let Some(info_str) = batch.info_str() {
                    instrumentation.controller_info_received(frame_received_at);
                    stream_state.info(format_args!(
                        "Received info line for {}: {}",
                        event_type_partition, info_str
                    ));
                }

                if batch.is_keep_alive_line() {
                    instrumentation.controller_keep_alive_received(frame_received_at);
                    stream_state.debug(format_args!(
                        "Keep alive line received for {}.",
                        event_type_partition
                    ));
                    continue;
                } else {
                    last_events_received_at = now;
                    let bytes = batch.bytes().len();
                    instrumentation.controller_batch_received(frame_received_at, bytes);
                    DispatcherMessage::BatchWithEvents(event_type_partition, batch)
                }
            }
        };

        let was_batch_with_events = msg_for_dispatcher.is_batch_with_events();
        if batch_lines_sink.send(msg_for_dispatcher).is_err() {
            stream_state.request_stream_cancellation();
            break;
        } else if was_batch_with_events {
            // Only here we know for sure whether we sent events
            instrumentation.consumer_batches_in_flight_inc();
            batches_sent_to_dispatcher += 1;
        }
    }

    drop(stream);
    drop(partition_tracker);

    stream_state.debug(format_args!(
        "Streaming ending after {:?}.",
        stream_started_at.elapsed()
    ));

    // Wait for the infrastructure to completely shut down before making further connect attempts for
    // new streams
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

    // Check whether there were unprocessed batches to fix the in flight metrics
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

/// Did we get a frame or not?
enum WaitForFirstFrameResult<C, T> {
    /// If we got a frame, we contain the woken up dispatcher
    GotTheFrame {
        active_dispatcher: ActiveDispatcher<'static, C>,
        stream: T,
        batch_lines_sink: UnboundedSender<DispatcherMessage>,
    },
    /// Simply return the dispatcher to be woken up a next connect attempt
    Aborted {
        sleeping_dispatcher: SleepingDispatcher<C>,
    },
}

/// Wait for the first frame
///
/// If we got one, put it in front of the stream again and also wake up the dispatcher
///
/// * If there is nothing received on the stream in time or if it even ended we return with an `Ok`
/// since another connect attempt might be eligible
/// * If a "real" error occurs an error is returned to abort the `Consumer`
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
    let nothing_received_since = now; // Just pretend we received something now to have a start
    let stream_dead_policy = stream_state.config().stream_dead_policy;
    let warn_no_frames = stream_state.config().warn_no_frames.into_duration();
    let warn_no_events = stream_state.config().warn_no_events.into_duration();

    let mut stream = stream.boxed();
    // wait for the first frame from Nakadi and maybe abort if none arrives in time
    let (active_dispatcher, batch_lines_sink, first_frame) = {
        loop {
            if let Some(next) = stream.next().await {
                match next {
                    Ok(BatchLineMessage::StreamEnded) => {
                        stream_state.warn(format_args!(
                            "Stream ended before receiving a batch line after {:?}",
                            nothing_received_since.elapsed()
                        ));
                        let sleeping_dispatcher = sleep_ticker.join().await?;
                        return Ok(WaitForFirstFrameResult::Aborted {
                            sleeping_dispatcher,
                        });
                    }
                    Ok(BatchLineMessage::BatchLine(first_frame)) => {
                        stream_state.info(format_args!(
                            "Received first frame after {:?}.",
                            nothing_received_since.elapsed()
                        ));
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
                        let elapsed = nothing_received_since.elapsed();
                        if elapsed >= warn_no_frames {
                            stream_state.warn(format_args!("No first frame for {:?}", elapsed));
                            stream_state
                                .instrumentation()
                                .controller_no_frames_warning(elapsed);
                        }
                        if elapsed >= warn_no_events {
                            stream_state.warn(format_args!("No first event for {:?}", elapsed));
                            stream_state
                                .instrumentation()
                                .controller_no_events_warning(elapsed);
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
                stream_state.warn(format_args!(
                    "Stream ended without `BatchLineMessage::StreamEnded` after {:?}. This should not happen.",
                    nothing_received_since.elapsed()
                ));
                let sleeping_dispatcher = sleep_ticker.join().await?;
                return Ok(WaitForFirstFrameResult::Aborted {
                    sleeping_dispatcher,
                });
            }
        }
    };

    // Recreate the stream by appending the rest to the already received first batch line
    let stream = stream::once(async { Ok(BatchLineMessage::BatchLine(first_frame)) }).chain(stream);

    Ok(WaitForFirstFrameResult::GotTheFrame {
        active_dispatcher,
        stream,
        batch_lines_sink,
    })
}

/// Creates a stream of batches and also injects ticks into the stream of frames from Nakadi
/// which creates tick messages within the stream of batches.
///
/// The created stream will also have a `BatchLineMessage::StreamEnded` appended as a final message
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
