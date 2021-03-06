//! The controller controls the life cycle of the `Consumer` over multiple streams
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{stream, Stream, StreamExt, TryStreamExt};
use tokio::{
    self,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::interval_at,
};
use tokio_stream::wrappers::{IntervalStream, UnboundedReceiverStream};

use crate::api::{BytesStream, SubscriptionCommitApi};
use crate::components::{
    streams::{
        EventStream, EventStreamBatch, EventStreamError, EventStreamErrorKind, FramedStream,
    },
    StreamingEssentials,
};
use crate::consumer::{ConsumerAbort, ConsumerError, ConsumerErrorKind, TickIntervalMillis};
use crate::instrumentation::Instruments;
use crate::internals::{
    dispatcher::{ActiveDispatcher, Dispatcher, DispatcherMessage, SleepingDispatcher},
    StreamState,
};
use crate::logging::Logger;

mod connect_stream;
mod consume_to_end;
mod sleep_ticker;
pub(crate) mod types;

use sleep_ticker::SleepTicker;
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

    let _guard: ConsumerStoppedGuard = params
        .lifecycle_listeners
        .on_consumer_started(consumer_state.subscription_id());

    let mut params = params;
    // Each iteration is the life cycle of a stream

    consumer_state.debug(format_args!("Starting consumer loop"));

    loop {
        if consumer_state.global_cancellation_requested() {
            return ConsumerAbort::UserInitiated;
        }

        let (sleeping_dispatcher_returned, params_returned) =
            match stream_life_cycle(params, sleeping_dispatcher).await {
                Ok(returned) => returned,
                Err(err) => {
                    consumer_state.debug(format_args!("Stream ended with error. Cancel consumer."));
                    consumer_state.request_global_cancellation();
                    return err;
                }
            };

        consumer_state.debug(format_args!("Lifecycle of a strem ended"));

        sleeping_dispatcher = sleeping_dispatcher_returned;
        params = params_returned;
    }
}

pub(crate) enum EventStreamMessage {
    Nakadi(NakadiMessage),
    Tick(Instant),
    /// We need this to notify the controller since the
    /// consumed stream will never end because of the ticks
    EventStreamEnded,
}

/// Consume a stream and return the (now inactive) sleeping dispatcher one finished
///
/// The error case does not return the dispatcher since we are about aborting the consumer.
async fn stream_life_cycle<C>(
    params: ControllerParams<C>,
    sleeping_dispatcher: SleepingDispatcher<C>,
) -> Result<(SleepingDispatcher<C>, ControllerParams<C>), ConsumerAbort>
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

    stream_state.reset_in_flight_stats();

    let _guard: StreamEndGuard = params
        .lifecycle_listeners
        .on_stream_connected(stream_state.subscription_id(), stream_state.stream_id());

    stream_state
        .logger()
        .info(format_args!("Connected to stream {}.", stream_id));

    // We inject ticks into the stream from Nakadi to be able to
    // act even though the stream might not deliver any data
    let stream = make_ticked_batch_line_stream(
        bytes_stream,
        stream_state.config().tick_interval,
        stream_state.clone(),
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
            } => return Ok((sleeping_dispatcher, params)),
        };

    // Once we received a frame we start consuming. The dispatcher
    // will run until it falls asleep because the stream ended or
    // was aborted
    let sleeping_dispatcher = consume_to_end::consume_stream_to_end(
        stream,
        active_dispatcher,
        batch_lines_sink,
        stream_state,
    )
    .await?;

    Ok((sleeping_dispatcher, params))
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
    event_stream: S,
    sleep_ticker: SleepTicker<C>,
    stream_state: StreamState,
) -> Result<
    WaitForFirstFrameResult<
        C,
        impl Stream<Item = Result<EventStreamMessage, EventStreamError>> + Send,
    >,
    ConsumerError,
>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    S: Stream<Item = Result<EventStreamMessage, EventStreamError>> + Send + 'static,
{
    let now = Instant::now();
    let no_events_received_since = now; // Just pretend we received something now to have a start
    let mut no_frame_received_since = now; // Just pretend we received something now to have a start
    let stream_dead_policy = stream_state.config().stream_dead_policy;
    let warn_no_frames = stream_state.config().warn_no_frames.into_duration();
    let warn_no_events = stream_state.config().warn_no_events.into_duration();

    let mut stream = event_stream.boxed();
    // wait for the first frame from Nakadi and maybe abort if none arrives in time
    let (active_dispatcher, batch_lines_sink, first_frame) = {
        loop {
            if let Some(next) = stream.next().await {
                match next {
                    Ok(EventStreamMessage::EventStreamEnded) => {
                        stream_state.info(format_args!(
                            "Stream ended before receiving a batch after {:?}",
                            no_events_received_since.elapsed()
                        ));
                        let sleeping_dispatcher = sleep_ticker.join().await?;
                        return Ok(WaitForFirstFrameResult::Aborted {
                            sleeping_dispatcher,
                        });
                    }
                    Ok(EventStreamMessage::Nakadi(NakadiMessage::Events(first_frame))) => {
                        stream_state.info(format_args!(
                            "Received first events batch frame after {:?}.",
                            no_events_received_since.elapsed()
                        ));
                        let sleeping_dispatcher = sleep_ticker.join().await?;
                        let (batch_lines_sink, batch_lines_receiver) =
                            unbounded_channel::<DispatcherMessage>();
                        let batch_lines_receiver =
                            UnboundedReceiverStream::new(batch_lines_receiver);
                        let active_dispatcher =
                            sleeping_dispatcher.start(stream_state.clone(), batch_lines_receiver);

                        break (active_dispatcher, batch_lines_sink, first_frame);
                    }
                    Ok(EventStreamMessage::Nakadi(NakadiMessage::KeepAlive)) => {
                        no_frame_received_since = Instant::now();
                        continue;
                    }
                    Ok(EventStreamMessage::Tick(_timestamp)) => {
                        if let Some(dead_for) = stream_dead_policy
                            .is_stream_dead(no_frame_received_since, no_events_received_since)
                        {
                            stream_state.warn(format_args!(
                                "The stream is dead boys... for {:?}",
                                dead_for
                            ));
                            stream_state.instrumentation().stream_dead(dead_for);
                            let sleeping_dispatcher = sleep_ticker.join().await?;
                            return Ok(WaitForFirstFrameResult::Aborted {
                                sleeping_dispatcher,
                            });
                        }
                        let no_frame_elapsed = no_frame_received_since.elapsed();
                        if no_frame_elapsed >= warn_no_frames {
                            stream_state.warn(format_args!("No frame for {:?}", no_frame_elapsed));
                            stream_state
                                .instrumentation()
                                .no_frames_warning(no_frame_elapsed);
                        }
                        let no_events_elapsed = no_events_received_since.elapsed();
                        if no_events_elapsed >= warn_no_events {
                            stream_state
                                .warn(format_args!("No first event for {:?}", no_events_elapsed));
                            stream_state
                                .instrumentation()
                                .no_events_warning(no_events_elapsed);
                        }
                    }
                    Err(batch_line_error) => match batch_line_error.kind() {
                        EventStreamErrorKind::Parser => {
                            return Err(ConsumerErrorKind::InvalidBatch.into())
                        }
                        EventStreamErrorKind::Io => {
                            let sleeping_dispatcher = sleep_ticker.join().await?;
                            return Ok(WaitForFirstFrameResult::Aborted {
                                sleeping_dispatcher,
                            });
                        }
                    },
                }
            } else {
                stream_state.info(format_args!(
                    "(Should not happen) Stream ended before receiving an events batch after {:?}",
                    no_events_received_since.elapsed()
                ));
                let sleeping_dispatcher = sleep_ticker.join().await?;
                return Ok(WaitForFirstFrameResult::Aborted {
                    sleeping_dispatcher,
                });
            }
        }
    };

    // Recreate the stream by appending the rest to the already received first batch line
    let stream = stream::once(async {
        Ok(EventStreamMessage::Nakadi(NakadiMessage::Events(
            first_frame,
        )))
    })
    .chain(stream);

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
    stream_state: StreamState,
) -> impl Stream<Item = Result<EventStreamMessage, EventStreamError>> + Send {
    let instrumentation = stream_state.instrumentation().clone();

    let frame_stream = FramedStream::new(bytes_stream, instrumentation.clone());

    let event_stream = EventStream::new(frame_stream, instrumentation.clone());
    let drained_stream = start_batch_drain(event_stream, stream_state);
    let drained_stream = UnboundedReceiverStream::new(drained_stream);

    let drained_stream = drained_stream
        .map_ok(EventStreamMessage::Nakadi)
        .chain(stream::once(async {
            Ok(EventStreamMessage::EventStreamEnded)
        }));

    let tick_interval = tick_interval.into_duration();
    let ticker = IntervalStream::new(interval_at(
        (Instant::now() + tick_interval).into(),
        tick_interval,
    ))
    .map(move |_| {
        instrumentation.stream_tick_emitted();
        Ok(EventStreamMessage::Tick(Instant::now()))
    });

    stream::select(drained_stream, ticker)
}

pub(crate) enum NakadiMessage {
    KeepAlive,
    Events(EventStreamBatch),
}

fn start_batch_drain<
    S: futures::Stream<Item = Result<EventStreamBatch, EventStreamError>> + Unpin + Send + 'static,
>(
    mut event_stream: S,
    stream_state: StreamState,
) -> UnboundedReceiver<Result<NakadiMessage, EventStreamError>> {
    let (sender, receiver) = unbounded_channel();

    let task = async move {
        let mut last_frame_received_at: Option<Instant> = None;
        let mut last_batch_frame_received_at: Option<Instant> = None;
        let mut last_received_frame_id = 0;
        while let Some(next_batch_line) = event_stream.next().await {
            if stream_state.cancellation_requested() {
                stream_state.debug(format_args!("[BATCH_DRAIN] Cancellation requested."));
                return;
            }

            match next_batch_line {
                Ok(batch_line) => {
                    let frame_started_at = batch_line.frame_started_at();
                    let frame_completed_at = batch_line.frame_completed_at();
                    let now = Instant::now();

                    // Only measure if we already have a previous batch
                    if let Some(last_received) = last_frame_received_at {
                        let frame_gap = now - last_received;
                        if frame_gap >= Duration::from_secs(30) {
                            stream_state.warn(format_args!(
                                "Huge frame gap of {:?} from frame #{} to frame #{}",
                                last_received.elapsed(),
                                last_received_frame_id,
                                batch_line.frame_id(),
                            ));
                        }
                    }

                    last_frame_received_at = Some(now);
                    last_received_frame_id = batch_line.frame_id();

                    if let Some(info_str) = batch_line.info_str() {
                        stream_state
                            .instrumentation
                            .info_frame_received(frame_started_at, frame_completed_at);
                        stream_state.info(format_args!(
                            "Received info line with frame #{} and {} uncommitted batch(es): {}",
                            batch_line.frame_id(),
                            stream_state.uncommitted_batches(),
                            info_str
                        ));
                    }

                    if batch_line.is_keep_alive_line() {
                        stream_state
                            .instrumentation
                            .keep_alive_frame_received(frame_started_at, frame_completed_at);
                        if sender.send(Ok(NakadiMessage::KeepAlive)).is_err() {
                            stream_state.warn(format_args!(
                                "Could not send keep alive for frame #{} to controller. Streaming stopped",
                                batch_line.frame_id()
                            ));
                            break;
                        }
                        continue;
                    }

                    let bytes = batch_line.bytes().len();
                    stream_state.instrumentation.batch_frame_received(
                        frame_started_at,
                        frame_completed_at,
                        bytes,
                    );

                    // Only measure if we already have a previous batch
                    if let Some(last_received) = last_batch_frame_received_at {
                        let events_batch_gap = now - last_received;
                        stream_state
                            .instrumentation
                            .batch_frame_gap(events_batch_gap);
                        last_batch_frame_received_at = Some(now);
                    } else {
                        last_batch_frame_received_at = Some(now);
                    }

                    let frame_id = batch_line.frame_id();
                    let stats = batch_line.stats();
                    if sender.send(Ok(NakadiMessage::Events(batch_line))).is_err() {
                        stream_state.warn(format_args!(
                            "Could not send events frame #{} to controller. Streaming stopped",
                            frame_id
                        ));
                        break;
                    } else {
                        stream_state.dispatched_events_batch(stats);
                    }
                }
                Err(err) => {
                    if let Err(Err(err)) = sender.send(Err(err)).map_err(|err| err.0) {
                        stream_state.warn(format_args!(
                            "Could not send error to controller. Error to send was: {}",
                            err
                        ));
                    }
                    break;
                }
            }
        }

        stream_state.debug(format_args!(
            "Batch drain exiting. {} uncommitted batches.",
            stream_state.uncommitted_batches()
        ));
    };

    tokio::spawn(task);

    receiver
}
