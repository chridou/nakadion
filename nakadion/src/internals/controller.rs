use std::sync::Arc;
use std::time::Duration;

use futures::{pin_mut, stream, StreamExt, TryFutureExt, TryStreamExt};
use std::future::Future;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
    time::interval,
};

use crate::nakadi_types::model::subscription::{StreamParameters, SubscriptionId};

use crate::api::{
    BytesStream, NakadionEssentials, SubscriptionCommitApi, SubscriptionStream,
    SubscriptionStreamApi,
};
use crate::consumer::{ConsumerError, ConsumerErrorKind, DispatchStrategy};
use crate::event_handler::{BatchHandler, BatchHandlerFactory};
use crate::event_stream::{BatchLineErrorKind, BatchLineStream, FramedStream};
use crate::internals::dispatcher::{
    ActiveDispatcher, Dispatcher, DispatcherMessage, SleepingDispatcher,
};

use super::{ConsumerState, StreamState};

#[derive(Clone)]
pub struct Controller<H, C> {
    params: ControllerParams<H, C>,
}

impl<H, C> Controller<H, C>
where
    C: NakadionEssentials + Clone,
    H: BatchHandler,
{
    pub fn new(params: ControllerParams<H, C>) -> Self {
        Self { params }
    }

    pub async fn start(self, consumer_state: ConsumerState) -> Result<(), ConsumerError> {
        let params = self.params;
        create_background_task(params).await
    }
}

async fn create_background_task<H, C>(
    mut params: ControllerParams<H, C>,
) -> Result<(), ConsumerError>
where
    C: NakadionEssentials + Clone,
    H: BatchHandler,
{
    let mut sleeping_dispatcher = Dispatcher::new(
        params.dispatch_strategy.clone(),
        Arc::clone(&params.handler_factory),
        params.api_client.clone(),
    );

    loop {
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

        let stream_state = params.consumer_state.stream_state(stream_id);
        let (returned_params, returned_dispatcher) =
            consume_stream(params, stream_state, bytes_stream, sleeping_dispatcher).await?;

        sleeping_dispatcher = returned_dispatcher;
        params = returned_params;

        if params.consumer_state.global_cancellation_requested() {
            return Err(ConsumerError::new(ConsumerErrorKind::UserAbort));
        }
    }
}

async fn consume_stream<H, C>(
    params: ControllerParams<H, C>,
    stream_state: StreamState,
    bytes_stream: BytesStream,
    dispatcher: SleepingDispatcher<H, C>,
) -> Result<(ControllerParams<H, C>, SleepingDispatcher<H, C>), ConsumerError>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    H: BatchHandler,
{
    let frame_stream = FramedStream::new(bytes_stream);
    let batch_stream = BatchLineStream::new(frame_stream).map_ok(DispatcherMessage::Batch);

    let ticker = interval(params.tick_interval).map(|_| Ok(DispatcherMessage::Tick));

    let merged = stream::select(batch_stream, ticker);

    let (batch_lines_sink, batch_lines_receiver) = unbounded_channel::<DispatcherMessage>();

    let active_dispatcher = dispatcher.start(stream_state.clone(), batch_lines_receiver);

    pin_mut!(merged);
    while let Some(msg) = merged.next().await {
        let dispatcher_message = match msg {
            Ok(msg) => msg,
            Err(batch_line_error) => match batch_line_error.kind() {
                BatchLineErrorKind::Parser => return Err(ConsumerErrorKind::InvalidBatch.into()),
                BatchLineErrorKind::Io => {
                    stream_state.request_stream_cancellation();
                    break;
                }
            },
        };

        let msg_for_dispatcher = match dispatcher_message {
            DispatcherMessage::Tick => DispatcherMessage::Tick,
            DispatcherMessage::Batch(batch) => {
                if batch.is_keep_alive_line() {
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

    let sleeping_dispatcher = active_dispatcher.join().await?;

    Ok((params, sleeping_dispatcher))
}

pub struct ControllerParams<H, C> {
    pub api_client: C,
    pub consumer_state: ConsumerState,
    pub dispatch_strategy: DispatchStrategy,
    pub handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
    pub tick_interval: Duration,
}

impl<H, C> Clone for ControllerParams<H, C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            api_client: self.api_client.clone(),
            consumer_state: self.consumer_state.clone(),
            dispatch_strategy: self.dispatch_strategy.clone(),
            handler_factory: Arc::clone(&self.handler_factory),
            tick_interval: self.tick_interval,
        }
    }
}

mod connect_stream {
    use crate::nakadi_types::{
        model::subscription::{StreamParameters, SubscriptionId},
        FlowId,
    };

    use crate::api::{NakadiApiError, SubscriptionStream, SubscriptionStreamApi};
    use crate::consumer::{ConsumerError, ConsumerErrorKind};

    use crate::internals::ConsumerState;

    pub async fn connect_with_retries<C: SubscriptionStreamApi>(
        api_client: C,
        consumer_state: ConsumerState,
    ) -> Result<SubscriptionStream, ConsumerError> {
        loop {
            if consumer_state.global_cancellation_requested() {
                return Err(ConsumerError::new(ConsumerErrorKind::UserAbort));
            }

            match connect(
                &api_client,
                consumer_state.subscription_id,
                &consumer_state.stream_params,
            )
            .await
            {
                Ok(stream) => return Ok(stream),
                Err(err) => {
                    continue;
                }
            }
        }
    }

    async fn connect<C: SubscriptionStreamApi>(
        client: &C,
        subscription_id: SubscriptionId,
        stream_params: &StreamParameters,
    ) -> Result<SubscriptionStream, NakadiApiError> {
        client
            .request_stream(subscription_id, stream_params, FlowId::default())
            .await
    }
}
