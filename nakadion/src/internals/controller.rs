use std::sync::Arc;

use crate::nakadi_types::{
    model::subscription::{StreamParameters, SubscriptionId},
    FlowId,
};

use std::future::Future;

use crate::api::{
    BytesStream, NakadionEssentials, SubscriptionCommitApi, SubscriptionStream,
    SubscriptionStreamApi,
};
use crate::event_stream::{BatchLineStream, FramedStream};

use super::{ConsumerState, StreamState};

pub async fn create_background_task<C>(params: ControllerParams<C>, consumer_state: ConsumerState)
where
    C: NakadionEssentials + Clone,
{
    loop {
        if consumer_state.global_cancellation_requested() {
            break;
        }

        let (stream_id, bytes_stream) = match connect_stream::connect_with_retries(
            params.clone(),
            consumer_state.clone(),
        )
        .await
        {
            Ok(stream) => stream.parts(),
            Err(err) => {
                consumer_state.request_global_cancellation();
                break;
            }
        };

        if consumer_state.global_cancellation_requested() {
            break;
        }

        let stream_state = consumer_state.stream_state(stream_id);
        consume_stream(params.clone(), stream_state, bytes_stream).await
    }
}

async fn consume_stream<C>(
    params: ControllerParams<C>,
    stream_state: StreamState,
    bytes_stream: BytesStream,
) where
    C: SubscriptionCommitApi + Clone + Send + 'static,
{
    let frame_stream = FramedStream::new(bytes_stream);
    let batch_stream = BatchLineStream::new(frame_stream);
}

#[derive(Clone)]
pub struct ControllerParams<C> {
    api_client: C,
    subscription_id: SubscriptionId,
    stream_params: StreamParameters,
}

impl<C> ControllerParams<C>
where
    C: NakadionEssentials,
{
    pub fn new(
        api_client: C,
        subscription_id: SubscriptionId,
        stream_params: StreamParameters,
    ) -> Self {
        Self {
            api_client,
            subscription_id,
            stream_params,
        }
    }
}

mod connect_stream {
    use crate::nakadi_types::{
        model::subscription::{StreamParameters, SubscriptionId},
        FlowId,
    };

    use crate::api::{NakadiApiError, SubscriptionStream, SubscriptionStreamApi};

    use crate::internals::ConsumerState;

    use super::ControllerParams;

    pub async fn connect_with_retries<C: SubscriptionStreamApi>(
        controller_params: ControllerParams<C>,
        consumer_state: ConsumerState,
    ) -> Result<SubscriptionStream, NakadiApiError> {
        connect(
            &controller_params.api_client,
            controller_params.subscription_id,
            &controller_params.stream_params,
        )
        .await
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
