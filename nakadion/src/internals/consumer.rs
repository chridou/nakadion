use std::sync::Arc;

use crate::nakadi_types::{
    model::subscription::{StreamParameters, SubscriptionId},
    FlowId,
};

use crate::api::{NakadionEssentials, SubscriptionStream, SubscriptionStreamApi};

pub struct Consumer<C>
where
    C: NakadionEssentials + Clone,
{
    api_client: C,
    subscription_id: SubscriptionId,
    stream_params: StreamParameters,
}

impl<C> Consumer<C>
where
    C: NakadionEssentials + Clone,
{
    pub fn xxx(&self) {}
}

mod connect_stream {
    use futures::FutureExt;

    use crate::nakadi_types::{
        model::subscription::{StreamParameters, SubscriptionId},
        FlowId,
    };

    use crate::api::{NakadiApiError, SubscriptionStream, SubscriptionStreamApi};

    pub async fn connect<C: SubscriptionStreamApi>(
        client: &C,
        subscription_id: SubscriptionId,
        stream_params: &StreamParameters,
        flow_id: FlowId,
    ) -> Result<SubscriptionStream, NakadiApiError> {
        client
            .request_stream(subscription_id, stream_params, flow_id)
            .await
    }
}
