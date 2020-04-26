use crate::api::{SubscriptionStreamApi, SubscriptionStreamChunks};
use crate::components::connector::Connector;
use crate::consumer::ConsumerAbort;
use crate::internals::ConsumerState;

/// Connects to a stream by creating a `Connector`
///
/// Connecting can be aborted via the `ConsumerState`
///
/// Failing to connect will always abort the `Consumer` so it is up to
/// the configuration of the connector to control connect behaviour.
pub(crate) async fn connect_with_retries<C: SubscriptionStreamApi>(
    stream_api: C,
    consumer_state: ConsumerState,
) -> Result<SubscriptionStreamChunks, ConsumerAbort>
where
    C: SubscriptionStreamApi + Send + Sync + 'static,
{
    let mut connector =
        Connector::new_with_config(stream_api, consumer_state.config().connect_config.clone());

    connector.set_logger(consumer_state.clone());
    connector.set_instrumentation(consumer_state.instrumentation().clone());
    let stream = connector
        .connect_abortable(consumer_state.subscription_id(), || {
            consumer_state.global_cancellation_requested()
        })
        .await?;
    Ok(stream)
}
