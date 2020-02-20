use nakadi_types::model::subscription::*;

use nakadion::api::{ApiClient, SubscriptionCommitApi};
use nakadion::components::connector::*;
use nakadion::instrumentation::Instrumentation;

use futures::{
    future::{self, TryFutureExt},
    stream::TryStreamExt,
};
use serde_json::Value;
use tokio::spawn;

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Error> {
    let client = ApiClient::builder().finish_from_env()?;

    let subscription_id = SubscriptionId::from_env()?;

    let connector_params = ConnectorParams {
        subscription_id,
        stream_params: StreamParameters::default(),
        connect_stream_timeout: ConnectStreamTimeoutSecs::default(),
        flow_id: None,
    };
    let connector = Connector::new(client.clone(), connector_params, None);

    let (stream_id, events_stream) = connector.events_stream::<Value>().await?;

    let f = events_stream.try_for_each(move |(meta, events)| {
        let client = client.clone();
        async move {
            println!("{:?}", events);
            let cursor = &[meta.cursor];
            client
                .commit_cursors(subscription_id, stream_id, cursor, RandomFlowId)
                .map_err(Error::from_error)
                .await?;
            Ok(())
        }
    });

    spawn(f).await.map_err(Error::from_error)?
}

#[cfg(not(feature = "reqwest"))]
fn main() {
    println!("Please enable the `reqwest` feature which is a default feature");
}
