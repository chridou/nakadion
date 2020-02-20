use nakadi_types::model::subscription::*;

use nakadion::api::{ApiClient, SubscriptionCommitApi};
use nakadion::components::connector::*;

use futures::{future::TryFutureExt, stream::TryStreamExt};
use serde_json::Value;
use tokio::spawn;

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Error> {
    let client = ApiClient::builder().finish_from_env()?;

    let subscription_id = SubscriptionId::from_env()?;

    let connector = client.connector();
    let (stream_id, events_stream) = Streamer(connector)
        .events_stream::<Value>(subscription_id)
        .await?;

    let f = events_stream.try_for_each(move |(meta, events)| {
        let client = client.clone();
        async move {
            if let Some(events) = events {
                println!("{:?}", events);
            } else {
                println!("empty line");
            }
            let cursor = &[meta.cursor];
            client
                .commit_cursors(subscription_id, stream_id, cursor, RandomFlowId)
                .map_err(|err| Error::new(format!("Could not commit: {}", err)))
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
