use nakadi_types::{model::subscription::*, NakadiBaseUrl, RandomFlowId};

use nakadion::api::{ApiClient, SchemaRegistryApi};
use nakadion::consumer::*;

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_client = ApiClient::builder().finish_from_env()?;

    println!("Query registered event types");
    let known_event_types = api_client.list_event_types(RandomFlowId).await?;
    println!(
        "There are {} event types registered",
        known_event_types.len()
    );

    Ok(())
}

#[cfg(not(feature = "reqwest"))]
fn main() {
    panic!("Please enable the `reqwest` feature which is a default feature");
}
