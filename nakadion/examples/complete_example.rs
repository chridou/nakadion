use nakadi_types::{model::subscription::*, NakadiBaseUrl, RandomFlowId};

use nakadion::api::{ApiClient, SchemaRegistryApi};
use nakadion::consumer::*;

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nakadi_base_url = NakadiBaseUrl::from_env()?;
    let api_client = ApiClient::builder().finish_from_env()?;

    println!("There should be no events");
    let known_event_types = api_client.list_event_types(RandomFlowId).await?;
    assert!(known_event_types.is_empty());

    Ok(())
}

#[cfg(not(feature = "reqwest"))]
fn main() {
    panic!("Please enable the `reqwest` feature which is a default feature");
}
