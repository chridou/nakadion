use nakadi_types::{model::subscription::*, NakadiBaseUrl, RandomFlowId};

use nakadion::api::{ApiClient, SchemaRegistryApi};
use nakadion::consumer::*;

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event_type_a = EventTypeName::new("Event Type A");
    let event_type_b = EventTypeName::new("Event Type B");

    let api_client = ApiClient::builder().finish_from_env()?;

    println!("Query registered event types");
    let known_event_types = api_client.list_event_types(RandomFlowId).await?;

    println!(
        "There are {} event types already registered.",
        known_event_types.len(),
    );

    for et in known_event_types {
        if et.name == event_type_a {
            println!("Deleting event type {}", event_type_a);
            api_client
                .delete_event_type(&event_type_a, RandomFlowId)
                .await?;
        }
        if et.name == event_type_b {
            println!("Deleting event type {}", event_type_b);
            api_client
                .delete_event_type(&event_type_b, RandomFlowId)
                .await?;
        }
    }

    Ok(())
}

#[cfg(not(feature = "reqwest"))]
fn main() {
    panic!("Please enable the `reqwest` feature which is a default feature");
}
