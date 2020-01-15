use nakadion::model::*;
use nakadion::nakadi_api::ApiClient;
use nakadion::nakadi_api::SchemaRegistryApi;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ApiClient::new_from_env()?;

    let mut event_types = client.list_event_types(FlowId::default()).await?;

    println!("Event types: {}", event_types.len());

    let event_type = event_types.pop().unwrap().name;

    let event_type = client
        .get_event_type(&event_type, FlowId::default())
        .await?;

    println!("{:#?}", event_type);

    Ok(())
}
