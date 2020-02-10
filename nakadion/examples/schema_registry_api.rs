use nakadi_types::model::event_type::*;
use nakadi_types::FlowId;
use nakadion::api::{ApiClient, MonitoringApi, SchemaRegistryApi};

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ApiClient::builder().finish_from_env()?;

    //    let mut event_types = client.list_event_types(FlowId::default()).await?;

    //    println!("Event types: {}", event_types.len());

    let event_type_name = EventTypeName::from_env()?;

    let event_type = client
        .get_event_type(&event_type_name, FlowId::default())
        .await?;

    println!("{:#?}", event_type);
    let event_type = client
        .get_event_type_partitions(&event_type_name, FlowId::default())
        .await?;

    println!("{:#?}", event_type);

    Ok(())
}

#[cfg(not(feature = "reqwest"))]
fn main() {
    println!("Please enable the `reqwest` feature which is a default feature");
}
