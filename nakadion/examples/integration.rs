use nakadi_types::{
    model::{event_type::*, subscription::*},
    RandomFlowId,
};

use nakadion::api::{ApiClient, SchemaRegistryApi};
use nakadion::consumer::*;

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event_type_a = EventTypeName::new("Event_Type_A");
    let event_type_b = EventTypeName::new("Event_Type_B");

    let api_client = ApiClient::builder().finish_from_env()?;

    println!("Query registered event types");
    let registered_event_types = api_client.list_event_types(RandomFlowId).await?;

    println!(
        "There are {} event types already registered.",
        registered_event_types.len(),
    );

    for et in registered_event_types {
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

    create_event_type_a(&api_client, &event_type_a).await?;
    create_event_type_b(&api_client, &event_type_b).await?;

    Ok(())
}

async fn create_event_type_a(
    api_client: &ApiClient,
    name: &EventTypeName,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Create event type {}", name);
    let event_type = EventTypeInput::builder()
        .name(name.clone())
        .owning_application("test-app")
        .category(Category::Business)
        .enrichment_strategy(EnrichmentStrategy::MetadataEnrichment)
        .compatibility_mode(CompatibilityMode::None)
        .schema(
            EventTypeSchemaInput::builder()
                .schema_type(SchemaType::JsonSchema)
                .schema(
                    r#"{
                        "description":"test event a",
                        "properties": {
                            "count": {
                                type: "int"
                            }
                        },
                        required: [
                            "count"
                        ]
                }"#,
                )
                .build()?,
        )
        .partition_strategy(PartitionStrategy::Hash)
        .partition_key_fields(PartitionKeyFields::default().partition_key("count"))
        .cleanup_policy(CleanupPolicy::Delete)
        .default_statistic(EventTypeStatistics::new(100, 1_000, 2, 2))
        .options(EventTypeOptions::default())
        .audience(EventTypeAudience::CompanyInternal)
        .build()?;

    api_client
        .create_event_type(&event_type, RandomFlowId)
        .await?;
    Ok(())
}

async fn create_event_type_b(
    api_client: &ApiClient,
    name: &EventTypeName,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Create event type {}", name);
    let event_type = EventTypeInput::builder()
        .name(name.clone())
        .owning_application("test-app")
        .category(Category::Data)
        .enrichment_strategy(EnrichmentStrategy::MetadataEnrichment)
        .compatibility_mode(CompatibilityMode::None)
        .schema(
            EventTypeSchemaInput::builder()
                .schema_type(SchemaType::JsonSchema)
                .schema(
                    r#"{
                        "description":"test event b",
                        "properties": {
                            "count": {
                                type: "int"
                            }
                        },
                        required: [
                            "count"
                        ]
                }"#,
                )
                .build()?,
        )
        .partition_strategy(PartitionStrategy::Hash)
        .partition_key_fields(PartitionKeyFields::default().partition_key("count"))
        .cleanup_policy(CleanupPolicy::Delete)
        .default_statistic(EventTypeStatistics::new(100, 1_000, 4, 4))
        .options(EventTypeOptions::default())
        .audience(EventTypeAudience::CompanyInternal)
        .build()?;

    api_client
        .create_event_type(&event_type, RandomFlowId)
        .await?;
    Ok(())
}

#[cfg(not(feature = "reqwest"))]
fn main() {
    panic!("Please enable the `reqwest` feature which is a default feature");
}
