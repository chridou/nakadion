use serde::{Deserialize, Serialize};
use tokio::spawn;

use nakadi_types::{
    model::{
        event::{publishable::*, *},
        event_type::*,
        subscription::*,
    },
    RandomFlowId,
};

use nakadion::api::{ApiClient, SchemaRegistryApi};
use nakadion::consumer::*;
use nakadion::publisher::*;

const N_EVENTS: usize = 10_000;

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event_type_a = EventTypeName::new("Event_Type_A");
    let event_type_b = EventTypeName::new("Event_Type_B");

    let api_client = ApiClient::builder().finish_from_env()?;
    let mut publisher = Publisher::new(api_client.clone());
    publisher.on_retry(|err, d| println!("Publish attempt failed (retry in {:?}): {}", d, err));

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

    let join1 = spawn(publish_events_a(publisher.clone(), event_type_a.clone()));
    let join2 = spawn(publish_events_b(publisher.clone(), event_type_b.clone()));

    join1.await??;
    join2.await??;

    Ok(())
}

async fn create_event_type_a(api_client: &ApiClient, name: &EventTypeName) -> Result<(), Error> {
    println!("Create event type {}", name);
    let event_type = EventTypeInput::builder()
        .name(name.clone())
        .owning_application("test-app")
        .category(Category::Business)
        .enrichment_strategy(EnrichmentStrategy::MetadataEnrichment)
        .compatibility_mode(CompatibilityMode::None)
        .schema(EventTypeSchemaInput::json_schema_parsed(
            r#"{"description":"test event b","properties":{"count":{"type":"integer"}},"required":["count"]}"#
        )?)
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

async fn create_event_type_b(api_client: &ApiClient, name: &EventTypeName) -> Result<(), Error> {
    println!("Create event type {}", name);
    let event_type = EventTypeInput::builder()
        .name(name.clone())
        .owning_application("test-app")
        .category(Category::Data)
        .enrichment_strategy(EnrichmentStrategy::MetadataEnrichment)
        .compatibility_mode(CompatibilityMode::None)
        .schema(EventTypeSchemaInput::json_schema_parsed(
            r#"{"description":"test event b","properties":{"count":{"type":"integer"}},"required":["count"]}"#
        )?)
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

async fn publish_events_a<P>(publisher: P, name: EventTypeName) -> Result<(), Error>
where
    P: PublishesEvents + Send + Sync + 'static,
{
    let events: Vec<_> = (0..N_EVENTS)
        .map(|count| Payload { count })
        .map(|payload| BusinessEventPub {
            payload,
            metadata: EventMetaDataPub::new(Eid),
        })
        .collect();

    for batch in events.chunks(100) {
        if let Err(err) = publisher.publish_events(&name, batch, ()).await {
            println!("{:#?}", err);
            return Err(Error::from_error(err));
        }
    }

    println!("Published {} events for event type {}", N_EVENTS, name);

    Ok(())
}

async fn publish_events_b<P>(publisher: P, name: EventTypeName) -> Result<(), Error>
where
    P: PublishesEvents + Send + Sync + 'static,
{
    let events: Vec<_> = (0..N_EVENTS)
        .map(|count| Payload { count })
        .map(|data| DataChangeEventPub {
            data_type: "integer".into(),
            data_op: DataOp::Snapshot,
            data,
            metadata: EventMetaDataPub::new(Eid),
        })
        .collect();

    for batch in events.chunks(100) {
        if let Err(err) = publisher.publish_events(&name, batch, ()).await {
            println!("{:#?}", err);
            return Err(Error::from_error(err));
        }
    }

    println!("Published {} events for event type {}", N_EVENTS, name);

    Ok(())
}
#[cfg(not(feature = "reqwest"))]
fn main() {
    panic!("Please enable the `reqwest` feature which is a default feature");
}

type EventAPub = BusinessEventPub<Payload>;
type EventBPub = DataChangeEvent<Payload>;

#[derive(Debug, Serialize, Deserialize)]
struct Payload {
    count: usize,
}
