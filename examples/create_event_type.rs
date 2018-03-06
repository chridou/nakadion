extern crate env_logger;
#[macro_use]
extern crate log;
extern crate nakadion;

use std::env;
use std::io::Write;

use nakadion::auth::*;
use nakadion::maintenance::*;

use log::LevelFilter;
use env_logger::Builder;

const EVENT_TYPE_NAME: &'static str = "my-event-type";

pub struct AccessTokenProvider;

impl ProvidesAccessToken for AccessTokenProvider {
    fn get_token(&self) -> Result<Option<AccessToken>, TokenError> {
        Ok(None)
    }
}

fn main() {
    let mut builder = Builder::new();

    builder
        .format(|buf, record| writeln!(buf, "{} - {}", record.level(), record.args()))
        .filter(None, LevelFilter::Info);

    if let Ok(rust_log) = env::var("RUST_LOG") {
        builder.parse(&rust_log);
    }

    builder.init();

    let event_schema = EventTypeSchema {
        version: Some("0.1".into()),
        schema_type: SchemaType::JsonSchema,
        schema: "{ \"properties\": {\"fortune\": {\"type\": \"string\"} }, \
                 \"required\": [\"fortune\"] }"
            .into(),
    };

    let event_definition = EventTypeDefinition {
        name: EVENT_TYPE_NAME.into(),
        owning_application: "test-suite".into(),
        category: EventCategory::Data,
        enrichment_strategies: vec![EnrichmentStrategy::MetadataEnrichment],
        schema: event_schema,
        partition_strategy: Some(PartitionStrategy::Hash),
        compatibility_mode: Some(CompatibilityMode::Forward),
        partition_key_fields: Some(vec!["fortune".into()]),
        default_statistic: None,
    };

    let maintenance_client = MaintenanceClient::new("http://localhost:8080", AccessTokenProvider);

    info!("Create event type");
    maintenance_client
        .create_event_type(&event_definition)
        .unwrap();

    info!("Create subscription");

    let request = CreateSubscriptionRequest {
        owning_application: "test-suite".into(),
        event_types: vec![EVENT_TYPE_NAME.into()],
    };

    let subscription_status = maintenance_client.create_subscription(&request).unwrap();
    info!("{:#?}", subscription_status);

    info!("Create subscription a second time");

    let subscription_status = maintenance_client.create_subscription(&request).unwrap();
    info!("{:#?}", subscription_status);

    info!("Delete subscription");

    maintenance_client
        .delete_subscription(&subscription_status.subscription().id)
        .unwrap();

    info!("Delete event type");

    maintenance_client
        .delete_event_type(EVENT_TYPE_NAME)
        .unwrap();
}
