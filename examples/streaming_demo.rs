extern crate env_logger;
extern crate failure;
#[macro_use]
extern crate log;
extern crate nakadion;
#[macro_use]
extern crate serde;

use std::env;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use failure::Error;
use nakadion::auth::*;
use nakadion::api_client::*;
use nakadion::*;
use nakadion::streaming_client::*;

use log::LevelFilter;
use env_logger::Builder;

const EVENT_TYPE_NAME: &'static str = "my-event-type";

pub struct AccessTokenProvider;

impl ProvidesAccessToken for AccessTokenProvider {
    fn get_token(&self) -> Result<Option<AccessToken>, TokenError> {
        Ok(None)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    fortune: String,
}

pub struct DemoHandlerFactory {
    pub state: Arc<AtomicUsize>,
}

impl HandlerFactory for DemoHandlerFactory {
    type Handler = DemoHandler;

    fn create_handler(&self, partition: &PartitionId) -> Self::Handler {
        info!("Creating handler for {}", partition);

        DemoHandler {
            state: self.state.clone(),
        }
    }
}

pub struct DemoHandler {
    pub state: Arc<AtomicUsize>,
}

impl BatchHandler for DemoHandler {
    fn handle(&self, event_type: EventType, events: &[u8]) -> ProcessingStatus {
        ProcessingStatus::processed(0)
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
        schema: "{ \"properties\": {\"fortune\": {\"type\": \
                 \"string\"} }, \"required\": [\"fortune\"] }"
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

    let api_client = ::nakadion::api_client::ConfigBuilder::default()
        .nakadi_host("http://localhost:8080")
        .build_client(AccessTokenProvider)
        .unwrap();

    info!("Create event type");
    if let Err(err) = api_client.create_event_type(&event_definition) {
        error!("Could not create event type: {}", err);
    }

    info!("Create subscription");
    let request = CreateSubscriptionRequest {
        owning_application: "test-suite".into(),
        event_types: vec![EVENT_TYPE_NAME.into()],
    };

    let subscription_status = api_client.create_subscription(&request).unwrap();
    info!("{:#?}", subscription_status);

    if let Err(err) = consume(
        subscription_status.subscription().id.clone(),
        api_client.clone(),
    ) {
        error!("Aborting: {}", err);
    }

    info!("Delete subscription");
    api_client
        .delete_subscription(&subscription_status.subscription().id)
        .unwrap();

    info!("Delete event type");
    api_client.delete_event_type(EVENT_TYPE_NAME).unwrap();
}

fn consume(subscription_id: SubscriptionId, api_client: NakadiApiClient) -> Result<(), Error> {
    let config_builder =
        ::nakadion::streaming_client::ConfigBuilder::default().nakadi_host("http://localhost:8080");

    let streaming_client = config_builder.build_client(AccessTokenProvider)?;

    let handler_factory = DemoHandlerFactory {
        state: Arc::new(AtomicUsize::new(0)),
    };

    let nakadion = Nakadion::start(
        subscription_id,
        streaming_client,
        api_client,
        handler_factory,
        CommitStrategy::AllBatches,
    )?;

    thread::sleep(Duration::from_secs(300));

    nakadion.stop();

    nakadion.block();
    Ok(())
}
