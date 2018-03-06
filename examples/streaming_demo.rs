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
use nakadion::maintenance::*;
use nakadion::*;

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
    fn handle(&self, event_type: EventType, events: &[u8]) -> AfterBatchAction {
        AfterBatchAction::Continue
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
        schema: "{ \"properties\": {\"fortune\": {\"type\": \"string\"} }, \"required\": [\"fortune\"] }".into(),
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
    if let Err(err) = maintenance_client.create_event_type(&event_definition) {
        error!("Could not create event type: {}", err);
    }

    info!("Create subscription");
    let request = CreateSubscriptionRequest {
        owning_application: "test-suite".into(),
        event_types: vec![EVENT_TYPE_NAME.into()],
    };

    let subscription_status = maintenance_client.create_subscription(&request).unwrap();
    info!("{:#?}", subscription_status);

    if let Err(err) = consume(subscription_status.subscription().id.clone()) {
        error!("Aborting: {}", err);
    }

    info!("Delete subscription");
    maintenance_client
        .delete_subscription(&subscription_status.subscription().id)
        .unwrap();

    info!("Delete event type");
    maintenance_client
        .delete_event_type(EVENT_TYPE_NAME)
        .unwrap();
}

fn consume(subscription_id: SubscriptionId) -> Result<(), Error> {
    let config_builder = ClientConfigBuilder::default()
        .nakadi_host("http://localhost:8080".into())
        .subscription_id(subscription_id);

    let config = config_builder.build().unwrap();

    let client = Client::new(config, AccessTokenProvider)?;

    let handler_factory = DemoHandlerFactory {
        state: Arc::new(AtomicUsize::new(0)),
    };

    let consumer = Nakadion::start(client, handler_factory, CommitStrategy::AllBatches)?;

    thread::sleep(Duration::from_secs(300));

    consumer.stop();

    consumer.block();
    Ok(())
}
