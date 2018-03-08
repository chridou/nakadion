extern crate chrono;
extern crate env_logger;
extern crate failure;
#[macro_use]
extern crate log;
extern crate nakadion;
#[macro_use]
extern crate serde;
extern crate serde_json;
extern crate uuid;

use std::env;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use chrono::DateTime;
use chrono::offset::Utc;
use uuid::Uuid;

use failure::Error;
use nakadion::auth::*;
use nakadion::api_client::*;
use nakadion::*;
use nakadion::streaming_client::*;
use nakadion::events::*;
use nakadion::FlowId;
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
pub struct EventData {
    fortune: String,
}

#[derive(Serialize)]
pub struct OutgoingEvent {
    data: EventData,
    data_op: String,
    metadata: OutgoingMetadata,
    data_type: String,
}

#[derive(Deserialize)]
pub struct IncomingEvent {
    data: EventData,
    data_op: String,
    metadata: IncomingMetadata,
    data_type: String,
}

impl OutgoingEvent {
    pub fn new() -> OutgoingEvent {
        OutgoingEvent {
            data_op: "S".into(),
            data_type: "hallo".into(),
            data: EventData {
                fortune: Uuid::new_v4().to_string(),
            },
            metadata: OutgoingMetadata {
                eid: Uuid::new_v4(),
                event_type: None,
                occurred_at: Utc::now(),
                parent_eids: Vec::new(),
                partition: None,
            },
        }
    }
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

impl TypedBatchHandler for DemoHandler {
    type Event = IncomingEvent;
    fn handle(&self, events: Vec<IncomingEvent>) -> TypedProcessingStatus {
        self.state.fetch_add(events.len(), Ordering::Relaxed);
        TypedProcessingStatus::Processed
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
        default_statistic: Some(EventTypeStatistics {
            messages_per_minute: 6000000,
            message_size: 500,
            read_parallelism: 16,
            write_parallelism: 16,
        }),
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

    publish();

    if let Err(err) = consume() {
        error!("Aborting: {}", err);
    }

    info!("Delete subscription");
    api_client
        .delete_subscription(&subscription_status.subscription().id)
        .unwrap();

    info!("Delete event type");
    api_client.delete_event_type(EVENT_TYPE_NAME).unwrap();
}

fn consume() -> Result<(), Error> {
    let count = Arc::new(AtomicUsize::new(0));

    let handler_factory = DemoHandlerFactory {
        state: count.clone(),
    };

    let nakadion_builder = NakadionBuilder::default()
        .nakadi_host("http://localhost:8080")
        .subscription_discovery(SubscriptionDiscovery::OwningApplication(
            "test-suite".into(),
            vec![EVENT_TYPE_NAME.into()],
        ))
        .max_uncommitted_events(10000)
        .batch_limit(100);

    let nakadion =
        nakadion_builder.build_and_start_without_metrics(handler_factory, AccessTokenProvider)?;

    thread::sleep(Duration::from_secs(90));

    info!("Events consumed: {}", count.load(Ordering::Relaxed));

    nakadion.stop();

    nakadion.block_until_stopped();
    Ok(())
}

fn publish() {
    thread::spawn(move || {
        let publisher =
            nakadion::publisher::NakadiPublisher::new("http://localhost:8080", AccessTokenProvider);

        let mut count = 0;

        for _ in 0..10_000 {
            let mut events = Vec::new();
            for _ in 0..100 {
                count += 1;
                let event = OutgoingEvent::new();
                events.push(event);
            }
            if let Err(err) = publisher.publish_events(
                EVENT_TYPE_NAME,
                &events,
                Some(FlowId::default()),
                Duration::from_millis(500),
            ) {
                error!("{}", err);
            }
        }

        info!("{} events published", count);
    });
}
