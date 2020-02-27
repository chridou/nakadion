use futures::future::{BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use tokio::{spawn, time::delay_for};

use nakadi_types::{
    model::{
        event::{publishable::*, *},
        event_type::*,
        subscription::*,
    },
    RandomFlowId,
};

use nakadion::api::{api_ext::SubscriptionApiExt, ApiClient, SchemaRegistryApi, SubscriptionApi};
use nakadion::consumer::*;
use nakadion::handler::*;
use nakadion::publisher::*;

const N_EVENTS: usize = 10_000;
const EVENT_TYPE_A: &str = "Event_Type_A";
const EVENT_TYPE_B: &str = "Event_Type_B";

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event_type_a = EventTypeName::new(EVENT_TYPE_A);
    let event_type_b = EventTypeName::new(EVENT_TYPE_B);

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

    create_event_type_a(&api_client).await?;
    create_event_type_b(&api_client).await?;

    let join1 = spawn(publish_events_a(publisher.clone()));
    let join2 = spawn(publish_events_b(publisher.clone()));

    join1.await??;
    join2.await??;

    println!("Create subscription for {}", EVENT_TYPE_A);
    let subscription_input = SubscriptionInput::builder()
        .owning_application("nakadi_test")
        .consumer_group("test_app_1")
        .event_types(EventTypeNames::default().event_type_name(EVENT_TYPE_A))
        .read_from(ReadFrom::Begin)
        .authorization(
            SubscriptionAuthorization::default()
                .admin(("*", "*"))
                .reader(("*", "*")),
        )
        .finish_for_create()?;
    let subscription = api_client
        .create_subscription(&subscription_input, ())
        .await?;

    let subscription_id = subscription.id;

    consume_event_type_a(api_client.clone(), subscription_id).await?;

    println!(
        "Create subscription for {} & {}",
        EVENT_TYPE_A, EVENT_TYPE_B
    );
    let subscription_input = SubscriptionInput::builder()
        .owning_application("nakadi_test")
        .consumer_group("test_app_2")
        .event_types(
            EventTypeNames::default()
                .event_type_name(EVENT_TYPE_A)
                .event_type_name(EVENT_TYPE_B),
        )
        .read_from(ReadFrom::Begin)
        .authorization(
            SubscriptionAuthorization::default()
                .admin(("*", "*"))
                .reader(("*", "*")),
        )
        .finish_for_create()?;
    let subscription = api_client
        .create_subscription(&subscription_input, ())
        .await?;

    let subscription_id = subscription.id;

    consume_event_types_ab(api_client.clone(), subscription_id).await?;

    println!("FINISHED WITH SUCCESS");

    Ok(())
}

async fn create_event_type_a(api_client: &ApiClient) -> Result<(), Error> {
    println!("Create event type {}", EVENT_TYPE_A);
    let event_type = EventTypeInput::builder()
        .name(EVENT_TYPE_A)
        .owning_application("test-app")
        .category(Category::Business)
        .enrichment_strategy(EnrichmentStrategy::MetadataEnrichment)
        .compatibility_mode(CompatibilityMode::None)
        .schema(EventTypeSchemaInput::json_schema_parsed(
            r#"{"description":"test event b","properties":{"count":{"type":"integer"}},"required":["count"]}"#
        )?)
        .partition_strategy(PartitionStrategy::Hash)
        .partition_key_fields("count")
        .cleanup_policy(CleanupPolicy::Delete)
        .default_statistic(EventTypeStatistics::new(100, 1_000, 2, 2))
        .options(EventTypeOptions::default())
        .audience(EventTypeAudience::CompanyInternal)
        .authorization( EventTypeAuthorization::new(("*", "*"), ("*", "*"), ("*", "*")))
        .build()?;

    api_client
        .create_event_type(&event_type, RandomFlowId)
        .await?;
    Ok(())
}

async fn create_event_type_b(api_client: &ApiClient) -> Result<(), Error> {
    println!("Create event type {}", EVENT_TYPE_B);
    let event_type = EventTypeInput::builder()
        .name(EVENT_TYPE_B)
        .owning_application("test-app")
        .category(Category::Data)
        .enrichment_strategy(EnrichmentStrategy::MetadataEnrichment)
        .compatibility_mode(CompatibilityMode::None)
        .schema(EventTypeSchemaInput::json_schema_parsed(
            r#"{"description":"test event b","properties":{"count":{"type":"integer"}},"required":["count"]}"#
        )?)
        .partition_strategy(PartitionStrategy::Hash)
        .partition_key_fields("count")
        .cleanup_policy(CleanupPolicy::Delete)
        .default_statistic(EventTypeStatistics::new(100, 1_000, 4, 4))
        .options(EventTypeOptions::default())
        .audience(EventTypeAudience::CompanyInternal)
        .authorization( EventTypeAuthorization::new(("*", "*"), ("*", "*"), ("*", "*")))
        .build()?;

    api_client
        .create_event_type(&event_type, RandomFlowId)
        .await?;
    Ok(())
}

async fn publish_events_a<P>(publisher: P) -> Result<(), Error>
where
    P: PublishesEvents + Send + Sync + 'static,
{
    let name = EventTypeName::new(EVENT_TYPE_A);
    let events: Vec<_> = (0..N_EVENTS)
        .map(|n| Payload { count: n + 1 })
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

async fn publish_events_b<P>(publisher: P) -> Result<(), Error>
where
    P: PublishesEvents + Send + Sync + 'static,
{
    let name = EventTypeName::new(EVENT_TYPE_B);
    let events: Vec<_> = (0..N_EVENTS)
        .map(|n| Payload { count: n + 1 })
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

type EventA = BusinessEvent<Payload>;
type EventB = DataChangeEvent<Payload>;

async fn consume_event_type_a(
    api_client: ApiClient,
    subscription_id: SubscriptionId,
) -> Result<(), Error> {
    println!("Consume event type {}", EVENT_TYPE_A);
    consume_subscription(
        api_client.clone(),
        subscription_id,
        HandlerAFactory::new(),
        50_005_000,
        DispatchMode::AllSeq,
    )
    .await?;
    consume_subscription(
        api_client.clone(),
        subscription_id,
        HandlerAFactory::new(),
        50_005_000,
        DispatchMode::EventTypePar,
    )
    .await?;
    consume_subscription(
        api_client.clone(),
        subscription_id,
        HandlerAFactory::new(),
        50_005_000,
        DispatchMode::EventTypePartitionPar,
    )
    .await?;

    Ok(())
}

async fn consume_event_types_ab(
    api_client: ApiClient,
    subscription_id: SubscriptionId,
) -> Result<(), Error> {
    println!("Consume event types {} & {}", EVENT_TYPE_A, EVENT_TYPE_B);
    consume_subscription(
        api_client.clone(),
        subscription_id,
        HandlerABFactory::new(),
        100_010_000,
        DispatchMode::AllSeq,
    )
    .await?;
    consume_subscription(
        api_client.clone(),
        subscription_id,
        HandlerABFactory::new(),
        100_010_000,
        DispatchMode::EventTypePar,
    )
    .await?;
    consume_subscription(
        api_client.clone(),
        subscription_id,
        HandlerABFactory::new(),
        100_010_000,
        DispatchMode::EventTypePartitionPar,
    )
    .await?;

    Ok(())
}

async fn consume_subscription<F: BatchHandlerFactory + GetSum>(
    api_client: ApiClient,
    subscription_id: SubscriptionId,
    factory: F,
    target_value: usize,
    dispatch_mode: DispatchMode,
) -> Result<(), Error> {
    println!("Consume with dispatch mode {}", dispatch_mode);

    println!("Reset cursors");
    api_client
        .reset_cursors_to_begin(subscription_id, ())
        .await?;

    let check_value = factory.get();

    let consumer = Consumer::builder_from_env()?
        .subscription_id(subscription_id)
        .dispatch_mode(dispatch_mode)
        .build_with(api_client.clone(), factory, StdOutLogger::default())?;

    println!("Consume");
    let (consumer_handle, consuming) = consumer.start();

    spawn(wait_for_all_consumed(
        subscription_id,
        consumer_handle,
        api_client,
    ));

    consuming.await.into_result()?;
    println!("Consumed");

    assert_eq!(check_value.load(Ordering::SeqCst), target_value);

    Ok(())
}

async fn wait_for_all_consumed(
    subscription_id: SubscriptionId,
    handle: ConsumerHandle,
    api_client: ApiClient,
) -> Result<(), Error> {
    loop {
        let stats = api_client
            .get_subscription_stats(subscription_id, false, ())
            .await?;

        if stats.all_consumed() {
            handle.stop();
            break;
        }

        delay_for(Duration::from_secs(1)).await;
    }
    Ok(())
}

struct HandlerA {
    sum: Arc<AtomicUsize>,
    last_received: usize,
}

impl EventsHandler for HandlerA {
    type Event = EventA;

    fn handle<'a>(
        &'a mut self,
        events: Vec<Self::Event>,
        _meta: BatchMeta<'a>,
    ) -> BoxFuture<'a, EventsPostAction> {
        async move {
            for event in events {
                let n = event.payload.count;
                if self.last_received < n {
                    self.sum.fetch_add(n, Ordering::SeqCst);
                    self.last_received = n;
                }
            }

            EventsPostAction::Commit
        }
        .boxed()
    }
}

struct HandlerAFactory {
    sum: Arc<AtomicUsize>,
}

impl HandlerAFactory {
    pub fn new() -> Self {
        Self {
            sum: Arc::new(Default::default()),
        }
    }
}

impl BatchHandlerFactory for HandlerAFactory {
    fn handler<'a>(
        &'a self,
        _assignment: &'a HandlerAssignment,
    ) -> BoxFuture<'a, Result<Box<dyn BatchHandler>, Error>> {
        async move {
            Ok(Box::new(HandlerA {
                sum: Arc::clone(&self.sum),
                last_received: 0,
            }) as Box<_>)
        }
        .boxed()
    }
}

impl GetSum for HandlerAFactory {
    fn get(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.sum)
    }
}

trait GetSum {
    fn get(&self) -> Arc<AtomicUsize>;
}

struct HandlerB {
    sum: Arc<AtomicUsize>,
    last_received: usize,
}

impl EventsHandler for HandlerB {
    type Event = EventB;

    fn handle<'a>(
        &'a mut self,
        events: Vec<Self::Event>,
        _meta: BatchMeta<'a>,
    ) -> BoxFuture<'a, EventsPostAction> {
        async move {
            for event in events {
                let n = event.data.count;
                if self.last_received < n {
                    self.sum.fetch_add(n, Ordering::SeqCst);
                    self.last_received = n;
                }
            }

            EventsPostAction::Commit
        }
        .boxed()
    }
}

struct HandlerABFactory {
    event_type_a: EventTypeName,
    event_type_b: EventTypeName,
    sum: Arc<AtomicUsize>,
}

impl HandlerABFactory {
    pub fn new() -> Self {
        Self {
            sum: Arc::new(Default::default()),
            event_type_a: EVENT_TYPE_A.into(),
            event_type_b: EVENT_TYPE_B.into(),
        }
    }
}

impl GetSum for HandlerABFactory {
    fn get(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.sum)
    }
}

impl BatchHandlerFactory for HandlerABFactory {
    fn handler<'a>(
        &'a self,
        assignment: &'a HandlerAssignment,
    ) -> BoxFuture<'a, Result<Box<dyn BatchHandler>, Error>> {
        async move {
            if let Some(event_type) = assignment.event_type() {
                if *event_type == self.event_type_a {
                    Ok(Box::new(HandlerA {
                        sum: Arc::clone(&self.sum),
                        last_received: 0,
                    }) as Box<_>)
                } else if *event_type == self.event_type_b {
                    Ok(Box::new(HandlerB {
                        sum: Arc::clone(&self.sum),
                        last_received: 0,
                    }) as Box<_>)
                } else {
                    Err(Error::new(format!("invalid assignment: {}", assignment)))
                }
            } else {
                Err(Error::new(format!("invalid assignment: {}", assignment)))
            }
        }
        .boxed()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Payload {
    count: usize,
}
