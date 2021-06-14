//! This example is the integration test used with CI.
//!
//! It should not be executed against production systems.
//!
//! The included `docker-compose.yml` can be used to start a local
//! Nakadi.
//!
//! 1) Start Nakadi with `docker-compose up` in the project directory
//! 2) Run with `cargo run --release --example integration --features="slog reqwest"`
//!
//! To enable debug logging use `export NAKADION_DEBUG_LOGGING_ENABLED=true`
//!
//! The feature `slog` can be omitted to simply use std::out as the log target.

#[cfg(feature = "reqwest")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("===================");
    println!("= BASIC SCHEDULER =");
    println!("===================");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime.block_on(run::run())
}

#[cfg(not(feature = "reqwest"))]
fn main() {
    panic!("Please enable the `reqwest` feature which is a default feature");
}

#[cfg(feature = "reqwest")]
mod run {
    const N_EVENTS: usize = 100_000;
    const EVENT_TYPE_A: &str = "Event_Type_A";
    const EVENT_TYPE_B: &str = "Event_Type_B";

    use futures::{future::BoxFuture, FutureExt, StreamExt};
    use serde::{Deserialize, Serialize};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;

    use tokio::{spawn, time::sleep};

    use nakadi_types::{
        NakadiBaseUrl, RandomFlowId,
        {
            event::{publishable::*, *},
            event_type::*,
            subscription::*,
        },
    };

    use nakadion::api::{api_ext::SubscriptionApiExt, *};
    use nakadion::auth::NoAuthAccessTokenProvider;
    use nakadion::consumer::*;
    use nakadion::handler::*;
    use nakadion::publisher::*;

    #[cfg(feature = "slog")]
    fn logger() -> impl LoggingAdapter + Clone {
        use slog::{o, Drain};

        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        let logger = slog::Logger::root(drain, o!());

        let config = LogConfig::from_env().unwrap();
        SlogLoggingAdapter::new_with_config(logger, config)
    }

    #[cfg(not(feature = "slog"))]
    fn logger() -> impl LoggingAdapter + Clone {
        let config = LogConfig::from_env().unwrap();
        StdOutLoggingAdapter::new(config)
    }

    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let _ = logger();
        let nakadi_base_url = NakadiBaseUrl::try_from_env()?
            .unwrap_or_else(|| "http://localhost:8080".parse().unwrap());

        let mut api_client = ApiClient::default_builder()
            .nakadi_base_url(nakadi_base_url)
            .finish(NoAuthAccessTokenProvider)?;

        api_client.set_on_retry(|err, retry_in| {
            println!("WARNING! Request failed (retry in {:?}): {}", retry_in, err)
        });

        remove_subscriptions(api_client.clone()).await?;

        api_check(&api_client).await?;

        let mut publisher = Publisher::new(api_client.clone());
        publisher.set_logger(StdOutLoggingAdapter::default());

        println!("Query registered event types");
        let registered_event_types = api_client.list_event_types(RandomFlowId).await?;

        println!(
            "There are {} event types already registered.",
            registered_event_types.len(),
        );

        let event_type_a = EventTypeName::new(EVENT_TYPE_A);
        let event_type_b = EventTypeName::new(EVENT_TYPE_B);

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
                    .admin(("user", get_nakadi_user()))
                    .reader(("user", get_nakadi_user())),
            )
            .finish_for_create()?;
        let subscription = api_client
            .create_subscription(&subscription_input, ())
            .await?;

        let subscription_id = subscription.id;

        consume_event_type_a(api_client.clone(), subscription_id).await?;

        println!("Delete subscription");
        api_client
            .delete_subscription(subscription_id, RandomFlowId)
            .await?;

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
                    .admin(("user", get_nakadi_user()))
                    .reader(("user", get_nakadi_user())),
            )
            .finish_for_create()?;
        let subscription = api_client
            .create_subscription(&subscription_input, ())
            .await?;

        let subscription_id = subscription.id;

        consume_event_types_ab(api_client.clone(), subscription_id).await?;

        println!("Delete subscription");
        api_client
            .delete_subscription(subscription_id, RandomFlowId)
            .await?;

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
            .map(|data| BusinessEventPub {
                data,
                metadata: EventMetaDataPub::new(Eid),
            })
            .collect();

        for batch in events.chunks(500) {
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

        for batch in events.chunks(50) {
            if let Err(err) = publisher.publish_events(&name, batch, ()).await {
                println!("{:#?}", err);
                return Err(Error::from_error(err));
            }
        }

        println!("Published {} events for event type {}", N_EVENTS, name);

        Ok(())
    }

    type EventA = BusinessEvent<Payload>;
    type EventB = DataChangeEvent<Payload>;

    async fn consume_event_type_a(
        api_client: ApiClient,
        subscription_id: SubscriptionId,
    ) -> Result<(), Error> {
        let sp1 = StreamParameters::default()
            .batch_limit(100)
            .stream_limit(11_000)
            .batch_flush_timeout_secs(1)
            .max_uncommitted_events(10_000);

        let sp2 = StreamParameters::default()
            .batch_limit(100)
            .batch_flush_timeout_secs(1)
            .max_uncommitted_events(10_000);
        let params = vec![sp1, sp2];

        for param in params {
            println!("Consume event type {}", EVENT_TYPE_A);
            consume_subscription(
                api_client.clone(),
                subscription_id,
                HandlerAFactory::new(),
                N_EVENTS,
                DispatchMode::AllSeq,
                param.clone(),
            )
            .await?;
            consume_subscription(
                api_client.clone(),
                subscription_id,
                HandlerAFactory::new(),
                N_EVENTS,
                DispatchMode::EventTypePar,
                param.clone(),
            )
            .await?;
            consume_subscription(
                api_client.clone(),
                subscription_id,
                HandlerAFactory::new(),
                N_EVENTS,
                DispatchMode::EventTypePartitionPar,
                param,
            )
            .await?;
        }
        Ok(())
    }

    async fn consume_event_types_ab(
        api_client: ApiClient,
        subscription_id: SubscriptionId,
    ) -> Result<(), Error> {
        let sp1 = StreamParameters::default()
            .batch_limit(100)
            .stream_limit(15_000)
            .batch_flush_timeout_secs(1)
            .max_uncommitted_events(10_000);

        let sp2 = StreamParameters::default()
            .batch_limit(100)
            .batch_flush_timeout_secs(1)
            .max_uncommitted_events(10_000);
        let params = vec![sp1, sp2];

        for param in params {
            println!("Consume event types {} & {}", EVENT_TYPE_A, EVENT_TYPE_B);
            consume_subscription(
                api_client.clone(),
                subscription_id,
                HandlerABFactory::new(),
                2 * N_EVENTS,
                DispatchMode::EventTypePar,
                param.clone(),
            )
            .await?;
            consume_subscription(
                api_client.clone(),
                subscription_id,
                HandlerABFactory::new(),
                2 * N_EVENTS,
                DispatchMode::EventTypePartitionPar,
                param,
            )
            .await?;
        }
        Ok(())
    }

    async fn consume_subscription<F: BatchHandlerFactory + GetSum>(
        api_client: ApiClient,
        subscription_id: SubscriptionId,
        factory: F,
        target_value: usize,
        dispatch_mode: DispatchMode,
        params: StreamParameters,
    ) -> Result<(), Error> {
        println!("Consume with dispatch mode {}", dispatch_mode);

        println!("Reset cursors");
        api_client
            .reset_cursors_to_begin(subscription_id, ())
            .await?;

        let check_value = factory.get();

        let logging_adapter = logger();

        let consumer = Consumer::builder_from_env()?
            .subscription_id(subscription_id)
            .dispatch_mode(dispatch_mode)
            .stream_parameters(params)
            .configure_committer(|cfg| cfg.commit_strategy(CommitStrategy::after_seconds(1)))
            .build_with_tracker(api_client.clone(), factory, logging_adapter)?;

        println!("Consume");
        let (consumer_handle, consuming) = consumer.start();

        spawn(wait_for_all_consumed(
            subscription_id,
            consumer_handle,
            api_client,
        ))
        .await
        .map_err(Error::from_error)??;

        let _ = consuming.await.into_result()?;

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

            println!("{:?} unconsumed events", stats.unconsumed_events());
            if stats.all_events_consumed() {
                println!("ALL EVENTS CONSUMED");
                handle.stop();
                break;
            }

            sleep(Duration::from_secs(5)).await;
        }
        Ok(())
    }

    struct HandlerA {
        sum: Arc<AtomicUsize>,
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
                    let _ = event.data.count;
                    self.sum.fetch_add(1, Ordering::SeqCst);
                }

                EventsPostAction::Commit
            }
            .boxed()
        }

        fn deserialize_on(&mut self, _n_bytes: usize) -> SpawnTarget {
            SpawnTarget::Dedicated
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
                    let _ = event.data.count;
                    self.sum.fetch_add(1, Ordering::SeqCst);
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
                        }) as Box<_>)
                    } else if *event_type == self.event_type_b {
                        Ok(Box::new(HandlerB {
                            sum: Arc::clone(&self.sum),
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

    async fn remove_subscriptions(api_client: ApiClient) -> Result<(), Error> {
        for event_type in [EVENT_TYPE_A, EVENT_TYPE_B].iter() {
            let event_type = EventTypeName::new(*event_type);
            let mut stream = api_client.list_subscriptions(
                Some(&event_type),
                None,
                None,
                None,
                false,
                RandomFlowId,
            );

            while let Some(r) = stream.next().await {
                match r {
                    Err(err) => return Err(err.into()),
                    Ok(subscription) => {
                        println!("Delete subscription:\n{:#?}", subscription);
                        api_client
                            .delete_subscription(subscription.id, RandomFlowId)
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn api_check(api_client: &ApiClient) -> Result<(), Error> {
        println!("======= API CHECK START ======");
        let event_type_name = EventTypeName::new("api_test_event");

        if let Err(err) = api_client.delete_event_type(&event_type_name, ()).await {
            println!("Event not {} not deleted: {}", event_type_name, err);
        } else {
            println!("Event {} deleted", event_type_name);
        }

        println!("Create event type {}", event_type_name);
        let event_type = EventTypeInput::builder()
        .name(event_type_name.clone())
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
        .default_statistic(EventTypeStatistics::new(100, 1_000, 5, 5))
        .options(EventTypeOptions::default())
        .audience(EventTypeAudience::CompanyInternal)
        .build()?;

        api_client
            .create_event_type(&event_type, RandomFlowId)
            .await?;

        println!("Publish api check events");
        let events: Vec<_> = (0..1_000)
            .map(|n| Payload { count: n + 1 })
            .map(|data| BusinessEventPub {
                data,
                metadata: EventMetaDataPub::new(Eid),
            })
            .collect();

        let publisher = Publisher::new(api_client.clone());
        for batch in events.chunks(100) {
            if let Err(err) = publisher.publish_events(&event_type_name, batch, ()).await {
                println!("{:#?}", err);
                return Err(Error::from_error(err));
            }
        }

        check_monitoring_api(api_client, &event_type_name).await?;
        check_schema_registry_api(api_client, &event_type_name).await?;
        check_subscription_api(api_client, &event_type_name).await?;

        api_client.delete_event_type(&event_type_name, ()).await?;

        println!("======= API CHECK END ======");

        Ok(())
    }

    async fn check_monitoring_api(
        api_client: &ApiClient,
        event_type_name: &EventTypeName,
    ) -> Result<(), Error> {
        println!("Monitoring: get event type partitions");
        let partitions = api_client
            .get_event_type_partitions(&event_type_name, ())
            .await?;

        assert_eq!(partitions.len(), 5);
        /*
            let mut queries = Vec::new();
            for partition in partitions {
                let partition_id = partition.into_partition_id();
                let query = CursorDistanceQuery::new(
                    partition_id.clone().join_into_cursor(CursorOffset::Begin),
                    partition_id.join_into_cursor(CursorOffset::Begin),
                );
                queries.push(query);
            }
            println!("Monitoring: get cursor distances");
            let res = api_client
                .get_cursor_distances(event_type_name, &queries, ())
                .await?;
            println!("Cursor distances: {:?}", res);

            println!("Monitoring: get cursor lag");
            let cursors = queries
                .into_iter()
                .map(|q| q.initial_cursor)
                .collect::<Vec<_>>();
            let res = api_client
                .get_cursor_lag(event_type_name, &cursors, ())
                .await?;
            println!("Cursor lag: {:?}", res);
        */
        Ok(())
    }

    async fn check_schema_registry_api(
        api_client: &ApiClient,
        event_type_name: &EventTypeName,
    ) -> Result<(), Error> {
        println!("Schema registry: get event type");
        let event_type = api_client.get_event_type(&event_type_name, ()).await?;

        println!("{:#?}", event_type);

        Ok(())
    }

    async fn check_subscription_api(
        api_client: &ApiClient,
        event_type_name: &EventTypeName,
    ) -> Result<(), Error> {
        let subscription_input = SubscriptionInput::builder()
            .owning_application("nakadi_test")
            .consumer_group("test_app_2")
            .event_types(EventTypeNames::default().event_type_name(event_type_name.clone()))
            .read_from(ReadFrom::Begin)
            .authorization(
                SubscriptionAuthorization::default()
                    .admin(("user", get_nakadi_user()))
                    .reader(("user", get_nakadi_user())),
            )
            .finish_for_create()?;

        println!("Create subscription");
        let subscription = api_client
            .create_subscription(&subscription_input, ())
            .await?;

        let subscription_id = subscription.id;

        println!("Get subscription");
        let subscription = api_client
            .get_subscription(subscription_id, RandomFlowId)
            .await?;

        println!("Subscription:\n {:#?}\n", subscription);

        println!("Get subscription cursors");
        let subscription = api_client
            .get_subscription_cursors(subscription_id, RandomFlowId)
            .await?;

        println!("Committed offsets:\n {:#?}\n", subscription);

        println!("Get subscription stats");
        let stats = api_client
            .get_subscription_stats(subscription_id, false, RandomFlowId)
            .await?;

        println!("Stats:\n {:#?}\n", stats);

        println!("Delete subscription");
        api_client
            .delete_subscription(subscription_id, RandomFlowId)
            .await?;

        Ok(())
    }

    fn get_nakadi_user() -> String {
        std::env::var("NAKADI_USER")
            .ok()
            .unwrap_or_else(|| "adminClientId".to_owned())
    }
}
