use nakadi_types::subscription::*;

use nakadion::api::ApiClient;
use nakadion::consumer::*;

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ApiClient::builder().finish_from_env()?;

    let subscription_id = SubscriptionId::from_env()?;

    let mut builder = Consumer::builder_from_env()?
        .subscription_id(subscription_id)
        .handler_inactivity_timeout_secs(65)
        .configure_committer(|cfg| {
            cfg.attempt_timeout_millis(1500)
                .commit_strategy(CommitStrategy::after_seconds(1))
        })
        .configure_connector(|cfg| {
            cfg.attempt_timeout_secs(5)
                .configure_stream_parameters(|p| p.batch_limit(100).max_uncommitted_events(5000))
        })
        .warn_no_frames_secs(10);

    // This is not necessary and just used
    // to print the configured values
    builder.apply_defaults();
    println!("{:#?}", builder);

    let consumer = builder.build_with(
        client,
        handler::MyHandlerFactory,
        StdOutLoggingAdapter::default(),
    )?;

    let (_handle, consuming) = consumer.start();

    let outcome = consuming.await;

    println!("{}", outcome.as_reason());

    Ok(())
}

#[cfg(not(feature = "reqwest"))]
fn main() {
    println!("Please enable the `reqwest` feature which is a default feature");
}

mod handler {
    use std::time::SystemTime;

    use futures::future::{BoxFuture, FutureExt};

    use nakadi_types::event::DataChangeEvent;
    use nakadion::handler::*;

    pub struct MyHandler {
        events_received: usize,
    }

    impl EventsHandler for MyHandler {
        type Event = DataChangeEvent<serde_json::Value>;
        fn handle<'a>(
            &'a mut self,
            events: Vec<Self::Event>,
            meta: BatchMeta<'a>,
        ) -> EventsHandlerFuture {
            async move {
                self.events_received += events.len();
                if meta.frame_id % 100 == 0 {
                    let time = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    println!(
                        "[{}] events: {} - frame id: {}",
                        time, self.events_received, meta.frame_id,
                    );
                }
                EventsPostAction::Commit
            }
            .boxed()
        }
    }

    pub struct MyHandlerFactory;

    impl BatchHandlerFactory for MyHandlerFactory {
        fn handler(
            &self,
            _assignment: &HandlerAssignment,
        ) -> BoxFuture<Result<Box<dyn BatchHandler>, Error>> {
            async { Ok(Box::new(MyHandler { events_received: 0 }) as Box<_>) }.boxed()
        }
    }
}
