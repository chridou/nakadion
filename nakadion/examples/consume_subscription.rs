use slog::Drain;
use slog::{o, Logger};
use slog_async;
use slog_term;

use nakadi_types::model::subscription::*;

use nakadion::api::ApiClient;
use nakadion::consumer::*;

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = Logger::root(drain, o!());

    let client = ApiClient::builder().finish_from_env()?;

    let subscription_id = SubscriptionId::from_env()?;

    let mut builder = Consumer::builder_from_env()?
        .subscription_id(subscription_id)
        .inactivity_timeout_secs(30)
        .commit_attempt_timeout_millis(1500)
        .connect_stream_timeout_secs(5)
        .warn_stream_stalled_secs(10)
        .configure_stream_parameters(|p| {
            p.batch_limit(10)
                .max_uncommitted_events(1000)
                .stream_timeout_secs(60)
        });

    // This is not necessary and just used
    // to print the configured values
    builder.apply_defaults();
    println!("{:#?}", builder);

    let consumer =
        builder.build_with(client, handler::MyHandlerFactory, StdOutLogger::default())?;

    let (_handle, consuming) = consumer.start();

    let outcome = consuming.await;

    if let Some(err) = outcome.error() {
        println!("{}", err);
    } else {
        println!("{}", outcome.is_aborted());
    }

    Ok(())
}

#[cfg(not(feature = "reqwest"))]
fn main() {
    println!("Please enable the `reqwest` feature which is a default feature");
}

mod handler {
    use futures::future::{BoxFuture, FutureExt};

    use nakadion::handler::*;

    pub struct MyHandler {
        events_received: usize,
    }

    impl EventsHandler for MyHandler {
        type Event = serde_json::Value;
        fn handle<'a>(
            &'a mut self,
            events: Vec<Self::Event>,
            meta: BatchMeta<'a>,
        ) -> EventsHandlerFuture {
            self.events_received += events.len();

            async move {
                if meta.frame_id % 5_000 == 0 {
                    println!(
                        "events: {} - frame id: {}",
                        self.events_received, meta.frame_id,
                    );
                }
                EventsPostAction::Commit
            }
            .boxed()
        }
    }

    pub struct MyHandlerFactory;

    impl BatchHandlerFactory for MyHandlerFactory {
        type Handler = MyHandler;

        fn handler(
            &self,
            _assignment: &HandlerAssignment,
        ) -> BoxFuture<Result<Self::Handler, Error>> {
            async { Ok(MyHandler { events_received: 0 }) }.boxed()
        }
    }
}
