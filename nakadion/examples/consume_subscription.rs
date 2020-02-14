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
        .commit_timeout_millis(1500)
        .configure_stream_parameters(|p| p.batch_limit(10).max_uncommitted_events(1000))
        .connect_stream_timeout_secs(5);

    // This is not necessary and just used
    // to print the configured values
    builder.apply_defaults();
    println!("{:#?}", builder);

    let consumer =
        builder.build_with(client, handler::MyHandlerFactory, SlogLogger::new(logger))?;

    let (_handle, task) = consumer.start();

    let outcome = task.await;

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
        count: usize,
    }

    impl BatchHandler for MyHandler {
        fn handle<'a>(
            &'a mut self,
            events: Bytes,
            meta: BatchMeta<'a>,
        ) -> BoxFuture<'a, BatchPostAction> {
            self.count += 1;

            async move {
                let batch_id = meta.batch_id;
                println!(
                    "batches: {} - batch id: {} - bytes: {}",
                    self.count,
                    batch_id,
                    events.len()
                );
                BatchPostAction::commit_no_hint()
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
            async { Ok(MyHandler { count: 0 }) }.boxed()
        }
    }
}
