use nakadi_types::model::subscription::*;

use nakadion::api::ApiClient;
use nakadion::consumer::*;

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ApiClient::builder().finish_from_env()?;

    let subscription_id = SubscriptionId::from_env()?;

    let consumer = Consumer::builder_from_env()?
        .subscription_id(subscription_id)
        .inactivity_timeout_secs(10)
        .connect_stream_timeout_secs(5)
        .finish_with(client, handler::MyHandlerFactory, StdLogger::new())?;

    let (handle, task) = consumer.start();

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

    use nakadion::event_handler::*;

    pub struct MyHandler {
        count: usize,
    }

    impl BatchHandler for MyHandler {
        fn handle<'a>(
            &'a mut self,
            _events: Bytes,
            meta: BatchMeta<'a>,
        ) -> BoxFuture<'a, BatchPostAction> {
            self.count += 1;

            async move {
                let batch_id = meta.batch_id;
                println!("{}: {}", self.count, batch_id);
                if batch_id > 100 {
                    BatchPostAction::AbortStream
                } else {
                    BatchPostAction::commit_no_hint()
                }
            }
            .boxed()
        }
    }

    pub struct MyHandlerFactory;

    impl BatchHandlerFactory for MyHandlerFactory {
        type Handler = MyHandler;

        fn handler(
            &self,
            assignment: &HandlerAssignment,
        ) -> BoxFuture<Result<Self::Handler, Error>> {
            async { Ok(MyHandler { count: 0 }) }.boxed()
        }
    }
}
