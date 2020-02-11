use nakadi_types::model::subscription::*;

use nakadion::api::ApiClient;
use nakadion::consumer::*;

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ApiClient::builder().finish_from_env()?;

    let subscription_id = SubscriptionId::from_env()?;

    let consumer = Consumer::builder()
        .subscription_id(subscription_id)
        .finish_with(client, handler::MyHandlerFactory, PrintLogger)?;

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

    pub struct MyHandler;

    impl BatchHandler for MyHandler {
        fn handle(&mut self, events: Bytes, meta: BatchMeta) -> BoxFuture<BatchPostAction> {
            println!("{}", meta.batch_id);

            let batch_id = meta.batch_id;
            async move {
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
        ) -> BoxFuture<Result<Self::Handler, GenericError>> {
            async { Ok(MyHandler) }.boxed()
        }
    }
}
