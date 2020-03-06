# Nakadion

![CI](https://github.com/chridou/nakadion/workflows/CI/badge.svg)

A client for the [Nakadi](http://nakadi.io) Event Broker.

## Summary

`Nakadion` is client that connects to the Nakadi Subscription API. It
does all the cursor management so that users can concentrate on
implementing their logic for processing events. The code implemented
to process events by a user does not get in touch with the internals of Nakadi.

`Nakadion` is almost completely configurable from environment variables.

Please have a look at the documentation of [Nakadi](http://nakadi.io)
first to become comfortable with the concepts of Nakadi.

Currently `Nakadion` only works with the `tokio` runtime. Further execution
environments might be added in the future.

## How to use

To run this example the following environment variables need to be set:

* `NAKADION_NAKADI_BASE_URL`
* `NAKADION_SUBSCRIPTION_ID`
* `NAKADION_ACCESS_TOKEN_FIXED` with a valid token or `NAKADION_ACCESS_TOKEN_ALLOW_NONE=true`

```rust
use nakadion::api::ApiClient;
use nakadion::consumer::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ApiClient::builder().finish_from_env()?;

    let consumer = Consumer::builder_from_env()?.build_with(
        client,
        handler::MyHandlerFactory,
        StdOutLogger::default(),
    )?;

    let (_handle, consuming) = consumer.start();

    let _ = consuming.await.into_result()?;

    Ok(())
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
            _meta: BatchMeta<'a>,
        ) -> EventsHandlerFuture {
            async move {
                self.events_received += events.len();
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
```

### Performance
Nakadion is not meant to be used in a high performance scenario. It uses asynchronous IO.
Nevertheless it is easily possible to consume tens of thousands events per second depending
on the complexity of your processing logic.


## Recent Changes

See CHANGELOG

## License

Nakadion is distributed under the terms of both the MIT license and the Apache License (Version
2.0).

See LICENSE-APACHE and LICENSE-MIT for details.

License: Apache-2.0/MIT
