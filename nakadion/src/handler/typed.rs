use serde::de::DeserializeOwned;

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};

use super::{BatchHandler, BatchMeta, BatchPostAction};

/// This is basically the same as a `ProcessingStatus` but returned
/// from a `EventsHandler`.
///
/// It is not necessary to report the number of processed events since
/// the `EventsHandler` itself keeps track of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventsPostAction {
    /// Commit the batch
    Commit,
    /// Do not commit the batch and continue
    ///
    /// Use if committed "manually" within the handler
    DoNotCommit,
    /// Abort the current stream and reconnect
    AbortStream(String),
    /// Abort the consumption and shut down
    ShutDown(String),
}

impl EventsPostAction {
    pub fn commit() -> Self {
        EventsPostAction::Commit
    }
}

impl From<tokio::task::JoinError> for BatchPostAction {
    fn from(v: tokio::task::JoinError) -> Self {
        BatchPostAction::ShutDown(v.to_string())
    }
}

pub enum SpawnTarget {
    /// Use the current executor
    Executor,
    /// Use/spawn a dedicated thread
    Dedicated,
}

/// Basically the same a `BatchHandler` with the difference that
/// deserialized events are passed to the processing logic.
///
/// This is basically a convinience handler.
///
/// The events must implement `serde`s `DeserializeOwned`.
///
/// # Hint
///
/// The `handle` method gets called on `&mut self`.
/// # Example
///
/// ```norun
/// /// use nakadion::{EventType, EventsHandler, TypedProcessingStatus};
///
/// // Use a struct to maintain state
/// struct MyHandler {
///     pub count: i32,
/// }
///
/// #[derive(Deserialize)]
/// struct MyEvent(i32);
///
/// // Implement the processing logic by implementing `BatchHandler`
/// impl EventsHandler for MyHandler {
///     type Event = MyEvent;
///
///     fn handle(&mut self, events: Vec<MyEvent>) -> TypedProcessingStatus {
///         for MyEvent(amount) in events {
///             self.count += amount;
///         }
///         TypedProcessingStatus::Processed
///     }
/// }
///
/// // Handler creation will be done by `HandlerFactory`
/// let mut handler = MyHandler { count: 0 };
///
/// // This will be done by Nakadion
/// handler.handle(vec![MyEvent(1), MyEvent(2)]);
///
/// assert_eq!(handler.count, 3);
/// ```
pub trait EventsHandler {
    type Event: DeserializeOwned + Send + 'static;
    /// Execute the processing logic with a deserialized batch of events.
    fn handle<'a>(
        &'a mut self,
        events: Vec<Self::Event>,
        meta: BatchMeta<'a>,
    ) -> BoxFuture<'a, EventsPostAction>;

    // A handler which is invoked if deserialization of the
    // whole events batch at once failed.
    fn handle_deserialization_errors<'a>(
        &'a mut self,
        results: Vec<EventDeserializationResult<Self::Event>>,
        _meta: BatchMeta<'a>,
    ) -> BoxFuture<'a, EventsPostAction> {
        let num_events = results.len();
        let num_failed = results.iter().filter(|r| r.is_err()).count();
        async move {
            EventsPostAction::ShutDown(format!(
                "{} out of {} events were not deserializable.",
                num_failed, num_events
            ))
        }
        .boxed()
    }

    fn deserialize_on(&mut self) -> SpawnTarget {
        SpawnTarget::Executor
    }
}

pub type EventDeserializationResult<T> = Result<T, (serde_json::Value, serde_json::Error)>;

impl<T> BatchHandler for T
where
    T: EventsHandler + Send + 'static,
    T::Event: DeserializeOwned + Send + 'static,
{
    fn handle<'a>(
        &'a mut self,
        events: Bytes,
        meta: BatchMeta<'a>,
    ) -> BoxFuture<'a, BatchPostAction> {
        async move {
            let de_res = match self.deserialize_on() {
                SpawnTarget::Executor => serde_json::from_slice::<Vec<T::Event>>(&events),
                SpawnTarget::Dedicated => {
                    match tokio::task::spawn_blocking({
                        let events = events.clone();
                        move || serde_json::from_slice::<Vec<T::Event>>(&events)
                    })
                    .await
                    {
                        Ok(e) => e,
                        Err(err) => return err.into(),
                    }
                }
            };
            match de_res {
                Ok(events) => {
                    let n = events.len();

                    match EventsHandler::handle(self, events, meta).await {
                        EventsPostAction::Commit => BatchPostAction::commit(n),
                        EventsPostAction::DoNotCommit => BatchPostAction::do_not_commit(n),
                        EventsPostAction::AbortStream(reason) => {
                            BatchPostAction::AbortStream(reason)
                        }
                        EventsPostAction::ShutDown(reason) => BatchPostAction::ShutDown(reason),
                    }
                }
                Err(_) => {
                    let de_res = match self.deserialize_on() {
                        SpawnTarget::Executor => try_deserialize_individually::<T::Event>(&events),
                        SpawnTarget::Dedicated => {
                            match tokio::task::spawn_blocking({
                                let events = events.clone();
                                move || try_deserialize_individually::<T::Event>(&events)
                            })
                            .await
                            {
                                Ok(e) => e,
                                Err(err) => return err.into(),
                            }
                        }
                    };
                    match de_res {
                        Ok(results) => {
                            let n = results.len();
                            match self.handle_deserialization_errors(results, meta).await {
                                EventsPostAction::Commit => BatchPostAction::commit(n),
                                EventsPostAction::DoNotCommit => BatchPostAction::do_not_commit(n),
                                EventsPostAction::AbortStream(reason) => {
                                    BatchPostAction::AbortStream(reason)
                                }
                                EventsPostAction::ShutDown(reason) => {
                                    BatchPostAction::ShutDown(reason)
                                }
                            }
                        }
                        Err(err) => {
                            let reason = format!(
                                "An error occured deserializing event for error handling: {}",
                                err
                            );
                            BatchPostAction::ShutDown(reason)
                        }
                    }
                }
            }
        }
        .boxed()
    }
}

// This function clones the ast before deserializing... but we are in an
// exceptional case anyways...
fn try_deserialize_individually<T: DeserializeOwned + Send + 'static>(
    events: &[u8],
) -> Result<Vec<EventDeserializationResult<T>>, serde_json::Error> {
    let deserialized_json_asts: Vec<serde_json::Value> = serde_json::from_slice(events)?;

    let mut results = Vec::with_capacity(deserialized_json_asts.len());

    for ast in deserialized_json_asts {
        let ast2 = ast.clone();
        match serde_json::from_value(ast) {
            Ok(event) => results.push(Ok(event)),
            Err(err) => results.push(Err((ast2, err))),
        }
    }

    Ok(results)
}
