/// This is basically the same as a `ProcessingStatus` but returned
/// from a `EventsHandler`.
///
/// It is not necessary to report the number of processed events since
/// the `EventsHandler` itself keeps track of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventsPostAction {
    /// Commit the batch
    Commit,
    /// Do not commit the batch and continue
    ///
    /// Use if committed "manually" within the handler
    DoNotCommit,
    /// Abort the current stream and reconnect
    AbortStream,
    /// Abort the consumption and shut down
    ShutDown,
}

impl EventsPostAction {
    pub fn commit -> Self {
        EventsPostAction::Commit
    }
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
    type Event: DeserializeOwned;
    /// Execute the processing logic with a deserialized batch of events.
    fn handle(&mut self, events: Vec<Self::Event>, meta: BatchMetadata) -> TypedProcessingStatus;

    // A handler which is invoked if deserialization of the
    // whole events batch at once failed.
    fn handle_deserialization_errors(
        &mut self,
        results: Vec<EventDeserializationResult<Self::Event>>,
        meta: BatchMetadata,
    ) -> TypedProcessingStatus {
        let num_events = results.len();
        let num_failed = results.iter().filter(|r| r.is_err()).count();
        error!(
            "Failed to deserialize {} out of {} events",
            num_failed, num_events
        );
        TypedProcessingStatus::Failed
    }
}

pub type EventDeserializationResult<T> = Result<T, (serde_json::Value, serde_json::Error)>;

impl<T, E> BatchHandler for T
where
    T: EventsHandler<Event = E>,
    E: DeserializeOwned,
{
    fn handle(&mut self, events: &[u8], meta: BatchMetadata) -> ProcessingStatus {
        match serde_json::from_slice::<Vec<E>>(events) {
            Ok(events) => {
                let n = events.len();

                match EventsHandler::handle(self, events, meta) {
                    TypedProcessingStatus::Processed => ProcessingStatus::processed(n),
                    TypedProcessingStatus::Failed => ProcessingStatus::Failed,
                }
            }
            Err(_) => match try_deserialize_individually::<E>(events) {
                Ok(results) => {
                    let n = results.len();
                    match self.handle_deserialization_errors(results, meta) {
                        TypedProcessingStatus::Processed => ProcessingStatus::processed(n),
                        TypedProcessingStatus::Failed => ProcessingStatus::Failed,
                    }
                }
                Err(err) => {
                    error!(
                        "An error occured deserializing event for error handling: {}",
                        err
                    );
                    ProcessingStatus::Failed
                }
            },
        }
    }
}

// This function clones the ast before deserializing... but we are in an
// exceptional case anyways...
fn try_deserialize_individually<T: DeserializeOwned>(
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
