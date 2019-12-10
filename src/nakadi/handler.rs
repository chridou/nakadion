//! Handler for handling events and implementing event processing logic
use serde::de::DeserializeOwned;
use serde_json;

use nakadi::model::PartitionId;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SubscriptionCursor {
    pub partition: PartitionId,
    pub offset: String,
    pub event_type: String,
}

/// This struct must be returned after processing a batch
/// to tell nakadion how to continue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessingStatus {
    /// The cursor of the just processed batch
    /// can be committed to make progrss on the stream.
    ///
    /// Optionally the number of processed events can be provided
    /// to help with deciding on when to commit the cursor.
    ///
    /// The number of events should be the number of events that were in the
    /// batch.
    Processed(Option<usize>),
    /// Processing failed. Do not commit the cursor. This
    /// always ends in the streaming being aborted for the current
    /// stream.
    ///
    /// A reason must be given which will be logged.
    Failed { reason: String },
}

impl ProcessingStatus {
    /// Cursor can be committed and no information on
    /// how many events were processed is given.
    pub fn processed_no_hint() -> ProcessingStatus {
        ProcessingStatus::Processed(None)
    }

    /// Cursor can be committed and a hint on
    /// how many events were processed is given.
    pub fn processed(num_events_hint: usize) -> ProcessingStatus {
        ProcessingStatus::Processed(Some(num_events_hint))
    }

    /// Processing events failed with the given reason.
    pub fn failed<T: Into<String>>(reason: T) -> ProcessingStatus {
        ProcessingStatus::Failed {
            reason: reason.into(),
        }
    }
}

/// A handler that contains batch processing logic.
///
/// This trait will be called by Nakadion when a batch has to
/// be processed. The `BatchHandler` only receives an `EventType`
/// and a slice of bytes that contains the batch.
///
/// The `events` slice always contains a JSON encoded array of events.
///
/// # Hint
///
/// The `handle` method gets called on `&mut self`.
///
/// # Example
///
/// ```rust
/// use nakadion::{BatchHandler, SubscriptionCursor, ProcessingStatus, PartitionId};
///
/// // Use a struct to maintain state
/// struct MyHandler {
///     pub count: i32,
/// }
///
/// // Implement the processing logic by implementing `BatchHandler`
/// impl BatchHandler for MyHandler {
///     fn handle(&mut self, _cursor: &SubscriptionCursor, _events: &[u8]) -> ProcessingStatus {
///         self.count += 1;
///         ProcessingStatus::processed_no_hint()
///     }
/// }
///
/// // Handler creation will be done by `HandlerFactory`
/// let mut handler = MyHandler { count: 0 };
///
/// // This will be done by Nakadion
/// let cursor = SubscriptionCursor {
///    partition: PartitionId::new("1"),
///    offset: "53".to_string(),
///    event_type: "test_event".to_string(),
/// };
/// let status = handler.handle(&cursor, &[]);
///
/// assert_eq!(handler.count, 1);
/// assert_eq!(status, ProcessingStatus::Processed(None));
/// ```
pub trait BatchHandler {
    /// Handle the events.
    ///
    /// Calling this method may never panic!
    fn handle(&mut self, cursor: &SubscriptionCursor, events: &[u8]) -> ProcessingStatus;
}

/// An error that can happen when the `HandlerFactory` was not able to create
/// a new handler. This will abort the consumption of the current stream.
#[derive(Debug, Fail)]
#[fail(display = "{}", message)]
pub struct CreateHandlerError {
    pub message: String,
}

/// A factory that creates `BatchHandler`s.
///
/// # Usage
///
/// A `HandlerFactory` can be used in two ways:
///
/// * It does not contain any state it shares with the created `BatchHandler`s.
/// This is useful when incoming data is partitioned in a way that all
/// `BatchHandler`s act only on data that never appears on another partition.
///
/// * It contains state that is shared with the `BatchHandler`s. E.g. a cache
/// that conatins data that can appear on other partitions.
/// # Example
///
/// ```rust
/// use std::sync::{Arc, Mutex};
///
/// use nakadion::{
///     BatchHandler, CreateHandlerError, SubscriptionCursor, HandlerFactory, PartitionId, ProcessingStatus,
/// };
///
/// // Use a struct to maintain state
/// struct MyHandler(Arc<Mutex<i32>>);
///
/// // Implement the processing logic by implementing `BatchHandler`
/// impl BatchHandler for MyHandler {
///     fn handle(&mut self, _cursor: &SubscriptionCursor, _events: &[u8]) -> ProcessingStatus {
///         *self.0.lock().unwrap() += 1;
///         ProcessingStatus::processed_no_hint()
///     }
/// }
///
/// // We keep shared state for all handlers in the `HandlerFactory`
/// struct MyHandlerFactory(Arc<Mutex<i32>>);
///
/// // Now we implement the trait `HandlerFactory` to control how
/// // our `BatchHandler`s are created
/// impl HandlerFactory for MyHandlerFactory {
///     type Handler = MyHandler;
///     fn create_handler(
///         &self,
///         _partition: &PartitionId,
///     ) -> Result<Self::Handler, CreateHandlerError> {
///         Ok(MyHandler(self.0.clone()))
///     }
/// }
///
/// let count = Arc::new(Mutex::new(0));
///
/// let factory = MyHandlerFactory(count.clone());
///
/// // Handler creation will be invoked by Nakadion
/// let mut handler1 = factory.create_handler(&PartitionId::new("1")).unwrap();
/// let mut handler2 = factory.create_handler(&PartitionId::new("2")).unwrap();
///
/// // This will be done by Nakadion
/// let cursor = SubscriptionCursor {
///    partition: PartitionId::new("1"),
///    offset: "53".to_string(),
///    event_type: "test_event".to_string(),
/// };
/// let status1 = handler1.handle(&cursor, &[]);
///
/// assert_eq!(*count.lock().unwrap(), 1);
/// assert_eq!(status1, ProcessingStatus::Processed(None));
///
/// // This will be done by Nakadion
/// let cursor = SubscriptionCursor {
///    partition: PartitionId::new("2"),
///    offset: "54".to_string(),
///    event_type: "test_event".to_string(),
/// };
/// let status2 = handler2.handle(&cursor, &[]);
///
/// assert_eq!(*count.lock().unwrap(), 2);
/// assert_eq!(status2, ProcessingStatus::Processed(None));
/// ```
pub trait HandlerFactory {
    type Handler: BatchHandler + Send + 'static;
    fn create_handler(&self, partition: &PartitionId) -> Result<Self::Handler, CreateHandlerError>;
}

/// This is basically the same as a `ProcessingStatus` but returned
/// from a `TypedBatchHandler`.
///
/// It is not necessary to report the number of processed events since
/// the `TypedBatchHandler` itself keeps track of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedProcessingStatus {
    /// All events were processed and the cursor may be committed to
    /// make progress on the stream.
    Processed,
    /// Processing events failed and the stream should be aborted.
    Failed { reason: String },
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
/// /// use nakadion::{EventType, TypedBatchHandler, TypedProcessingStatus};
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
/// impl TypedBatchHandler for MyHandler {
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
pub trait TypedBatchHandler {
    type Event: DeserializeOwned;
    /// Execute the processing logic with a deserialized batch of events.
    fn handle(
        &mut self,
        cursor: &SubscriptionCursor,
        events: Vec<Self::Event>,
    ) -> TypedProcessingStatus;

    // A handler which is invoked if deserialization of the
    // whole events batch at once failed.
    fn handle_deserialization_errors(
        &mut self,
        _cursor: &SubscriptionCursor,
        results: Vec<EventDeserializationResult<Self::Event>>,
    ) -> TypedProcessingStatus {
        let num_events = results.len();
        let num_failed = results.iter().filter(|r| r.is_err()).count();
        TypedProcessingStatus::Failed {
            reason: format!(
                "Failed to deserialize {} out of {} events",
                num_failed, num_events
            ),
        }
    }
}

pub type EventDeserializationResult<T> = Result<T, (serde_json::Value, serde_json::Error)>;

impl<T, E> BatchHandler for T
where
    T: TypedBatchHandler<Event = E>,
    E: DeserializeOwned,
{
    fn handle(&mut self, cursor: &SubscriptionCursor, events: &[u8]) -> ProcessingStatus {
        match serde_json::from_slice::<Vec<E>>(events) {
            Ok(events) => {
                let n = events.len();

                match TypedBatchHandler::handle(self, cursor, events) {
                    TypedProcessingStatus::Processed => ProcessingStatus::processed(n),
                    TypedProcessingStatus::Failed { reason } => ProcessingStatus::Failed { reason },
                }
            }
            Err(_) => match try_deserialize_individually::<E>(events) {
                Ok(results) => {
                    let n = results.len();
                    match self.handle_deserialization_errors(cursor, results) {
                        TypedProcessingStatus::Processed => ProcessingStatus::processed(n),
                        TypedProcessingStatus::Failed { reason } => {
                            ProcessingStatus::Failed { reason }
                        }
                    }
                }
                Err(err) => ProcessingStatus::Failed {
                    reason: err.to_string(),
                },
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

#[test]
fn parse_cursor() {
    use serde_json;
    let cursor_sample = r#"{"partition":"6","offset":"543","#.to_owned()
        + r#""event_type":"order.ORDER_RECEIVED","cursor_token":"#
        + r#""b75c3102-98a4-4385-a5fd-b96f1d7872f2"}"#;

    let parsed: SubscriptionCursor = serde_json::from_str(&cursor_sample).unwrap();

    let expected = SubscriptionCursor {
        partition: PartitionId::new("6"),
        offset: "543".to_string(),
        event_type: "order.ORDER_RECEIVED".to_string(),
    };

    assert_eq!(parsed, expected);
}
