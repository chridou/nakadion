//! Handler for handling events and implementing event processing logic
use serde::de::DeserializeOwned;
use serde_json;

use nakadi::model::{EventType, PartitionId};

/// This struct must be returned after processing a batch
/// to tell nakadion how to continue.
#[derive(Debug)]
pub enum ProcessingStatus {
    /// The cursor of the just processed batch
    /// can be committed to make progrss on the stream.
    ///
    /// Optionally the number of processed events can be provided
    /// to help with deciding on when to commit the cursor.
    ///
    /// The number of events should be the number of events that were in the batch.
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
pub trait BatchHandler {
    /// Handle the events.
    ///
    /// Calling this method may never panic!
    fn handle(&mut self, event_type: EventType, events: &[u8]) -> ProcessingStatus;
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
/// This is useful when incoming data is partitioned in a way that all `BatchHandler`s
/// act only on data that never appears on another partition.
///
/// * It contains state that is shared with the `BatchHandler`s. E.g. a cache that
/// conatins data that can appear on other partitions.
pub trait HandlerFactory {
    type Handler: BatchHandler + Send + 'static;
    fn create_handler(&self, partition: &PartitionId) -> Result<Self::Handler, CreateHandlerError>;
}

/// This is basically the same as a `ProcessingStatus` but returned
/// from a `TypedBatchHandler`.
///
/// It is not necessary to report the number of processed events since
/// the `TypedBatchHandler` itself keeps track of them.
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
pub trait TypedBatchHandler {
    type Event: DeserializeOwned;
    /// Execute the processing logic with a deserialized batch of events.
    fn handle(&mut self, events: Vec<Self::Event>) -> TypedProcessingStatus;
}

impl<T, E> BatchHandler for T
where
    T: TypedBatchHandler<Event = E>,
    E: DeserializeOwned,
{
    fn handle(&mut self, event_type: EventType, events: &[u8]) -> ProcessingStatus {
        let events: Vec<E> = match serde_json::from_slice(events) {
            Ok(events) => events,
            Err(err) => {
                return ProcessingStatus::Failed {
                    reason: format!(
                        "Could not deserialize events(event type: {}): {}",
                        event_type.0, err
                    ),
                }
            }
        };

        let n = events.len();

        match TypedBatchHandler::handle(self, events) {
            TypedProcessingStatus::Processed => ProcessingStatus::processed(n),
            TypedProcessingStatus::Failed { reason } => ProcessingStatus::Failed { reason },
        }
    }
}
