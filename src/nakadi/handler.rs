use serde::de::DeserializeOwned;
use serde_json;

use nakadi::model::{EventType, PartitionId};

#[derive(Debug)]
pub enum ProcessingStatus {
    Processed(Option<usize>),
    Failed { reason: String },
}

impl ProcessingStatus {
    pub fn processed_no_hint() -> ProcessingStatus {
        ProcessingStatus::Processed(None)
    }

    pub fn processed(num_events_hint: usize) -> ProcessingStatus {
        ProcessingStatus::Processed(Some(num_events_hint))
    }

    pub fn failed<T: Into<String>>(reason: T) -> ProcessingStatus {
        ProcessingStatus::Failed {
            reason: reason.into(),
        }
    }
}

pub trait BatchHandler {
    /// Handle the events.
    ///
    /// Calling this method may never panic!
    fn handle(&self, event_type: EventType, events: &[u8]) -> ProcessingStatus;
}

pub trait HandlerFactory {
    type Handler: BatchHandler + Send + 'static;
    fn create_handler(&self, partition: &PartitionId) -> Self::Handler;
}

pub enum TypedProcessingStatus {
    Processed,
    Failed { reason: String },
}

pub trait TypedBatchHandler {
    type Event: DeserializeOwned;
    fn handle(&self, events: Vec<Self::Event>) -> TypedProcessingStatus;
}

impl<T, E> BatchHandler for T
where
    T: TypedBatchHandler<Event = E>,
    E: DeserializeOwned,
{
    fn handle(&self, event_type: EventType, events: &[u8]) -> ProcessingStatus {
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
