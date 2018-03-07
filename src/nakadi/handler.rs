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
