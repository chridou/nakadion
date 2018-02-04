use nakadi::model::EventType;

pub enum AfterBatchAction {
    Continue,
    Abort { reason: String },
}

pub trait BatchHandler {
    /// Handle the events.
    ///
    /// Calling this method may never panic!
    fn handle(&self, event_type: EventType, events: &[u8]) -> AfterBatchAction;
}

pub trait HandlerFactory {
    type Handler: BatchHandler + Send + 'static;
    fn create_handler(&self) -> Self::Handler;
}
