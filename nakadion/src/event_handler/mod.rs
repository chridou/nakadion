use std::time::Instant;

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};

use crate::nakadi_types::{
    model::{event_type::EventTypeName, partition::PartitionId, subscription::SubscriptionCursor},
    GenericError,
};

pub struct BatchMeta<'a> {
    pub cursor: &'a SubscriptionCursor,
    pub received_at: Instant,
    pub batch_id: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchPostAction {
    /// Commit the batch
    Commit { n_events: Option<usize> },
    /// Do not commit the batch and continue
    ///
    /// Use if committed "manually" within the handler
    DoNotCommit { n_events: Option<usize> },
    /// Abort the current stream and reconnect
    AbortStream,
    /// Abort the consumption and shut down
    ShutDown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InactivityAnswer {
    KeepMeAlive,
    KillMe,
}

pub trait BatchHandler: Send + Sync + 'static {
    fn handle(&mut self, events: Bytes, meta: BatchMeta) -> BoxFuture<BatchPostAction>;
    fn on_inactivity_detected(&self, _inactive_since: Instant) -> InactivityAnswer {
        InactivityAnswer::KillMe
    }
}

pub struct HandlerAssignment<'a> {
    pub event_type: Option<&'a EventTypeName>,
    pub partition: Option<&'a PartitionId>,
}

pub trait BatchHandlerFactory: Send + Sync {
    type Handler: BatchHandler;

    fn handler(&self, req: HandlerAssignment) -> BoxFuture<Result<Self::Handler, GenericError>>;
}
