use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;

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

impl InactivityAnswer {
    pub fn should_kill(self) -> bool {
        self == InactivityAnswer::KillMe
    }

    pub fn should_stay_alive(self) -> bool {
        self == InactivityAnswer::KeepMeAlive
    }
}

pub trait BatchHandler: Send + Sync + 'static {
    fn handle(&mut self, events: Bytes, meta: BatchMeta) -> BoxFuture<BatchPostAction>;
    fn on_inactivity_detected(
        &mut self,
        inactive_for: Duration,
        last_activity: Instant,
    ) -> InactivityAnswer {
        InactivityAnswer::KeepMeAlive
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum HandlerAssignment {
    Unspecified,
    EventType(EventTypeName),
    EventTypePartition(EventTypeName, PartitionId),
}

pub trait BatchHandlerFactory: Send + Sync {
    type Handler: BatchHandler;

    fn handler(
        &self,
        assignment: &HandlerAssignment,
    ) -> BoxFuture<Result<Self::Handler, GenericError>>;
}
