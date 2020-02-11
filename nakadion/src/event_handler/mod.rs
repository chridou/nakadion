use std::fmt;
use std::time::{Duration, Instant};

pub use bytes::Bytes;
use futures::future::BoxFuture;

use crate::nakadi_types::model::{
    event_type::EventTypeName, partition::PartitionId, subscription::SubscriptionCursor,
};

pub use crate::nakadi_types::GenericError;

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

impl BatchPostAction {
    pub fn commit_no_hint() -> Self {
        BatchPostAction::Commit { n_events: None }
    }

    pub fn commit(n_events: usize) -> Self {
        BatchPostAction::DoNotCommit {
            n_events: Some(n_events),
        }
    }
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

pub trait BatchHandler: Send + 'static {
    fn handle<'a>(&'a mut self, events: Bytes, meta: BatchMeta<'a>) -> BoxFuture<BatchPostAction>;
    fn on_inactivity_detected(
        &mut self,
        _inactive_for: Duration,
        _last_activity: Instant,
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

impl fmt::Display for HandlerAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandlerAssignment::Unspecified => write!(f, "[unspecified]")?,
            HandlerAssignment::EventType(ref event_type) => {
                write!(f, "[event_type={}]", event_type)?
            }
            HandlerAssignment::EventTypePartition(ref event_type, ref partition) => {
                write!(f, "[event_type={}, partition={}]", event_type, partition)?
            }
        }

        Ok(())
    }
}

impl HandlerAssignment {
    pub fn event_type(&self) -> Option<&EventTypeName> {
        self.event_type_and_partition().0
    }

    pub fn partition(&self) -> Option<&PartitionId> {
        self.event_type_and_partition().1
    }

    pub fn event_type_and_partition(&self) -> (Option<&EventTypeName>, Option<&PartitionId>) {
        match self {
            HandlerAssignment::Unspecified => (None, None),
            HandlerAssignment::EventType(event_type) => (Some(&event_type), None),
            HandlerAssignment::EventTypePartition(ref event_type, ref partition) => {
                (Some(&event_type), Some(&partition))
            }
        }
    }
}

pub trait BatchHandlerFactory: Send + Sync + 'static {
    type Handler: BatchHandler;

    fn handler(
        &self,
        assignment: &HandlerAssignment,
    ) -> BoxFuture<Result<Self::Handler, GenericError>>;
}
