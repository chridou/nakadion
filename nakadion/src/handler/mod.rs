//! Kit for creating a a handler for batches of events
//!
//! Start here if you want to implement a handler for processing of events
use std::fmt;
use std::time::{Duration, Instant};

pub use bytes::Bytes;
use futures::future::BoxFuture;

pub type BatchHandlerFuture<'a> = BoxFuture<'a, BatchPostAction>;

use crate::nakadi_types::model::{
    event_type::EventTypeName,
    partition::PartitionId,
    subscription::{EventTypePartition, EventTypePartitionLike as _, SubscriptionCursor},
};

pub use crate::nakadi_types::Error;

mod typed;
pub use typed::*;

pub struct BatchMeta<'a> {
    pub cursor: &'a SubscriptionCursor,
    pub received_at: Instant,
    pub batch_id: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchPostAction {
    /// Commit the batch
    Commit { n_events: Option<usize> },
    /// Do not commit the batch and continue
    ///
    /// Use if committed "manually" within the handler
    DoNotCommit { n_events: Option<usize> },
    /// Abort the current stream and reconnect
    AbortStream(String),
    /// Abort the consumption and shut down
    ShutDown(String),
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

    pub fn do_not_commit_no_hint() -> Self {
        BatchPostAction::DoNotCommit { n_events: None }
    }

    pub fn do_not_commit(n_events: usize) -> Self {
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

/// A handler that implemets batch processing logic.
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
/// use futures::FutureExt;
///
/// use nakadion::handler::{BatchHandler, BatchPostAction, BatchMeta, Bytes, BatchHandlerFuture};
/// use nakadion::nakadi_types::model::subscription::EventTypeName;
///
/// // Use a struct to maintain state
/// struct MyHandler {
///     pub count: i32,
/// }
///
/// // Implement the processing logic by implementing `BatchHandler`
/// impl BatchHandler for MyHandler {
///     fn handle(&mut self, _events: Bytes, _meta: BatchMeta) -> BatchHandlerFuture {
///         async move {
///             self.count += 1;
///             BatchPostAction::commit_no_hint()
///         }.boxed()
///     }
/// }
/// ```
pub trait BatchHandler: Send + 'static {
    fn handle<'a>(&'a mut self, events: Bytes, meta: BatchMeta<'a>) -> BatchHandlerFuture<'a>;
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
    EventTypePartition(EventTypePartition),
}

impl fmt::Display for HandlerAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandlerAssignment::Unspecified => write!(f, "[unspecified]")?,
            HandlerAssignment::EventType(ref event_type) => {
                write!(f, "[event_type={}]", event_type)?
            }
            HandlerAssignment::EventTypePartition(ref event_type_partition) => write!(
                f,
                "[event_type={}, partition={}]",
                event_type_partition.event_type(),
                event_type_partition.partition()
            )?,
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
            HandlerAssignment::EventTypePartition(ref etp) => {
                (Some(etp.event_type()), Some(etp.partition()))
            }
        }
    }
}

/// A factory that creates `BatchHandler`s.
///
/// # Usage
///
/// A `BatchHandlerFactory` can be used in two ways:
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
/// use futures::{FutureExt, future::BoxFuture};
///
/// use nakadion::handler::*;
///
/// // Use a struct to maintain state
/// struct MyHandler(Arc<Mutex<i32>>);
///
/// // Implement the processing logic by implementing `BatchHandler`
/// impl BatchHandler for MyHandler {
///     fn handle(&mut self, _events: Bytes, _meta: BatchMeta) -> BatchHandlerFuture {
///         async move {
///             *self.0.lock().unwrap() += 1;
///             BatchPostAction::commit_no_hint()
///         }.boxed()
///     }
/// }
///
/// // We keep shared state for all handlers in the `BatchHandlerFactory`
/// struct MyBatchHandlerFactory(Arc<Mutex<i32>>);
///
/// // Now we implement the trait `BatchHandlerFactory` to control how
/// // our `BatchHandler`s are created
/// impl BatchHandlerFactory for MyBatchHandlerFactory {
///     type Handler = MyHandler;
///     fn handler(
///         &self,
///         _assignment: &HandlerAssignment,
///     ) ->  BoxFuture<Result<Self::Handler, Error>> {
///         async move {
///             Ok(MyHandler(self.0.clone()))
///         }.boxed()
///     }
/// }
///
/// let count = Arc::new(Mutex::new(0));
///
/// let factory = MyBatchHandlerFactory(count.clone());
/// ```
pub trait BatchHandlerFactory: Send + Sync + 'static {
    type Handler: BatchHandler;

    fn handler(&self, assignment: &HandlerAssignment) -> BoxFuture<Result<Self::Handler, Error>>;
}
