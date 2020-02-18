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
    subscription::{EventTypePartition, EventTypePartitionLike as _, StreamId, SubscriptionCursor},
};

pub use crate::nakadi_types::Error;

mod typed;
pub use typed::*;

/// Information on the current batch passed to a `BatchHandler`.
///
/// The `frame_id` is monotonically increasing for each `BatchHandler`
/// within a stream(same `StreamId`)
/// as long a s a dispatch strategy which keeps the ordering of
/// events is chosen. There may be gaps between the ids.
pub struct BatchMeta<'a> {
    pub stream_id: StreamId,
    pub cursor: &'a SubscriptionCursor,
    pub received_at: Instant,
    pub frame_id: usize,
}

/// Returned by a `BatchHandler` and tell `Nakadion`
/// how to continue.
#[derive(Debug, Clone)]
pub enum BatchPostAction {
    /// Commit the batch
    Commit(BatchStats),
    /// Do not commit the batch and continue
    ///
    /// Use if committed "manually" within the handler
    DoNotCommit(BatchStats),
    /// Abort the current stream and reconnect
    AbortStream(String),
    /// Abort the consumption and shut down
    ShutDown(String),
}

impl BatchPostAction {
    pub fn commit_no_stats() -> Self {
        BatchPostAction::Commit(BatchStats::default())
    }

    pub fn commit(n_events: usize, t_deserialize: Duration) -> Self {
        BatchPostAction::Commit(BatchStats {
            n_events: Some(n_events),
            t_deserialize: Some(t_deserialize),
        })
    }

    pub fn do_not_commit_no_stats() -> Self {
        BatchPostAction::DoNotCommit(BatchStats::default())
    }

    pub fn do_not_commit(n_events: usize, t_deserialize: Duration) -> Self {
        BatchPostAction::DoNotCommit(BatchStats {
            n_events: Some(n_events),
            t_deserialize: Some(t_deserialize),
        })
    }
}

/// Statistics on the processed batch
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct BatchStats {
    /// The number of events handled
    pub n_events: Option<usize>,
    /// The time it took to deserialize the batch
    pub t_deserialize: Option<Duration>,
}

/// Returned by a `BatchHandler` when queried
/// on inactivity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InactivityAnswer {
    KeepMeAlive,
    KillMe,
}

impl InactivityAnswer {
    /// Returns `true` if the `BatchHandler` should be killed.
    pub fn should_kill(self) -> bool {
        self == InactivityAnswer::KillMe
    }

    /// Returns `true` if the `BatchHandler` should stay alive.
    pub fn should_stay_alive(self) -> bool {
        self == InactivityAnswer::KeepMeAlive
    }
}

/// A handler that implements batch processing logic.
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
///             BatchPostAction::commit_no_stats()
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

/// Defines what a `BatchHandler` will receive.
///
/// This value should the same for the whole lifetime of the
/// `BatchHandler`. "Should" because in the end it is the
/// `BatchHandlerFactory` which returns `BatchHandler`s. But it
/// is guaranteed that `Nakadion` will only pass events to a `BatchHandler`
/// as defined by the `DispatchStrategy`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum HandlerAssignment {
    /// Everything can be passed to the `BatchHandler`.
    Unspecified,
    /// The `BatchHandler` will only receive events
    /// of the given event type but from any partition.
    EventType(EventTypeName),
    /// The `BatchHandler` will only receive events
    /// of the given event type on the given partition.
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
/// that contains data that can appear on other partitions.
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
///             BatchPostAction::commit_no_stats()
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
    /// New `BatchHandler` was requested.
    ///
    /// `assignment` defines for what event types and partitions the returned
    /// `BatchHandler` will be used. `Nakadion` guarantees that this will stay true
    /// over the whole lifetime of the `BatchHandler`.
    ///
    /// Returning an `Error` aborts the `Consumer`.
    ///
    /// It is up to the `BatchHandlerFactory` on whether it respects `assignment`.
    fn handler(
        &self,
        assignment: &HandlerAssignment,
    ) -> BoxFuture<Result<Box<dyn BatchHandler>, Error>>;
}
