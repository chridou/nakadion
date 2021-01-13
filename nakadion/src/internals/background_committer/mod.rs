use std::{
    collections::{hash_map::Entry, HashMap},
    time::{Duration, Instant},
};

use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};
use futures::{
    future::{BoxFuture, FutureExt},
    TryFutureExt,
};
use tokio::{spawn, time::sleep};

use crate::api::SubscriptionCommitApi;
use crate::{
    components::committer::Committer,
    instrumentation::Instruments,
    logging::Logger,
    nakadi_types::{Error, FlowId},
};

use nakadi_types::subscription::{EventTypePartition, EventTypePartitionLike, SubscriptionCursor};
use pending_cursors::PendingCursors;

use super::StreamState;

mod pending_cursors;

const DELAY_NO_CURSOR: Duration = Duration::from_millis(50);

#[derive(Debug, Clone)]
pub struct CommitItem {
    pub cursor: SubscriptionCursor,
    pub frame_started_at: Instant,
    pub frame_completed_at: Instant,
    pub frame_id: usize,
    pub n_events: usize,
}

impl CommitItem {
    fn etp(&self) -> EventTypePartition {
        EventTypePartition::new(
            self.cursor.event_type().as_ref(),
            self.cursor.partition().as_ref(),
        )
    }
}

// The reason why data was committed if working in background mode
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CommitTrigger {
    /// The deadline to commit was reached
    Deadline { n_batches: usize, n_events: usize },
    /// Enough events were received
    Events { n_batches: usize, n_events: usize },
    /// Enough cursors were received
    Batches { n_batches: usize, n_events: usize },
}

impl CommitTrigger {
    pub fn stats(&self) -> (usize, usize) {
        match *self {
            CommitTrigger::Deadline {
                n_batches,
                n_events,
            } => (n_batches, n_events),
            CommitTrigger::Events {
                n_batches,
                n_events,
            } => (n_batches, n_events),
            CommitTrigger::Batches {
                n_batches,
                n_events,
            } => (n_batches, n_events),
        }
    }

    pub fn n_batches(&self) -> usize {
        self.stats().0
    }

    pub fn n_events(&self) -> usize {
        self.stats().1
    }
}

pub(crate) fn start_committer<C>(
    client: C,
    stream_state: StreamState,
) -> (CommitHandle, BoxFuture<'static, Result<(), Error>>)
where
    C: SubscriptionCommitApi + Send + Sync + 'static,
{
    let mut committer = Committer::new(
        client,
        stream_state.subscription_id(),
        stream_state.stream_id(),
    );

    committer.set_instrumentation(stream_state.instrumentation().clone());
    committer.set_logger(stream_state.clone());
    committer.set_config(stream_state.config().commit_config.clone());

    let (tx, to_commit) = unbounded();

    let (io_sender, io_receiver) = unbounded();

    let join_handle_dispatch_cursors = spawn(run_dispatch_cursors(
        to_commit,
        io_sender,
        stream_state.clone(),
    ));

    let join_handle_io_loop = spawn(commit_io_loop_task(
        committer,
        io_receiver,
        stream_state.clone(),
    ));

    let f = async move {
        join_handle_dispatch_cursors
            .inspect_err(|err| {
                stream_state.warn(format_args!(
                    "Committer dispatch loop exited with error: {}",
                    err,
                ));
                stream_state.request_stream_cancellation();
            })
            .await
            .map_err(Error::new)?;
        join_handle_io_loop
            .inspect_err(|err| {
                stream_state.warn(format_args!("Committer io loop exited with error: {}", err,));
                stream_state.request_stream_cancellation();
            })
            .inspect_ok(|r| {
                if r.is_err() {
                    stream_state.request_stream_cancellation();
                }
            })
            .await
            .map_err(Error::new)??;
        stream_state.debug(format_args!("Committer stopped normally"));
        Ok(())
    }
    .boxed();

    (CommitHandle { sender: tx }, f)
}

async fn run_dispatch_cursors(
    cursors_to_commit: Receiver<CommitItem>,
    io_sender: Sender<Vec<(EventTypePartition, CommitEntry)>>,
    stream_state: StreamState,
) {
    stream_state.debug(format_args!("Committer starting"));

    let config = &stream_state.config().commit_config;
    let instrumentation = stream_state.instrumentation();

    let mut pending = PendingCursors::new(
        config.commit_strategy.unwrap_or_default(),
        config
            .clone()
            .stream_commit_timeout_secs
            .unwrap_or_default(),
    );

    let (mut next_commit_latest_at, commit_latest_after_interval) = if let Some(interval) = config
        .commit_strategy
        .unwrap_or_default()
        .commit_after_secs_interval()
    {
        (Some(Instant::now()), Duration::from_secs(interval.into()))
    } else {
        (None, Duration::from_secs(0))
    };

    loop {
        if stream_state.cancellation_requested() {
            stream_state.debug(format_args!(
                "[DISPATCH_CURSORS_LOOP] Cancellation requested."
            ));
            break;
        }

        let cursor_received = match cursors_to_commit.try_recv() {
            Ok(next) => {
                instrumentation
                    .cursor_to_commit_received(next.frame_started_at, next.frame_completed_at);
                pending.add(next);
                true
            }
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Disconnected) => {
                stream_state.debug(format_args!(
                    "Channel closed. Last handle gone. Exiting committer."
                ));

                break;
            }
        };

        let deadline_elapsed = if let Some(next_commit_latest_at) = next_commit_latest_at {
            next_commit_latest_at <= Instant::now()
        } else {
            false
        };

        let trigger = match (pending.commit_required(Instant::now()), deadline_elapsed) {
            (Some(trigger), _) => Some(trigger),
            (None, true) => pending.create_deadline_trigger(),
            (None, false) => None,
        };

        let trigger = if let Some(trigger) = trigger {
            if next_commit_latest_at.is_some() {
                // We are defintely going to commit. So if there is an interval, set a new deadline now.
                next_commit_latest_at = Some(Instant::now() + commit_latest_after_interval);
            }
            trigger
        } else {
            if !cursor_received {
                // Wait a bit because the channel was empty
                sleep(DELAY_NO_CURSOR).await;
            }
            continue;
        };

        stream_state.debug(format_args!("Commit triggered: {:?}", trigger));
        instrumentation.cursors_commit_triggered(trigger);

        let items = pending.drain_reset();

        if io_sender.send(items).is_err() {
            stream_state.error(format_args!(
                "Failed to send cursors to commmit to io task because \
                    the channel is closed. Exiting."
            ));
            break;
        }
    }

    let remaining_to_commit = pending.drain_reset();
    if !remaining_to_commit.is_empty() {
        stream_state.warn(format_args!(
            "There are still {} cursors to commit on shutdown.",
            remaining_to_commit.len()
        ));

        if io_sender.send(remaining_to_commit).is_err() {
            stream_state.error(format_args!(
                "Failed to send remaining cursors to the io task"
            ));
        }
    }

    drop(cursors_to_commit);
    drop(io_sender);

    stream_state.debug(format_args!("Committer dispatch loop exiting normally"));
}

async fn commit_io_loop_task<C>(
    mut committer: Committer<C>,
    io_receiver: Receiver<Vec<(EventTypePartition, CommitEntry)>>,
    stream_state: StreamState,
) -> Result<(), Error>
where
    C: SubscriptionCommitApi + Send + Sync + 'static,
{
    let mut collected_items_to_commit = HashMap::<EventTypePartition, CommitEntry>::default();
    let mut cursors_to_commit = Vec::default();

    let first_cursor_warning_threshold = pending_cursors::safe_commit_timeout(
        committer
            .config()
            .stream_commit_timeout_secs
            .unwrap_or_default()
            .into(),
    );

    loop {
        match io_receiver.try_recv() {
            Ok(next_cursors) => {
                for (etp, commit_entry) in next_cursors {
                    match collected_items_to_commit.entry(etp) {
                        Entry::Vacant(e) => {
                            e.insert(commit_entry);
                        }
                        Entry::Occupied(mut e) => {
                            e.get_mut().item_to_commit = commit_entry.item_to_commit
                        }
                    }
                }
            }
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {
                if collected_items_to_commit.is_empty() {
                    sleep(DELAY_NO_CURSOR).await;
                    continue;
                }
            }
        }

        cursors_to_commit.clear();
        let mut effective_batches_to_be_committed = 0;
        let mut effective_events_to_be_committed = 0;
        for commit_entry in collected_items_to_commit.values() {
            let commit_item = &commit_entry.item_to_commit;
            effective_batches_to_be_committed += commit_entry.n_batches;
            effective_events_to_be_committed += commit_entry.n_events;
            let cursor = commit_item.cursor.clone();
            let first_cursor_age = commit_entry.first_frame_started_at.elapsed();
            let last_cursor_age = commit_entry.item_to_commit.frame_started_at.elapsed();
            let warning = first_cursor_age >= first_cursor_warning_threshold;
            if warning {
                stream_state.warn(format_args!(
                    "About to commit a dangerously old cursor ({:?}) for {}. \
                    First frame is #{}, last frame is #{}. \
                    The threshold is {:?}",
                    first_cursor_age,
                    cursor.to_event_type_partition(),
                    commit_entry.first_frame_id,
                    commit_item.frame_id,
                    first_cursor_warning_threshold,
                ));
            }
            committer.instrumentation.cursor_ages_on_commit_attempt(
                first_cursor_age,
                last_cursor_age,
                warning,
            );
            cursors_to_commit.push(cursor);
        }

        committer.set_flow_id(FlowId::random());
        match committer.commit(&cursors_to_commit).await {
            Ok(_) => {
                stream_state.debug(format_args!(
                    "Committed {} cursors for {} batches and {} events.",
                    cursors_to_commit.len(),
                    effective_batches_to_be_committed,
                    effective_events_to_be_committed,
                ));
                stream_state.batches_committed(
                    effective_batches_to_be_committed,
                    effective_events_to_be_committed,
                );
                collected_items_to_commit.clear();
                continue;
            }
            Err(err) => {
                if err.is_recoverable() {
                    stream_state.warn(format_args!(
                        "Failed to commit {} cursors for {} batches and {} events (recoverable): {}",
                        cursors_to_commit.len(),
                        effective_batches_to_be_committed,
                        effective_events_to_be_committed,
                       err
                    ));
                    sleep(Duration::from_millis(100)).await;
                } else {
                    stream_state.error(format_args!(
                        "Failed to commit {} cursors for {} batches and {} events (unrecoverable): {}",
                        cursors_to_commit.len(),
                        effective_batches_to_be_committed,
                        effective_events_to_be_committed,
                    err
                    ));
                    stream_state.request_stream_cancellation();
                    return Err(Error::from_error(err));
                }
            }
        };
    }

    drop(io_receiver);

    if !collected_items_to_commit.is_empty() {
        // try to commit the rest
        let mut effective_batches_to_be_committed = 0;
        let mut effective_events_to_be_committed = 0;
        let cursors: Vec<_> = collected_items_to_commit
            .into_iter()
            .map(|(_, commit_entry)| {
                effective_batches_to_be_committed += commit_entry.n_batches;
                effective_events_to_be_committed += commit_entry.n_events;
                let first_cursor_age = commit_entry.first_frame_started_at.elapsed();
                let last_cursor_age = commit_entry.item_to_commit.frame_started_at.elapsed();
                let warning = first_cursor_age >= first_cursor_warning_threshold;
                if warning {
                    stream_state.warn(format_args!(
                        "About to commit a dangerously old cursor ({:?}) for {}. \
                        First frame is #{}, last frame is #{}. \
                        The threshold is {:?}",
                        first_cursor_age,
                        commit_entry.item_to_commit.cursor.to_event_type_partition(),
                        commit_entry.first_frame_id,
                        commit_entry.item_to_commit.frame_id,
                        first_cursor_warning_threshold,
                    ));
                }
                stream_state.instrumentation.cursor_ages_on_commit_attempt(
                    first_cursor_age,
                    last_cursor_age,
                    warning,
                );

                commit_entry.item_to_commit.cursor
            })
            .collect();
        let n_to_commit = cursors.len();

        committer.set_flow_id(FlowId::random());
        match committer.commit(&cursors).await {
            Ok(_) => {
                stream_state.batches_committed(
                    effective_batches_to_be_committed,
                    effective_events_to_be_committed,
                );
                stream_state.info(format_args!(
                    "Committed {} final cursors for {} batches and {} events.",
                    n_to_commit,
                    effective_batches_to_be_committed,
                    effective_events_to_be_committed
                ));
            }
            Err(err) => {
                stream_state.warn(format_args!(
                    "Failed to commit {} final cursors for {} batches and {} events: {}",
                    n_to_commit,
                    effective_batches_to_be_committed,
                    effective_events_to_be_committed,
                    err
                ));
                stream_state.request_stream_cancellation();
                return Err(Error::from_error(err));
            }
        };
    }

    committer
        .logger
        .debug(format_args!("Committer io loop loop exiting normally"));

    Ok(())
}

pub struct CommitEntry {
    pub first_frame_started_at: Instant,
    pub first_frame_id: usize,
    pub n_batches: usize,
    pub n_events: usize,
    pub item_to_commit: CommitItem,
}
/// Handle to send commit messages to the background task.
///
/// The background task will stop, once the last handle is dropped.
#[derive(Clone)]
pub struct CommitHandle {
    sender: Sender<CommitItem>,
}

impl CommitHandle {
    /// Commit the given cursor with additional information packed in the struct
    /// `CommitData`.
    ///
    /// Fails if the data could not be send which means
    /// that the backend is gone. The appropriate action is then
    /// to stop streaming.
    pub fn commit(&self, to_commit: CommitItem) -> Result<(), CommitItem> {
        if let Err(err) = self.sender.send(to_commit) {
            Err(err.0)
        } else {
            Ok(())
        }
    }
}
