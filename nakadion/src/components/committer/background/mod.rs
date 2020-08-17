use std::time::{Duration, Instant};

use futures::future::{BoxFuture, FutureExt};
use tokio::{
    spawn,
    sync::mpsc::{error::TryRecvError, unbounded_channel, UnboundedReceiver},
    time::delay_for,
};

use crate::api::SubscriptionCommitApi;
use crate::nakadi_types::{Error, FlowId};

use super::*;

use pending_cursors::PendingCursors;

mod pending_cursors;

pub fn start<C>(committer: Committer<C>) -> (CommitHandle, BoxFuture<'static, Result<(), Error>>)
where
    C: SubscriptionCommitApi + Send + Sync + 'static,
{
    let (tx, to_commit) = unbounded_channel();

    let join_handle = spawn(run_committer(to_commit, committer));

    let f = async move { join_handle.await.map_err(Error::new)? }.boxed();

    (CommitHandle { sender: tx }, f)
}

async fn run_committer<C>(
    mut cursors_to_commit: UnboundedReceiver<CommitData>,
    mut committer: Committer<C>,
) -> Result<(), Error>
where
    C: SubscriptionCommitApi + Send + Sync + 'static,
{
    committer.logger.debug(format_args!("Committer starting"));

    let config = committer.config.clone();
    let instrumentation = committer.instrumentation.clone();

    let mut pending = PendingCursors::new(
        config.commit_strategy.unwrap_or_default(),
        config
            .clone()
            .stream_commit_timeout_secs
            .unwrap_or_default(),
    );
    let delay_on_no_cursor = Duration::from_millis(50);

    let mut next_commit_earliest_at = Instant::now();

    loop {
        let now = Instant::now();
        let cursor_received = match cursors_to_commit.try_recv() {
            Ok(next) => {
                instrumentation
                    .cursor_to_commit_received(next.frame_started_at, next.frame_completed_at);
                pending.add(next, now);
                true
            }
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Closed) => {
                committer.logger.debug(format_args!(
                    "Channel closed. Last handle gone. Exiting committer."
                ));

                break;
            }
        };

        if next_commit_earliest_at > now {
            continue;
        }

        let trigger = match pending.commit_required(now) {
            Some(trigger) => trigger,
            None => {
                if !cursor_received {
                    // Wait a bit because the channel was empty
                    delay_for(delay_on_no_cursor).await;
                }
                continue;
            }
        };

        let cursors: Vec<_> = pending.cursors().collect();
        committer.set_flow_id(FlowId::random());
        committer.instrumentation.cursors_commit_triggered(trigger);
        match committer.commit(&cursors).await {
            Ok(_) => {
                pending.reset();
            }
            Err(err) => {
                if err.is_recoverable() {
                    committer.logger.warn(format_args!(
                        "Failed to commit cursors (recoverable): {}",
                        err
                    ));
                    next_commit_earliest_at = Instant::now() + Duration::from_millis(500)
                } else {
                    committer.logger.error(format_args!(
                        "Failed to commit cursors (unrecoverable): {}",
                        err
                    ));
                    return Err(Error::from_error(err));
                }
            }
        };
    }

    drop(cursors_to_commit);

    committer
        .logger
        .debug(format_args!("Committer loop exited"));

    if !pending.is_empty() {
        // try to commit the rest
        let cursors: Vec<_> = pending.into_cursors().collect();
        let n_to_commit = cursors.len();

        committer.set_flow_id(FlowId::random());
        match committer.commit(&cursors).await {
            Ok(_) => {
                committer
                    .logger
                    .debug(format_args!("Committed {} final cursors.", n_to_commit));
            }
            Err(err) => {
                committer.logger.warn(format_args!(
                    "Failed to commit {} final cursors: {}",
                    n_to_commit, err
                ));
            }
        };
    }

    committer.logger.debug(format_args!("Committer stopped"));

    Ok(())
}
