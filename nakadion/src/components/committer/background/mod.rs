use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use futures::future::{BoxFuture, FutureExt};
use tokio::{
    spawn,
    sync::mpsc::{error::TryRecvError, unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::delay_for,
};

use crate::api::SubscriptionCommitApi;
use crate::nakadi_types::{Error, FlowId};

use super::*;

use pending_cursors::PendingCursors;

mod pending_cursors;

const DELAY_NO_CURSOR: Duration = Duration::from_millis(50);

pub fn start<C>(committer: Committer<C>) -> (CommitHandle, BoxFuture<'static, Result<(), Error>>)
where
    C: SubscriptionCommitApi + Send + Sync + 'static,
{
    let (tx, to_commit) = unbounded_channel();

    let (io_sender, io_receiver) = unbounded_channel();

    let logger = Arc::clone(&committer.logger);
    let instrumentation = committer.instrumentation();
    let config = committer.config().clone();

    let join_handle_dispatch_cursors = spawn(run_dispatch_cursors(
        to_commit,
        io_sender,
        logger,
        instrumentation,
        config,
    ));

    let join_handle_io_loop = spawn(commit_io_loop_task(committer, io_receiver));

    let f = async move {
        join_handle_dispatch_cursors.await.map_err(Error::new)?;
        join_handle_io_loop.await.map_err(Error::new)??;
        Ok(())
    }
    .boxed();

    (CommitHandle { sender: tx }, f)
}

async fn run_dispatch_cursors(
    mut cursors_to_commit: UnboundedReceiver<CommitItem>,
    io_sender: UnboundedSender<Vec<(EventTypePartition, CommitItem)>>,
    logger: Arc<dyn Logger>,
    instrumentation: Instrumentation,
    config: CommitConfig,
) {
    logger.debug(format_args!("Committer starting"));

    let mut pending = PendingCursors::new(
        config.commit_strategy.unwrap_or_default(),
        config
            .clone()
            .stream_commit_timeout_secs
            .unwrap_or_default(),
    );

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
                logger.debug(format_args!(
                    "Channel closed. Last handle gone. Exiting committer."
                ));

                break;
            }
        };

        let trigger = match pending.commit_required(now) {
            Some(trigger) => trigger,
            None => {
                if !cursor_received {
                    // Wait a bit because the channel was empty
                    delay_for(DELAY_NO_CURSOR).await;
                }
                continue;
            }
        };

        instrumentation.cursors_commit_triggered(trigger);

        let items = pending.drain_reset();

        if io_sender.send(items).is_err() {
            logger.error(format_args!(
                "Failed to send cursors to commmit to io task because \
                    the channel is closed. Exiting."
            ));
            break;
        }
    }

    drop(cursors_to_commit);
    drop(io_sender);

    logger.debug(format_args!("Committer stopped"));
}

async fn commit_io_loop_task<C>(
    mut committer: Committer<C>,
    mut io_receiver: UnboundedReceiver<Vec<(EventTypePartition, CommitItem)>>,
) -> Result<(), Error>
where
    C: SubscriptionCommitApi + Send + Sync + 'static,
{
    let mut collected_items_to_commit = HashMap::<EventTypePartition, CommitItem>::default();
    let mut cursors_to_commit = Vec::default();

    loop {
        match io_receiver.try_recv() {
            Ok(next_cursors) => {
                for (etp, commit_data) in next_cursors {
                    collected_items_to_commit.insert(etp, commit_data);
                }
            }
            Err(TryRecvError::Closed) => break,
            Err(TryRecvError::Empty) => {
                if collected_items_to_commit.is_empty() {
                    delay_for(DELAY_NO_CURSOR).await;
                    continue;
                }
            }
        }

        cursors_to_commit.clear();
        for cursor in collected_items_to_commit
            .values()
            .map(|item| item.cursor.clone())
        {
            cursors_to_commit.push(cursor);
        }

        committer.set_flow_id(FlowId::random());
        match committer.commit(&cursors_to_commit).await {
            Ok(_) => {
                collected_items_to_commit.clear();
                continue;
            }
            Err(err) => {
                if err.is_recoverable() {
                    committer.logger.warn(format_args!(
                        "Failed to commit cursors (recoverable): {}",
                        err
                    ));
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

    drop(io_receiver);

    committer
        .logger
        .debug(format_args!("Committer io loop loop exited"));

    if !collected_items_to_commit.is_empty() {
        // try to commit the rest
        let cursors: Vec<_> = collected_items_to_commit
            .into_iter()
            .map(|(_, item)| item.cursor)
            .collect();
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

    Ok(())
}
