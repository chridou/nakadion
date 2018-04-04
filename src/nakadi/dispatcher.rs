//! The processor orchestrates the workers

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use nakadi::Lifecycle;
use nakadi::batch::Batch;
use nakadi::committer::Committer;
use nakadi::handler::HandlerFactory;
use nakadi::metrics::MetricsCollector;
use nakadi::model::{PartitionId, StreamId};
use nakadi::worker::Worker;

/// The dispatcher takes batch lines and sends them to the workers.
///
/// It is also responsible for creating and destroying workers.
///
/// The dispatcher uses its own background thread.
pub struct Dispatcher {
    /// Send batches with this sender
    sender: mpsc::Sender<Batch>,
    lifecycle: Lifecycle,
}

impl Dispatcher {
    pub fn start<HF, M>(
        handler_factory: Arc<HF>,
        committer: Committer,
        metrics_collector: M,
        min_idle_worker_lifetime: Option<Duration>,
    ) -> Dispatcher
    where
        HF: HandlerFactory + Send + Sync + 'static,
        M: MetricsCollector + Clone + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel();

        let lifecycle = Lifecycle::default();

        let handle = Dispatcher {
            lifecycle: lifecycle.clone(),
            sender,
        };

        start_dispatcher_loop(
            receiver,
            lifecycle,
            handler_factory,
            committer,
            metrics_collector,
            min_idle_worker_lifetime,
        );

        handle
    }

    pub fn is_running(&self) -> bool {
        self.lifecycle.running()
    }

    pub fn stop(&self) {
        self.lifecycle.request_abort()
    }

    pub fn dispatch(&self, batch: Batch) -> Result<(), String> {
        if let Err(err) = self.sender.send(batch) {
            Err(format!(
                "Could not send batch on channel to worker thread: {}",
                err
            ))
        } else {
            Ok(())
        }
    }
}

fn start_dispatcher_loop<HF, M>(
    receiver: mpsc::Receiver<Batch>,
    lifecycle: Lifecycle,
    handler_factory: Arc<HF>,
    committer: Committer,
    metrics_collector: M,
    min_idle_worker_lifetime: Option<Duration>,
) where
    HF: HandlerFactory + Send + Sync + 'static,
    M: MetricsCollector + Clone + Send + 'static,
{
    let builder = thread::Builder::new().name("nakadion-dispatcher".into());
    builder
        .spawn(move || {
            dispatcher_loop(
                receiver,
                lifecycle,
                handler_factory,
                committer,
                metrics_collector,
                min_idle_worker_lifetime,
            )
        })
        .unwrap();
}

fn dispatcher_loop<HF, M>(
    receiver: mpsc::Receiver<Batch>,
    lifecycle: Lifecycle,
    handler_factory: Arc<HF>,
    committer: Committer,
    metrics_collector: M,
    min_idle_worker_lifetime: Option<Duration>,
) where
    HF: HandlerFactory,
    M: MetricsCollector + Clone + Send + 'static,
{
    metrics_collector.dispatcher_current_workers(0);

    let stream_id = committer.stream_id().clone();
    let mut workers: Vec<(Worker, Instant)> = Vec::with_capacity(32);
    let mut idle_workers_last_checked = Instant::now();

    info!("[Dispatcher, stream={}] Started.", committer.stream_id(),);
    loop {
        if lifecycle.abort_requested() {
            info!(
                "[Dispatcher, stream={}] Stop requested externally.",
                stream_id
            );

            break;
        }

        if idle_workers_last_checked.elapsed() >= Duration::from_secs(5) {
            if let Some(min_idle_worker_lifetime) = min_idle_worker_lifetime {
                workers = kill_idle_workers(
                    workers,
                    &metrics_collector,
                    min_idle_worker_lifetime,
                    &stream_id,
                );
                idle_workers_last_checked = Instant::now()
            }
        }

        let batch = match receiver.recv_timeout(Duration::from_millis(5)) {
            Ok(batch) => batch,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                info!(
                    "[Dispatcher, stream={}] Channel disconnected. Stopping.",
                    stream_id
                );

                break;
            }
        };

        if batch.batch_line.events().is_none() {
            error!(
                "[Dispatcher, stream={}] Received a keep alive batch!. Stopping.",
                stream_id
            );

            break;
        };

        let partition = match batch.batch_line.partition_str() {
            Ok(partition) => PartitionId(partition.into()),
            Err(err) => {
                error!(
                    "[Dispatcher, stream={}] Partition id not UTF-8!. Stopping. - {}",
                    stream_id, err
                );

                break;
            }
        };

        let worker_idx = workers.iter().position(|w| w.0.partition() == &partition);

        let worker = if let Some(idx) = worker_idx {
            let &mut (ref worker, ref mut last_used) = &mut workers[idx];
            *last_used = Instant::now();
            worker
        } else {
            info!(
                "[Dispatcher, stream={}] Creating new worker for partition {}",
                stream_id, partition
            );

            let handler = match handler_factory.create_handler(&partition) {
                Ok(handler) => handler,
                Err(err) => {
                    error!("Could not create handler: {}", err);
                    break;
                }
            };

            let worker = Worker::start(
                handler,
                committer.clone(),
                partition.clone(),
                metrics_collector.clone(),
            );
            workers.push((worker, Instant::now()));
            metrics_collector.dispatcher_current_workers(workers.len());
            &workers[workers.len() - 1].0
        };

        if let Err(err) = worker.process(batch) {
            error!(
                "[Dispatcher, stream={}] Worker did not accept batch. Stopping. - {}",
                stream_id, err
            );
            break;
        }
    }

    workers.iter().for_each(|w| w.0.stop());

    info!(
        "[Dispatcher, stream={}] Waiting for workers to stop",
        stream_id
    );

    while workers.iter().any(|w| w.0.running()) {
        thread::sleep(Duration::from_millis(10));
    }

    metrics_collector.dispatcher_current_workers(0);

    info!("[Dispatcher, stream={}] All wokers stopped.", stream_id);

    lifecycle.stopped();
    info!("[Dispatcher, stream={}] Stopped.", stream_id);
}

fn kill_idle_workers(
    workers: Vec<(Worker, Instant)>,
    metrics_collector: &MetricsCollector,
    min_idle_worker_lifetime: Duration,
    stream: &StreamId,
) -> Vec<(Worker, Instant)> {
    let mut survivors = Vec::new();
    let mut stopped = Vec::new();

    for (worker, last_used) in workers {
        if last_used.elapsed() >= min_idle_worker_lifetime {
            info!(
                "[Dispatcher, stream={}] Stopping idle worker for partition '{}'",
                stream,
                worker.partition()
            );
            worker.stop();
            stopped.push(worker)
        } else {
            survivors.push((worker, last_used));
        }
    }

    while stopped.iter().any(|w| w.running()) {
        thread::sleep(Duration::from_millis(5));
    }

    if stopped.len() > 0 {
        metrics_collector.dispatcher_current_workers(survivors.len());
    }

    survivors
}
