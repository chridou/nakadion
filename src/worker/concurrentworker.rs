use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender, SyncSender, Receiver};
use std::thread::{self, JoinHandle};
use std::io::{BufRead, BufReader};
use std::env;
use std::time::Duration;

use serde_json::{self, Value};

use ::*;
use super::connector::{NakadiConnector, Checkpoints};

const RETRY_MILLIS: &'static [u64] = &[10, 20, 50, 100, 200, 300, 400, 500, 1000, 2000, 5000,
                                       10000, 30000, 60000, 300000, 600000];


/// The settings for a concurrent worker.
#[derive(Builder, Debug)]
pub struct ConcurrentWorkerSettings {
    /// The maximum number of workers to create.
    ///
    /// If not set, the number of workers will be the number of partitions initially consumed.
    /// If set it must be greater than 0 and will be at max the
    /// number of partitions initially read from.
    /// The default is `None`
    #[builder(default="None")]
    max_workers: Option<usize>,
    /// The buffer size for each worker.
    ///
    /// The default is 10.
    #[builder(default="10")]
    worker_buffer_size: usize,
}

impl ConcurrentWorkerSettings {
    /// Builds the settings from environment variables.
    ///
    /// The following environment variables are available:
    ///
    /// * `NAKADION_MAX_WORKERS`
    /// * `NAKADION_WORKER_BUFFER_SIZE`
    pub fn from_env() -> Result<Self, String> {
        ConcurrentWorkerSettingsBuilder::from_env().and_then(|b| b.build())
    }
}

impl ConcurrentWorkerSettingsBuilder {
    /// Creates a `ConcurrentWorkerSettingsBuilder` with values from environment variables.
    ///
    /// The following environment variables are available:
    ///
    /// * `NAKADION_MAX_WORKERS`
    /// * `NAKADION_WORKER_BUFFER_SIZE`
    pub fn from_env() -> Result<ConcurrentWorkerSettingsBuilder, String> {
        let mut builder = ConcurrentWorkerSettingsBuilder::default();
        builder.fill_with_env()?;
        Ok(builder)
    }

    /// Fills the `ConcurrentWorkerSettingsBuilder` with values from environment variables.
    ///
    /// The following environment variables are available:
    ///
    /// * `NAKADION_MAX_WORKERS`
    /// * `NAKADION_WORKER_BUFFER_SIZE`
    pub fn fill_with_env(&mut self) -> Result<(), String> {
        if let Some(env_val) = env::var("NAKADION_MAX_WORKERS").ok() {
            let max_workers =
                env_val
                    .parse()
                    .map_err(|err| format!("Could not parse 'NAKADION_MAX_WORKERS': {}", err))?;
            self.max_workers(Some(max_workers));
        };

        if let Some(env_val) = env::var("NAKADION_WORKER_BUFFER_SIZE").ok() {
            let worker_buffer_size =
                env_val
                    .parse()
                    .map_err(|err| {
                                 format!("Could not parse 'NAKADION_WORKER_BUFFER_SIZE': {}", err)
                             })?;
            self.worker_buffer_size(worker_buffer_size);
        };

        Ok(())
    }
}

enum LeaderMessage {
    Start,
    Stop,
    Batch(StreamId, String),
}

enum PartitionWorkerMessage {
    Stopped(PartitionWorkerId),
}


/// A worker that consumes events concurrently.
///
/// ** WORK IN PROGRESS **
pub struct ConcurrentWorker {
    subscription_id: SubscriptionId,
    is_running: Arc<AtomicBool>,
}

impl ConcurrentWorker {
    pub fn new<C: NakadiConnector, H: Handler + 'static>
        (connector: Arc<C>,
         handler: H,
         subscription_id: SubscriptionId,
         settings: ConcurrentWorkerSettings)
         -> Result<(Self, JoinHandle<()>), String> {
        let num_partitions = connector
            .stream_info(&subscription_id)
            .map(|info| info.max_partitions())
            .map_err(|err| format!("Could not get stream info: {}", err))?;

        let max_workers = if let Some(max_workers) = settings.max_workers {
            max_workers
        } else {
            num_partitions
        };

        let num_workers = ::std::cmp::min(num_partitions, max_workers);
        info!("Subscription {} is has {} partitions. Max workers is {}. Will create {} workers.",
              subscription_id.0,
              num_partitions,
              max_workers,
              num_workers);

        if num_workers == 0 {
            return Err("Number of workers must not result in 0.".to_string());
        }

        let (to_leader_tx, leader_rx) = mpsc::channel::<PartitionWorkerMessage>();

        let handler = Arc::new(handler);
        let mut workers = Vec::new();
        for id in 0..num_workers {
            let to_leader_tx = to_leader_tx.clone();
            let (tx, rx) = mpsc::sync_channel::<LeaderMessage>(settings.worker_buffer_size);

            PartionWorker::start(connector.clone(),
                                 handler.clone(),
                                 rx,
                                 to_leader_tx,
                                 subscription_id.clone(),
                                 PartitionWorkerId(id));
            workers.push((PartitionWorkerId(id), tx));
        }

        let is_running = Arc::new(AtomicBool::new(true));


        let leader_subsrciption_id = subscription_id.clone();
        let leader_connector = connector.clone();
        let leader_is_running = is_running.clone();
        let handle = thread::spawn(move || {
            let workers = workers;
            let leader_subsrciption_id = leader_subsrciption_id;
            let leader_connector = leader_connector;
            let leader_rx = leader_rx;
            let leader_is_running = leader_is_running;
            let leader = LeaderInternal {
                connector: &*leader_connector,
                workers: workers.as_slice(),
                subscription_id: &leader_subsrciption_id,
                partition_reveiver: &leader_rx,
                is_running: &leader_is_running,
            };

            leader.run_a_loop();

            info!("Leader stopped.");

        });

        Ok((ConcurrentWorker {
                subscription_id: subscription_id.clone(),
                is_running: is_running,
            },
            handle))
    }
}

impl Worker for ConcurrentWorker {
    /// Returns true if the worker is still running.
    fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    /// Stops the worker.
    fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed)
    }

    /// Gets the `SubscriptionId` the worker is listening to.
    fn subscription_id(&self) -> &SubscriptionId {
        &self.subscription_id
    }
}

struct LeaderInternal<'a, C: NakadiConnector> {
    connector: &'a C,
    workers: &'a [(PartitionWorkerId, SyncSender<LeaderMessage>)],
    subscription_id: &'a SubscriptionId,
    partition_reveiver: &'a Receiver<PartitionWorkerMessage>,
    is_running: &'a AtomicBool,
}

impl<'a, C: NakadiConnector> LeaderInternal<'a, C> {
    pub fn run_a_loop(&self) {
        for &(_, ref w) in self.workers {
            w.send(LeaderMessage::Start).unwrap();
        }

        loop {
            info!("No connection to Nakadi. Requesting connection...");
            let (src, stream_id) = if let Some(r) = self.connect() {
                r
            } else {
                warn!("Connection attempt aborted. Stopping the worker.");
                break;
            };

            let buffered_reader = BufReader::new(src);

            let mut lines_read: usize = 0;
            for line in buffered_reader.lines() {
                match line {
                    Ok(line) => {
                        lines_read += 1;
                        match self.process_line(stream_id.clone(), line) {
                            Ok(()) => (),
                            Err(err) => {
                                error!("An error occured processing the batch for stream {}. \
                                        Reconnecting. Error: {}",
                                       stream_id.0,
                                       err);
                                break;
                            }
                        }
                    }
                    Err(err) => {
                        error!("Stream {} was closed unexpectedly: {}", stream_id.0, err);
                        break;
                    }
                }
            }
        }
    }


    fn connect(&self) -> Option<(C::StreamingSource, StreamId)> {
        let mut attempt = 0;
        while self.is_running.load(Ordering::Relaxed) {
            attempt += 1;
            info!("Connecting to Nakadi(attempt {}).", attempt);
            match self.connector.read(self.subscription_id) {
                Ok(r) => {
                    info!("Connected to Nakadi. Stream id is {}", (r.1).0);
                    return Some(r);
                }
                Err(ClientError(ClientErrorKind::Conflict(msg), _)) => {
                    warn!("{}. Maybe there are no shards to read from left. Waiting 10 seconds.",
                          msg);
                    let pause = Duration::from_secs(10);
                    thread::sleep(pause);
                }
                Err(err) => {
                    error!("Failed to connect to Nakadi: {}", err);
                    let pause = retry_pause(attempt - 1);
                    thread::sleep(pause);
                }
            }
        }
        None
    }

    fn process_line(&self, stream_id: StreamId, line: String) -> ClientResult<()> {
        // This is a hack. We might later want to extract the partition manually.
        match serde_json::from_str::<DeserializedCursor>(line.as_ref()) {
            Ok(DeserializedCursor { cursor }) => {
                let partition_id = cursor.partition.0;
                let idx = partition_id % self.workers.len();
                if let Err(err) = self.workers[idx]
                       .1
                       .send(LeaderMessage::Batch(stream_id, line)) {
                    bail!(ClientErrorKind::Internal(format!("Could not send batch to worker: {}",
                                                            err)));
                } else {
                    Ok(())
                }
            }
            Err(err) => {
                bail!(ClientErrorKind::UnparsableBatch(format!("Could not parse '{}': {}",
                                                               line,
                                                               err)))
            }
        }
    }
}

#[derive(Deserialize)]
struct DeserializedCursor {
    cursor: DeserializedPartitionId,
}

#[derive(Deserialize)]
struct DeserializedPartitionId {
    partition: PartitionId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionWorkerId(pub usize);

struct PartionWorker;

impl PartionWorker {
    pub fn start<C: Checkpoints + Send + Sync + 'static>(
        checkpointer: Arc<C>,
        handler: Arc<Handler + Sync>,
        batch_receiver: Receiver<LeaderMessage>,
        send_to_leader: Sender<PartitionWorkerMessage>,
        subscription_id: SubscriptionId,
worker_id: PartitionWorkerId){

        let handler = handler.clone();
        let checkpointer = checkpointer.clone();
        let worker_id_2 = worker_id.clone();

        thread::spawn(move || {
            let worker = PartitionWorkerInternal {
                handler: &*handler,
                checkpointer: &*checkpointer,
                batch_receiver: batch_receiver,
                send_to_leader: send_to_leader,
                subscription_id: subscription_id,
                worker_id: worker_id_2,
            };

            worker.run_a_loop();

            info!("PartitionWorker {}: Worker stopped.", worker_id.0);
        });
    }
}

struct PartitionWorkerInternal<'a> {
    handler: &'a Handler,
    checkpointer: &'a Checkpoints,
    batch_receiver: Receiver<LeaderMessage>,
    send_to_leader: Sender<PartitionWorkerMessage>,
    subscription_id: SubscriptionId,
    worker_id: PartitionWorkerId,
}

impl<'a> PartitionWorkerInternal<'a> {
    fn run_a_loop(&self) {
        info!("PartitionWorker {}: Worker started", self.worker_id.0);
        let mut current_stream_id: Option<StreamId> = None;
        let mut discard_until_stop = false;
        let mut num_disarded = 0;
        loop {
            let msg = match self.batch_receiver.recv() {
                Ok(msg) => msg,
                Err(err) => {
                    error!("PartitionWorker {}: Worker disconnected. \
                            Sending stop to worker. Error: {}",
                           self.worker_id.0,
                           err);
                    if let Err(err) =
                        self.send_to_leader
                            .send(PartitionWorkerMessage::Stopped(self.worker_id.clone())) {
                        error!("PartitionWorker {}: Failed to send 'Stopped' message to worker. \
                                Stopping anyways.",
                               self.worker_id.0);
                    }
                    return;
                }
            };

            match msg {
                LeaderMessage::Start => {
                    info!("PartitionWorker {}: Received start signal.",
                          self.worker_id.0);
                }
                LeaderMessage::Batch(stream_id, batch) => {
                    if discard_until_stop {
                        num_disarded += 1;
                        continue;
                    }
                    let current_taken = current_stream_id.take();
                    match current_taken {
                        None => {
                            info!("PartitionWorker {}: First stream id {} received.",
                                  self.worker_id.0,
                                  stream_id.0);
                            current_stream_id = Some(stream_id.clone());
                        }
                        Some(current) => {
                            if current.0 != stream_id.0 {
                                info!("PartitionWorker {}: Stream id changed from {} to {}.",
                                      self.worker_id.0,
                                      current.0,
                                      stream_id.0);
                                current_stream_id = Some(stream_id.clone());
                            } else {
                                current_stream_id = Some(current);
                            }
                        }
                    }
                    match self.process_line(&stream_id, batch) {
                        Ok(action) => {
                            match action {
                                AfterBatchAction::Stop | AfterBatchAction::Abort => {
                                    info!("PartitionWorker {}: Request to stop by handler. \
                                           Forwarding to worker.",
                                          self.worker_id.0);
                                    discard_until_stop = true;
                                    if let Err(err) =
                                        self.send_to_leader
                                            .send(PartitionWorkerMessage::Stopped(self.worker_id
                                                                                      .clone())) {
                                        error!("PartitionWorker {}: Failed to send \
                                                'Stopped' message to worker. \
                                                Stopping anyways.",
                                               self.worker_id.0);
                                        return;
                                    }
                                }
                                _ => (),
                            }
                        }
                        Err(err) => {
                            error!("PartitionWorker {}: Failed to process line. \
                                    Sending stop to worker. Error: {}",
                                   self.worker_id.0,
                                   err);
                            discard_until_stop = true;
                            if let Err(err) =
                                self.send_to_leader
                                    .send(PartitionWorkerMessage::Stopped(self.worker_id.clone())) {
                                error!("PartitionWorker {}: Failed to send \
                                        'Stopped' message to worker. Stopping anyways.",
                                       self.worker_id.0);
                                return;
                            }
                            discard_until_stop = true;
                        }
                    }
                }
                LeaderMessage::Stop => {
                    if num_disarded > 0 {
                        warn!("PartitionWorker {}: Receive Stop signal. \
                               {} messages have been disacrded.",
                              self.worker_id.0,
                              num_disarded);
                    }
                    if let Err(err) =
                        self.send_to_leader
                            .send(PartitionWorkerMessage::Stopped(self.worker_id.clone())) {
                        error!("PartitionWorker {}: Failed to send 'Stopped' message to worker. \
                                Stopping anyways. {} messages have been disacrded.",
                               self.worker_id.0,
                               num_disarded);
                    }
                    return;
                }
            }
        }
    }

    fn process_line(&self, stream_id: &StreamId, line: String) -> ClientResult<AfterBatchAction> {
        match serde_json::from_str::<DeserializedBatch>(line.as_ref()) {
            Ok(DeserializedBatch { cursor, events }) => {
                // This is a hack. We might later want to extract the slice manually.
                let events_json = events.unwrap_or(Vec::new());
                let events_str = serde_json::to_string(events_json.as_slice()).unwrap();
                let batch_info = BatchInfo {
                    stream_id: stream_id.clone(),
                    cursor: cursor.clone(),
                };
                Ok(self.process_batch(batch_info, events_str.as_ref()))
            }
            Err(err) => {
                bail!(ClientErrorKind::UnparsableBatch(format!("Could not parse '{}': {}",
                                                               line,
                                                               err)))
            }
        }
    }

    fn process_batch(&self, batch_info: BatchInfo, batch: &str) -> AfterBatchAction {
        let cloned_info = batch_info.clone();
        match self.handler.handle(batch, batch_info) {
            AfterBatchAction::Continue => {
                self.checkpoint(cloned_info);
                AfterBatchAction::Continue
            }
            AfterBatchAction::ContinueNoCheckpoint => AfterBatchAction::ContinueNoCheckpoint,
            AfterBatchAction::Stop => {
                self.checkpoint(cloned_info);
                AfterBatchAction::Stop
            }
            AfterBatchAction::Abort => {
                warn!("PartitionWorker {}: Abort. Skipping checkpointing on stream {}.",
                      self.worker_id.0,
                      cloned_info.stream_id.0);
                AfterBatchAction::Abort
            }
        }
    }


    fn checkpoint(&self, batch_info: BatchInfo) {
        let BatchInfo { stream_id, cursor } = batch_info;
        let cursors = &[cursor];
        let mut attempt = 0;
        while attempt < 5 {
            if attempt > 0 {
                let pause = retry_pause(attempt - 1);
                thread::sleep(pause)
            }
            attempt += 1;
            match self.checkpointer
                      .checkpoint(&stream_id, &self.subscription_id, cursors) {
                Ok(()) => return,
                Err(err) => {
                    if attempt > 5 {
                        error!("PartitionWorker {}: Finally gave up to checkpoint cursor after {} \
                                attempts.",
                               self.worker_id.0,
                               err);
                        return;
                    } else {
                        warn!("PartitionWorker {}: Failed to checkpoint to Nakadi: {}",
                              self.worker_id.0,
                              err);
                    }
                }
            }
        }
        error!("PartitionWorker {}: Checkpointing aborted due to worker shutdown.",
               self.worker_id.0);
    }
}

#[derive(Deserialize)]
struct DeserializedBatch {
    cursor: Cursor,
    events: Option<Vec<Value>>,
}


fn retry_pause(retry: usize) -> Duration {
    let idx = ::std::cmp::min(retry, RETRY_MILLIS.len() - 1);
    ::std::time::Duration::from_millis(RETRY_MILLIS[idx])
}
