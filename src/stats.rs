use std::sync::Mutex;
use libmetrics::metrics::{Meter, StdMeter};
use histogram::Histogram;

#[derive(Serialize, Default)]
pub struct WorkerStats {
    /// Statistics regarding the user supplied batch handler
    pub handler: HandlerStats,
    /// General statistics about stream consumption
    pub stream: StreamStats,
    /// Statistics regarding Checkpointing
    pub checkpointing: CheckpointingStats,
}

#[derive(Serialize, Default)]
pub struct HandlerStats {
    /// The number of batches passed to the handler batch per second
    pub batches_per_second: MeterSnapshot,
    /// Distribution of bytes over batches
    pub bytes_per_batch: HistogramSnapshot,
    /// Distribution of processing time for a handler call
    pub processing_durations: HistogramSnapshot,
    /// The number of bytes passed to the handler as a batch per second
    pub bytes_per_second: MeterSnapshot,
}


#[derive(Serialize, Default)]
pub struct StreamStats {
    /// The number of lines received including keep alives lines
    pub lines_per_second: MeterSnapshot,
    /// The distribution of bytes per line received including keep alive lines
    pub bytes_per_line: HistogramSnapshot,
    /// The number of bytes received per second including keep alive lines
    pub bytes_per_second: MeterSnapshot,
    /// The number of keep alive lines received per second
    pub keep_alives_per_second: MeterSnapshot,
    /// The distribution of the number of lines received via connections
    pub lines_per_connection: HistogramSnapshot,
    /// The distribution of the time connections were active
    pub connection_duration: HistogramSnapshot,
    /// The distribution of the time when there was no connection
    pub no_connection_duration: HistogramSnapshot,
    /// The number of batches dropped due to a changed `StreamId` or shutdown.
    ///
    /// This mostly apllies only when using the concurrent worker.
    pub batches_dropped_per_second: MeterSnapshot,
}


#[derive(Serialize, Default)]
pub struct CheckpointingStats {
    /// The number of checkpoints committed per second
    pub checkpoints_per_second: MeterSnapshot,
    /// The distribution of times it took to checkpoint
    pub checkpointing_durations: HistogramSnapshot,
    /// The number of failed attempts to checkpoint.
    ///
    /// This includes retries which might later on succeeed.
    pub checkpointing_errors_per_second: MeterSnapshot,
    /// The number of finally failed checkpointing attempts per second
    pub checkpointing_failures_per_second: MeterSnapshot,
    /// The distribution of times it took until checkpointing finally failed
    pub checkpointing_failures_durations: HistogramSnapshot,
}

#[derive(Serialize, Default)]
pub struct MeterSnapshot {
    pub count: i64,
    pub one_minute_rate: f64,
    pub five_minute_rate: f64,
    pub fifteen_minute_rate: f64,
    pub mean: f64,
}

/// Contains the snapshot of a `Histogram`
#[derive(Serialize, Default)]
pub struct HistogramSnapshot {
    #[serde(skip_serializing_if="Option::is_none")]
    pub min: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub max: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub mean: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub std_var: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub std_dev: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub p75: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub p95: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub p98: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub p99: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub p999: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub p9999: Option<u64>,
}
