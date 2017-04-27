use std::sync::Mutex;
use libmetrics::metrics::{Meter, StdMeter};
use histogram::Histogram;

#[derive(Serialize, Default)]
pub struct WorkerStats {
    pub handler: HandlerStats,
    pub stream: StreamStats,
    pub checkpointing: CheckpointingStats,
}

#[derive(Serialize, Default)]
pub struct HandlerStats {
    pub batches_per_second: MeterSnapshot,
    pub bytes_per_batch: HistogramSnapshot,
    pub processing_durations: HistogramSnapshot,
    pub bytes_per_second: MeterSnapshot,
}


#[derive(Serialize, Default)]
pub struct StreamStats {
    pub lines_per_second: MeterSnapshot,
    pub bytes_per_line: HistogramSnapshot,
    pub bytes_per_second: MeterSnapshot,
    pub keep_alives_per_second: MeterSnapshot,
    pub connection_duration: HistogramSnapshot,
    pub batches_dropped_per_second: MeterSnapshot,
}


#[derive(Serialize, Default)]
pub struct CheckpointingStats {
    pub checkpoints_per_second: MeterSnapshot,
    pub checkpointing_durations: HistogramSnapshot,
    pub checkpointing_errors_per_second: MeterSnapshot,
    pub checkpointing_failures_per_second: MeterSnapshot,
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
