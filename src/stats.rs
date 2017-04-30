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
    /// Distribution of processing time for handler calls in microseconds
    pub processing_durations: HistogramSnapshot,
    /// The number of bytes passed to the handler as a batch per second
    pub bytes_per_second: MeterSnapshot,
}


#[derive(Serialize, Default)]
pub struct StreamStats {
    /// The number of lines received including keep alive lines
    pub lines_per_second: MeterSnapshot,
    /// The distribution of bytes per line received including keep alive lines
    pub bytes_per_line: HistogramSnapshot,
    /// The number of bytes received per second including keep alive lines
    pub bytes_per_second: MeterSnapshot,
    /// The number of keep alive lines received per second
    pub keep_alives_per_second: MeterSnapshot,
    /// The distribution of the number of lines received via connections
    /// including keep alive lines
    pub lines_per_connection: HistogramSnapshot,
    /// The distribution of the time connections were active in seconds up to 48h
    pub connection_duration: HistogramSnapshot,
    /// The distribution of the time when there was no connection in milliseconds up to 10 hours
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
    /// The distribution of times it took to checkpoint in milliseconds up to 61 minutes
    pub checkpointing_durations: HistogramSnapshot,
    /// The number of failed attempts to checkpoint.
    ///
    /// This includes retries which might later on succeeed.
    pub checkpointing_errors_per_second: MeterSnapshot,
    /// The number of finally failed checkpointing attempts per second
    pub checkpointing_failures_per_second: MeterSnapshot,
    /// The distribution of times it took until checkpointing finally failed
    /// in milliseconds up to 61 minutes.
    pub checkpointing_failures_durations: HistogramSnapshot,
}

#[derive(Serialize, Default)]
pub struct MeterSnapshot {
    /// The number of collected values since start.
    pub count: i64,
    pub one_minute_rate: f64,
    pub five_minute_rate: f64,
    pub fifteen_minute_rate: f64,
    /// The mean of all collected values since start
    pub mean: f64,
}

/// Contains the snapshot of a `Histogram`
///
/// The underlying histogram is a [HdrHistogram](https://hdrhistogram.github.io/HdrHistogram).
/// Descriptions were taken and slightly modified from crate `hdrsample`.
#[derive(Serialize, Default)]
pub struct HistogramSnapshot {
    /// Contains the total number of samples recorded.
    pub count: u64,
    /// Contains the lowest recorded value level in the histogram.
    /// If the histogram has no recorded values, the value returned will be 0.
    pub min: u64,
    /// Contains the highest recorded value level in the histogram.
    /// If the histogram has no recorded values, the value returned is undefined.
    pub max: u64,
    /// Contains the computed mean value of all recorded values in the histogram.
    pub mean: f64,
    /// Contains the computed standard deviation of all recorded values in the histogram
    pub std_dev: f64,
    /// Contains the lowest discernible value for the histogram in its current configuration.
    pub low: u64,
    /// Contains the highest trackable value for the histogram in its current configuration.
    pub high: u64,
    /// Contains the number of significant value digits kept by this histogram.
    pub sigfig: u32,
    /// Contains the current number of distinct counted values in the histogram.
    pub len: usize,
    /// Contains the percentiles
    pub percentiles: HistogramPercentiles,
}

#[derive(Serialize, Default)]
pub struct HistogramPercentiles {
    pub p75: u64,
    pub p95: u64,
    pub p98: u64,
    pub p99: u64,
    pub p999: u64,
    pub p9999: u64,
}
