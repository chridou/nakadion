use std::sync::Mutex;
use libmetrics::metrics::{Meter, StdMeter};
use histogram::Histogram;


#[derive(Serialize)]
pub struct MeterValues {
    count: i64,
    one_minute_rate: f64,
    five_minute_rate: f64,
    fifteen_minute_rate: f64,
    mean: f64,
}

impl MeterValues {
    pub fn new(meter: &Meter) -> MeterValues {
        let snapshot = meter.snapshot();
        MeterValues {
            count: snapshot.count,
            one_minute_rate: snapshot.rates[0],
            five_minute_rate: snapshot.rates[1],
            fifteen_minute_rate: snapshot.rates[2],
            mean: snapshot.mean,
        }
    }
}

/// Contains the snapshot of a `Histogram`
#[derive(Serialize)]
pub struct HistogramValues {
    #[serde(skip_serializing_if="Option::is_none")]
    min: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    max: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    mean: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    std_var: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    std_dev: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    p75: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    p95: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    p98: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    p99: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    p999: Option<u64>,
}

impl HistogramValues {
    pub fn new(histogram: &Histogram) -> HistogramValues {
        HistogramValues {
            min: histogram.minimum().ok(),
            max: histogram.maximum().ok(),
            mean: histogram.mean().ok(),
            std_var: histogram.stdvar().ok(),
            std_dev: histogram.stddev(),
            p75: histogram.percentile(75.0).ok(),
            p95: histogram.percentile(95.0).ok(),
            p98: histogram.percentile(98.0).ok(),
            p99: histogram.percentile(99.0).ok(),
            p999: histogram.percentile(99.9).ok(),
        }
    }

    pub fn new_locked(histogram: &Mutex<Histogram>) -> HistogramValues {
        let guard = histogram.lock().unwrap();
        HistogramValues::new(&guard)
    }
}
