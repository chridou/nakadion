use std::fmt;
use std::sync::Arc;

pub trait Instruments {}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MetricsDetailLevel {
    Low,
    Medium,
    High,
}

impl Default for MetricsDetailLevel {
    fn default() -> Self {
        MetricsDetailLevel::Medium
    }
}

#[derive(Clone)]
pub struct Instrumentation {
    instr: InstrumentationSelection,
    detail: MetricsDetailLevel,
}

impl Instrumentation {
    pub fn off() -> Self {
        {
            Instrumentation {
                instr: InstrumentationSelection::Off,
                detail: MetricsDetailLevel::default(),
            }
        }
    }

    pub fn new<I>(instruments: I, detail: MetricsDetailLevel) -> Self
    where
        I: Instruments + Send + Sync + 'static,
    {
        Instrumentation {
            instr: InstrumentationSelection::Custom(Arc::new(instruments)),
            detail,
        }
    }
}

impl Default for Instrumentation {
    fn default() -> Self {
        Instrumentation::off()
    }
}

impl fmt::Debug for Instrumentation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Instrumentation")?;
        Ok(())
    }
}

#[derive(Clone)]
enum InstrumentationSelection {
    Off,
    Custom(Arc<dyn Instruments + Send + Sync>),
}
