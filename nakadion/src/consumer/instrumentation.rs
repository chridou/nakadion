use std::fmt;

pub trait Instrumented {}

#[derive(Clone)]
pub enum Instrumentation {
    Off,
}

impl Default for Instrumentation {
    fn default() -> Self {
        Instrumentation::Off
    }
}

impl fmt::Debug for Instrumentation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Instrumentation::Off => write!(f, "Instrumentation(Off)")?,
        }
        Ok(())
    }
}
