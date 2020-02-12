pub trait Instrumented {}

#[derive(Debug, Clone)]
pub enum Instrumentation {
    Off,
}

impl Default for Instrumentation {
    fn default() -> Self {
        Instrumentation::Off
    }
}
