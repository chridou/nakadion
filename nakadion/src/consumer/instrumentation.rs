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
