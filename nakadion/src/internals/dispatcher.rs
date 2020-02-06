pub enum DispatchStrategy {
    Single,
    EventType,
    EventTypePartition,
}

pub struct SleepingDispatcher;

pub struct ActiveDispatcher;

mod dispatch_single {

    use crate::internals::worker::*;

    pub struct Sleeping<H> {
        worker: SleepingWorker<H>,
    }

    pub struct Active<H> {
        worker: ActiveWorker<H>,
    }
}

mod dispatch_event_type {}

mod dispatch_event_type_partition {}
