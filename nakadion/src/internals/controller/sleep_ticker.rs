use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use tokio::{self, time::delay_for};

use crate::internals::{dispatcher::SleepingDispatcher, ConsumerState};
use crate::logging::Logger;

/// The sleep ticker calls the tick method on the sleeping dispatcher
/// while waiting for the first frame.
pub(crate) struct SleepTicker<C> {
    join_handle: Option<tokio::task::JoinHandle<SleepingDispatcher<C>>>,
    wake_up: Arc<AtomicBool>,
}

impl<C> SleepTicker<C>
where
    C: Send + 'static,
{
    pub fn start(
        sleeping_dispatcher: SleepingDispatcher<C>,
        consumer_state: ConsumerState,
    ) -> Self {
        consumer_state.debug(format_args!("Starting sleep ticker."));
        let wake_up = Arc::new(AtomicBool::new(false));
        let join_handle =
            Self::tick_sleeping(Arc::clone(&wake_up), sleeping_dispatcher, consumer_state);

        Self {
            join_handle: Some(join_handle),
            wake_up,
        }
    }

    /// Join for the sleeping dispatcher and stop the ticker
    pub fn join(mut self) -> tokio::task::JoinHandle<SleepingDispatcher<C>> {
        self.join_handle.take().unwrap()
    }

    fn tick_sleeping(
        wake_up: Arc<AtomicBool>,
        mut sleeping_dispatcher: SleepingDispatcher<C>,
        consumer_state: ConsumerState,
    ) -> tokio::task::JoinHandle<SleepingDispatcher<C>>
    where
        C: Send + 'static,
    {
        consumer_state.info(format_args!("Waiting for connection and incoming frames"));
        let delay = consumer_state.config().tick_interval.into_duration();

        let started = Instant::now();

        let mut last_wait_notification = Instant::now();

        let mut next_tick_at = started + delay;
        let sleep = async move {
            consumer_state.debug(format_args!("Starting sleep ticker loop"));
            loop {
                if wake_up.load(Ordering::SeqCst) || consumer_state.global_cancellation_requested()
                {
                    consumer_state.debug(format_args!("Woke up!"));
                    break;
                }
                let now = Instant::now();
                if now <= next_tick_at {
                    sleeping_dispatcher.tick();
                    next_tick_at = now + delay;
                }
                delay_for(Duration::from_millis(10)).await;
                if last_wait_notification.elapsed() > Duration::from_secs(15) {
                    consumer_state.info(format_args!(
                        "Still waiting for connection and incoming frames (waiting for {:?})",
                        started.elapsed()
                    ));
                    last_wait_notification = Instant::now();
                }
            }

            sleeping_dispatcher
        };

        tokio::spawn(sleep)
    }
}

impl<C> Drop for SleepTicker<C> {
    fn drop(&mut self) {
        self.wake_up.store(true, Ordering::SeqCst)
    }
}
