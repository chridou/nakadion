use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use tokio::{self, time::delay_for};

use crate::consumer::Config;
use crate::handler::{BatchHandler, BatchHandlerFactory};
use crate::internals::{dispatcher::SleepingDispatcher, ConsumerState};
use crate::logging::Logs;

pub(crate) struct ControllerParams<H, C> {
    pub api_client: C,
    pub consumer_state: ConsumerState,
    pub handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
}

impl<H, C> ControllerParams<H, C> {
    pub fn config(&self) -> &Config {
        &self.consumer_state.config()
    }
}

impl<H, C> Clone for ControllerParams<H, C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            api_client: self.api_client.clone(),
            consumer_state: self.consumer_state.clone(),
            handler_factory: Arc::clone(&self.handler_factory),
        }
    }
}

pub(crate) struct SleepTicker<H, C> {
    join_handle: Option<tokio::task::JoinHandle<SleepingDispatcher<H, C>>>,
    wake_up: Arc<AtomicBool>,
}

impl<H, C> SleepTicker<H, C>
where
    H: BatchHandler,
    C: Send + 'static,
{
    pub fn start(
        sleeping_dispatcher: SleepingDispatcher<H, C>,
        consumer_state: ConsumerState,
    ) -> Self {
        let wake_up = Arc::new(AtomicBool::new(false));
        let join_handle =
            Self::tick_sleeping(Arc::clone(&wake_up), sleeping_dispatcher, consumer_state);

        Self {
            join_handle: Some(join_handle),
            wake_up,
        }
    }

    pub fn join(mut self) -> tokio::task::JoinHandle<SleepingDispatcher<H, C>> {
        self.join_handle.take().unwrap()
    }

    fn tick_sleeping(
        wake_up: Arc<AtomicBool>,
        mut sleeping_dispatcher: SleepingDispatcher<H, C>,
        consumer_state: ConsumerState,
    ) -> tokio::task::JoinHandle<SleepingDispatcher<H, C>>
    where
        H: BatchHandler,
        C: Send + 'static,
    {
        let delay = consumer_state.config().tick_interval.into_duration();

        let mut last_wait_notification = Instant::now();
        consumer_state.info(format_args!("Waiting for connection"));
        let sleep = async move {
            loop {
                if wake_up.load(Ordering::SeqCst) || consumer_state.global_cancellation_requested()
                {
                    consumer_state.debug(format_args!("Woke up!"));
                    break;
                }
                sleeping_dispatcher.tick();
                delay_for(delay).await;
                if last_wait_notification.elapsed() > Duration::from_secs(10) {
                    consumer_state.info(format_args!("Waiting for incoming batches..."));
                    last_wait_notification = Instant::now();
                }
            }

            sleeping_dispatcher
        };

        tokio::spawn(sleep)
    }
}

impl<H, C> Drop for SleepTicker<H, C> {
    fn drop(&mut self) {
        self.wake_up.store(true, Ordering::SeqCst)
    }
}
