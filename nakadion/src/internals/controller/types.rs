use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use tokio::{self, time::delay_for};

use crate::consumer::Config;
use crate::handler::BatchHandlerFactory;
use crate::internals::{dispatcher::SleepingDispatcher, ConsumerState};
use crate::logging::Logger;

pub(crate) struct ControllerParams<C> {
    pub api_client: C,
    pub consumer_state: ConsumerState,
    pub handler_factory: Arc<dyn BatchHandlerFactory>,
}

impl<C> ControllerParams<C> {
    pub fn config(&self) -> &Config {
        &self.consumer_state.config()
    }
}

impl<C> Clone for ControllerParams<C>
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
        let wake_up = Arc::new(AtomicBool::new(false));
        let join_handle =
            Self::tick_sleeping(Arc::clone(&wake_up), sleeping_dispatcher, consumer_state);

        Self {
            join_handle: Some(join_handle),
            wake_up,
        }
    }

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
                    consumer_state.info(format_args!("Waiting for incoming frames..."));
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
