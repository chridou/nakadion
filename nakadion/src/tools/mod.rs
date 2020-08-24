pub mod subscription_stats {
    use std::time::{Duration, Instant};

    use tokio::{
        sync::mpsc::{self, error::TryRecvError},
        time::delay_for,
    };

    use crate::nakadi_types::subscription::{StreamId, SubscriptionId};
    use crate::{
        api::SubscriptionApi,
        consumer::LifecycleListener,
        logging::{LoggingAdapter, LoggingContext},
    };

    pub trait Instruments {
        /// Tracks the number of unconsumed events.
        fn unconsumed_events(&self, n_unconsumed: usize);
    }

    pub struct SubscriptionStatsReporter {
        sender: mpsc::UnboundedSender<Message>,
    }

    impl SubscriptionStatsReporter {
        pub fn new<C, I, L>(client: C, instrumentation: I, logger: L) -> Self
        where
            I: Instruments + Clone + Send + 'static,
            C: SubscriptionApi + Clone + Send + Sync + 'static,
            L: LoggingAdapter + Send + 'static,
        {
            let (sender, receiver) = mpsc::unbounded_channel();

            let looper = Looper::new(client, instrumentation, receiver, logger);

            tokio::spawn(looper.loop_around());

            Self { sender }
        }
    }

    impl LifecycleListener for SubscriptionStatsReporter {
        fn on_stream_connected(&self, subscription_id: SubscriptionId, stream_id: StreamId) {
            let _ = self
                .sender
                .send(Message::StreamStart(subscription_id, stream_id));
        }
        fn on_stream_ended(&self, subscription_id: SubscriptionId, stream_id: StreamId) {
            let _ = self
                .sender
                .send(Message::StreamEnd(subscription_id, stream_id));
        }
    }

    enum Message {
        StreamStart(SubscriptionId, StreamId),
        StreamEnd(SubscriptionId, StreamId),
        Noop,
    }

    struct ConnectedLooper<C, I, L> {
        client: C,
        instrumentation: I,
        stream_id: StreamId,
        subscription_id: SubscriptionId,
        receiver: mpsc::UnboundedReceiver<Message>,
        interval: Duration,
        logger: L,
    }

    impl<C, I, L> ConnectedLooper<C, I, L>
    where
        I: Instruments + Clone + Send + 'static,
        C: SubscriptionApi + Clone + Send + Sync + 'static,
        L: LoggingAdapter + Send + 'static,
    {
        async fn loop_around(mut self) -> Option<DisconnectedLooper<C, I, L>> {
            let mut next_check = Instant::now();
            loop {
                let message = match self.receiver.try_recv() {
                    Ok(msg) => msg,
                    Err(TryRecvError::Closed) => break,
                    Err(TryRecvError::Empty) => Message::Noop,
                };

                match message {
                    Message::StreamStart(subscription_id, stream_id) => {
                        self.stream_id = stream_id;
                        self.subscription_id = subscription_id;
                        next_check = Instant::now();
                    }
                    Message::StreamEnd(_, _) => {
                        return Some(DisconnectedLooper {
                            client: self.client,
                            instrumentation: self.instrumentation,
                            receiver: self.receiver,
                            interval: self.interval,
                            logger: self.logger,
                        })
                    }
                    Message::Noop => {}
                }

                if next_check <= Instant::now() {
                    let maybe_stats = self
                        .client
                        .get_subscription_stats(self.subscription_id, false, ())
                        .await;

                    match maybe_stats {
                        Ok(stats) => {
                            next_check = Instant::now() + self.interval;
                            if let Some(n_unconsumed) =
                                stats.unconsumed_stream_events(self.stream_id)
                            {
                                self.instrumentation.unconsumed_events(n_unconsumed)
                            } else {
                                self.instrumentation.unconsumed_events(0)
                            }
                        }
                        Err(err) => {
                            let context = LoggingContext {
                                subscription_id: Some(self.subscription_id),
                                stream_id: Some(self.stream_id),
                                event_type: None,
                                partition_id: None,
                            };
                            self.logger.warn(
                                &context,
                                format_args!("Failed to query stream stats: {}", err),
                            );
                            next_check = Instant::now() + Duration::from_secs(3);
                        }
                    }
                }

                delay_for(Duration::from_secs(1)).await;
            }

            let context = LoggingContext {
                subscription_id: Some(self.subscription_id),
                stream_id: Some(self.stream_id),
                event_type: None,
                partition_id: None,
            };
            self.logger.warn(
                &context,
                format_args!(
                    "Stream stats tracker terminating \
                while stream still active"
                ),
            );

            None
        }
    }

    struct DisconnectedLooper<C, I, L> {
        client: C,
        instrumentation: I,
        receiver: mpsc::UnboundedReceiver<Message>,
        interval: Duration,
        logger: L,
    }

    impl<C, I, L> DisconnectedLooper<C, I, L>
    where
        I: Instruments + Clone + Send + 'static,
        C: SubscriptionApi + Clone + Send + Sync + 'static,
        L: LoggingAdapter + Send + 'static,
    {
        async fn loop_around(mut self) -> Option<ConnectedLooper<C, I, L>> {
            loop {
                let message = match self.receiver.try_recv() {
                    Ok(msg) => msg,
                    Err(TryRecvError::Closed) => break,
                    Err(TryRecvError::Empty) => Message::Noop,
                };

                match message {
                    Message::StreamStart(subscription_id, stream_id) => {
                        return Some(ConnectedLooper {
                            client: self.client,
                            instrumentation: self.instrumentation,
                            stream_id,
                            subscription_id,
                            receiver: self.receiver,
                            interval: self.interval,
                            logger: self.logger,
                        })
                    }
                    Message::StreamEnd(_, _) => {}
                    Message::Noop => {}
                }

                delay_for(Duration::from_secs(1)).await;
            }

            self.logger.info(
                &LoggingContext::default(),
                format_args!("Stream stats tracker terminating."),
            );

            None
        }
    }

    enum Looper<C, I, L> {
        Connected(ConnectedLooper<C, I, L>),
        Disconnected(DisconnectedLooper<C, I, L>),
    }

    impl<C, I, L> Looper<C, I, L>
    where
        I: Instruments + Clone + Send + 'static,
        C: SubscriptionApi + Clone + Send + Sync + 'static,
        L: LoggingAdapter + Send + 'static,
    {
        pub fn new(
            client: C,
            instrumentation: I,
            receiver: mpsc::UnboundedReceiver<Message>,
            logger: L,
        ) -> Self {
            logger.info(
                &LoggingContext::default(),
                format_args!("Stream stats tracker starting."),
            );

            let looper = DisconnectedLooper {
                client,
                instrumentation,
                receiver,
                interval: Duration::from_secs(41),
                logger,
            };

            Self::Disconnected(looper)
        }

        pub async fn loop_around(self) {
            let mut looper = self;
            loop {
                let maybe_next = match looper {
                    Looper::Connected(inner) => inner.loop_around().await.map(Looper::Disconnected),
                    Looper::Disconnected(inner) => inner.loop_around().await.map(Looper::Connected),
                };

                if let Some(l) = maybe_next {
                    looper = l;
                } else {
                    break;
                }
            }
        }
    }
}
