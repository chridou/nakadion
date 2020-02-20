use std::error::Error as StdError;
use std::fmt;

pub mod streams;

#[derive(Debug)]
pub struct IoError(pub String);

impl IoError {
    pub fn new<T: Into<String>>(s: T) -> Self {
        Self(s.into())
    }
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        Ok(())
    }
}

impl StdError for IoError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

pub mod connector {
    use std::error::Error as StdError;
    use std::fmt;
    use std::time::Instant;

    use futures::{
        future::{BoxFuture, FutureExt},
        stream::{BoxStream, StreamExt},
    };
    use http::StatusCode;
    use serde::de::DeserializeOwned;
    use serde_json;
    use tokio::time::timeout;

    pub use crate::components::{
        streams::{BatchLine, BatchLineError, NakadiFrame},
        IoError,
    };

    pub use crate::api::BytesStream;
    pub use crate::api::{NakadiApiError, SubscriptionStreamApi, SubscriptionStreamChunks};
    pub use crate::consumer::ConnectStreamTimeoutSecs;
    use crate::instrumentation::{Instrumentation, Instruments};
    pub use crate::nakadi_types::{
        model::subscription::{StreamId, StreamParameters, SubscriptionCursor, SubscriptionId},
        Error, FlowId, RandomFlowId,
    };

    pub type ConnectResult<T> = Result<T, ConnectError>;
    pub type ConnectFuture<'a, T> = BoxFuture<'a, ConnectResult<T>>;

    pub type EventsStream<E> =
        BoxStream<'static, Result<(StreamedBatchMeta, Option<Vec<E>>), Error>>;
    pub type FramesStream = BoxStream<'static, Result<NakadiFrame, IoError>>;
    pub type BatchStream = BoxStream<'static, Result<BatchLine, BatchLineError>>;

    #[derive(Debug, Clone)]
    pub struct StreamedBatchMeta {
        pub cursor: SubscriptionCursor,
        pub received_at: Instant,
        pub frame_id: usize,
    }

    pub trait Connects {
        fn instrumentation(&self) -> Instrumentation {
            Instrumentation::default()
        }
        fn chunked_stream(&self) -> ConnectFuture<SubscriptionStreamChunks>;
        fn frame_stream(&self) -> ConnectFuture<(StreamId, FramesStream)>
        where
            Self: Sync,
        {
            async move {
                let (stream_id, chunks) = self.chunked_stream().await?.parts();

                let framed =
                    crate::components::streams::FramedStream::new(chunks, self.instrumentation());

                Ok((stream_id, framed.boxed()))
            }
            .boxed()
        }

        fn batch_stream(&self) -> ConnectFuture<(StreamId, BatchStream)>
        where
            Self: Sync,
        {
            async move {
                let (stream_id, frames) = self.frame_stream().await?;

                let lines = crate::components::streams::BatchLineStream::new(
                    frames,
                    self.instrumentation(),
                );

                Ok((stream_id, lines.boxed()))
            }
            .boxed()
        }

        fn events_stream<E: DeserializeOwned>(&self) -> ConnectFuture<(StreamId, EventsStream<E>)>
        where
            Self: Sync,
        {
            async move {
                let (stream_id, batches) = self.batch_stream().await?;

                let events = batches.map(|r| match r {
                    Ok(batch_line) => {
                        let cursor = batch_line.cursor_deserialized::<SubscriptionCursor>()?;
                        let meta = StreamedBatchMeta {
                            cursor,
                            received_at: batch_line.received_at(),
                            frame_id: batch_line.frame_id(),
                        };

                        if let Some(events_bytes) = batch_line.events_bytes() {
                            let events = serde_json::from_slice::<Vec<E>>(&events_bytes)?;
                            Ok((meta, Some(events)))
                        } else {
                            Ok((meta, None))
                        }
                    }
                    Err(err) => Err(Error::from_error(err)),
                });

                Ok((stream_id, events.boxed()))
            }
            .boxed()
        }
    }

    #[derive(Debug, Clone)]
    pub struct ConnectorParams {
        pub subscription_id: SubscriptionId,
        pub stream_params: StreamParameters,
        pub connect_stream_timeout: ConnectStreamTimeoutSecs,
        pub flow_id: Option<FlowId>,
    }

    pub struct Connector<C> {
        inner: ConnectorInner<C>,
    }

    impl<C> Connector<C>
    where
        C: SubscriptionStreamApi + Sync + 'static,
    {
        pub fn new(
            stream_client: C,
            params: ConnectorParams,
            instrumentation: Option<Instrumentation>,
        ) -> Self {
            Self {
                inner: ConnectorInner {
                    stream_client,
                    instrumentation: instrumentation.unwrap_or_default(),
                    params,
                },
            }
        }
    }

    impl<C> Connects for Connector<C>
    where
        C: SubscriptionStreamApi + Sync + Send + 'static,
    {
        fn instrumentation(&self) -> Instrumentation {
            self.inner.instrumentation.clone()
        }
        fn chunked_stream(&self) -> ConnectFuture<SubscriptionStreamChunks> {
            async move {
                let inner = &self.inner;
                let flow_id = inner
                    .params
                    .flow_id
                    .clone()
                    .unwrap_or_else(|| RandomFlowId.into());
                let f = inner.stream_client.request_stream(
                    inner.params.subscription_id,
                    &inner.params.stream_params,
                    flow_id,
                );
                let connect_timeout = inner.params.connect_stream_timeout.into_duration();
                let started = Instant::now();
                match timeout(connect_timeout, f).await {
                    Ok(Ok(stream)) => {
                        inner
                            .instrumentation
                            .stream_connect_attempt_success(started.elapsed());
                        Ok(stream)
                    }
                    Ok(Err(api_error)) => {
                        inner
                            .instrumentation
                            .stream_connect_attempt_failed(started.elapsed());
                        Err(api_error.into())
                    }
                    Err(err) => {
                        inner
                            .instrumentation
                            .stream_connect_attempt_failed(started.elapsed());
                        Err(ConnectError::io()
                            .context(format!(
                                "Connecting to Nakadi for a stream timed ot after {:?}.",
                                started.elapsed()
                            ))
                            .caused_by(err))
                    }
                }
            }
            .boxed()
        }
    }

    struct ConnectorInner<C> {
        stream_client: C,
        instrumentation: Instrumentation,
        params: ConnectorParams,
    }

    impl From<NakadiApiError> for ConnectError {
        fn from(api_error: NakadiApiError) -> Self {
            if let Some(status) = api_error.status() {
                match status {
                    StatusCode::NOT_FOUND => ConnectError::not_found().caused_by(api_error),
                    StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => {
                        ConnectError::access_denied().caused_by(api_error)
                    }
                    StatusCode::BAD_REQUEST => ConnectError::bad_request().caused_by(api_error),
                    StatusCode::UNPROCESSABLE_ENTITY => {
                        ConnectError::unprocessable().caused_by(api_error)
                    }
                    _ => ConnectError::other().caused_by(api_error),
                }
            } else if api_error.is_io_error() {
                ConnectError::bad_request().caused_by(api_error)
            } else {
                ConnectError::other().caused_by(api_error)
            }
        }
    }

    #[derive(Debug)]
    pub struct ConnectError {
        context: Option<String>,
        kind: ConnectErrorKind,
        source: Option<Box<dyn StdError + Send + 'static>>,
    }

    impl ConnectError {
        pub fn new(kind: ConnectErrorKind) -> Self {
            Self {
                context: None,
                kind,
                source: None,
            }
        }

        pub fn not_found() -> Self {
            Self::new(ConnectErrorKind::SubscriptionNotFound)
        }
        pub fn access_denied() -> Self {
            Self::new(ConnectErrorKind::AccessDenied)
        }
        pub fn unprocessable() -> Self {
            Self::new(ConnectErrorKind::Unprocessable)
        }
        pub fn bad_request() -> Self {
            Self::new(ConnectErrorKind::BadRequest)
        }
        pub fn io() -> Self {
            Self::new(ConnectErrorKind::Io)
        }
        pub fn other() -> Self {
            Self::new(ConnectErrorKind::Other)
        }

        pub fn context<T: Into<String>>(mut self, context: T) -> Self {
            self.context = Some(context.into());
            self
        }

        pub fn caused_by<E: StdError + Send + 'static>(mut self, source: E) -> Self {
            self.source = Some(Box::new(source));
            self
        }

        pub fn kind(&self) -> ConnectErrorKind {
            self.kind
        }
    }

    impl StdError for ConnectError {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            self.source
                .as_ref()
                .map(|e| &**e as &(dyn StdError + 'static))
        }
    }

    impl fmt::Display for ConnectError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            if let Some(ref context) = self.context {
                write!(f, "{}", context)?;
            } else {
                write!(f, "could not connect to stream: {}", self.kind)?;
            }
            Ok(())
        }
    }

    impl From<ConnectError> for Error {
        fn from(err: ConnectError) -> Self {
            Error::from_error(err)
        }
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    pub enum ConnectErrorKind {
        SubscriptionNotFound,
        AccessDenied,
        Unprocessable,
        BadRequest,
        Io,
        Other,
    }

    impl fmt::Display for ConnectErrorKind {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                ConnectErrorKind::SubscriptionNotFound => write!(f, "subscription not found")?,
                ConnectErrorKind::AccessDenied => write!(f, "access denied")?,
                ConnectErrorKind::Unprocessable => write!(f, "unprocessable")?,
                ConnectErrorKind::BadRequest => write!(f, "bad request")?,
                ConnectErrorKind::Io => write!(f, "io")?,
                ConnectErrorKind::Other => write!(f, "other")?,
            }
            Ok(())
        }
    }
}
