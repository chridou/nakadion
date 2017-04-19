use std::io::Read;
use std::time::Duration;

use url::Url;
use hyper::Client;
use hyper::net::HttpsConnector;
use hyper_native_tls::NativeTlsClient;
use hyper::header::{Authorization, Bearer, ContentType, Headers};
use hyper::status::StatusCode;
use serde_json;

use super::*;
use ProvidesToken;

header! { (XNakadiStreamId, "X-Nakadi-StreamId") => [String] }

/// Connects to `Nakadi` and reads the stream-
pub trait ReadsStream {
    type StreamingSource: Read;

    /// Attempts to get data from the stream. Also returns the `StreamId`
    /// which must be used for checkpointing.
    ///
    /// Starts a new stream for reading events from this subscription. The data will be
    /// automatically rebalanced between streams of one subscription.
    /// The minimal consumption unit is a partition,
    /// so it is possible to start as
    /// many streams as the total number of partitions in event-types of this subscription.
    /// The rebalance currently
    /// only operates with the number of partitions so the amount of data in
    /// event-types/partitions is not considered during autorebalance.
    /// The position of the consumption is managed by Nakadi. The client is required
    /// to commit the cursors he gets in a stream.
    fn read(&self,
            subscription: &SubscriptionId)
            -> ClientResult<(Self::StreamingSource, StreamId)>;
}

/// Checkpoints cursors
pub trait Checkpoints {
    /// Checkpoint `Cursor`s.
    /// Make sure you use the same `StreamId` with which
    /// you retrieved the cursor.
    ///
    /// Endpoint for committing offsets of the subscription.
    /// If there is uncommited data, and no commits happen
    /// for 60 seconds, then Nakadi will consider the client to be gone,
    /// and will close the connection. As long
    /// as no events are sent, the client does not need to commit.
    /// If the connection is closed, the client has 60 seconds to commit the
    /// events it received, from the moment
    /// they were sent. After that, the connection will be considered closed,
    /// and it will not be possible to do commit with that `X-Nakadi-StreamId` anymore.
    /// When a batch is committed that also automatically commits all previous batches
    /// that were sent in a stream for this partition.
    fn checkpoint(&self,
                  stream_id: &StreamId,
                  subscription: &SubscriptionId,
                  cursors: &[Cursor])
                  -> ClientResult<()>;
}

/// Connects to `Nakadi` for checkpointing and consuming events.
pub trait NakadiConnector: ReadsStream + Checkpoints + Send + Sync + 'static {
    fn settings(&self) -> &ConnectorSettings;
}

/// Settings for establishing a connection to `Nakadi`.
#[derive(Builder)]
#[builder(pattern="owned")]
pub struct ConnectorSettings {
    /// Maximum number of empty keep alive batches to get in a row before closing the connection.
    /// If 0 or undefined will send keep alive messages indefinitely.
    #[builder(default="0")]
    pub stream_keep_alive_limit: usize,
    /// Maximum number of `Event`s in this stream (over all partitions being streamed in this
    /// connection).
    ///
    /// * If 0 or undefined, will stream batches indefinitely.
    /// * Stream initialization will fail if `stream_limit` is lower than `batch_limit`.
    #[builder(default="0")]
    pub stream_limit: usize,
    /// Maximum time in seconds a stream will live before connection is closed by the server.
    ///
    /// If 0 or unspecified will stream indefinitely.
    /// If this timeout is reached, any pending messages (in the sense of `stream_limit`) will be flushed
    /// to the client.
    /// Stream initialization will fail if `stream_timeout` is lower than `batch_flush_timeout`.
    #[builder(default="Duration::from_secs(0)")]
    pub stream_timeout: Duration,
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    ///
    ///  * If the amount of buffered Events reaches `batch_limit` before this `batch_flush_timeout`
    ///  is reached, the messages are immediately flushed to the client and batch flush timer is reset.
    ///  * If 0 or undefined, will assume 30 seconds.
    #[builder(default="Duration::from_secs(30)")]
    pub batch_flush_timeout: Duration,
    ///  Maximum number of `Event`s in each chunk (and therefore per partition) of the stream.
    ///
    ///  * If 0 or unspecified will buffer Events indefinitely and flush on reaching of
    ///  `batch_flush_timeout`.
    #[builder(default="50")]
    pub batch_limit: usize,
    pub nakadi_host: Url,
}

/// A `NakadiConnector` using `Hyper` for dispatching requests.
pub struct HyperClientConnector<T: ProvidesToken> {
    client: Client,
    token_provider: T,
    settings: ConnectorSettings,
    authenticate_when_checkpointing: bool,
}

impl<T: ProvidesToken> HyperClientConnector<T> {
    pub fn new(token_provider: T, nakadi_host: Url) -> HyperClientConnector<T> {
        let client = create_hyper_client();
        let settings = ConnectorSettingsBuilder::default()
            .nakadi_host(nakadi_host)
            .build()
            .unwrap();
        HyperClientConnector::with_client_and_settings(client, token_provider, settings)
    }

    pub fn with_client(client: Client,
                       token_provider: T,
                       nakadi_host: Url)
                       -> HyperClientConnector<T> {
        let settings = ConnectorSettingsBuilder::default()
            .nakadi_host(nakadi_host)
            .build()
            .unwrap();
        HyperClientConnector::with_client_and_settings(client, token_provider, settings)
    }

    pub fn with_settings(token_provider: T,
                         settings: ConnectorSettings)
                         -> HyperClientConnector<T> {
        let client = create_hyper_client();
        HyperClientConnector::with_client_and_settings(client, token_provider, settings)
    }

    pub fn with_client_and_settings(client: Client,
                                    token_provider: T,
                                    settings: ConnectorSettings)
                                    -> HyperClientConnector<T> {
        HyperClientConnector {
            client: client,
            token_provider: token_provider,
            settings: settings,
            authenticate_when_checkpointing: false,
        }
    }
}

impl<T: ProvidesToken> NakadiConnector for HyperClientConnector<T> {
    fn settings(&self) -> &ConnectorSettings {
        &self.settings
    }
}

impl<T: ProvidesToken> ReadsStream for HyperClientConnector<T> {
    type StreamingSource = ::hyper::client::response::Response;

    fn read(&self,
            subscription: &SubscriptionId)
            -> ClientResult<(Self::StreamingSource, StreamId)> {
        let settings = &self.settings;
        let url = format!("{}/subscriptions/{}/events?stream_keep_alive_limit={}&stream_limit={}&stream_timeout={}&batch_flush_timeout={}&batch_limit={}",
                          settings.nakadi_host,
                          subscription.0,
                          settings.stream_keep_alive_limit,
                          settings.stream_limit,
                          settings.stream_timeout.as_secs(),
                          settings.batch_flush_timeout.as_secs(),
                          settings.batch_limit);

        let mut headers = Headers::new();
        if let Some(token) = self.token_provider.get_token()? {
            headers.set(Authorization(Bearer { token: token.0 }));
        };

        let request = self.client.get(&url).headers(headers);


        match request.send() {
            Ok(rsp) => {
                match rsp.status {
                    StatusCode::Ok => {
                        let stream_id = if let Some(stream_id) = rsp.headers
                            .get::<XNakadiStreamId>()
                            .map(|v| StreamId(v.to_string())) {
                            stream_id
                        } else {
                            bail!(ClientErrorKind::InvalidResponse("The response lacked the \
                                                                    'X-Nakadi-StreamId' header."
                                .to_string()))
                        };
                        Ok((rsp, stream_id))
                    }
                    StatusCode::BadRequest => {
                        bail!(ClientErrorKind::Request(rsp.status.to_string()))
                    }
                    StatusCode::NotFound => {
                        bail!(ClientErrorKind::NoSubscription(rsp.status.to_string()))
                    }
                    StatusCode::Forbidden => {
                        bail!(ClientErrorKind::Forbidden(rsp.status.to_string()))
                    }
                    StatusCode::Conflict => {
                        bail!(ClientErrorKind::Conflict(rsp.status.to_string()))
                    }
                    other_status => bail!(other_status.to_string()),
                }
            }
            Err(err) => bail!(ClientErrorKind::Connection(err.to_string())),
        }
    }
}

impl<T: ProvidesToken> Checkpoints for HyperClientConnector<T> {
    fn checkpoint(&self,
                  stream_id: &StreamId,
                  subscription: &SubscriptionId,
                  cursors: &[Cursor])
                  -> ClientResult<()> {
        let payload: Vec<u8> = serde_json::to_vec(&CursorContainer { items: cursors }).unwrap();

        let url = format!("{}/subscriptions/{}/cursors",
                          self.settings.nakadi_host,
                          subscription.0);


        let mut headers = Headers::new();

        let token = if self.authenticate_when_checkpointing {
            self.token_provider.get_token()?
        } else {
            None
        };

        if let Some(token) = token {
            headers.set(Authorization(Bearer { token: token.0 }));
        };

        headers.set(XNakadiStreamId(stream_id.0.clone()));
        headers.set(ContentType::json());

        let request = self.client
            .post(&url)
            .headers(headers)
            .body(payload.as_slice());

        match request.send() {
            Ok(rsp) => {
                match rsp.status {
                    StatusCode::NoContent => Ok(()),
                    StatusCode::Ok => Ok(()),
                    StatusCode::BadRequest => {
                        bail!(ClientErrorKind::Request(rsp.status.to_string()))
                    }
                    StatusCode::NotFound => {
                        bail!(ClientErrorKind::NoSubscription(rsp.status.to_string()))
                    }
                    StatusCode::Forbidden => {
                        bail!(ClientErrorKind::Forbidden(rsp.status.to_string()))
                    }
                    StatusCode::UnprocessableEntity => {
                        bail!(ClientErrorKind::CursorUnprocessable(rsp.status.to_string()))
                    }
                    other_status => bail!(other_status.to_string()),
                }
            }
            Err(err) => bail!(ClientErrorKind::Connection(err.to_string())),
        }
    }
}

fn create_hyper_client() -> Client {
    let ssl = NativeTlsClient::new().unwrap();
    let connector = HttpsConnector::new(ssl);
    let mut client = Client::with_connector(connector);
    client.set_read_timeout(None);
    client.set_write_timeout(None);
    client
}

/// Nedeed to serialize cursors when checkpointing.
#[derive(Serialize)]
struct CursorContainer<'a> {
    items: &'a [Cursor],
}
