use std::io::Read;
use std::time::Duration;

use url::Url;
use hyper::Client;
use hyper::header::{Authorization, Bearer, ContentType, Headers};
use hyper::status::StatusCode;
use serde_json;

use super::*;
use ::ProvidesToken;

header! { (XNakadiStreamId, "X-Nakadi-StreamId") => [String] }

/// Connects to `Nakadi` for checkpointing and consuming events.
pub trait NakadiConnector : Send + Sync + 'static {
    type StreamingSource: Read;

    /// Attempts to get data from the stream. Also returns the `StreamId` which must be used
    /// for checkpointing.
    fn read(&self,
            subscription: &SubscriptionId)
            -> ClientResult<(Self::StreamingSource, StreamId)>;

    /// Checkpoint `Cursor`s. Make sure you use the same `StreamId` with which
    /// you retrieved the cursor. 
    fn checkpoint(&self,
                    stream_id: &StreamId,
                    subscription: &SubscriptionId,
                    cursors: &[Cursor])
                    -> ClientResult<()>;

    fn settings(&self) -> &ConnectorSettings;
}

/// Settings for establishing a connection to `Nakadi`.
#[derive(Builder)]
#[builder(pattern="owned")]
pub struct ConnectorSettings {
    #[builder(default="10")]
    pub stream_keep_alive_limit: usize,
    #[builder(default="5000")]
    pub stream_limit: usize,
    #[builder(default="Duration::from_secs(300)")]
    pub stream_timeout: Duration,
    #[builder(default="Duration::from_secs(15)")]
    pub batch_flush_timeout: Duration,
    #[builder(default="50")]
    pub batch_limit: usize,
    pub nakadi_host: Url,
}

/// A `NakadiConnector` using `Hyper` for dispatching requests.
pub struct HyperClientConnector<T: ProvidesToken> {
    client: Client,
    token_provider: T,
    settings: ConnectorSettings,
}

impl<T: ProvidesToken> HyperClientConnector<T> {
    pub fn new(token_provider: T,
                nakadi_host: Url)
                -> HyperClientConnector<T> {
                    let client = Client::new();
      let settings =
            ConnectorSettingsBuilder::default().nakadi_host(nakadi_host).build().unwrap();
        HyperClientConnector::with_client_and_settings(client,token_provider, settings)
      }

    pub fn with_client(client: Client,
               token_provider: T,
                                 nakadi_host: Url)
                                -> HyperClientConnector<T> {
        let settings =
            ConnectorSettingsBuilder::default().nakadi_host(nakadi_host).build().unwrap();
        HyperClientConnector::with_client_and_settings(client,token_provider, settings)
    }

        pub fn with_settings(
               token_provider: T,
               settings: ConnectorSettings)
                                -> HyperClientConnector<T> {
        let client = Client::new();
        HyperClientConnector::with_client_and_settings(client,token_provider, settings)
    }

       pub fn with_client_and_settings(
                client: Client,
                token_provider: T,
                settings: ConnectorSettings)
                -> HyperClientConnector<T> {
        HyperClientConnector {
            client: client,
            token_provider: token_provider,
            settings: settings,
        }
    }
}

impl<T: ProvidesToken> NakadiConnector for HyperClientConnector<T> {
    type StreamingSource = ::hyper::client::response::Response;

    fn read(&self,
            subscription: &SubscriptionId)
            -> ClientResult<(Self::StreamingSource, StreamId)> {
        let settings = &self.settings;
        let url =
            format!("{}/subscriptions/{}/events?stream_keep_alive_limit={}&stream_limit={}&stream_timeout={}&batch_flush_timeout={}&batch_limit={}",
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
                                                                    'X-Nakadi-StreamId' \
                                                                    header."
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

    fn checkpoint(&self,
                    stream_id: &StreamId,
                    subscription: &SubscriptionId,
                    cursors: &[Cursor])
                    -> ClientResult<()> {
        let payload: Vec<u8> = serde_json::to_vec(&CursorContainer{ items: cursors }).unwrap();

        let url = format!("{}/subscriptions/{}/cursors",
                            self.settings.nakadi_host,
                            subscription.0);


        let mut headers = Headers::new();
        if let Some(token) = self.token_provider.get_token()? {
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

    fn settings(&self) -> &ConnectorSettings {
        &self.settings
    }
}

#[derive(Serialize)]
struct CursorContainer<'a> {
    items: &'a [Cursor]
}
