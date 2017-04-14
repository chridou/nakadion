use uuid::Uuid;

use EventType;

mod clienterrors;

pub use self::connector::HyperClientConnector;
pub use self::clienterrors::*;


#[derive(Clone, Debug)]
pub struct SubscriptionId(Uuid);

pub struct StreamId(String);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cursor {
    pub partition: usize,
    pub offset: String,
    pub event_type: EventType,
    pub cursor_token: Uuid,
}

mod connector {
    use std::io::Read;
    use std::time::Duration;

    use url::Url;
    use hyper::Client;
    use hyper::header::{Authorization,  Bearer, ContentType};
    use hyper::status::StatusCode;
    use serde_json;

    use super::*;
    use ::{ ProvidesToken};
 
    header! { (XNakadiStreamId, "X-Nakadi-StreamId") => [String] }
 
    pub trait ClientConnector {
        type StreamingSource: Read;

        fn read(&self, subscription: &SubscriptionId) -> ClientResult<(Self::StreamingSource, StreamId)>;
        fn checkpoint(&self, stream_id: &StreamId, subscription: &SubscriptionId, cursor: &Cursor) -> ClientResult<()>;

        fn settings(&self) -> &ConnectorSettings;
    }

    #[derive(Builder)]
    #[builder(pattern="owned")]
    pub struct ConnectorSettings {
        #[builder(default="10")]
        /// 
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

    pub struct HyperClientConnector<T: ProvidesToken> {
        client: Client,
        token_provider: T,
        settings: ConnectorSettings,
    }

    impl<T: ProvidesToken> HyperClientConnector<T> {
        pub fn new(client: Client, token_provider: T, settings: ConnectorSettings) -> HyperClientConnector<T> {
            HyperClientConnector {
                client: client,
                token_provider: token_provider,
                settings: settings,

            }
        }

        pub fn new_with_defaults(client: Client, nakadi_host: Url, token_provider: T) -> HyperClientConnector<T> {
            let settings = ConnectorSettingsBuilder::default().nakadi_host(nakadi_host).build().unwrap();
            HyperClientConnector::new(client, token_provider, settings)
        }
    }

    impl<T: ProvidesToken> ClientConnector for HyperClientConnector<T> {
        type StreamingSource = ::hyper::client::response::Response;

        fn read(&self, subscription: &SubscriptionId) -> ClientResult<(Self::StreamingSource, StreamId)> {
            let token = self.token_provider.get_token()?;
            let settings = &self.settings;
            let url = format!(
                "{}/subscriptions/{}/events?stream_keep_alive_limit={}&stream_limit={}&stream_timeout={}&batch_flush_timeout={}&batch_limit={}", 
                settings.nakadi_host,
                subscription.0,
                settings.stream_keep_alive_limit,
                settings.stream_limit,
                settings.stream_timeout.as_secs(),
                settings.batch_flush_timeout.as_secs(),
                settings.batch_limit);

            let request = self.client.get(&url).header(Authorization(Bearer { token: token.0}));

            match request.send() {
                Ok(rsp) => match rsp.status {
                    StatusCode::Ok => {
                        let stream_id = if let Some(stream_id) = rsp.headers.get::<XNakadiStreamId>().map(|v| StreamId(v.to_string())) {
                            stream_id
                        } else {
                            bail!(ClientErrorKind::InvalidResponse("The response lacked the 'X-Nakadi-StreamId' header.".to_string()))
                        };
                        Ok((rsp, stream_id))
                    },
                    StatusCode::BadRequest =>  bail!(ClientErrorKind::Request(rsp.status.to_string())),
                    StatusCode::NotFound =>  bail!(ClientErrorKind::NoSubscription(rsp.status.to_string())),
                    StatusCode::Forbidden =>  bail!(ClientErrorKind::Forbidden(rsp.status.to_string())),
                    StatusCode::Conflict =>  bail!(ClientErrorKind::Conflict(rsp.status.to_string())),
                    other_status => bail!(other_status.to_string())
                },
                Err(err) => bail!(ClientErrorKind::Connection(err.to_string()))
           }
        }

        fn checkpoint(&self, stream_id: &StreamId, subscription: &SubscriptionId, cursor: &Cursor) -> ClientResult<()> {
            let token = self.token_provider.get_token()?;
            let payload: Vec<u8> = serde_json::to_vec(cursor).unwrap();

            let url = format!(
                "{}/subscriptions/{}/cursors", 
                self.settings.nakadi_host,
                subscription.0);

            let request = self.client.post(&url)
                .header(Authorization(Bearer { token: token.0}))
                .header(XNakadiStreamId(stream_id.0.clone()))
                .header(ContentType::json())
                .body(payload.as_slice());

            match request.send() {
                Ok(rsp) => match rsp.status {
                    StatusCode::NoContent => Ok(()),
                    StatusCode::Ok => Ok(()),
                    StatusCode::BadRequest =>  bail!(ClientErrorKind::Request(rsp.status.to_string())),
                    StatusCode::NotFound =>  bail!(ClientErrorKind::NoSubscription(rsp.status.to_string())),
                    StatusCode::Forbidden =>  bail!(ClientErrorKind::Forbidden(rsp.status.to_string())),
                    StatusCode::UnprocessableEntity =>  bail!(ClientErrorKind::CursorUnprocessable(rsp.status.to_string())),
                    other_status => bail!(other_status.to_string())
                 },
                Err(err) => bail!(ClientErrorKind::Connection(err.to_string()))
            }
        }

        fn settings(&self) -> &ConnectorSettings {
            &self.settings
        }
    }
}