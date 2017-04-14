use uuid::Uuid;

use EventType;

mod clienterrors;

pub use self::connector::HyperClientConnector;
pub use self::clienterrors::*;

#[derive(Clone, Debug)]
pub struct SubscriptionId(Uuid);

pub struct Cursor {
    pub partition: usize,
    pub offset: usize,
    pub event_type: EventType,
    pub cursor_token: Uuid,
}

mod connector {
    use std::io::Read;
    use std::time::Duration;
    use hyper::Client;

    use super::*;
    use ::client::clienterrors::*;

    pub trait ClientConnector {
        type StreamingSource: Read;

        fn read(&self) -> ClientResult<Self::StreamingSource>;
        fn checkpoint(&self, cursor: &Cursor) -> ClientResult<()>;

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
        pub subscription_id: SubscriptionId,
    }

    pub struct HyperClientConnector {
        client: Client,
        settings: ConnectorSettings,
    }

    impl HyperClientConnector {
        pub fn new(client: Client, settings: ConnectorSettings) -> HyperClientConnector {
            HyperClientConnector {
                client: client,
                settings: settings,
            }
        }

        pub fn new_with_defaults(client: Client, subscription_id: SubscriptionId) -> HyperClientConnector {
            let settings = ConnectorSettingsBuilder::default().subscription_id(subscription_id).build().unwrap();
            HyperClientConnector::new(client, settings)
        }
    }

    impl ClientConnector for HyperClientConnector {
        type StreamingSource = ::hyper::client::response::Response;

        fn read(&self) -> ClientResult<Self::StreamingSource> {
            
            unimplemented!()
        }

        fn checkpoint(&self, cursor: &Cursor) -> ClientResult<()> {
            unimplemented!()
        }

        fn settings(&self) -> &ConnectorSettings {
            &self.settings
        }
       
    }
}