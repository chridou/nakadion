use std::sync::Arc;

use uuid::Uuid;
use serde_json::Value;

use super::EventType;

mod clienterrors;
mod connector;
mod worker;

pub use self::connector::{ClientConnector, HyperClientConnector, ConnectorSettings, ConnectorSettingsBuilder};
pub use self::clienterrors::*;
pub use self::worker::NakadiWorker;

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

/// Describes what to do after a batch has been processed.
///
/// Use to control what should happen next.
#[derive(Debug)]
pub enum AfterBatchAction {
    /// Checkpoint and get next
    Continue,
    /// Checkpoint then stop.
    Stop,
    /// Stop without checkpointing
    Abort
}

pub trait Handler: Send + Sync + 'static {
    fn handle(&self, batch: Vec<Value>) -> AfterBatchAction;
}

pub struct NakadiClient<C: ClientConnector> {
    worker: NakadiWorker,
    connector: Arc<C>,
}

impl<C: ClientConnector> NakadiClient<C> {
    pub fn new<H: Handler>(subscription_id: SubscriptionId, connector: Arc<C>, handler: H) -> Self {
        let worker = NakadiWorker::new(connector.clone(), handler, subscription_id);
        NakadiClient {
            worker: worker,
            connector: connector,
        }
    }

    pub fn is_running(&self) -> bool {
        self.worker.is_running()
    }

    pub fn stop(&self) {
        self.worker.stop();
    }

    pub fn connector(&self) -> &C {
        &self.connector
    }

    pub fn subscription_id(&self) -> &SubscriptionId {
        self.worker.subscription_id()
    }
}


