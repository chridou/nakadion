use std::sync::Arc;
use std::time::Duration;
use std::io::Read;

use serde::Serialize;
use serde_json;

use reqwest::{Client as HttpClient, Response};
use reqwest::StatusCode;
use reqwest::header::{Authorization, Bearer};
use backoff::{Error as BackoffError, ExponentialBackoff, Operation};

use auth::{AccessToken, ProvidesAccessToken};
use nakadi::model::FlowId;

header! { (XFlowId, "X-Flow-Id") => [String] }

pub struct NakadiPublisher {
    nakadi_base_url: String,
    http_client: HttpClient,
    token_provider: Arc<ProvidesAccessToken>,
}

impl NakadiPublisher {
    pub fn new<U: Into<String>, T: ProvidesAccessToken + 'static>(
        nakadi_base_url: U,
        token_provider: T,
    ) -> NakadiPublisher {
        NakadiPublisher {
            nakadi_base_url: nakadi_base_url.into(),
            http_client: HttpClient::new(),
            token_provider: Arc::new(token_provider),
        }
    }

    pub fn with_shared_access_token_provider<U: Into<String>>(
        nakadi_base_url: U,
        token_provider: Arc<ProvidesAccessToken>,
    ) -> NakadiPublisher {
        NakadiPublisher {
            nakadi_base_url: nakadi_base_url.into(),
            http_client: HttpClient::new(),
            token_provider: token_provider,
        }
    }

    pub fn publish_raw(
        &self,
        event_type: &str,
        bytes: Vec<u8>,
        flow_id: Option<FlowId>,
    ) -> Result<PublishStatus, PublishError> {
        let url = format!("{}/event-types/{}/events", self.nakadi_base_url, event_type);

        let flow_id = flow_id.unwrap_or_else(|| FlowId::default());

        let mut op = || match publish_events(
            &self.http_client,
            &url,
            &*self.token_provider,
            bytes.clone(),
            &flow_id,
        ) {
            Ok(publish_status) => Ok(publish_status),
            Err(err) => {
                if err.is_retry_suggested() {
                    Err(BackoffError::Transient(err))
                } else {
                    Err(BackoffError::Permanent(err))
                }
            }
        };

        let notify = |err, dur| {
            warn!("Publish error happened {:?}: {}", dur, err);
        };

        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(Duration::from_secs(5));
        backoff.initial_interval = Duration::from_millis(50);
        backoff.multiplier = 1.5;

        match op.retry_notify(&mut backoff, notify) {
            Ok(publish_status) => Ok(publish_status),
            Err(BackoffError::Transient(err)) => Err(err),
            Err(BackoffError::Permanent(err)) => Err(err),
        }
    }

    pub fn publish_events<T: Serialize>(
        &self,
        event_type: &str,
        events: &[T],
        flow_id: Option<FlowId>,
    ) -> Result<PublishStatus, PublishError> {
        let bytes = match serde_json::to_vec(events) {
            Ok(bytes) => bytes,
            Err(err) => return Err(PublishError::Serialization(err.to_string())),
        };
        self.publish_raw(event_type, bytes, flow_id)
    }
}

fn publish_events(
    client: &HttpClient,
    url: &str,
    token_provider: &ProvidesAccessToken,
    bytes: Vec<u8>,
    flow_id: &FlowId,
) -> Result<PublishStatus, PublishError> {
    let mut request_builder = client.post(url);

    match token_provider.get_token() {
        Ok(Some(AccessToken(token))) => {
            request_builder.header(Authorization(Bearer { token }));
        }
        Ok(None) => (),
        Err(err) => return Err(PublishError::Token(err.to_string())),
    };

    request_builder.header(XFlowId(flow_id.0.clone()));

    match request_builder.body(bytes).send() {
        Ok(ref mut response) => match response.status() {
            StatusCode::Ok => Ok(PublishStatus::AllEventsPublished),
            StatusCode::MultiStatus => Ok(PublishStatus::NotAllEventsPublished),
            StatusCode::Unauthorized => {
                let msg = read_response_body(response);
                Err(PublishError::Unauthorized(msg, flow_id.clone()))
            }
            StatusCode::Forbidden => {
                let msg = read_response_body(response);
                Err(PublishError::Forbidden(msg, flow_id.clone()))
            }
            StatusCode::UnprocessableEntity => {
                let msg = read_response_body(response);
                Err(PublishError::UnprocessableEntity(msg, flow_id.clone()))
            }
            _ => {
                let msg = read_response_body(response);
                Err(PublishError::Other(msg, flow_id.clone()))
            }
        },
        Err(err) => Err(PublishError::Other(format!("{}", err), flow_id.clone())),
    }
}

fn read_response_body(response: &mut Response) -> String {
    let mut buf = String::new();
    response
        .read_to_string(&mut buf)
        .map(|_| buf)
        .unwrap_or("<Could not read body.>".to_string())
}

#[derive(Debug)]
pub enum PublishStatus {
    AllEventsPublished,
    NotAllEventsPublished,
}

#[derive(Fail, Debug)]
pub enum PublishError {
    #[fail(display = "Unauthorized(FlowId: {}): {}", _1, _0)]
    Unauthorized(String, FlowId),
    /// Already exists
    #[fail(display = "Forbidden(FlowId: {}): {}", _1, _0)]
    Forbidden(String, FlowId),
    #[fail(display = "Unprocessable Entity(FlowId: {}): {}", _1, _0)]
    UnprocessableEntity(String, FlowId),
    #[fail(display = "Could not serialize events: {}", _0)]
    Serialization(String),
    #[fail(display = "An error occured: {}", _0)]
    Token(String),
    #[fail(display = "An error occured(FlowId: {}): {}", _1, _0)]
    Other(String, FlowId),
}

impl PublishError {
    pub fn is_retry_suggested(&self) -> bool {
        match *self {
            PublishError::Unauthorized(_, _) => true,
            PublishError::Forbidden(_, _) => false,
            PublishError::UnprocessableEntity(_, _) => false,
            PublishError::Serialization(_) => false,
            PublishError::Token(_) => true,
            PublishError::Other(_, _) => true,
        }
    }
}
