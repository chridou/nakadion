use std::sync::Arc;
use std::time::Duration;
use std::io::Read;

use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

use reqwest::{Client as HttpClient, Response};
use reqwest::StatusCode;
use reqwest::header::{Authorization, Bearer, Headers};
use backoff::{Error as BackoffError, ExponentialBackoff, Operation};
use failure::*;

use auth::{AccessToken, ProvidesAccessToken};

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

    pub fn publish(&self, event_type: &str, bytes: Vec<u8>) -> Result<PublishStatus, PublishError> {
        let url = format!("{}/event-types/{}/events", self.nakadi_base_url, event_type);

        let mut op = || match publish_events(
            &self.http_client,
            &url,
            &*self.token_provider,
            bytes.clone(),
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
}

fn publish_events(
    client: &HttpClient,
    url: &str,
    token_provider: &ProvidesAccessToken,
    bytes: Vec<u8>,
) -> Result<PublishStatus, PublishError> {
    let mut request_builder = client.post(url);

    match token_provider.get_token() {
        Ok(Some(AccessToken(token))) => {
            request_builder.header(Authorization(Bearer { token }));
        }
        Ok(None) => (),
        Err(err) => return Err(PublishError::Other(err.to_string())),
    };

    match request_builder.body(bytes).send() {
        Ok(ref mut response) => match response.status() {
            StatusCode::Ok => Ok(PublishStatus::AllEventsPublished),
            StatusCode::MultiStatus => Ok(PublishStatus::NotAllEventsPublished),
            StatusCode::Unauthorized => {
                let msg = read_response_body(response);
                Err(PublishError::Unauthorized(msg))
            }
            StatusCode::Forbidden => {
                let msg = read_response_body(response);
                Err(PublishError::Forbidden(msg))
            }
            StatusCode::UnprocessableEntity => {
                let msg = read_response_body(response);
                Err(PublishError::UnprocessableEntity(msg))
            }
            _ => {
                let msg = read_response_body(response);
                Err(PublishError::Other(msg))
            }
        },
        Err(err) => Err(PublishError::Other(format!("{}", err))),
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
    #[fail(display = "Unauthorized: {}", _0)] Unauthorized(String),
    /// Already exists
    #[fail(display = "Forbidden: {}", _0)]
    Forbidden(String),
    #[fail(display = "Unprocessable Entity: {}", _0)] UnprocessableEntity(String),
    #[fail(display = "An error occured: {}", _0)] Other(String),
}

impl PublishError {
    pub fn is_retry_suggested(&self) -> bool {
        match *self {
            PublishError::Unauthorized(_) => true,
            PublishError::Forbidden(_) => false,
            PublishError::UnprocessableEntity(_) => false,
            PublishError::Other(_) => true,
        }
    }
}
