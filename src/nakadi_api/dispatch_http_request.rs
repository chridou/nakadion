use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::Stream;
use http::{Request, Response, Result as HttpResult, StatusCode};
use http_api_problem::HttpApiProblem;
use serde::{de::DeserializeOwned, Serialize};
use url::Url;

use crate::event_stream::EventStream;
use crate::model::FlowId;

use super::StreamParameters;

pub struct ResponseFuture {
    inner: Box<dyn Future<Output = HttpResult<Bytes>> + Send + 'static>,
}

pub trait DispatchHttpRequest {
    fn dispatch(&self, req: Request<Vec<u8>>) -> ResponseFuture;
}

#[derive(Debug)]
pub struct RemoteCallError {
    pub(crate) message: Option<String>,
    pub(crate) status_code: Option<StatusCode>,
    pub(crate) cause: Option<Box<dyn Error + Send + 'static>>,
    pub(crate) problem: Option<HttpApiProblem>,
    kind: RemoteCallErrorKind,
}

impl RemoteCallError {
    pub(crate) fn new<M: Into<String>>(
        kind: RemoteCallErrorKind,
        message: M,
        status_code: Option<StatusCode>,
    ) -> Self {
        Self {
            message: Some(message.into()),
            status_code,
            kind,
            problem: None,
            cause: None,
        }
    }

    pub fn is_server(&self) -> bool {
        self.kind == RemoteCallErrorKind::Server
    }

    pub fn is_client(&self) -> bool {
        self.kind == RemoteCallErrorKind::Client
    }

    pub fn is_serialization(&self) -> bool {
        self.kind == RemoteCallErrorKind::Serialization
    }

    pub fn is_io(&self) -> bool {
        self.kind == RemoteCallErrorKind::Io
    }

    pub fn is_other(&self) -> bool {
        self.kind == RemoteCallErrorKind::Other
    }

    pub fn status_code(&self) -> Option<StatusCode> {
        self.status_code
    }

    pub fn problem(&self) -> Option<&HttpApiProblem> {
        self.problem.as_ref()
    }

    pub fn is_retry_suggested(&self) -> bool {
        match self.kind {
            RemoteCallErrorKind::Client => false,
            RemoteCallErrorKind::Server => true,
            RemoteCallErrorKind::Serialization => false,
            RemoteCallErrorKind::Io => true,
            RemoteCallErrorKind::Other => false,
        }
    }

    pub(crate) fn with_cause<E: Error + Send + 'static>(mut self, cause: E) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }
}

pub type RemoteCallResult<T> = Result<T, RemoteCallError>;

impl fmt::Display for RemoteCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RemoteCallErrorKind::*;

        match self.kind {
            Client => {
                write!(f, "client error")?;
            }
            Server => {
                write!(f, "server error")?;
            }
            Serialization => {
                write!(f, "serialization error")?;
            }
            Io => {
                write!(f, "io error")?;
            }
            Other => {
                write!(f, "other error")?;
            }
        }

        if let Some(status_code) = self.status_code {
            write!(f, " - status: {}", status_code)?;
        }

        if let Some(ref message) = self.message {
            write!(f, " - message: {}", message)?;
        } else if let Some(detail) = self.problem.as_ref().and_then(|p| p.detail.as_ref()) {
            write!(f, " - message: {}", detail)?;
        }

        Ok(())
    }
}

impl Error for RemoteCallError {
    fn cause(&self) -> Option<&Error> {
        self.cause.as_ref().map(|e| &**e as &Error)
    }
}

impl From<RemoteCallErrorKind> for RemoteCallError {
    fn from(kind: RemoteCallErrorKind) -> Self {
        Self {
            message: None,
            status_code: None,
            cause: None,
            problem: None,
            kind,
        }
    }
}

impl From<serde_json::Error> for RemoteCallError {
    fn from(err: serde_json::Error) -> Self {
        Self {
            message: Some("de/-serialization error".to_string()),
            status_code: None,
            cause: Some(Box::new(err)),
            problem: None,
            kind: RemoteCallErrorKind::Serialization,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteCallErrorKind {
    Client,
    Server,
    Serialization,
    Io,
    Other,
}
