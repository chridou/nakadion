use std::error::Error as StdError;
use std::fmt;

use super::IoError;
use bytes::Bytes;
use futures::future::BoxFuture;
use http::{Request, Response};

#[cfg(feature = "reqwest")]
pub use reqwest_dispatch_http_request::ReqwestDispatchHttpRequest;

use super::BytesStream;

pub type ResponseFuture<'a> = BoxFuture<'a, Result<Response<BytesStream>, RemoteCallError>>;

/// A common trait for dispatching Http requests.
///
/// This trait is used to enable pluggable
/// HTTP clients
pub trait DispatchHttpRequest {
    fn dispatch<'a>(&'a self, req: Request<Bytes>) -> ResponseFuture<'a>;
}

/// An error with can be caused by a remote call.
///
/// This is a low level error.
#[derive(Debug)]
pub struct RemoteCallError {
    message: Option<String>,
    cause: Option<Box<dyn StdError + Send + 'static>>,
    detail: RemoteCallErrorDetail,
}

impl RemoteCallError {
    pub fn new_io() -> Self {
        Self {
            message: None,
            cause: None,
            detail: RemoteCallErrorDetail::Io,
        }
    }

    pub fn new_other() -> Self {
        Self {
            message: None,
            cause: None,
            detail: RemoteCallErrorDetail::Other,
        }
    }

    pub fn with_message<M: Into<String>>(mut self, message: M) -> Self {
        self.message = Some(message.into());
        self
    }

    pub fn with_cause<E: StdError + Send + 'static>(mut self, cause: E) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    pub fn is_io(&self) -> bool {
        self.detail.is_io()
    }

    pub fn is_other(&self) -> bool {
        self.detail.is_other()
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_ref().map(|m| &**m)
    }
}

pub type RemoteCallResult<T> = Result<T, RemoteCallError>;

impl fmt::Display for RemoteCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.detail {
            RemoteCallErrorDetail::Io => {
                write!(f, "io error")?;
            }
            RemoteCallErrorDetail::Other => {
                write!(f, "other error")?;
            }
        }

        if let Some(ref message) = self.message {
            write!(f, " - message: {}", message)?;
        }

        Ok(())
    }
}

impl StdError for RemoteCallError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.cause.as_ref().map(|e| &**e as &dyn StdError)
    }
}

impl From<IoError> for RemoteCallError {
    fn from(err: IoError) -> Self {
        Self::new_io().with_message(err.0)
    }
}

impl From<http::header::InvalidHeaderValue> for RemoteCallError {
    fn from(err: http::header::InvalidHeaderValue) -> Self {
        RemoteCallError::new_other()
            .with_message("invalid header value")
            .with_cause(err)
    }
}

impl From<http::uri::InvalidUri> for RemoteCallError {
    fn from(err: http::uri::InvalidUri) -> Self {
        RemoteCallError::new_other()
            .with_message("invalid URI")
            .with_cause(err)
    }
}

impl From<tokio::time::Elapsed> for RemoteCallError {
    fn from(err: tokio::time::Elapsed) -> Self {
        RemoteCallError::new_io()
            .with_message("a timeout occurred")
            .with_cause(err)
    }
}

#[derive(Debug)]
enum RemoteCallErrorDetail {
    Other,
    Io,
}

impl RemoteCallErrorDetail {
    pub fn is_io(&self) -> bool {
        match self {
            RemoteCallErrorDetail::Io => true,
            _ => false,
        }
    }

    pub fn is_other(&self) -> bool {
        match self {
            RemoteCallErrorDetail::Other => true,
            _ => false,
        }
    }
}

#[cfg(feature = "reqwest")]
mod reqwest_dispatch_http_request {
    use futures::{stream::TryStreamExt, FutureExt, StreamExt};
    use http::{Request, Response};
    use reqwest::{Client, Request as RRequest};

    use super::*;

    #[derive(Clone)]
    pub struct ReqwestDispatchHttpRequest {
        client: Client,
    }

    impl DispatchHttpRequest for ReqwestDispatchHttpRequest {
        fn dispatch(&self, req: Request<Bytes>) -> ResponseFuture {
            async move {
                let (parts, body) = req.into_parts();

                let url = parts.uri.to_string().parse().map_err(|err| {
                    RemoteCallError::new_other()
                        .with_message("invalid url")
                        .with_cause(err)
                })?;

                let mut request = RRequest::new(parts.method, url);

                for (k, v) in parts.headers {
                    if let Some(k) = k {
                        request.headers_mut().append(k, v);
                    }
                }

                *request.body_mut() = Some(body.into());

                let reqwest_response = self.client.execute(request).await?;

                let status = reqwest_response.status();
                let headers = reqwest_response.headers().clone();
                let version = reqwest_response.version();

                let bytes_stream = reqwest_response
                    .bytes_stream()
                    .map_err(|err| IoError(err.to_string()))
                    .boxed();
                let mut response = Response::new(bytes_stream);

                *response.status_mut() = status;
                *response.headers_mut() = headers;
                *response.version_mut() = version;

                Ok(response)
            }
            .boxed()
        }
    }

    impl Default for ReqwestDispatchHttpRequest {
        fn default() -> Self {
            let client = Client::new();
            Self { client }
        }
    }

    impl From<reqwest::Error> for RemoteCallError {
        fn from(err: reqwest::Error) -> Self {
            if err.is_timeout() {
                return RemoteCallError::new_io()
                    .with_message("Request timeout")
                    .with_cause(err);
            }

            RemoteCallError::new_other()
                .with_message(err.to_string())
                .with_cause(err)
        }
    }
}
