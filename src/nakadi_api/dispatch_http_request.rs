use std::error::Error;
use std::fmt;

use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream, FutureExt, TryFutureExt, TryStreamExt};
use http::{Request, StatusCode};
use http_api_problem::HttpApiProblem;
use serde::de::DeserializeOwned;
use serde_json;

pub type BytesStream<'a> = BoxStream<'a, Result<Bytes, IoError>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorExpectation {
    /// Expect Nakadi to return a problem JSON
    Problem,
    /// Expect Nakadi to return a body that needs further evaluation
    ContextualBody,
}

pub type ResponseFuture<'a> = BoxFuture<'a, Result<(StatusCode, BytesStream<'a>), RemoteCallError>>;
pub type TypedResponseFuture<'a, T> = BoxFuture<'a, Result<(StatusCode, T), RemoteCallError>>;

pub trait DispatchHttpRequest {
    fn dispatch(&self, req: Request<Bytes>, error_expectation: ErrorExpectation) -> ResponseFuture;

    fn dispatch_for_entity<T: DeserializeOwned>(
        &self,
        req: Request<Bytes>,
        error_expectation: ErrorExpectation,
    ) -> TypedResponseFuture<T>
    where
        Self: Sync,
    {
        async move {
            let (status, mut bytes_stream) = self.dispatch(req, error_expectation).await?;

            let mut all_bytes = Vec::new();

            while let Some(next_bytes) = bytes_stream.try_next().await? {
                all_bytes.extend(next_bytes);
            }

            let deserialized = serde_json::from_slice(&all_bytes[..])?;
            Ok((status, deserialized))
        }
        .boxed()
    }
}

#[derive(Debug)]
pub struct RemoteCallError {
    message: Option<String>,
    cause: Option<Box<dyn Error + Send + 'static>>,
    detail: RemoteCallErrorDetail,
}

pub struct IoError(pub String);

impl RemoteCallError {
    pub fn new_problem<T: Into<HttpApiProblem>>(problem: T) -> Self {
        Self {
            message: None,
            cause: None,
            detail: RemoteCallErrorDetail::ResponseProblem(problem.into()),
        }
    }

    pub fn new_http<S: Into<StatusCode>, B: Into<Bytes>>(status: S, body: B) -> Self {
        Self {
            message: None,
            cause: None,
            detail: RemoteCallErrorDetail::ResponseBody(status.into(), body.into()),
        }
    }

    pub fn new_io() -> Self {
        Self {
            message: None,
            cause: None,
            detail: RemoteCallErrorDetail::Io,
        }
    }

    pub fn new_serialization() -> Self {
        Self {
            message: None,
            cause: None,
            detail: RemoteCallErrorDetail::Serialization,
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

    pub fn with_cause<E: Error + Send + 'static>(mut self, cause: E) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    pub fn is_server(&self) -> bool {
        self.detail.is_server()
    }

    pub fn is_client(&self) -> bool {
        self.detail.is_client()
    }

    pub fn is_serialization(&self) -> bool {
        self.detail.is_serialization()
    }

    pub fn is_io(&self) -> bool {
        self.detail.is_io()
    }

    pub fn is_other(&self) -> bool {
        self.detail.is_other()
    }

    pub fn status_code(&self) -> Option<StatusCode> {
        self.detail.status_code()
    }

    pub fn problem(&self) -> Option<&HttpApiProblem> {
        self.detail.problem()
    }

    pub fn response_body(&self) -> Option<&[u8]> {
        self.detail.response_body()
    }

    pub fn is_retry_suggested(&self) -> bool {
        self.detail.is_retry_suggested()
    }
}

pub type RemoteCallResult<T> = Result<T, RemoteCallError>;

impl fmt::Display for RemoteCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.detail {
            RemoteCallErrorDetail::ResponseProblem(_) => {}
            RemoteCallErrorDetail::ResponseBody(_, _) => {}
            RemoteCallErrorDetail::Serialization => {
                write!(f, "serialization error")?;
            }
            RemoteCallErrorDetail::Io => {
                write!(f, "io error")?;
            }
            RemoteCallErrorDetail::Other => {
                write!(f, "other error")?;
            }
        }

        if let Some(status_code) = self.status_code() {
            write!(f, " - status: {}", status_code)?;
        }

        if let Some(ref message) = self.message {
            write!(f, " - message: {}", message)?;
        } else if let Some(detail) = self.problem().and_then(|p| p.detail.as_ref()) {
            write!(f, " - message: {}", detail)?;
        }

        Ok(())
    }
}

impl Error for RemoteCallError {
    fn cause(&self) -> Option<&dyn Error> {
        self.cause.as_ref().map(|e| &**e as &dyn Error)
    }
}

impl From<serde_json::Error> for RemoteCallError {
    fn from(err: serde_json::Error) -> Self {
        Self::new_serialization().with_cause(err)
    }
}

impl From<HttpApiProblem> for RemoteCallError {
    fn from(problem: HttpApiProblem) -> Self {
        Self::new_problem(problem)
    }
}

impl From<IoError> for RemoteCallError {
    fn from(err: IoError) -> Self {
        Self::new_io().with_message(err.0)
    }
}

#[derive(Debug)]
enum RemoteCallErrorDetail {
    ResponseProblem(HttpApiProblem),
    ResponseBody(StatusCode, Bytes),
    Other,
    Serialization,
    Io,
}

impl RemoteCallErrorDetail {
    pub fn is_server(&self) -> bool {
        match self {
            RemoteCallErrorDetail::ResponseProblem(ref problem) => {
                problem.status.map(|s| s.is_server_error()).unwrap_or(true)
            }
            RemoteCallErrorDetail::ResponseBody(status, _) => status.is_server_error(),
            _ => false,
        }
    }

    pub fn is_client(&self) -> bool {
        match self {
            RemoteCallErrorDetail::ResponseProblem(ref problem) => {
                problem.status.map(|s| s.is_client_error()).unwrap_or(false)
            }
            RemoteCallErrorDetail::ResponseBody(status, _) => status.is_client_error(),
            _ => false,
        }
    }

    pub fn is_serialization(&self) -> bool {
        match self {
            RemoteCallErrorDetail::Serialization => true,
            _ => false,
        }
    }

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

    pub fn status_code(&self) -> Option<StatusCode> {
        match self {
            RemoteCallErrorDetail::ResponseProblem(ref problem) => problem.status,
            RemoteCallErrorDetail::ResponseBody(status, _) => Some(*status),
            _ => None,
        }
    }

    pub fn problem(&self) -> Option<&HttpApiProblem> {
        match self {
            RemoteCallErrorDetail::ResponseProblem(ref problem) => Some(problem),
            _ => None,
        }
    }

    pub fn response_body(&self) -> Option<&[u8]> {
        match self {
            RemoteCallErrorDetail::ResponseBody(_, ref body) => Some(&body),
            _ => None,
        }
    }

    pub fn is_retry_suggested(&self) -> bool {
        match self {
            RemoteCallErrorDetail::ResponseProblem(ref problem) => {
                problem.status.map(|s| s.is_server_error()).unwrap_or(true)
            }
            RemoteCallErrorDetail::ResponseBody(status, _) => status.is_server_error(),
            RemoteCallErrorDetail::Other => false,
            RemoteCallErrorDetail::Serialization => false,
            RemoteCallErrorDetail::Io => true,
        }
    }
}

mod reqwest_dispatch_http_request {
    use futures::{stream::TryStreamExt, FutureExt, StreamExt};
    use reqwest::{Client, Request as RRequest, Response};

    use super::*;

    #[derive(Clone)]
    pub struct ReqwestDispatchHttpRequest {
        client: Client,
    }

    impl DispatchHttpRequest for ReqwestDispatchHttpRequest {
        fn dispatch(
            &self,
            req: Request<Bytes>,
            error_expectation: ErrorExpectation,
        ) -> ResponseFuture {
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

                let response = self.client.execute(request).await?;
                evaluate_response(response, error_expectation).await
            }
            .boxed()
        }
    }

    async fn evaluate_response(
        response: Response,
        error_expectation: ErrorExpectation,
    ) -> Result<(StatusCode, BytesStream<'static>), RemoteCallError> {
        let status = response.status();
        if status.is_success() {
            let body_stream = response
                .bytes_stream()
                .map_err(|err| IoError(err.to_string()));
            return Ok((status, body_stream.boxed()));
        }

        match error_expectation {
            ErrorExpectation::ContextualBody => {
                let bytes = response.bytes().await?;
                Err(RemoteCallError::new_http(status, bytes))
            }
            ErrorExpectation::Problem => {
                let bytes = response.bytes().await?;
                let problem: HttpApiProblem =
                    serde_json::from_slice(&bytes.to_vec()).map_err(|err| {
                        RemoteCallError::new_http(status, bytes)
                            .with_message("Expected an HTTP problem but failed to parse it")
                            .with_cause(err)
                    })?;
                Err(RemoteCallError::new_problem(problem))
            }
        }
    }

    impl From<reqwest::Error> for RemoteCallError {
        fn from(err: reqwest::Error) -> Self {
            if let Some(status) = err.status() {
                return RemoteCallError::new_problem(status)
                    .with_message("Problem generated by Nakadion")
                    .with_cause(err);
            }

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
