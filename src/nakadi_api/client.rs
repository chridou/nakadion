use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::Stream;
use http::StatusCode;
use http_api_problem::HttpApiProblem;
use serde::{de::DeserializeOwned, Serialize};
use url::Url;

#[derive(Clone)]
pub struct ApiClient<C> {
    base_url: Arc<Url>,
    dispatch_http_request: C,
}
