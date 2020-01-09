use std::convert::TryInto;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::Stream;
use http::{Error as HttpError, StatusCode, Uri};
use http_api_problem::HttpApiProblem;
use serde::{de::DeserializeOwned, Serialize};
use url::Url;

use super::dispatch_http_request::DispatchHttpRequest;

#[derive(Clone)]
pub struct ApiClient<D> {
    base_uri: Arc<Uri>,
    dispatch_http_request: D,
}

impl<D> ApiClient<D>
where
    D: DispatchHttpRequest,
{
    pub fn with_dispatcher<U>(
        base_uri: U,
        dispatch_http_request: D,
    ) -> Result<Self, Box<dyn Error + 'static>>
    where
        U: TryInto<Uri>,
        U::Error: Error + 'static,
    {
        Ok(Self {
            dispatch_http_request,
            base_uri: Arc::new(base_uri.try_into().map_err(Box::new)?),
        })
    }
}
