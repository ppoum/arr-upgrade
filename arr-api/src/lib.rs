use std::borrow::Cow;

use reqwest::RequestBuilder;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

pub mod radarr;

pub(crate) trait RequestPayload: Clone {
    fn add_to_request(&self, request_builder: RequestBuilder) -> RequestBuilder;
}

#[derive(Clone)]
pub(crate) struct Json<T: Serialize + Clone>(pub T);

impl<T: Serialize + Clone> RequestPayload for Json<T> {
    #[inline]
    fn add_to_request(&self, request_builder: RequestBuilder) -> RequestBuilder {
        request_builder.json(&self.0)
    }
}

impl<T: Serialize + Clone> From<T> for Json<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl RequestPayload for () {
    #[inline]
    fn add_to_request(&self, request_builder: RequestBuilder) -> RequestBuilder {
        // Noop
        request_builder
    }
}

pub(crate) trait RequestPayloadExt {
    fn arr_params(self, payload: &impl RequestPayload) -> Self;
}

impl RequestPayloadExt for RequestBuilder {
    fn arr_params(self, payload: &impl RequestPayload) -> Self {
        payload.add_to_request(self)
    }
}

pub(crate) trait ArrRequest {
    type Params: RequestPayload;
    type Response: DeserializeOwned;

    const METHOD: reqwest::Method;

    fn to_url(&self) -> Cow<'_, str>;
    fn params(&self) -> Cow<'_, Self::Params>;
}

pub(crate) trait Client {
    async fn send<R: ArrRequest>(&self, request: R) -> Result<R::Response, ArrError>;
}

#[derive(Debug, Error)]
pub enum ArrError {
    #[error("request is unauthorized")]
    Unauthorized,
    #[error("unsupported API version: {0}")]
    UnsupportedApiVersion(String),
    #[error("timed out")]
    Timeout,
    #[error("error sending request: {0}")]
    Http(#[from] reqwest::Error),
}
