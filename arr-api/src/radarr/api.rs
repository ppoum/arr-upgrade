use serde::Deserialize;

use crate::ArrRequest;

pub struct ApiInfo;
#[derive(Debug, Deserialize)]
pub struct ApiInfoResponse {
    pub current: String,
    pub deprecated: Vec<String>,
}
impl ArrRequest for ApiInfo {
    type Params = ();

    type Response = ApiInfoResponse;

    const METHOD: reqwest::Method = reqwest::Method::GET;

    fn to_url(&self) -> std::borrow::Cow<'_, str> {
        "/api".into()
    }

    fn params(&self) -> &Self::Params {
        &()
    }
}

pub struct AllMovies;
#[derive(Debug, Deserialize)]
pub struct Movie {
    pub id: u32,
    pub title: String,
    pub monitored: bool,
}
impl ArrRequest for AllMovies {
    type Params = ();

    type Response = Vec<Movie>;

    const METHOD: reqwest::Method = reqwest::Method::GET;

    fn to_url(&self) -> std::borrow::Cow<'_, str> {
        "/api/v3/movie".into()
    }

    fn params(&self) -> &Self::Params {
        &()
    }
}
