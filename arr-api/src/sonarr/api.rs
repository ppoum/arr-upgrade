use std::{borrow::Cow, fmt::Display};

use serde::Deserialize;

use crate::ArrRequest;

pub(super) struct ApiInfoRequest;

#[derive(Debug, Deserialize)]
pub struct ApiInfoResponse {
    pub current: String,
    pub deprecated: Vec<String>,
}

impl ArrRequest for ApiInfoRequest {
    type Params = ();

    type Response = ApiInfoResponse;

    const METHOD: reqwest::Method = reqwest::Method::GET;

    fn to_url(&self) -> std::borrow::Cow<'_, str> {
        "/api".into()
    }

    fn params(&self) -> std::borrow::Cow<'_, Self::Params> {
        Cow::Owned(())
    }
}

#[derive(Debug, Copy, Clone, Deserialize)]
#[serde(transparent)]
pub struct SonarrId(pub u32);

#[derive(Debug, Copy, Clone, Deserialize, sqlx::Type, PartialEq, Eq, Hash)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct TvdbId(pub u32);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Series {
    pub id: SonarrId,
    pub tvdb_id: TvdbId,
    pub title: String,
    pub monitored: bool,
    pub seasons: Vec<Season>,
}

impl Display for Series {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.title, self.tvdb_id.0)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Season {
    pub season_number: u32,
    pub monitored: bool,
}

pub(super) struct AllSeriesRequest;

impl ArrRequest for AllSeriesRequest {
    type Params = ();

    type Response = Vec<Series>;

    const METHOD: reqwest::Method = reqwest::Method::GET;

    fn to_url(&self) -> Cow<'_, str> {
        "/api/v3/series".into()
    }

    fn params(&self) -> Cow<'_, Self::Params> {
        Cow::Owned(())
    }
}
