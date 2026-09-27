use std::{borrow::Cow, fmt::Display};

use serde::{Deserialize, Serialize};

use crate::{ArrRequest, Json};

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

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Copy, Clone, Deserialize)]
#[serde(transparent)]
pub struct CommandId(pub u32);

impl Display for CommandId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub(super) struct SeriesSearchRequest(pub SonarrId);

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(super) struct SeriesSearchParams {
    name: String,
    series_id: SonarrId,
}

#[derive(Debug, Deserialize)]
pub(super) struct SeriesSearchResponse {
    pub id: CommandId,
}

impl ArrRequest for SeriesSearchRequest {
    type Params = Json<SeriesSearchParams>;

    type Response = SeriesSearchResponse;

    const METHOD: reqwest::Method = reqwest::Method::POST;

    fn to_url(&self) -> Cow<'_, str> {
        "/api/v3/command".into()
    }

    fn params(&self) -> Cow<'_, Self::Params> {
        Cow::Owned(Json(SeriesSearchParams {
            name: "SeriesSearch".to_owned(),
            series_id: self.0,
        }))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommandStatus {
    Queued,
    Started,
    Completed,
    Failed,
    Aborted,
    Cancelled,
    Orphaned,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommandResult {
    Unknown,
    Successful,
    Unsuccessful,
}

#[derive(Debug, Deserialize)]
pub struct CommandInfo {
    pub status: CommandStatus,
    pub result: CommandResult,
}

pub(super) struct CommandInfoRequest(pub CommandId);

impl ArrRequest for CommandInfoRequest {
    type Params = ();

    type Response = CommandInfo;

    const METHOD: reqwest::Method = reqwest::Method::GET;

    fn to_url(&self) -> Cow<'_, str> {
        format!("/api/v3/command/{}", self.0).into()
    }

    fn params(&self) -> Cow<'_, Self::Params> {
        Cow::Owned(())
    }
}
