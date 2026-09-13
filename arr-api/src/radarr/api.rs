use std::{
    borrow::Cow,
    fmt::{self, Display},
};

use serde::{Deserialize, Serialize};

use crate::{ArrRequest, Json};

pub(super) struct ApiInfo;
#[derive(Debug, Deserialize)]
pub struct ApiInfoResponse {
    pub current: String,
    pub deprecated: Vec<String>,
}
impl ArrRequest for ApiInfo {
    type Params = ();

    type Response = ApiInfoResponse;

    const METHOD: reqwest::Method = reqwest::Method::GET;

    fn to_url(&self) -> Cow<'_, str> {
        "/api".into()
    }

    fn params(&self) -> Cow<'_, Self::Params> {
        Cow::Owned(())
    }
}

pub(super) struct AllMovies;
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

    fn params(&self) -> Cow<'_, Self::Params> {
        Cow::Owned(())
    }
}

pub(super) struct MoviesSearch {
    pub ids: Vec<u32>,
}
#[derive(Debug, Serialize, Clone)]
pub(super) struct MoviesSearchParams {
    name: String,
    #[serde(rename = "movieIds")]
    ids: Vec<u32>,
}

#[derive(Debug, Deserialize)]
pub(super) struct MoviesSearchResponse {
    pub id: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct CommandId(pub u32);
impl Display for CommandId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl ArrRequest for MoviesSearch {
    type Params = Json<MoviesSearchParams>;

    type Response = MoviesSearchResponse;

    const METHOD: reqwest::Method = reqwest::Method::POST;

    fn to_url(&self) -> std::borrow::Cow<'_, str> {
        "/api/v3/command".into()
    }

    fn params(&self) -> Cow<'_, Self::Params> {
        // TODO: maybe can rework better to not clone (init once in constructor?)
        Cow::Owned(
            MoviesSearchParams {
                name: "MoviesSearch".to_owned(),
                ids: self.ids.clone(),
            }
            .into(),
        )
    }
}

pub(super) struct CommandInfoRequest {
    pub id: CommandId,
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
impl Display for CommandResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::Successful => write!(f, "Successful"),
            Self::Unsuccessful => write!(f, "Unsuccessful"),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CommandInfo {
    pub status: CommandStatus,
    pub result: CommandResult,
}

impl ArrRequest for CommandInfoRequest {
    type Params = ();

    type Response = CommandInfo;

    const METHOD: reqwest::Method = reqwest::Method::GET;

    fn to_url(&self) -> Cow<'_, str> {
        format!("/api/v3/command/{}", self.id).into()
    }

    fn params(&self) -> Cow<'_, Self::Params> {
        Cow::Owned(())
    }
}
