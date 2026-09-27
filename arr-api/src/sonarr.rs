use std::time::Duration;

use reqwest::StatusCode;
use tokio::time::Instant;

use crate::{
    ArrError, Client, RequestPayloadExt,
    sonarr::api::{
        AllSeriesRequest, ApiInfoRequest, ApiInfoResponse, CommandId, CommandInfo,
        CommandInfoRequest, CommandResult, CommandStatus, Series, SeriesSearchRequest, SonarrId,
    },
};

pub mod api;

const SUPPORTED_API_VERSION: &str = "v3";

pub struct SonarrClient {
    url: String,
    api_key: String,
}

impl Client for SonarrClient {
    async fn send<R: crate::ArrRequest>(&self, request: R) -> Result<R::Response, crate::ArrError> {
        let full_url = format!("{}{}", self.url, request.to_url());
        let client = reqwest::Client::new();
        let builder = client
            .request(R::METHOD, full_url)
            .header("X-Api-Key", &self.api_key)
            .arr_params(request.params().as_ref());

        let response = builder.send().await?;
        match response.error_for_status() {
            Ok(r) => Ok(r.json().await?),
            Err(e) if matches!(e.status(), Some(StatusCode::UNAUTHORIZED)) => {
                Err(ArrError::Unauthorized)
            }
            Err(e) => Err(e.into()),
        }
    }
}

impl SonarrClient {
    pub fn new(url: String, api_key: String) -> Self {
        Self { url, api_key }
    }

    pub async fn check(&self) -> Result<(), ArrError> {
        let info = self.api_info().await?;
        if info.current != SUPPORTED_API_VERSION {
            Err(ArrError::UnsupportedApiVersion(info.current))
        } else {
            Ok(())
        }
    }

    pub async fn api_info(&self) -> Result<ApiInfoResponse, ArrError> {
        self.send(ApiInfoRequest).await
    }

    pub async fn list_series(&self) -> Result<Vec<Series>, ArrError> {
        self.send(AllSeriesRequest).await
    }

    pub async fn search_series(&self, id: SonarrId) -> Result<CommandId, ArrError> {
        self.send(SeriesSearchRequest(id))
            .await
            .map(|response| response.id)
    }

    pub async fn get_command_info(&self, id: CommandId) -> Result<CommandInfo, ArrError> {
        self.send(CommandInfoRequest(id)).await
    }

    /// Blocks until the command is done executing. This occurs when the command's status is no
    /// longer `Queued` or `Started`. Note that this includes error states, such as `Failed` or
    /// `Aborted`.
    pub async fn block_for_command_execution(
        &self,
        id: CommandId,
        timeout: Option<Duration>,
    ) -> Result<CommandResult, ArrError> {
        const FREQ: Duration = Duration::from_secs(1);
        let start = Instant::now();
        loop {
            if let Some(timeout) = timeout
                && (Instant::now() - start) >= timeout
            {
                return Err(ArrError::Timeout);
            }

            let info = self.get_command_info(id).await?;
            if !matches!(info.status, CommandStatus::Queued | CommandStatus::Started) {
                return Ok(info.result);
            }
            tokio::time::sleep(FREQ).await;
        }
    }
}
