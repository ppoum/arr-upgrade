use std::time::{Duration, Instant};

use reqwest::StatusCode;

use crate::{
    ArrError, Client, RequestPayloadExt,
    radarr::api::{
        AllMovies, ApiInfo, ApiInfoResponse, CommandId, CommandInfo, CommandInfoRequest,
        CommandResult, CommandStatus, Movie, MoviesSearch,
    },
};

pub mod api;

const SUPPORTED_API_VERSION: &str = "v3";

pub struct RadarrClient {
    url: String,
    api_key: String,
}

impl Client for RadarrClient {
    async fn send<R: crate::ArrRequest>(&self, request: R) -> Result<R::Response, ArrError> {
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

impl RadarrClient {
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
        self.send(ApiInfo).await
    }

    pub async fn list_movies(&self) -> Result<Vec<Movie>, ArrError> {
        self.send(AllMovies).await
    }

    pub async fn search_movies(&self, ids: Vec<u32>) -> Result<CommandId, ArrError> {
        let request = MoviesSearch { ids };
        let response = self.send(request).await?;
        Ok(CommandId(response.id))
    }

    pub async fn get_command_info(&self, id: CommandId) -> Result<CommandInfo, ArrError> {
        self.send(CommandInfoRequest { id }).await
    }

    /// Blocks until the command's status becomes `Completed`. Returns the [CommandResult].
    pub async fn wait_for_command_completed(
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
            if matches!(info.status, CommandStatus::Completed) {
                return Ok(info.result);
            }
            tokio::time::sleep(FREQ).await;
        }
    }
}
