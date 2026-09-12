use std::{collections::HashMap, path::PathBuf, time::Duration};

use anyhow::Context;
use arr_api::{ArrError, radarr::RadarrClient};
use rand::seq::SliceRandom;

use crate::db::DatabaseActorMethods;

mod config;
mod db;

const CONFIG_DIR_ENVVAR: &str = "ARR_UPGRADE_CONFIG";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let config_dir = std::env::var(CONFIG_DIR_ENVVAR)
        .with_context(|| format!("failed to read config directory from {CONFIG_DIR_ENVVAR}"))?;

    let db_actor = db::start_db(format!("sqlite://{}/arr_upgrade.db", config_dir)).await?;

    let config_file_path = PathBuf::from(config_dir.clone()).join("config.toml");
    let config = config::load_config(config_file_path)?;

    let radarr_clients: HashMap<String, RadarrClient> = config
        .radarr
        .into_iter()
        .map(|(key, config)| (key, RadarrClient::new(config.url, config.api_key)))
        .collect();

    for (name, client) in radarr_clients {
        client
            .check()
            .await
            .with_context(|| "error connecting to radarr")?;

        let mut movies = client
            .list_movies()
            .await
            .with_context(|| "failed to list all movies")?;

        // TMP: take 10 random movies and add to db
        movies.shuffle(&mut rand::rng());
        let movies: Vec<_> = movies.into_iter().take(10).collect();
        db_actor.sync_media(name.clone(), movies).await;

        if let Some(oldest) = db_actor.get_oldest_movies(name.clone(), 5).await {
            log::info!("Oldest movies are: {oldest:?}");
            if let Some(m) = oldest.first() {
                log::info!("TMP: Starting search for {m}");
                let id = match client.search_movies(vec![*m]).await {
                    Ok(i) => i,
                    Err(e) => {
                        log::warn!("TMP err: {e}");
                        continue;
                    }
                };
                log::info!("Started job with id {}", id.0);
                match client
                    .wait_for_command_completed(id, Some(Duration::from_secs(60)))
                    .await
                {
                    Ok(r) => {
                        log::info!("Job completed with: {r}");
                        db_actor.mark_movies_checked(name, vec![*m]).await;
                    }
                    Err(ArrError::Timeout) => log::warn!("Timed out waiting for job"),
                    Err(e) => log::error!("Unexpected error: {e}"),
                }
            }
        } else {
            log::error!("Error fetching movies from db");
        }
    }
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
