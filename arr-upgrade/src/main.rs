use std::{collections::HashMap, path::PathBuf};

use anyhow::Context;
use arr_api::radarr::RadarrClient;

mod config;

const CONFIG_DIR_ENVVAR: &str = "ARR_UPGRADE_CONFIG";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_dir = std::env::var(CONFIG_DIR_ENVVAR)
        .with_context(|| format!("failed to read config directory from {CONFIG_DIR_ENVVAR}"))?;

    let config_file_path = PathBuf::from(config_dir.clone()).join("config.toml");
    let config = config::load_config(config_file_path)?;

    let radarr_clients: HashMap<String, RadarrClient> = config
        .radarr
        .into_iter()
        .map(|(key, config)| (key, RadarrClient::new(config.url, config.api_key)))
        .collect();

    for (_name, client) in radarr_clients {
        client
            .check()
            .await
            .with_context(|| "error connecting to radarr")?;

        println!("Connected");

        let movies = client
            .list_movies()
            .await
            .with_context(|| "failed to list all movies")?;
        println!("Got {} movies", movies.len());

        for movie in movies.iter().take(10) {
            println!("{movie:?}");
        }
    }
    Ok(())
}
