use std::{path::PathBuf, time::Duration};

use anyhow::Context;
use arr_api::radarr::RadarrClient;

use crate::radarr::start_radarr_handler;

mod config;
mod db;
mod radarr;

const CONFIG_DIR_ENVVAR: &str = "ARR_UPGRADE_CONFIG";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let config_dir = std::env::var(CONFIG_DIR_ENVVAR)
        .with_context(|| format!("failed to read config directory from {CONFIG_DIR_ENVVAR}"))?;

    let db_actor = db::start_db(format!("sqlite://{}/arr_upgrade.db", config_dir)).await?;

    let config_file_path = PathBuf::from(config_dir.clone()).join("config.toml");
    let config = config::load_config(config_file_path)?;

    for (name, instance) in config.get_radarr_instances() {
        let client = RadarrClient::new(instance.url, instance.api_key);
        start_radarr_handler(
            name,
            client,
            db_actor.clone(),
            instance.count,
            instance.frequency,
        )
        .await
        .with_context(|| "failed to start radarr instance")?;
    }
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
