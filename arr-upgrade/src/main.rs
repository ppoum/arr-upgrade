use std::{path::PathBuf, str::FromStr, time::Duration};

use anyhow::Context;
use arr_api::{radarr::RadarrClient, sonarr::SonarrClient};
use cron::Schedule;

use crate::{radarr::start_radarr_handler, sonarr::start_sonarr_handler};

mod config;
mod db;
mod radarr;
mod sonarr;

const CONFIG_DIR_ENVVAR: &str = "ARR_UPGRADE_CONFIG";
const FREQ_NATURAL_VALUES: [(&str, &str); 4] = [
    ("hourly", "0 0 * * * *"),
    ("daily", "0 0 0 * * *"),
    ("weekly", "0 0 0 * * 0"),
    ("monthly", "0 0 0 1 * *"),
];

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
        let schedule = parse_schedule(instance.frequency)
            .with_context(|| format!("invalid frequency for radarr-{name}"))
            .unwrap();
        start_radarr_handler(name, client, db_actor.clone(), instance.count, schedule)
            .await
            .with_context(|| "failed to start radarr instance")?;
    }

    for (name, instance) in config.get_sonarr_instances() {
        let client = SonarrClient::new(instance.url, instance.api_key);
        let schedule = parse_schedule(instance.frequency)
            .with_context(|| format!("invalid frequency for sonarr-{name}"))
            .unwrap();
        start_sonarr_handler(
            name,
            client,
            db_actor.clone(),
            instance.count,
            schedule,
            instance.search_granularity,
        )
        .await
        .with_context(|| "failed to start sonarr instance")?;
    }

    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

/// Converts the frequency config value to a cron schedule. Handles the supported natural language cases
fn parse_schedule(frequency_str: String) -> Result<Schedule, cron::error::Error> {
    let cron = if let Some((_, nat)) = FREQ_NATURAL_VALUES
        .iter()
        .find(|(s, _)| s.eq_ignore_ascii_case(&frequency_str))
    {
        nat.to_string()
    } else {
        frequency_str
    };
    cron::Schedule::from_str(&cron)
}
