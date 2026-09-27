use std::{sync::Arc, time::Duration};

use anyhow::{Context, bail};
use arr_api::sonarr::{SonarrClient, api::Series};
use chrono::Local;
use cron::Schedule;

use crate::{
    config::SonarrSearchGranularity,
    db::{DatabaseActor, DatabaseActorMethods},
};

const _SONARR_SEASON_SEARCH_TIMEOUT: Duration = Duration::from_mins(10);
const _SONARR_SHOW_SEARCH_TIMEOUT: Duration = Duration::from_mins(20);
const HANDLER_ERROR_RETRY_DELAY: Duration = Duration::from_mins(5);

pub async fn start_sonarr_handler(
    name: String,
    client: SonarrClient,
    db_actor: DatabaseActor,
    count: u32,
    schedule: Schedule,
    granularity: SonarrSearchGranularity,
) -> anyhow::Result<()> {
    if count == 0 {
        bail!("count must be greated than 0");
    }

    tokio::spawn(sonarr_handler(
        name,
        client,
        db_actor,
        count,
        schedule,
        granularity,
    ));
    Ok(())
}

async fn sonarr_handler(
    name: String,
    client: SonarrClient,
    db_actor: DatabaseActor,
    _count: u32,
    schedule: Schedule,
    _granularity: SonarrSearchGranularity,
) {
    log::info!("Started sonarr-{name}");

    let mut wait_for_schedule = false;
    loop {
        if wait_for_schedule {
            if let Some(next) = schedule.upcoming(Local).next() {
                // If err, duration was less than 0, can assume we need to instantly execute
                let duration = (next - Local::now()).to_std().unwrap_or_default();
                log::debug!(
                    "radarr-{name}: sleeping until {} - {}s",
                    next,
                    duration.as_secs()
                );
                tokio::time::sleep(duration).await;
            } else {
                log::warn!(
                    "radarr-{name}: unable to find next scheduled occurrence, retrying in 5 minutes"
                );
                wait_for_schedule = false;
                tokio::time::sleep(Duration::from_mins(5)).await;
                continue;
            };
        }

        if let Err(e) = client.check().await {
            log::error!("radarr-{name}: connection failed, retrying in 5 minutes. {e}");
            wait_for_schedule = false;
            tokio::time::sleep(HANDLER_ERROR_RETRY_DELAY).await;
            continue;
        }

        // Next time, will have to wait for schedule
        wait_for_schedule = true;
        if let Err(e) = search_series(name.clone(), &client, &db_actor).await {
            log::error!("sonarr-{name}: error searching series: {e:#}");
        }
    }
}

async fn search_series(
    name: String,
    client: &SonarrClient,
    db_actor: &DatabaseActor,
) -> anyhow::Result<()> {
    let _series = sync_series(name, client, db_actor).await?;
    Ok(())
}

async fn sync_series(
    name: String,
    client: &SonarrClient,
    db_actor: &DatabaseActor,
) -> anyhow::Result<Arc<Vec<Series>>> {
    log::debug!("sonarr-{name}: syncing series");
    let series = client
        .list_series()
        .await
        .with_context(|| "failed to get series list from sonarr")?;
    log::info!("sonarr-{name}: found {} series", series.len());

    let series = Arc::new(series);
    db_actor.sync_series(name, series.clone()).await;
    Ok(series)
}
