use std::{sync::Arc, time::Duration};

use anyhow::{Context, bail};
use arr_api::sonarr::{
    SonarrClient,
    api::{
        CommandResult::{self},
        Series,
    },
};
use chrono::Local;
use cron::Schedule;
use lib::sleep_or_cancel;
use tokio::select;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    config::SonarrSearchGranularity,
    db::{DatabaseActor, DatabaseActorMethods},
};

const SONARR_SEASON_SEARCH_TIMEOUT: Duration = Duration::from_mins(10);
const SONARR_SERIES_SEARCH_TIMEOUT: Duration = Duration::from_mins(20);
const HANDLER_ERROR_RETRY_DELAY: Duration = Duration::from_mins(5);

#[expect(clippy::too_many_arguments)]
pub async fn start_sonarr_handler(
    name: String,
    client: SonarrClient,
    db_actor: DatabaseActor,
    count: u32,
    schedule: Schedule,
    granularity: SonarrSearchGranularity,
    tracker: &TaskTracker,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    if count == 0 {
        bail!("count must be greated than 0");
    }

    tracker.spawn(sonarr_handler(
        name,
        client,
        db_actor,
        count,
        schedule,
        granularity,
        cancel_token,
    ));
    Ok(())
}

async fn sonarr_handler(
    name: String,
    client: SonarrClient,
    db_actor: DatabaseActor,
    count: u32,
    schedule: Schedule,
    granularity: SonarrSearchGranularity,
    cancel_token: CancellationToken,
) {
    log::info!("Started sonarr-{name}");
    let client = Arc::new(client);

    let mut wait_for_schedule = false;
    while !cancel_token.is_cancelled() {
        if wait_for_schedule {
            if let Some(next) = schedule.upcoming(Local).next() {
                // If err, duration was less than 0, can assume we need to instantly execute
                let duration = (next - Local::now()).to_std().unwrap_or_default();
                log::debug!(
                    "radarr-{name}: sleeping until {} - {}s",
                    next,
                    duration.as_secs()
                );
                if sleep_or_cancel(duration, &cancel_token).await.is_err() {
                    break;
                }
            } else {
                log::warn!(
                    "radarr-{name}: unable to find next scheduled occurrence, retrying in 5 minutes"
                );
                wait_for_schedule = false;
                let _ = sleep_or_cancel(Duration::from_mins(5), &cancel_token).await;
                continue;
            };
        }

        if let Err(e) = client.check().await {
            log::error!("radarr-{name}: connection failed, retrying in 5 minutes. {e}");
            wait_for_schedule = false;
            let _ = sleep_or_cancel(HANDLER_ERROR_RETRY_DELAY, &cancel_token).await;
            continue;
        }

        // Next time, will have to wait for schedule
        wait_for_schedule = true;
        let result = match granularity {
            SonarrSearchGranularity::Show => {
                search_series(
                    name.clone(),
                    count,
                    client.clone(),
                    &db_actor,
                    &cancel_token,
                )
                .await
            }
            SonarrSearchGranularity::Season => {
                search_seasons(
                    name.clone(),
                    count,
                    client.clone(),
                    &db_actor,
                    &cancel_token,
                )
                .await
            }
        };
        if let Err(e) = result {
            log::error!("sonarr-{name}: error during search: {e:#}");
        }
    }
    log::debug!("sonarr-{name}: shutting down actor");
}

async fn search_series(
    name: String,
    count: u32,
    client: Arc<SonarrClient>,
    db_actor: &DatabaseActor,
    cancel_token: &CancellationToken,
) -> anyhow::Result<()> {
    let series = sync_series(name.clone(), &client, db_actor).await?;
    let oldest_ids = db_actor
        .get_oldest_series(name.clone(), count)
        .await
        .with_context(|| "failed to get oldest series from database")?;

    let series_cnt = oldest_ids.len();
    let mut handles = Vec::with_capacity(series_cnt);

    let oldest_series = series.iter().filter(|s| oldest_ids.contains(&s.tvdb_id));
    for series in oldest_series {
        log::debug!("sonarr-{name}: starting search for {series}");
        let client = client.clone();
        let id = series.id;
        let handle = tokio::spawn(async move {
            let command_id = client
                .search_series(id)
                .await
                .with_context(|| "failed to start series search")?;
            client
                .block_for_command_execution(command_id, Some(SONARR_SERIES_SEARCH_TIMEOUT))
                .await
                .with_context(|| "error waiting for command to complete")
        });
        handles.push((series, handle));
    }

    for (series, handle) in handles {
        // NOTE: waiting for jobs can be quite slow, and we must take the cancel token into
        // consideration here
        let result = select! {
            result = handle => { result.with_context(|| "search command failed to execute") }
            // If cancelled, return early
            _ = cancel_token.cancelled() => return Ok(())
        }?;
        match result.with_context(|| "search command failed to complete")? {
            CommandResult::Successful => {
                log::info!("sonarr-{name}: successfully searched for {series}");
                db_actor
                    .mark_series_checked(name.clone(), series.tvdb_id)
                    .await;
            }
            other => log::warn!("sonarr-{name}: search for series {series} failed - {other}"),
        }
    }
    Ok(())
}

async fn search_seasons(
    name: String,
    count: u32,
    client: Arc<SonarrClient>,
    db_actor: &DatabaseActor,
    cancel_token: &CancellationToken,
) -> anyhow::Result<()> {
    let series = sync_series(name.clone(), &client, db_actor).await?;
    let oldest_ids = db_actor
        .get_oldest_seasons(name.clone(), count)
        .await
        .with_context(|| "failed to get oldest seasons from database")?;

    let season_cnt = oldest_ids.len();
    let mut handles = Vec::with_capacity(season_cnt);

    // Map [TvdbId] to full [Series] object *if* it exists and has the specified season
    let oldest_seasons = series.iter().filter_map(|series| {
        match oldest_ids.iter().find(|(id, _)| series.tvdb_id == *id) {
            Some((id, season_number)) if series.has_season_number(*season_number) => {
                Some((series, *season_number))
            }
            _ => None,
        }
    });
    for (series, season_number) in oldest_seasons {
        log::debug!("sonarr-{name}: starting search for {series} season {season_number}");
        let client = client.clone();
        let id = series.id;
        let handle = tokio::spawn(async move {
            let command_id = client
                .search_season(id, season_number)
                .await
                .with_context(|| "failed to start series search")?;
            client
                .block_for_command_execution(command_id, Some(SONARR_SEASON_SEARCH_TIMEOUT))
                .await
                .with_context(|| "error waiting for command to complete")
        });
        handles.push((series, season_number, handle));
    }

    for (series, season_number, handle) in handles {
        // NOTE: waiting for jobs can be quite slow, and we must take the cancel token into
        // consideration here
        let result = select! {
            result = handle => { result.with_context(|| "search command failed to execute") }
            // If cancelled, return early
            _ = cancel_token.cancelled() => return Ok(())
        }?;

        match result.with_context(|| "search command failed to complete")? {
            CommandResult::Successful => {
                log::info!(
                    "sonarr-{name}: successfully search for {series} season {season_number}"
                );
                db_actor
                    .mark_season_checked(name.clone(), series.tvdb_id, season_number)
                    .await;
            }
            other => log::warn!(
                "sonarr-{name}: search for series {series} season {season_number} failed - {other}"
            ),
        }
    }

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
