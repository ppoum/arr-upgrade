use std::{str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, bail};
use arr_api::radarr::{RadarrClient, api::CommandResult};
use chrono::Local;
use cron::Schedule;
use futures::future::join_all;

use crate::db::{DatabaseActor, DatabaseActorMethods};

const FREQ_NATURAL_VALUES: [(&str, &str); 4] = [
    ("hourly", "0 0 * * * *"),
    ("daily", "0 0 0 * * *"),
    ("weekly", "0 0 0 * * 0"),
    ("monthly", "0 0 0 1 * *"),
];

/// Timeout before giving up on movie search. Since trackers can be slow and radarr has a max
/// parallel search count, high `count` config values on instances might result in very long jobs.
const RADARR_MOVIE_SEARCH_TIMEOUT: Duration = Duration::from_mins(10);
const HANDLER_ERROR_RETRY_DELAY: Duration = Duration::from_mins(5);

pub async fn start_radarr_handler(
    name: String,
    client: RadarrClient,
    db_actor: DatabaseActor,
    count: u32,
    frequency: String,
) -> anyhow::Result<()> {
    if count == 0 {
        bail!("count must be greater than 0");
    }
    let schedule =
        parse_schedule(frequency).with_context(|| "failed to parse cron schedule string")?;

    tokio::spawn(radarr_handler(name, client, db_actor, count, schedule));
    Ok(())
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

async fn radarr_handler(
    name: String,
    client: RadarrClient,
    db_actor: DatabaseActor,
    count: u32,
    schedule: Schedule,
) {
    log::info!("Started radarr-{name}");
    let client = Arc::new(client);

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
        if let Err(e) = search_movies(name.clone(), client.clone(), &db_actor, count).await {
            log::error!("Error searching movies for radarr-{}: {e:#}", name);
        }
    }
}

async fn search_movies(
    name: String,
    client: Arc<RadarrClient>,
    db_actor: &DatabaseActor,
    count: u32,
) -> anyhow::Result<()> {
    sync_movies(name.clone(), &client, db_actor).await?;

    let movie_ids = match db_actor.get_oldest_movies(name.clone(), count).await {
        Some(ids) => ids,
        None => bail!("DB actor did not answer"),
    };

    // Search movies in parallel, let radarr handle the queue
    let mut command_ids = Vec::with_capacity(movie_ids.len());
    for movie_id in &movie_ids {
        // TODO: would be nice to be able to log movie name
        log::debug!("radarr-{name}: starting search for {movie_id}");
        let command_id = client
            .search_movies(vec![*movie_id])
            .await
            .with_context(|| "failed to start movie search")?;
        command_ids.push((*movie_id, command_id));
    }

    let mut handles = Vec::with_capacity(movie_ids.len());
    for (movie_id, command_id) in command_ids {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            client
                .wait_for_command_completed(command_id, Some(RADARR_MOVIE_SEARCH_TIMEOUT))
                .await
                .map(|res| (movie_id, res))
        }));
    }

    let results = join_all(handles).await;
    let mut searched_ids = Vec::with_capacity(movie_ids.len());
    for result in results {
        let r = match result {
            Ok(r) => r,
            Err(e) => return Err(e).with_context(|| "search command failed to execute"),
        };

        match r {
            Ok((movie_id, CommandResult::Successful)) => searched_ids.push(movie_id),
            Ok((movie_id, res)) => {
                log::warn!("Search for id {movie_id} was not successful - {res}")
            }
            Err(e) => return Err(e).with_context(|| "search command failed to complete"),
        }
    }

    db_actor
        .mark_movies_checked(name.clone(), searched_ids)
        .await;
    Ok(())
}

/// Sync all movies with the database
async fn sync_movies(
    name: String,
    client: &RadarrClient,
    db_actor: &DatabaseActor,
) -> anyhow::Result<()> {
    log::debug!("radarr-{name}: syncing movies");
    let movies = client
        .list_movies()
        .await
        .with_context(|| "failed to get movie list from radarr")?;
    log::info!(
        "radarr-{name}: found {} movies on radarr-{name}",
        movies.len()
    );

    db_actor.sync_media(name, movies).await;
    Ok(())
}
