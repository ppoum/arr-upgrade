use std::{sync::Arc, time::Duration};

use anyhow::{Context, bail};
use arr_api::radarr::{
    RadarrClient,
    api::{CommandResult, Movie},
};
use chrono::Local;
use cron::Schedule;
use lib::sleep_or_cancel;
use tokio::select;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::db::{DatabaseActor, DatabaseActorMethods};

/// Timeout before giving up on movie search. Since trackers can be slow and radarr has a max
/// parallel search count, high `count` config values on instances might result in very long jobs.
const RADARR_MOVIE_SEARCH_TIMEOUT: Duration = Duration::from_mins(10);
const HANDLER_ERROR_RETRY_DELAY: Duration = Duration::from_mins(5);

pub async fn start_radarr_handler(
    name: String,
    client: RadarrClient,
    db_actor: DatabaseActor,
    count: u32,
    schedule: Schedule,
    tracker: &TaskTracker,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    if count == 0 {
        bail!("count must be greater than 0");
    }

    tracker.spawn(radarr_handler(
        name,
        client,
        db_actor,
        count,
        schedule,
        cancel_token,
    ));
    Ok(())
}

async fn radarr_handler(
    name: String,
    client: RadarrClient,
    db_actor: DatabaseActor,
    count: u32,
    schedule: Schedule,
    cancel_token: CancellationToken,
) {
    log::info!("Started radarr-{name}");
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
        };

        if let Err(e) = client.check().await {
            log::error!("radarr-{name}: connection failed, retrying in 5 minutes. {e}");
            wait_for_schedule = false;
            let _ = sleep_or_cancel(HANDLER_ERROR_RETRY_DELAY, &cancel_token).await;
            continue;
        }

        // Next time, will have to wait for schedule
        wait_for_schedule = true;
        if let Err(e) = search_movies(
            name.clone(),
            client.clone(),
            &db_actor,
            count,
            &cancel_token,
        )
        .await
        {
            log::error!("radarr-{name}: error searching movies: {e:#}");
        }
    }
    log::debug!("radarr-{name}: shutting down actor");
}

/// Main search execution loop - updates internal state for instance and starts search
async fn search_movies(
    name: String,
    client: Arc<RadarrClient>,
    db_actor: &DatabaseActor,
    count: u32,
    cancel_token: &CancellationToken,
) -> anyhow::Result<()> {
    let movies = sync_movies(name.clone(), &client, db_actor).await?;

    let oldest_ids = match db_actor.get_oldest_movies(name.clone(), count).await {
        Some(ids) => ids,
        None => bail!("DB actor did not answer"),
    };

    let movies = movies.iter().filter(|m| oldest_ids.contains(&m.tmdb_id));
    let movie_cnt = oldest_ids.len();

    // Search movies in parallel, let radarr handle the queue
    let mut command_ids = Vec::with_capacity(movie_cnt);
    for movie in movies {
        log::debug!("radarr-{name}: starting search for {movie}");
        let command_id = client
            .search_movies(vec![movie.id])
            .await
            .with_context(|| "failed to start movie search")?;
        command_ids.push((movie, command_id));
    }

    let mut handles = Vec::with_capacity(movie_cnt);
    for (movie, command_id) in command_ids {
        let client = client.clone();
        let handle = tokio::spawn(async move {
            client
                .block_for_command_execution(command_id, Some(RADARR_MOVIE_SEARCH_TIMEOUT))
                .await
        });
        handles.push((movie, handle));
    }

    // Get result for jobs
    let mut searched_ids = Vec::with_capacity(movie_cnt);
    for (movie, handle) in handles {
        // NOTE: waiting for jobs can be quite slow, and we must take the cancel token into
        // consideration here
        let result = select! {
            result = handle => { result.with_context(|| "search command failed to execute") }
            // If cancelled, return early
            _ = cancel_token.cancelled() => return Ok(())
        }?;

        match result.with_context(|| "search command failed to complete")? {
            CommandResult::Successful => {
                log::info!("radarr-{name}: successfully searched for {movie}");
                searched_ids.push(movie.tmdb_id);
            }
            res => log::warn!("radarr-{name}: search for {movie} failed - {res}"),
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
) -> anyhow::Result<Arc<Vec<Movie>>> {
    log::debug!("radarr-{name}: syncing movies");
    let movies = client
        .list_movies()
        .await
        .with_context(|| "failed to get movie list from radarr")?;
    log::info!("radarr-{name}: found {} movies", movies.len());

    let movies = Arc::new(movies);
    db_actor.sync_movies(name, movies.clone()).await;
    Ok(movies)
}
