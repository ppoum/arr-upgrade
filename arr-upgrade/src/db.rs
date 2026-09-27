use std::{str::FromStr, sync::Arc};

use anyhow::Context;
use arr_api::{
    radarr::api::{Movie, TmdbId},
    sonarr::api::{Series, TvdbId},
};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::db::{
    movies::{fetch_n_oldest_movies, sync_movies, update_checked_timestamp},
    series::{get_oldest_seasons, get_oldest_series, mark_tv_checked, sync_series},
};

mod movies;
mod series;

/// Default value to use when a `DATETIME` column has a NULL value. Derived from
/// `DateTime::MIN_UTC.to_rfc_3339_opts(SecondsFormat::Nanos, false)`
const DB_DATETIME_NULL_REPLACEMENT: &str = "-262143-01-01T00:00:00.000000000+00:00";

enum DatabaseMessage {
    SyncMovies {
        instance_name: String,
        movies: Arc<Vec<Movie>>,
    },
    GetOldestMovies {
        instance_name: String,
        count: u32,
        reply: oneshot::Sender<Vec<TmdbId>>,
    },
    MarkMoviesChecked {
        instance_name: String,
        ids: Vec<TmdbId>,
    },
    SyncSeries {
        instance_name: String,
        series: Arc<Vec<Series>>,
    },
    MarkTvChecked {
        instance_name: String,
        media: TvMedia,
    },
    GetOldestSeries {
        instance_name: String,
        count: u32,
        reply: oneshot::Sender<Vec<TvdbId>>,
    },
    GetOldestSeasons {
        instance_name: String,
        count: u32,
        reply: oneshot::Sender<Vec<(TvdbId, u32)>>,
    },
}

enum TvMedia {
    Series { id: TvdbId },
    Season { id: TvdbId, season: u32 },
}

pub async fn start_db(url: String) -> anyhow::Result<DatabaseActor> {
    let connection_options = SqliteConnectOptions::from_str(&url)
        .with_context(|| "failed to parse DB connection string")?
        .create_if_missing(true);
    let pool = SqlitePool::connect_with(connection_options).await?;

    log::info!("Running migrations");
    sqlx::migrate!("./migrations/")
        .run(&pool)
        .await
        .with_context(|| "failed to migrate db")?;

    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(start_actor(pool, rx));

    Ok(DatabaseActor(tx))
}

#[derive(Clone)]
pub struct DatabaseActor(Sender<DatabaseMessage>);

pub trait DatabaseActorMethods {
    /// Sync radarr movies to database
    async fn sync_movies(&self, instance_name: String, movies: Arc<Vec<Movie>>);

    /// Returns the `count` oldest movies in the database for the specified instance
    async fn get_oldest_movies(&self, instance_name: String, count: u32) -> Option<Vec<TmdbId>>;

    /// Updates the checked timestamp for the provided movies
    async fn mark_movies_checked(&self, instance_name: String, ids: Vec<TmdbId>);

    /// Sync sonarr series/seasons to the database
    async fn sync_series(&self, instance_name: String, series: Arc<Vec<Series>>);

    /// Updates the checked timestamp for all seasons of a series
    async fn mark_series_checked(&self, instance_name: String, id: TvdbId);

    /// Updates the checked timestamp for a single season of a series
    async fn mark_season_checked(&self, instance_name: String, id: TvdbId, season: u32);

    /// Returns the `count` oldest series in the database
    async fn get_oldest_series(&self, instance_name: String, count: u32) -> Option<Vec<TvdbId>>;

    /// Returns the `count` oldest seasons in the database
    async fn get_oldest_seasons(
        &self,
        instance_name: String,
        count: u32,
    ) -> Option<Vec<(TvdbId, u32)>>;
}

impl DatabaseActorMethods for DatabaseActor {
    async fn sync_movies(&self, instance_name: String, movies: Arc<Vec<Movie>>) {
        let msg = DatabaseMessage::SyncMovies {
            instance_name,
            movies,
        };

        let _ = self.0.send(msg).await;
    }

    async fn get_oldest_movies(&self, instance_name: String, count: u32) -> Option<Vec<TmdbId>> {
        let (tx, rx) = oneshot::channel();
        let msg = DatabaseMessage::GetOldestMovies {
            instance_name,
            count,
            reply: tx,
        };

        self.0.send(msg).await.ok()?;
        rx.await.ok()
    }

    async fn mark_movies_checked(&self, instance_name: String, ids: Vec<TmdbId>) {
        let msg = DatabaseMessage::MarkMoviesChecked { instance_name, ids };
        let _ = self.0.send(msg).await;
    }

    async fn sync_series(&self, instance_name: String, series: Arc<Vec<Series>>) {
        let msg = DatabaseMessage::SyncSeries {
            instance_name,
            series,
        };
        let _ = self.0.send(msg).await;
    }

    async fn mark_series_checked(&self, instance_name: String, id: TvdbId) {
        let msg = DatabaseMessage::MarkTvChecked {
            instance_name,
            media: TvMedia::Series { id },
        };
        let _ = self.0.send(msg).await;
    }

    async fn mark_season_checked(&self, instance_name: String, id: TvdbId, season: u32) {
        let msg = DatabaseMessage::MarkTvChecked {
            instance_name,
            media: TvMedia::Season { id, season },
        };
        let _ = self.0.send(msg).await;
    }

    async fn get_oldest_series(&self, instance_name: String, count: u32) -> Option<Vec<TvdbId>> {
        let (tx, rx) = oneshot::channel();
        let msg = DatabaseMessage::GetOldestSeries {
            instance_name,
            count,
            reply: tx,
        };
        self.0.send(msg).await.ok()?;
        rx.await.ok()
    }

    async fn get_oldest_seasons(
        &self,
        instance_name: String,
        count: u32,
    ) -> Option<Vec<(TvdbId, u32)>> {
        let (tx, rx) = oneshot::channel();
        let msg = DatabaseMessage::GetOldestSeasons {
            instance_name,
            count,
            reply: tx,
        };
        self.0.send(msg).await.ok()?;
        rx.await.ok()
    }
}

async fn start_actor(pool: SqlitePool, mut rx: Receiver<DatabaseMessage>) {
    log::trace!("DB actor spawned");

    while let Some(message) = rx.recv().await {
        match message {
            DatabaseMessage::SyncMovies {
                instance_name,
                movies,
            } => {
                if let Err(e) = sync_movies(&pool, &instance_name, &movies).await {
                    log::error!("Unable to sync movies");
                    log::debug!("ERROR: unable to sync movies: {e:#}");
                }
            }
            DatabaseMessage::GetOldestMovies {
                instance_name,
                count,
                reply,
            } => {
                let ids = fetch_n_oldest_movies(&pool, &instance_name, count).await;
                let _ = reply.send(ids);
            }
            DatabaseMessage::MarkMoviesChecked { instance_name, ids } => {
                if let Err(e) = update_checked_timestamp(&pool, &instance_name, &ids).await {
                    log::error!("Unable to check movies");
                    log::debug!("ERROR: unable to check movies: {e:#}");
                }
            }
            DatabaseMessage::SyncSeries {
                instance_name,
                series,
            } => {
                if let Err(e) = sync_series(&pool, &instance_name, &series).await {
                    log::error!("Unable to sync series");
                    log::debug!("ERROR: unable to sync series: {e:#}");
                }
            }
            DatabaseMessage::MarkTvChecked {
                instance_name,
                media,
            } => {
                if let Err(e) = mark_tv_checked(&pool, &instance_name, media).await {
                    log::error!("Unable to mark TV as checked");
                    log::debug!("ERROR: unable to check series/season: {e:#}");
                }
            }
            DatabaseMessage::GetOldestSeries {
                instance_name,
                count,
                reply,
            } => match get_oldest_series(&pool, &instance_name, count).await {
                Ok(res) => {
                    let _ = reply.send(res);
                }
                Err(e) => {
                    log::error!("Unable to find oldest series");
                    log::debug!("ERROR: unable to find oldest series: {e:#}");
                }
            },
            DatabaseMessage::GetOldestSeasons {
                instance_name,
                count,
                reply,
            } => match get_oldest_seasons(&pool, &instance_name, count).await {
                Ok(res) => {
                    let _ = reply.send(res);
                }
                Err(e) => {
                    log::error!("Unable to find oldest series");
                    log::debug!("ERROR: unable to find oldest series: {e:#}");
                }
            },
        }
        log::trace!("DB message handled");
    }

    log::info!("DB actor closing");
}
