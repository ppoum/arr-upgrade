use std::{str::FromStr, sync::Arc};

use anyhow::Context;
use arr_api::radarr::api::{Movie, TmdbId};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use tokio::sync::mpsc::{self, Sender};

mod actor;

enum DatabaseMessage {
    SyncMedia {
        instance_name: String,
        media: Arc<Vec<Movie>>,
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
    tokio::spawn(actor::start_actor(pool, rx));

    Ok(DatabaseActor(tx))
}

#[derive(Clone)]
pub struct DatabaseActor(Sender<DatabaseMessage>);

pub trait DatabaseActorMethods {
    /// Sync arr media to database
    async fn sync_media(&self, instance_name: String, media: Arc<Vec<Movie>>);

    /// Returns the `count` oldest movies in the database for the specified instance
    async fn get_oldest_movies(&self, instance_name: String, count: u32) -> Option<Vec<TmdbId>>;

    /// Updates the checked timestamp for the provided movies
    async fn mark_movies_checked(&self, instance_name: String, ids: Vec<TmdbId>);
}

impl DatabaseActorMethods for DatabaseActor {
    async fn sync_media(&self, instance_name: String, media: Arc<Vec<Movie>>) {
        let msg = DatabaseMessage::SyncMedia {
            instance_name,
            media,
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
}
