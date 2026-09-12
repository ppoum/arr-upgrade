use std::str::FromStr;

use anyhow::Context;
use arr_api::radarr::api::Movie;
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use tokio::sync::mpsc::{self, Sender};

mod actor;

enum DatabaseMessage {
    SyncMedia {
        instance_name: String,
        media: Vec<Movie>,
    },
    GetOldestMovies {
        instance_name: String,
        count: u32,
        reply: oneshot::Sender<Vec<u32>>,
    },
    MarkMoviesChecked {
        instance_name: String,
        ids: Vec<u32>,
    },
}

pub async fn start_db(url: String) -> anyhow::Result<DatabaseActor> {
    let connection_options = SqliteConnectOptions::from_str(&url)
        .with_context(|| "failed to parse DB connection string")?
        .create_if_missing(true);
    let pool = SqlitePool::connect_with(connection_options).await?;

    println!("TMP: Running migrations");
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
    async fn sync_media(&self, instance_name: String, media: Vec<Movie>);

    /// Returns the `count` oldest movies in the database for the specified instance
    async fn get_oldest_movies(&self, instance_name: String, count: u32) -> Option<Vec<u32>>;

    /// Updates the checked timestamp for the provided movies
    async fn mark_movies_checked(&self, instance_name: String, ids: Vec<u32>);
}

impl DatabaseActorMethods for DatabaseActor {
    async fn sync_media(&self, instance_name: String, media: Vec<Movie>) {
        let msg = DatabaseMessage::SyncMedia {
            instance_name,
            media,
        };

        let _ = self.0.send(msg).await;
    }

    async fn get_oldest_movies(&self, instance_name: String, count: u32) -> Option<Vec<u32>> {
        let (tx, rx) = oneshot::channel();
        let msg = DatabaseMessage::GetOldestMovies {
            instance_name,
            count,
            reply: tx,
        };

        self.0.send(msg).await.ok()?;
        rx.await.ok()
    }

    async fn mark_movies_checked(&self, instance_name: String, ids: Vec<u32>) {
        let msg = DatabaseMessage::MarkMoviesChecked { instance_name, ids };
        let _ = self.0.send(msg).await;
    }
}
