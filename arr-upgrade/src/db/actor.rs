use anyhow::{Context, bail};
use arr_api::radarr::api::{Movie, TmdbId};
use chrono::Utc;
use sqlx::{AssertSqlSafe, SqliteExecutor, SqlitePool, query, query_scalar};
use tokio::sync::mpsc::Receiver;

use crate::db::DatabaseMessage;

pub async fn start_actor(pool: SqlitePool, mut rx: Receiver<DatabaseMessage>) {
    log::trace!("DB actor spawned");
    while let Some(message) = rx.recv().await {
        match message {
            DatabaseMessage::SyncMedia {
                instance_name,
                media,
            } => {
                // TODO: handle different instance types (2 message types?)
                if let Err(e) = sync_movies(&pool, &instance_name, &media).await {
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
                    log::debug!("ERROR: unable to check movies: {e}");
                }
            }
        }
        log::trace!("DB message handled");
    }

    log::info!("DB actor closing");
}

/// Returns a list of all tracked movie IDs for an instance
async fn get_all_movie_ids(
    executor: impl SqliteExecutor<'_>,
    instance_name: &str,
) -> sqlx::Result<Vec<TmdbId>> {
    query_scalar!(
        r#"SELECT tmdb_id AS "tmdb_id: TmdbId" FROM radarr WHERE instance_name = $1"#,
        instance_name
    )
    .fetch_all(executor)
    .await
}

/// Iterates over the list of movies and creates new rows for new entries
async fn sync_movies(
    pool: &SqlitePool,
    instance_name: &str,
    movies: &[Movie],
) -> anyhow::Result<()> {
    let mut tx = pool
        .begin()
        .await
        .with_context(|| "failed to start transaction")?;

    let db_movies = get_all_movie_ids(pool, instance_name)
        .await
        .with_context(|| "failed to fetch existing movies from db")?;

    // Find movies to delete (now unmonitored or unreferenced)
    let delete_ids = db_movies
        .into_iter()
        .filter(|id| {
            movies
                .iter()
                .find(|&movie| movie.tmdb_id == *id && movie.monitored)
                .is_none()
        })
        .collect::<Vec<_>>();

    let mut insert_errs = vec![];
    for movie in movies {
        if !movie.monitored {
            log::trace!("Skipping unmonitored {movie} from {instance_name}",);
            continue;
        }
        log::trace!("Inserting {movie} from {instance_name}");
        if let Err(e) = query!(
            "INSERT OR IGNORE
            INTO radarr (instance_name, tmdb_id)
            VALUES ($1, $2)",
            instance_name,
            movie.tmdb_id
        )
        .execute(&mut *tx)
        .await
        {
            insert_errs.push((movie, e));
        }
    }

    if !insert_errs.is_empty() {
        let msg = insert_errs
            .iter()
            .map(|(movie, e)| format!("{movie} - {e}"))
            .collect::<Vec<_>>()
            .join(", ");
        bail!("failed to insert the following movies: {msg}");
    }

    delete_movies(&mut *tx, instance_name, &delete_ids)
        .await
        .with_context(|| "failed to delete existing outdated movies")?;

    tx.commit().await.with_context(|| "failed to commit")?;
    Ok(())
}

/// Delete IDs from the db
async fn delete_movies(
    executor: impl SqliteExecutor<'_>,
    instance_name: &str,
    ids: &[TmdbId],
) -> sqlx::Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    // Expand n ? into (?, ?, ...)
    let placeholder = std::iter::repeat_n("?".to_owned(), ids.len())
        .collect::<Vec<_>>()
        .join(", ");

    // SAFETY: IDs can only be integers, and the user provided value is never directly added to the
    // string. No injection possible
    let sql = format!(
        "DELETE FROM radarr WHERE instance_name = ? AND tmdb_id IN ({})",
        placeholder
    );

    let mut query = sqlx::query(AssertSqlSafe(sql)).bind(instance_name);

    for id in ids {
        query = query.bind(id);
    }
    query.execute(executor).await?;
    Ok(())
}

/// Fetch `count` IDs, sorted by oldest `last_check` value first
async fn fetch_n_oldest_movies(
    executor: impl SqliteExecutor<'_>,
    instance_name: &str,
    count: u32,
) -> Vec<TmdbId> {
    match query_scalar!(
        r#"SELECT tmdb_id
        AS "tmdb_id: TmdbId"
        FROM radarr
        WHERE instance_name = $1
        ORDER BY last_check ASC
        LIMIT $2"#,
        instance_name,
        count
    )
    .fetch_all(executor)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            log::error!("Failed to query oldest movies");
            log::debug!("ERROR: querying oldest {count} movies for {instance_name}: {e}");
            Default::default()
        }
    }
}

async fn update_checked_timestamp(
    executor: impl SqliteExecutor<'_>,
    instance_name: &str,
    ids: &[TmdbId],
) -> anyhow::Result<()> {
    let timestamp = Utc::now();
    if ids.is_empty() {
        return Ok(());
    }

    // Expand n ? into (?, ?, ...)
    let placeholder = std::iter::repeat_n("?".to_owned(), ids.len())
        .collect::<Vec<_>>()
        .join(", ");

    // SAFETY: IDs can only be integers, and the user provided value is never directly added to the
    // string. No injection possible
    let sql = format!(
        "UPDATE radarr SET last_check = ? WHERE instance_name = ? AND tmdb_id IN ({})",
        placeholder
    );

    let mut query = sqlx::query(AssertSqlSafe(sql))
        .bind(timestamp)
        .bind(instance_name);

    for id in ids {
        query = query.bind(id);
    }

    query.execute(executor).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arr_api::radarr::api::RadarrId;

    use super::*;

    /// When adding new movies, ensure that "adding" an existing row doesn't wipe its existing timestamp
    #[sqlx::test]
    async fn test_sync_movies_doesnt_overwrite(pool: SqlitePool) {
        const INSTANCE_NAME: &str = "instance";
        let timestamp = Utc::now();
        query!(
            r#"
            INSERT INTO radarr (tmdb_id, instance_name, last_check)
            VALUES
            (1, $1, $2) -- newest
            "#,
            INSTANCE_NAME,
            timestamp
        )
        .execute(&pool)
        .await
        .unwrap();

        sync_movies(
            &pool,
            INSTANCE_NAME,
            &[
                Movie {
                    id: RadarrId(1),
                    tmdb_id: TmdbId(1),
                    title: "".into(),
                    monitored: true,
                },
                Movie {
                    id: RadarrId(2),
                    tmdb_id: TmdbId(2),
                    title: "".into(),
                    monitored: true,
                },
            ],
        )
        .await
        .unwrap();

        // Assert that 1 still has its timestamp, 2 is a new row with no timestamps
        let records = query!(r#"SELECT tmdb_id, last_check FROM radarr"#)
            .fetch_all(&pool)
            .await
            .unwrap();

        for record in records {
            match record.tmdb_id {
                // Still existing timestamp
                1 => assert_eq!(record.last_check, Some(timestamp.naive_utc())),
                2 => assert_eq!(record.last_check, None),
                n => panic!("Unexpected id {n}"),
            }
        }
    }

    /// When adding new movies, ensure that we don't add unmonitored movies
    #[sqlx::test]
    async fn test_sync_movies_doesnt_add_unmonitored(pool: SqlitePool) {
        const INSTANCE_NAME: &str = "instance";
        sync_movies(
            &pool,
            INSTANCE_NAME,
            &[
                Movie {
                    id: RadarrId(1),
                    tmdb_id: TmdbId(1),
                    title: "".into(),
                    monitored: true,
                },
                Movie {
                    id: RadarrId(2),
                    tmdb_id: TmdbId(2),
                    title: "".into(),
                    monitored: false,
                },
            ],
        )
        .await
        .unwrap();

        // Assert that 1 is in the DB (monitored), but 2 isn't (unmonitored)
        let records = query!(r#"SELECT tmdb_id, last_check FROM radarr"#)
            .fetch_all(&pool)
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        let record = records.first().unwrap();
        assert_eq!(record.tmdb_id, 1);
    }

    /// When adding new movies, ensure that we remove movies that are no longer monitored / no
    /// longer present
    #[sqlx::test]
    async fn test_sync_movies_clears_unmonitored_removed(pool: SqlitePool) {
        const INSTANCE_NAME: &str = "instance";
        query!(
            r#"
            INSERT INTO radarr (tmdb_id, instance_name, last_check)
            VALUES
            (1, $1, null),
            (2, $1, null),
            (3, $1, null)
            "#,
            INSTANCE_NAME,
        )
        .execute(&pool)
        .await
        .unwrap();

        sync_movies(
            &pool,
            INSTANCE_NAME,
            &[
                Movie {
                    id: RadarrId(1),
                    tmdb_id: TmdbId(1),
                    title: "".into(),
                    monitored: true,
                },
                Movie {
                    id: RadarrId(2),
                    tmdb_id: TmdbId(2),
                    title: "".into(),
                    monitored: false,
                },
            ],
        )
        .await
        .unwrap();

        // Assert that only id 1 still in db (2 removed because unmonitored, 3 removed because no
        // longer in movie list)
        let records = query!(r#"SELECT tmdb_id, last_check FROM radarr"#)
            .fetch_all(&pool)
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        let record = records.first().unwrap();
        assert_eq!(record.tmdb_id, 1);
    }

    /// When rows with both timestamps and no timestamps exist, the no timestamps rows should be
    /// returned first.
    #[sqlx::test]
    async fn test_fetch_oldest_prioritizes_null_timestamp(pool: SqlitePool) {
        const INSTANCE_NAME: &str = "instance";
        let oldest = Utc::now();
        let newest = oldest + Duration::from_secs(60);
        query!(
            r#"
            INSERT INTO radarr (tmdb_id, instance_name, last_check)
            VALUES
            (1, $1, $3),   -- newest
            (2, $1, $2),   -- oldest
            (3, $1, null), -- no time
            (4, $1, $3),   -- newest
            (5, $1, null)  -- no time
            "#,
            INSTANCE_NAME,
            oldest,
            newest
        )
        .execute(&pool)
        .await
        .unwrap();

        // Assert null before oldest before newest
        let ids = fetch_n_oldest_movies(&pool, INSTANCE_NAME, 5)
            .await
            .iter()
            .map(|id| id.0)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![3, 5, 2, 1, 4]);
    }

    /// When calling [update_checked_timestamp], ensure that both null timestamps and existing
    /// timestamps are updated to the newest value. Also ensure other rows are not modified.
    #[sqlx::test]
    async fn test_update_checked_timestamp_overwrites_provided(pool: SqlitePool) {
        const INSTANCE_NAME: &str = "instance";
        let old_timestamp = Utc::now() - Duration::from_mins(60);
        query!(
            r#"
            INSERT INTO radarr (tmdb_id, instance_name, last_check)
            VALUES
            (1, $1, $2),
            (2, $1, null),
            (3, $1, $2)
            "#,
            INSTANCE_NAME,
            old_timestamp
        )
        .execute(&pool)
        .await
        .unwrap();

        update_checked_timestamp(&pool, INSTANCE_NAME, &[TmdbId(1), TmdbId(2)])
            .await
            .unwrap();

        // Assert that 1 and 2 have the new timestamps, and that 3 still has the old timestamp
        let records = query!(
            r#"
            SELECT tmdb_id, last_check FROM radarr
            WHERE instance_name = $1
            "#,
            INSTANCE_NAME
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        for record in records {
            match record.tmdb_id {
                // No longer old
                1 => assert_ne!(record.last_check, Some(old_timestamp.naive_utc())),
                // No longer none
                2 => assert_ne!(record.last_check, None),
                // Still old (unchanged)
                3 => assert_eq!(record.last_check, Some(old_timestamp.naive_utc())),
                n => panic!("Unexpected movie id {n}"),
            }
        }
    }

    // TODO: deletion tests
}
