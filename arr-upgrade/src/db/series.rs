use std::collections::HashMap;

use anyhow::Context;
use arr_api::sonarr::api::{Series, TvdbId};
use chrono::Utc;
use sqlx::{QueryBuilder, Sqlite, SqliteExecutor, SqlitePool, query};

use crate::db::{DB_DATETIME_NULL_REPLACEMENT, TvMedia};

/// Returns a list of all seasons for an instance, formatted as a dictionary index by the TVDb ID of
/// the series.
async fn get_all_seasons(
    executor: impl SqliteExecutor<'_>,
    instance_name: &str,
) -> sqlx::Result<HashMap<TvdbId, Vec<u32>>> {
    let records = query!(
        r#"SELECT tvdb_id AS "tvdb_id: TvdbId", season AS "season: u32" FROM sonarr
        WHERE instance_name = $1"#,
        instance_name
    )
    .map(|r| (r.tvdb_id, r.season))
    .fetch_all(executor)
    .await?;

    // Map the individual seasons to a vec grouped by tvdb_id
    let mut series = HashMap::new();
    for (tvdb_id, season) in records {
        series.entry(tvdb_id).or_insert_with(Vec::new).push(season);
    }

    Ok(series)
}

pub async fn sync_series(
    pool: &SqlitePool,
    instance_name: &str,
    series: &[Series],
) -> anyhow::Result<()> {
    let mut tx = pool
        .begin()
        .await
        .with_context(|| "failed to start transaction")?;

    let monitored_series = filter_monitored_seasons(series);
    let db_series = get_all_seasons(&mut *tx, instance_name).await?;

    // Find series/seasons in the monitored payload but not in the DB
    for (&id, seasons) in monitored_series.iter() {
        match db_series.get(&id) {
            Some(db_seasons) => {
                // Exclude seasons already in DB
                let missing_seasons = seasons
                    .iter()
                    .filter(|&monitored_season| !db_seasons.contains(monitored_season))
                    .cloned()
                    .collect::<Vec<_>>();
                insert_seasons(&mut *tx, instance_name, id, &missing_seasons).await?;
            }
            None => {
                // Insert all seasons into db
                insert_seasons(&mut *tx, instance_name, id, seasons).await?;
            }
        }
    }

    // Find series/seasons in the DB but not in the monitored payload
    for (series_id, db_seasons) in db_series {
        match monitored_series.get(&series_id) {
            Some(monitored_seasons) => {
                let deleted_seasons = db_seasons
                    .iter()
                    .filter(|&db_season| !monitored_seasons.contains(db_season))
                    .cloned()
                    .collect::<Vec<_>>();
                delete_seasons(&mut *tx, instance_name, series_id, &deleted_seasons).await?;
            }
            None => {
                // Not monitored at all, delete whole series
                delete_seasons(&mut *tx, instance_name, series_id, &db_seasons).await?;
            }
        }
    }

    tx.commit().await?;
    Ok(())
}

/// Convert a list of [Series] to a `HashMap<TvdbId, Vec<u32>`, with the list of `u32` corresponding
/// to the monitored season of the *monitored* series.
fn filter_monitored_seasons(series: &[Series]) -> HashMap<TvdbId, Vec<u32>> {
    let mut result = HashMap::new();
    for serie in series.iter().filter(|s| s.monitored) {
        let monitored_seasons: Vec<_> = serie
            .seasons
            .iter()
            .filter(|s| s.monitored)
            .map(|s| s.season_number)
            .collect();
        if !monitored_seasons.is_empty() {
            result.insert(serie.tvdb_id, monitored_seasons);
        }
    }
    result
}

async fn insert_seasons(
    executor: impl SqliteExecutor<'_>,
    instance_name: &str,
    tvdb_id: TvdbId,
    seasons: &[u32],
) -> sqlx::Result<()> {
    if seasons.is_empty() {
        return Ok(());
    }

    let mut query_builder: QueryBuilder<Sqlite> =
        QueryBuilder::new("INSERT INTO sonarr (tvdb_id, season, instance_name)");
    query_builder.push_values(seasons, |mut b, season| {
        b.push_bind(tvdb_id)
            .push_bind(season)
            .push_bind(instance_name);
    });

    let query = query_builder.build();
    query.execute(executor).await?;
    Ok(())
}

/// Delete a list of seasons for a specified series
async fn delete_seasons(
    executor: impl SqliteExecutor<'_>,
    instance_name: &str,
    tvdb_id: TvdbId,
    seasons: &[u32],
) -> sqlx::Result<()> {
    if seasons.is_empty() {
        // No seasons to delete
        return Ok(());
    }

    let mut query_builder = QueryBuilder::new("DELETE FROM sonarr WHERE instance_name = ");
    query_builder.push_bind(instance_name);
    query_builder.push(" AND tvdb_id = ");
    query_builder.push_bind(tvdb_id);
    query_builder.push(" AND season IN (");
    let mut separated = query_builder.separated(", ");
    for season in seasons {
        separated.push_bind(season);
    }
    separated.push_unseparated(")");

    let query = query_builder.build();
    query.execute(executor).await?;
    Ok(())
}

pub async fn mark_tv_checked(
    pool: &SqlitePool,
    instance_name: &str,
    check: TvMedia,
) -> anyhow::Result<()> {
    match check {
        TvMedia::Series { id } => update_timestamp_series(pool, instance_name, id).await,
        TvMedia::Season { id, season } => {
            update_timestamp_season(pool, instance_name, id, season).await
        }
    }
    .with_context(|| "failed to update timestamp in DB")
}

async fn update_timestamp_season(
    executor: impl SqliteExecutor<'_>,
    instance_name: &str,
    tvdb_id: TvdbId,
    season: u32,
) -> sqlx::Result<()> {
    let timestamp = Utc::now();
    query!(
        r#"UPDATE sonarr SET last_check = $1
        WHERE instance_name = $2 AND tvdb_id = $3 AND season = $4"#,
        timestamp,
        instance_name,
        tvdb_id,
        season
    )
    .execute(executor)
    .await?;
    Ok(())
}

async fn update_timestamp_series(
    executor: impl SqliteExecutor<'_>,
    instance_name: &str,
    tvdb_id: TvdbId,
) -> sqlx::Result<()> {
    let timestamp = Utc::now();
    query!(
        r#"UPDATE sonarr SET last_check = $1
        WHERE instance_name = $2 AND tvdb_id = $3"#,
        timestamp,
        instance_name,
        tvdb_id,
    )
    .execute(executor)
    .await?;
    Ok(())
}

pub async fn get_oldest_seasons(
    pool: &SqlitePool,
    instance_name: &str,
    count: u32,
) -> anyhow::Result<Vec<(TvdbId, u32)>> {
    query!(
        r#"SELECT tvdb_id AS "tvdb_id: TvdbId", season AS "season: u32"
        FROM sonarr
        WHERE instance_name = $1
        ORDER BY last_check ASC
        LIMIT $2"#,
        instance_name,
        &count
    )
    .map(|r| (r.tvdb_id, r.season))
    .fetch_all(pool)
    .await
    .with_context(|| "failed querying database")
}

pub async fn get_oldest_series(
    pool: &SqlitePool,
    instance_name: &str,
    count: u32,
) -> anyhow::Result<Vec<TvdbId>> {
    query!(
        r#"
        SELECT tvdb_id AS "tvdb_id!: TvdbId", MIN(COALESCE(last_check, $1)) AS last_check
        FROM sonarr
        WHERE instance_name = $2
        GROUP BY tvdb_id
        ORDER BY last_check ASC
        LIMIT $3
        "#,
        DB_DATETIME_NULL_REPLACEMENT,
        instance_name,
        count
    )
    .map(|r| r.tvdb_id)
    .fetch_all(pool)
    .await
    .with_context(|| "failed querying database")
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use arr_api::sonarr::api::{Season, SonarrId};
    use chrono::Utc;
    use sqlx::{SqlitePool, query_scalar};

    use super::*;

    const INSTANCE_NAME: &str = "instance";
    const TVDB_ID: TvdbId = TvdbId(1);

    #[sqlx::test]
    async fn get_all_seasons_groups_by_id(pool: SqlitePool) {
        query!(
            r#"
            INSERT INTO sonarr (tvdb_id, season, instance_name, last_check)
            VALUES
            (123, 1, $1, null),
            (123, 2, $1, null),
            (999, 2, $1, null),
            (123, 5, $1, null),
            (999, 1, $1, null)
            "#,
            INSTANCE_NAME
        )
        .execute(&pool)
        .await
        .unwrap();

        let series = get_all_seasons(&pool, INSTANCE_NAME).await.unwrap();
        assert_eq!(series.len(), 2);

        let first: HashSet<u32> = series
            .get(&TvdbId(123))
            .expect("Entry 123 should exist")
            .iter()
            .cloned()
            .collect();
        let expected_123: HashSet<u32> = [1, 2, 5].into_iter().collect();
        assert_eq!(first, expected_123);

        let second: HashSet<u32> = series
            .get(&TvdbId(999))
            .expect("Entry 999 should exist")
            .iter()
            .cloned()
            .collect();
        let expected_999: HashSet<u32> = [1, 2].into_iter().collect();
        assert_eq!(second, expected_999);
    }

    #[sqlx::test]
    async fn sync_series_adds_new_series(pool: SqlitePool) {
        const ID: TvdbId = TvdbId(1);
        let series = [Series {
            id: SonarrId(0),
            tvdb_id: ID,
            title: "".into(),
            monitored: true,
            seasons: vec![Season {
                season_number: 1,
                monitored: true,
            }],
        }];

        sync_series(&pool, INSTANCE_NAME, &series).await.unwrap();

        let series = get_all_seasons(&pool, INSTANCE_NAME).await.unwrap();
        let seasons = series.get(&ID).expect("entry should exsit");
        assert_eq!(seasons, &[1]);
    }

    #[sqlx::test]
    async fn sync_series_adds_new_seasons_to_existing_series(pool: SqlitePool) {
        const ID: TvdbId = TvdbId(1);

        // Insert existing series
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name) VALUES
            ($1, 1, $2), ($1, 2, $2)",
            ID,
            INSTANCE_NAME
        )
        .execute(&pool)
        .await
        .unwrap();

        // Insert extra seasons
        let series = [Series {
            id: SonarrId(0),
            tvdb_id: ID,
            title: "".into(),
            monitored: true,
            seasons: vec![
                Season {
                    season_number: 1,
                    monitored: true,
                },
                Season {
                    season_number: 2,
                    monitored: true,
                },
                Season {
                    season_number: 3,
                    monitored: true,
                },
                Season {
                    season_number: 4,
                    monitored: true,
                },
            ],
        }];

        sync_series(&pool, INSTANCE_NAME, &series).await.unwrap();

        let series = get_all_seasons(&pool, INSTANCE_NAME).await.unwrap();
        let seasons = series.get(&ID).expect("entry should exsit");
        assert_eq!(seasons, &[1, 2, 3, 4]);
    }

    #[sqlx::test]
    async fn sync_series_keeps_existing_timestamp(pool: SqlitePool) {
        const ID: TvdbId = TvdbId(1);

        let timestamp = Utc::now();
        // Insert existing series
        query!(
            "INSERT INTO sonarr (tvdb_id, season, last_check, instance_name) VALUES
            ($1, 1, $2, $3)",
            ID,
            timestamp,
            INSTANCE_NAME
        )
        .execute(&pool)
        .await
        .unwrap();

        // Insert extra seasons
        let series = [Series {
            id: SonarrId(0),
            tvdb_id: ID,
            title: "".into(),
            monitored: true,
            seasons: vec![
                Season {
                    season_number: 1,
                    monitored: true,
                },
                Season {
                    season_number: 2,
                    monitored: true,
                },
            ],
        }];
        sync_series(&pool, INSTANCE_NAME, &series).await.unwrap();

        let series = get_all_seasons(&pool, INSTANCE_NAME).await.unwrap();
        let mut seasons = series.get(&ID).expect("entry should exist").clone();
        seasons.sort();
        assert_eq!(seasons, &[1, 2]);

        // Assert that DB still has timestamp for pre-existing season
        let db_timestamp = query_scalar!(
            r#"
            SELECT last_check FROM sonarr WHERE
            instance_name = $1 AND tvdb_id = $2 AND season = 1"#,
            INSTANCE_NAME,
            ID
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(db_timestamp, Some(timestamp.naive_utc()));
    }

    #[sqlx::test]
    async fn sync_series_deletes_unmonitored_season(pool: SqlitePool) {
        const ID: TvdbId = TvdbId(1);

        // Insert existing series
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name) VALUES
            ($1, 1, $2), ($1, 2, $2)",
            ID,
            INSTANCE_NAME
        )
        .execute(&pool)
        .await
        .unwrap();

        // Season two now unmonitored
        let series = [Series {
            id: SonarrId(0),
            tvdb_id: ID,
            title: "".into(),
            monitored: true,
            seasons: vec![
                Season {
                    season_number: 1,
                    monitored: true,
                },
                Season {
                    season_number: 2,
                    monitored: false,
                },
            ],
        }];
        sync_series(&pool, INSTANCE_NAME, &series).await.unwrap();

        let series = get_all_seasons(&pool, INSTANCE_NAME).await.unwrap();
        let seasons = series.get(&ID).expect("entry should exsit");
        assert_eq!(seasons, &[1]);
    }

    #[sqlx::test]
    async fn sync_series_deletes_unmonitored_series(pool: SqlitePool) {
        const ID: TvdbId = TvdbId(1);

        // Insert existing series
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name) VALUES
            ($1, 1, $2), ($1, 2, $2)",
            ID,
            INSTANCE_NAME
        )
        .execute(&pool)
        .await
        .unwrap();

        // Whole series is now unmonitored
        let series = [Series {
            id: SonarrId(0),
            tvdb_id: ID,
            title: "".into(),
            monitored: false,
            seasons: vec![
                Season {
                    season_number: 1,
                    monitored: true,
                },
                Season {
                    season_number: 2,
                    monitored: true,
                },
            ],
        }];
        sync_series(&pool, INSTANCE_NAME, &series).await.unwrap();

        let series = get_all_seasons(&pool, INSTANCE_NAME).await.unwrap();
        assert!(!series.contains_key(&ID));
    }

    #[sqlx::test]
    async fn sync_series_deletes_removed_season(pool: SqlitePool) {
        const ID: TvdbId = TvdbId(1);

        // Insert existing series
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name) VALUES
            ($1, 1, $2), ($1, 2, $2)",
            ID,
            INSTANCE_NAME
        )
        .execute(&pool)
        .await
        .unwrap();

        // Season two no longer exists
        let series = [Series {
            id: SonarrId(0),
            tvdb_id: ID,
            title: "".into(),
            monitored: true,
            seasons: vec![Season {
                season_number: 1,
                monitored: true,
            }],
        }];
        sync_series(&pool, INSTANCE_NAME, &series).await.unwrap();

        let series = get_all_seasons(&pool, INSTANCE_NAME).await.unwrap();
        let seasons = series.get(&ID).expect("entry should exsit");
        assert_eq!(seasons, &[1]);
    }

    #[sqlx::test]
    async fn sync_series_deletes_removed_series(pool: SqlitePool) {
        const ID: TvdbId = TvdbId(1);

        // Insert existing series
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name) VALUES
            ($1, 1, $2), ($1, 2, $2)",
            ID,
            INSTANCE_NAME
        )
        .execute(&pool)
        .await
        .unwrap();

        // Series no longer exists
        let series = [];
        sync_series(&pool, INSTANCE_NAME, &series).await.unwrap();

        let series = get_all_seasons(&pool, INSTANCE_NAME).await.unwrap();
        assert!(series.is_empty());
    }

    /// When attempting to insert an iterator with 0 items, don't return an error
    #[sqlx::test]
    async fn insert_seasons_with_empty_seasons_works(pool: SqlitePool) {
        let seasons = &[];
        insert_seasons(&pool, "foo", TVDB_ID, seasons)
            .await
            .expect("call should not return an error");
    }

    /// When attempting to delete seasons with an iterator with 0 items, don't return an error
    #[sqlx::test]
    async fn delete_seasons_with_empty_seasons_works(pool: SqlitePool) {
        let seasons = &[];
        delete_seasons(&pool, "foo", TVDB_ID, seasons)
            .await
            .expect("call should not return an error");
    }

    #[sqlx::test]
    async fn mark_tv_checked_for_season_with_no_previous_timestamp_works(pool: SqlitePool) {
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name, last_check) VALUES
            ($1, 1, $2, null)",
            TVDB_ID,
            INSTANCE_NAME,
        )
        .execute(&pool)
        .await
        .unwrap();
        mark_tv_checked(
            &pool,
            INSTANCE_NAME,
            TvMedia::Season {
                id: TVDB_ID,
                season: 1,
            },
        )
        .await
        .unwrap();

        let row = query!(
            "SELECT season, last_check FROM sonarr WHERE instance_name = $1 AND tvdb_id = $2 AND season = 1",
            INSTANCE_NAME,
            TVDB_ID
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(row.last_check.is_some());
    }

    #[sqlx::test]
    async fn mark_tv_checked_for_season_with_existing_timestamp_updates_timestamp(
        pool: SqlitePool,
    ) {
        let old_timestamp = Utc::now() - Duration::from_secs(60);
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name, last_check) VALUES
            ($1, 1, $2, $3)",
            TVDB_ID,
            INSTANCE_NAME,
            old_timestamp
        )
        .execute(&pool)
        .await
        .unwrap();
        mark_tv_checked(
            &pool,
            INSTANCE_NAME,
            TvMedia::Season {
                id: TVDB_ID,
                season: 1,
            },
        )
        .await
        .unwrap();

        let row = query!(
            "SELECT season, last_check FROM sonarr WHERE instance_name = $1 AND tvdb_id = $2 AND season = 1",
            INSTANCE_NAME,
            TVDB_ID
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_ne!(row.last_check, Some(old_timestamp.naive_utc()));
    }

    /// If a series has multiple season, and we're updating the time stamp for one season, then the
    /// other season shall keep its old timestamp
    #[sqlx::test]
    async fn mark_tv_checked_doesnt_overwrite_unreferenced_season(pool: SqlitePool) {
        let old_timestamp = Utc::now() - Duration::from_secs(60);
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name, last_check) VALUES
            ($1, 1, $2, $3), ($1, 2, $2, $3)",
            TVDB_ID,
            INSTANCE_NAME,
            old_timestamp
        )
        .execute(&pool)
        .await
        .unwrap();
        mark_tv_checked(
            &pool,
            INSTANCE_NAME,
            TvMedia::Season {
                id: TVDB_ID,
                season: 1,
            },
        )
        .await
        .unwrap();

        // Fetch unchanged row
        let row = query!(
            "SELECT season, last_check FROM sonarr WHERE instance_name = $1 AND tvdb_id = $2 AND SEASON = 2",
            INSTANCE_NAME,
            TVDB_ID
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.last_check, Some(old_timestamp.naive_utc()));
    }

    #[sqlx::test]
    async fn mark_tv_checked_series_updates_all_seasons(pool: SqlitePool) {
        let old_timestamp = Utc::now() - Duration::from_secs(60);
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name, last_check) VALUES
            ($1, 1, $2, null), ($1, 2, $2, $3)",
            TVDB_ID,
            INSTANCE_NAME,
            old_timestamp
        )
        .execute(&pool)
        .await
        .unwrap();

        mark_tv_checked(&pool, INSTANCE_NAME, TvMedia::Series { id: TVDB_ID })
            .await
            .unwrap();

        // Get rows, ensure all have new timestamp
        let rows = query!(
            "SELECT season, last_check FROM sonarr WHERE instance_name = $1 AND tvdb_id = $2",
            INSTANCE_NAME,
            TVDB_ID
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(rows.len(), 2);

        for row in rows {
            match row.season {
                // No longer null
                1 => assert!(row.last_check.is_some()),
                // No longer old
                2 => assert_ne!(row.last_check, Some(old_timestamp.naive_utc())),
                n => panic!("unexpected season number {n}"),
            }
        }
    }

    /// If the last_check value is still null, that row must be considered older
    /// than any row with a timestamp
    #[sqlx::test]
    async fn get_oldest_seasons_prioritizes_null(pool: SqlitePool) {
        let timestamp = Utc::now();
        let old_timestamp = timestamp - Duration::from_secs(60);
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name, last_check) VALUES
            (1, 1, $1, $2), (2, 1, $1, $3), (2, 2, $1, null)",
            INSTANCE_NAME,
            timestamp,
            old_timestamp
        )
        .execute(&pool)
        .await
        .unwrap();

        let oldest = get_oldest_seasons(&pool, INSTANCE_NAME, 2).await.unwrap();
        assert_eq!(oldest.len(), 2);

        // Oldest should return the null entry first, then the `old_timestamp` entry
        let first = oldest.first().unwrap();
        assert_eq!(first, &(TvdbId(2), 2));

        let second = oldest.get(1).unwrap();
        assert_eq!(second, &(TvdbId(2), 1));
    }

    #[sqlx::test]
    async fn get_oldest_seasons_has_correct_order(pool: SqlitePool) {
        let timestamp1 = Utc::now();
        let timestamp2 = timestamp1 + Duration::from_secs(60);
        let timestamp3 = timestamp2 + Duration::from_secs(60);
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name, last_check) VALUES
            (1, 1, $1, $3), (2, 1, $1, $4), (2, 2, $1, $2)",
            INSTANCE_NAME,
            timestamp1,
            timestamp2,
            timestamp3,
        )
        .execute(&pool)
        .await
        .unwrap();

        let oldest = get_oldest_seasons(&pool, INSTANCE_NAME, 3).await.unwrap();
        assert_eq!(oldest.len(), 3);
        assert_eq!(&oldest, &[(TvdbId(2), 2), (TvdbId(1), 1), (TvdbId(2), 1)]);
    }

    #[sqlx::test]
    async fn get_oldest_series_has_correct_order(pool: SqlitePool) {
        let timestamp1 = Utc::now();
        let timestamp2 = timestamp1 + Duration::from_secs(60);
        let timestamp3 = timestamp2 + Duration::from_secs(60);
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name, last_check) VALUES
            (1, 1, $1, $3), (2, 1, $1, $4), (3, 1, $1, $2)",
            INSTANCE_NAME,
            timestamp1,
            timestamp2,
            timestamp3,
        )
        .execute(&pool)
        .await
        .unwrap();

        let oldest = get_oldest_series(&pool, INSTANCE_NAME, 3).await.unwrap();
        assert_eq!(oldest.len(), 3);
        assert_eq!(&oldest, &[TvdbId(3), TvdbId(1), TvdbId(2)]);
    }

    /// If a series has a season at timestamp 1 and 2, then the whole series is considered to have a
    /// timestamp of `1` since that is the oldest season.
    #[sqlx::test]
    async fn get_oldest_series_prefers_oldest_season(pool: SqlitePool) {
        // Create series 2 with timestamp 1,3 and series 1 with timestamp 2.
        // Since 2 has the oldest timestamp, it should come before 2
        let timestamp1 = Utc::now();
        let timestamp2 = timestamp1 + Duration::from_secs(60);
        let timestamp3 = timestamp2 + Duration::from_secs(60);
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name, last_check) VALUES
            (1, 1, $1, $3), (2, 1, $1, $4), (2, 2, $1, $2)",
            INSTANCE_NAME,
            timestamp1,
            timestamp2,
            timestamp3,
        )
        .execute(&pool)
        .await
        .unwrap();

        let oldest = get_oldest_series(&pool, INSTANCE_NAME, 2).await.unwrap();
        assert_eq!(oldest.len(), 2);
        assert_eq!(&oldest, &[TvdbId(2), TvdbId(1)]);
    }

    /// If a series has a season with a null timestamp, that should be considered over other timestamps
    #[sqlx::test]
    async fn get_oldest_series_handles_null(pool: SqlitePool) {
        // Give series 1 timestamp1, series 2 timestamp1 and null
        let timestamp1 = Utc::now();
        let timestamp2 = timestamp1 + Duration::from_secs(60);
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name, last_check) VALUES
            (1, 1, $1, $2), (2, 1, $1, $3), (2, 2, $1, null)",
            INSTANCE_NAME,
            timestamp1,
            timestamp2,
        )
        .execute(&pool)
        .await
        .unwrap();

        let oldest = get_oldest_series(&pool, INSTANCE_NAME, 2).await.unwrap();
        assert_eq!(oldest.len(), 2);
        assert_eq!(&oldest, &[TvdbId(2), TvdbId(1)]);
    }
}
