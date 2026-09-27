use std::collections::HashMap;

use anyhow::Context;
use arr_api::sonarr::api::{Series, TvdbId};
use sqlx::{QueryBuilder, Sqlite, SqliteExecutor, SqlitePool, query};

/// Returns a list of all seasons for an instance, formatted as a dictionary index by the TVDb ID of
/// the series.
pub async fn get_all_seasons(
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

pub async fn insert_seasons(
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
pub async fn delete_seasons(
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arr_api::sonarr::api::{Season, SonarrId};
    use chrono::Utc;
    use sqlx::{SqlitePool, query_scalar};

    use super::*;

    const INSTANCE_NAME: &str = "instance";

    #[sqlx::test]
    async fn get_all_seasons_groups_by_id(pool: SqlitePool) {
        const INSTANCE_NAME: &str = "instance";
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
        insert_seasons(&pool, "foo", TvdbId(0), seasons)
            .await
            .expect("call should not return an error");
    }

    #[sqlx::test]
    async fn insert_seasons_works_with_existing_seasons(pool: SqlitePool) {
        const INSTANCE_NAME: &str = "instance";
        const TVDB_ID: TvdbId = TvdbId(1);
        // Create pre-existing entries
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name) VALUES ($1, $2, $3)",
            TvdbId(1),
            1,
            INSTANCE_NAME
        )
        .execute(&pool)
        .await
        .unwrap();

        // Insert more seasons
        insert_seasons(&pool, INSTANCE_NAME, TVDB_ID, &[2, 3, 5])
            .await
            .unwrap();

        // Check inserted and pre-existing seasons still exist
        let all_seasons = get_all_seasons(&pool, INSTANCE_NAME).await.unwrap();
        let seasons = all_seasons.get(&TVDB_ID).expect("Entry should exist");
        assert_eq!(seasons, &vec![1, 2, 3, 5]);
    }

    /// When attempting to delete seasons with an iterator with 0 items, don't return an error
    #[sqlx::test]
    async fn delete_seasons_with_empty_seasons_works(pool: SqlitePool) {
        let seasons = &[];
        delete_seasons(&pool, "foo", TvdbId(0), seasons)
            .await
            .expect("call should not return an error");
    }

    #[sqlx::test]
    async fn delete_seasons_works(pool: SqlitePool) {
        const INSTANCE_NAME: &str = "instance";
        const TVDB_ID: TvdbId = TvdbId(1);
        // Create pre-existing entries
        query!(
            "INSERT INTO sonarr (tvdb_id, season, instance_name) VALUES
            ($2, 1, $1),
            ($2, 2, $1),
            ($2, 3, $1),
            ($2, 4, $1),
            ($2, 5, $1)
            ",
            INSTANCE_NAME,
            TvdbId(1),
        )
        .execute(&pool)
        .await
        .unwrap();

        // Delete some seasons
        delete_seasons(&pool, INSTANCE_NAME, TVDB_ID, &[2, 4])
            .await
            .unwrap();

        // Check undeleted seasons still remain
        let all_seasons = get_all_seasons(&pool, INSTANCE_NAME).await.unwrap();
        let seasons = all_seasons.get(&TVDB_ID).expect("Entry should exist");
        assert_eq!(seasons, &vec![1, 3, 5]);
    }
}
