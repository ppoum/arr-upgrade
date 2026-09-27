use std::collections::HashMap;

use arr_api::sonarr::api::TvdbId;
use sqlx::{QueryBuilder, Sqlite, SqliteExecutor, query};

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

    use sqlx::SqlitePool;

    use super::*;

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
