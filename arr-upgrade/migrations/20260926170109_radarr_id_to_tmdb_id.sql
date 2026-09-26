-- Previous table used the internal radarr ID, but this ID isn't stable enough. Drop all
-- data and create fresh table to force repopulation with new, valid IDs

DROP TABLE IF EXISTS radarr;

CREATE TABLE radarr (
        instance_name TEXT NOT NULL,
        tmdb_id INT NOT NULL,
        last_check DATETIME,
        PRIMARY KEY (instance_name, tmdb_id)
) WITHOUT ROWID;

-- Index sorted by oldest timestamp
CREATE INDEX idx_radarr_instance_last_check ON radarr (instance_name, last_check);
