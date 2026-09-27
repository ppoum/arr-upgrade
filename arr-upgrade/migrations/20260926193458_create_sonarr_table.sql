-- Create the table used by the sonarr instances

CREATE TABLE sonarr (
        instance_name TEXT NOT NULL,
        tvdb_id INT NOT NULL,
        season INT NOT NULL,
        last_check DATETIME,
        PRIMARY KEY (instance_name, tvdb_id, season)
) WITHOUT ROWID;

-- Index sorted by oldest timestamp
CREATE INDEX idx_sonarr_instance_last_check ON sonarr (instance_name, last_check);
