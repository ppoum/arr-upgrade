CREATE TABLE radarr (
        instance_name TEXT NOT NULL,
        movie_id INT NOT NULL,
        last_check DATETIME,
        PRIMARY KEY (instance_name, movie_id)
) WITHOUT ROWID;

-- Index sorted by oldest timestamp
CREATE INDEX idx_radarr_instance_last_check ON radarr (instance_name, last_check);
