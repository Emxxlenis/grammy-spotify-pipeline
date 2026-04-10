-- ============================================================
-- Star Schema — Data Warehouse
-- Workshop 2: ETL with Spotify + Grammy Awards
--
-- Fact grain: one row per (artist × track × year × award category)
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id   SERIAL PRIMARY KEY,
    artist_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_track (
    track_id         VARCHAR(100) PRIMARY KEY,
    track_name       TEXT,
    album_name       TEXT,
    genre            TEXT,
    explicit         BOOLEAN,
    duration_ms      INTEGER,
    danceability     FLOAT,
    energy           FLOAT,
    loudness         FLOAT,
    speechiness      FLOAT,
    acousticness     FLOAT,
    instrumentalness FLOAT,
    liveness         FLOAT,
    valence          FLOAT,
    tempo            FLOAT
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    year    INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_award (
    award_id       SERIAL PRIMARY KEY,
    category       TEXT NOT NULL,
    ceremony_title TEXT,
    nominee_name   TEXT
);

CREATE TABLE IF NOT EXISTS fact_music (
    fact_id      SERIAL PRIMARY KEY,
    artist_id    INTEGER REFERENCES dim_artist(artist_id),
    track_id     VARCHAR(100) REFERENCES dim_track(track_id),
    time_id      INTEGER REFERENCES dim_time(time_id),
    award_id     INTEGER REFERENCES dim_award(award_id),
    popularity   INTEGER,
    is_winner    BOOLEAN DEFAULT FALSE,
    is_nominated BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_fact_artist ON fact_music(artist_id);
CREATE INDEX IF NOT EXISTS idx_fact_time   ON fact_music(time_id);
CREATE INDEX IF NOT EXISTS idx_fact_award  ON fact_music(award_id);
CREATE INDEX IF NOT EXISTS idx_fact_winner ON fact_music(is_winner);
