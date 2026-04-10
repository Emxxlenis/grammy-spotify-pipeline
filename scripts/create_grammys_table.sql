-- Source table schema in grammys_db
-- Stores the raw Grammy Awards dataset (simulates an OLTP source)
CREATE TABLE IF NOT EXISTS grammy_awards (
    id       SERIAL PRIMARY KEY,
    year     INTEGER,
    title    TEXT,
    category TEXT,
    nominee  TEXT,
    artist   TEXT,
    workers  TEXT,
    img      TEXT,
    winner   BOOLEAN
);
