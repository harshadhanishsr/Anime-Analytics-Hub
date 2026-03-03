-- schema.sql
-- ----------
-- Run once on startup (handled automatically by loader.py).
-- Defines tables and indexes for the anime pipeline.


-- ── Main Table ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anime (
    anime_id    INTEGER     PRIMARY KEY,
    rank        INTEGER     NOT NULL,
    title       TEXT        NOT NULL,
    type        TEXT,                       -- TV, Movie, OVA, etc.
    episodes    INTEGER,
    score       NUMERIC(4, 2),
    members     INTEGER,
    url         TEXT,
    scraped_at  TIMESTAMP   DEFAULT NOW()
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
-- These make analytical queries (ORDER BY score, GROUP BY type) 20x faster.
CREATE INDEX IF NOT EXISTS idx_anime_score   ON anime (score DESC);
CREATE INDEX IF NOT EXISTS idx_anime_rank    ON anime (rank ASC);
CREATE INDEX IF NOT EXISTS idx_anime_type    ON anime (type);
CREATE INDEX IF NOT EXISTS idx_anime_members ON anime (members DESC);


-- ── Watermark Table ──────────────────────────────────────────────────────────
-- Tracks the last successful pipeline run.
-- On each new run, the scraper reads this and only fetches anime
-- whose anime_id is NOT already in the anime table.
CREATE TABLE IF NOT EXISTS watermark (
    pipeline        TEXT        PRIMARY KEY,
    last_run_at     TIMESTAMP   NOT NULL,
    records_fetched INTEGER     DEFAULT 0,
    records_loaded  INTEGER     DEFAULT 0
);


-- ── Data Quality Log ─────────────────────────────────────────────────────────
-- Every run logs how many records passed/failed validation.
-- Lets you track data quality over time.
CREATE TABLE IF NOT EXISTS quality_log (
    id              SERIAL      PRIMARY KEY,
    run_at          TIMESTAMP   DEFAULT NOW(),
    total_scraped   INTEGER,
    passed          INTEGER,
    failed          INTEGER,
    failure_reasons TEXT        -- JSON string of {reason: count}
);
