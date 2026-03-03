"""
pipeline/loader.py
-------------------
Step 3 of the ETL pipeline.

Features:
- Creates schema if not exists
- Bulk upsert (no duplicates ever)
- Logs quality metrics
- Updates watermark
- Fully Jikan API compatible
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, UTC
from sqlalchemy import create_engine, text

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config


# ── Schema Setup ──────────────────────────────────────────────────────────────
def create_schema(engine):
    schema_path = os.path.join(os.path.dirname(__file__), "..", "sql", "schema.sql")

    with open(schema_path, "r") as f:
        sql = f.read()

    with engine.begin() as conn:  # auto commit transaction
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))

    print("[loader] Schema and indexes ready.")


# ── Upsert (Bulk + Idempotent) ────────────────────────────────────────────────
UPSERT_SQL = """
    INSERT INTO anime (anime_id, rank, title, type, episodes, score, members, url, scraped_at)
    VALUES (:anime_id, :rank, :title, :type, :episodes, :score, :members, :url, :scraped_at)
    ON CONFLICT (anime_id) DO UPDATE SET
        rank       = EXCLUDED.rank,
        title      = EXCLUDED.title,
        type       = EXCLUDED.type,
        episodes   = EXCLUDED.episodes,
        score      = EXCLUDED.score,
        members    = EXCLUDED.members,
        url        = EXCLUDED.url,
        scraped_at = EXCLUDED.scraped_at;
"""


def upsert_anime(clean_df: pd.DataFrame, engine) -> int:
    if clean_df is None or clean_df.empty:
        print("[loader] No clean records to load.")
        return 0

    # Ensure URL column exists (Jikan safe)
    if "url" not in clean_df.columns:
        clean_df["url"] = None

    # Replace pandas NA with None
    records = (
        clean_df.where(pd.notnull(clean_df), None)
        .to_dict(orient="records")
    )

    with engine.begin() as conn:
        conn.execute(text(UPSERT_SQL), records)

    print(f"[loader] Upserted {len(records)} records into anime table.")
    return len(records)


# ── Quality Log ───────────────────────────────────────────────────────────────
def log_quality(clean_df: pd.DataFrame, rejected_df: pd.DataFrame, engine):
    rejected_df = rejected_df if rejected_df is not None else pd.DataFrame()

    total  = len(clean_df) + len(rejected_df)
    passed = len(clean_df)
    failed = len(rejected_df)

    reasons = {}
    if not rejected_df.empty and "rejection_reason" in rejected_df.columns:
        for reason in rejected_df["rejection_reason"]:
            for r in str(reason).split(", "):
                reasons[r] = reasons.get(r, 0) + 1

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO quality_log (run_at, total_scraped, passed, failed, failure_reasons)
            VALUES (NOW(), :total, :passed, :failed, :reasons)
        """), {
            "total":   total,
            "passed":  passed,
            "failed":  failed,
            "reasons": json.dumps(reasons),
        })

    print(f"[loader] Quality log: {passed}/{total} passed ({failed} rejected).")


# ── Watermark ─────────────────────────────────────────────────────────────────
def update_watermark(records_fetched: int, records_loaded: int, engine):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO watermark (pipeline, last_run_at, records_fetched, records_loaded)
            VALUES ('anime_etl', NOW(), :fetched, :loaded)
            ON CONFLICT (pipeline) DO UPDATE SET
                last_run_at     = EXCLUDED.last_run_at,
                records_fetched = EXCLUDED.records_fetched,
                records_loaded  = EXCLUDED.records_loaded;
        """), {
            "fetched": records_fetched,
            "loaded":  records_loaded
        })

    print(f"[loader] Watermark updated at {datetime.now(UTC).isoformat()}")


# ── Main Entry ────────────────────────────────────────────────────────────────
def run(clean_df: pd.DataFrame = None, rejected_df: pd.DataFrame = None):
    engine = create_engine(config.DB_URL)

    # Ensure schema exists
    create_schema(engine)

    # If standalone run, execute transform
    if clean_df is None:
        from pipeline.transform import run as transform_run
        clean_df, rejected_df = transform_run()

    # Upsert
    loaded = upsert_anime(clean_df, engine)

    # Quality log
    log_quality(
        clean_df,
        rejected_df if rejected_df is not None else pd.DataFrame(),
        engine
    )

    # Watermark
    total_fetched = len(clean_df) + (
        len(rejected_df) if rejected_df is not None else 0
    )

    update_watermark(
        records_fetched=total_fetched,
        records_loaded=loaded,
        engine=engine
    )


if __name__ == "__main__":
    run()