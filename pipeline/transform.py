"""
pipeline/transform.py
----------------------
Step 2 of the ETL pipeline.

Compatible with:
- Jikan API (structured numeric fields)
- HTML scraper fallback (string fields)

Produces:
- clean_df
- rejected_df
"""

import os
import sys
import json
import glob
import pandas as pd
from datetime import datetime, UTC

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config


# ── Load Raw Data ─────────────────────────────────────────────────────────────
def load_latest_raw() -> list[dict]:
    files = sorted(glob.glob(os.path.join(config.RAW_DIR, "anime_raw_*.json")))

    if not files:
        raise FileNotFoundError(
            f"No raw files found in {config.RAW_DIR}. Run scraper first."
        )

    latest = files[-1]
    print(f"[transform] Loading: {latest}")

    with open(latest, "r", encoding="utf-8") as f:
        return json.load(f)


# ── Clean ─────────────────────────────────────────────────────────────────────
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and casts all columns safely.
    Works for both numeric API fields and legacy string formats.
    """

    # Standardize column existence
    expected_cols = [
        "anime_id",
        "rank",
        "title",
        "type",
        "episodes",
        "score",
        "members",
        "scraped_at",
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # Strip whitespace from text columns safely
    text_cols = ["title", "type"]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(["None", "nan", ""], None)

    # Numeric casting (safe for API + strings)
    df["anime_id"] = pd.to_numeric(df["anime_id"], errors="coerce").astype("Int64")
    df["rank"]     = pd.to_numeric(df["rank"], errors="coerce").astype("Int64")
    df["score"]    = pd.to_numeric(df["score"], errors="coerce")
    df["episodes"] = pd.to_numeric(df["episodes"], errors="coerce").astype("Int64")
    df["members"]  = pd.to_numeric(df["members"], errors="coerce").astype("Int64")

    # Standardize timestamp
    df["scraped_at"] = pd.to_datetime(
        df["scraped_at"], errors="coerce", utc=True
    )

    return df


# ── Validate ──────────────────────────────────────────────────────────────────
def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rejected_rows = []
    clean_rows = []

    for _, row in df.iterrows():
        reasons = []

        # Rule 1: Title must exist
        if not row.get("title"):
            reasons.append("missing_title")

        # Rule 2: Score must exist and be within valid range
        if pd.isna(row.get("score")):
            reasons.append("missing_score")
        elif not (config.MIN_SCORE <= float(row["score"]) <= config.MAX_SCORE):
            reasons.append(f"score_out_of_range({row['score']})")

        # Rule 3: Rank must be valid
        if pd.isna(row.get("rank")) or int(row["rank"]) < config.MIN_RANK:
            reasons.append("invalid_rank")

        # Rule 4: anime_id required
        if pd.isna(row.get("anime_id")):
            reasons.append("missing_anime_id")

        if reasons:
            row = row.copy()
            row["rejection_reason"] = ", ".join(reasons)
            rejected_rows.append(row)
        else:
            clean_rows.append(row)

    clean_df = pd.DataFrame(clean_rows).reset_index(drop=True)
    rejected_df = pd.DataFrame(rejected_rows).reset_index(drop=True)

    print(f"[transform] Passed validation : {len(clean_df)}")
    print(f"[transform] Failed validation : {len(rejected_df)}")

    return clean_df, rejected_df


# ── Main Entry ────────────────────────────────────────────────────────────────
def run() -> tuple[pd.DataFrame, pd.DataFrame]:
    records = load_latest_raw()
    print(f"[transform] Raw records loaded: {len(records)}")

    df = pd.DataFrame(records)
    df = clean(df)
    clean_df, rejected_df = validate(df)

    return clean_df, rejected_df


if __name__ == "__main__":
    clean_df, rejected_df = run()

    print("\n── Sample Clean Records ──")
    print(
        clean_df[["anime_id", "title", "score", "rank"]]
        .head(10)
        .to_string(index=False)
    )

    if not rejected_df.empty:
        print("\n── Rejected Records ──")
        print(
            rejected_df[["title", "rejection_reason"]]
            .to_string(index=False)
        )