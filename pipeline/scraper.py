"""
pipeline/scraper.py
--------------------
Automatic ingestion strategy:

• First run  → Full historical load
• Later runs → Incremental newest-first load
• Stops early when known anime_id is found
• Uses exponential backoff
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, UTC
from sqlalchemy import create_engine, text, inspect

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config


# ─────────────────────────────────────────────────────────────
# Exponential Backoff
# ─────────────────────────────────────────────────────────────
def api_get(url, params=None, max_retries=5):
    delay = 1

    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=10)

            if r.status_code == 429:
                raise Exception("Rate limited")

            r.raise_for_status()
            return r.json()

        except Exception as e:
            print(f"[scraper] API error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2

    raise Exception("Max retries exceeded")


# ─────────────────────────────────────────────────────────────
# Check Existing IDs (Watermark)
# ─────────────────────────────────────────────────────────────
def get_existing_ids(engine) -> set:
    inspector = inspect(engine)

    if "anime" not in inspector.get_table_names():
        print("[scraper] No anime table found → first run detected.")
        return set()

    with engine.connect() as conn:
        result = conn.execute(text("SELECT anime_id FROM anime"))
        return {row[0] for row in result.fetchall()}


# ─────────────────────────────────────────────────────────────
# Format API Record
# ─────────────────────────────────────────────────────────────
def format_record(anime):
    return {
        "anime_id": anime.get("mal_id"),
        "rank": anime.get("rank"),
        "title": anime.get("title"),
        "type": anime.get("type"),
        "episodes": anime.get("episodes"),
        "score": anime.get("score"),
        "members": anime.get("members"),
        "url": anime.get("url"),
        "scraped_at": datetime.now(UTC).isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# FULL HISTORICAL LOAD
# ─────────────────────────────────────────────────────────────
def full_load():
    print("[scraper] Running FULL historical load")

    records = []
    page = 1

    while True:
        print(f"[scraper] Fetching page {page}")

        data = api_get(
            config.JIKAN_BASE_URL,
            {
                "page": page,
                "order_by": "start_date",
                "sort": "desc"
            }
        )

        anime_list = data.get("data", [])
        if not anime_list:
            break

        for anime in anime_list:
            records.append(format_record(anime))

        if not data.get("pagination", {}).get("has_next_page"):
            break

        page += 1

    print(f"[scraper] Full load fetched {len(records)} records")
    return records


# ─────────────────────────────────────────────────────────────
# INCREMENTAL LOAD
# ─────────────────────────────────────────────────────────────
def incremental_load(existing_ids):
    print("[scraper] Running INCREMENTAL load")

    records = []
    page = 1

    while page <= config.INCREMENTAL_PAGES:
        print(f"[scraper] Checking newest page {page}")

        data = api_get(
            config.JIKAN_BASE_URL,
            {
                "page": page,
                "order_by": "start_date",
                "sort": "desc"
            }
        )

        anime_list = data.get("data", [])
        if not anime_list:
            break

        stop_fetching = False

        for anime in anime_list:
            anime_id = anime.get("mal_id")

            if anime_id in existing_ids:
                print("[scraper] Encountered known anime_id → stopping early.")
                stop_fetching = True
                break

            records.append(format_record(anime))

        if stop_fetching:
            break

        page += 1

    print(f"[scraper] Incremental load fetched {len(records)} new records")
    return records


# ─────────────────────────────────────────────────────────────
# Save Raw JSON
# ─────────────────────────────────────────────────────────────
def save_raw(records):
    if not records:
        print("[scraper] No new records found.")
        return None

    os.makedirs(config.RAW_DIR, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(config.RAW_DIR, f"anime_raw_{ts}.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    print(f"[scraper] Saved {len(records)} records → {path}")
    return path


# ─────────────────────────────────────────────────────────────
# Main Entry
# ─────────────────────────────────────────────────────────────
def run():
    engine = create_engine(config.DB_URL)

    existing_ids = get_existing_ids(engine)

    if not existing_ids:
        records = full_load()
    else:
        records = incremental_load(existing_ids)

    return save_raw(records)


if __name__ == "__main__":
    run()