"""
config.py
---------
Central configuration file.
Automatically supports full + incremental ingestion.
"""

import os

# ── Database ────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST", "postgres")  # inside Docker
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "anime_db")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ── API Configuration ────────────────────────────────────────────────────────
JIKAN_BASE_URL = "https://api.jikan.moe/v4/anime"

# How many newest pages to check during incremental runs
INCREMENTAL_PAGES = 5


# ── Data Validation ─────────────────────────────────────────────────────────
MIN_SCORE = 1.0
MAX_SCORE = 10.0
MIN_RANK  = 1


# ── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(__file__)
RAW_DIR  = os.path.join(BASE_DIR, "data", "raw")