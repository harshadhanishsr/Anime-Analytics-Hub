"""
app/queries.py
---------------
Pre-built SQL queries for the Flask dashboard.
Each function returns a list of dicts — easy to pass to templates.
Decoupled from Flask so the same functions work in notebooks or tests.
"""

from sqlalchemy import create_engine, text
import config

def _get_engine():
    return create_engine(config.DB_URL)

def _query(sql: str, params: dict = None) -> list[dict]:
    """Runs a SQL query and returns rows as a list of dicts."""
    engine = _get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        cols   = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]


def summary_stats() -> dict:
    result = _query("""
        SELECT
            COUNT(*)                       AS total_anime,
            ROUND(AVG(score)::numeric, 2)  AS avg_score,
            MAX(score)                     AS max_score,
            SUM(members)                   AS total_members
        FROM anime WHERE score IS NOT NULL
    """)
    return result[0] if result else {}


def top_rated(limit: int = 25) -> list[dict]:
    return _query(
        "SELECT rank, title, type, score, episodes, members "
        "FROM anime ORDER BY score DESC NULLS LAST LIMIT :limit",
        {"limit": limit}
    )


def most_popular(limit: int = 25) -> list[dict]:
    return _query(
        "SELECT rank, title, members, score FROM anime "
        "WHERE members IS NOT NULL ORDER BY members DESC LIMIT :limit",
        {"limit": limit}
    )


def type_breakdown() -> list[dict]:
    return _query("""
        SELECT type, COUNT(*) AS total, ROUND(AVG(score)::numeric, 2) AS avg_score
        FROM anime WHERE type IS NOT NULL
        GROUP BY type ORDER BY total DESC
    """)


def pipeline_health() -> dict:
    """Shows watermark + latest quality log for the dashboard health card."""
    watermark = _query("SELECT * FROM watermark WHERE pipeline = 'anime_etl'")
    quality   = _query("SELECT * FROM quality_log ORDER BY run_at DESC LIMIT 1")
    return {
        "watermark": watermark[0] if watermark else {},
        "quality":   quality[0]   if quality   else {},
    }
