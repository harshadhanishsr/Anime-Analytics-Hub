# 🎌 Anime Analytics Hub

End-to-end ETL pipeline that scrapes anime data from MyAnimeList, transforms it with Pandas, stores it in PostgreSQL, and serves analytics through a Flask dashboard. Fully automated by Apache Airflow. One command to start everything.

---

## How It Works

```
docker-compose up
       │
       ├── PostgreSQL starts (port 5432)
       │
       └── Airflow starts (port 8080)
               │
               └── anime_etl DAG runs daily at 6am UTC
                       │
                       ├── scrape    → fetch only NEW anime (watermark)
                       ├── transform → Pandas clean + validate
                       └── load      → upsert into PostgreSQL
```

On the first run: scrapes all 100 anime.  
On every run after: **only fetches anime not already in the database** (watermark-based incremental load).

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/your/anime-analytics-hub
cd anime-analytics-hub

# 2. Start everything
docker-compose up

# 3. Open Airflow UI → enable the DAG → it runs automatically
#    http://localhost:8080   (admin / admin)

# 4. (Optional) Run Flask dashboard locally
pip install -r requirements.txt
python app/app.py
# → http://localhost:5000
```

---

## Project Structure

```
anime-analytics-hub/
│
├── pipeline/
│   ├── scraper.py       # Step 1: scrape MAL (rate limiter + watermark)
│   ├── transform.py     # Step 2: Pandas clean + validate
│   └── loader.py        # Step 3: upsert to PostgreSQL + update watermark
│
├── dags/
│   └── anime_etl_dag.py # Airflow DAG — chains all 3 steps daily
│
├── app/
│   ├── app.py           # Flask dashboard
│   ├── queries.py       # SQL queries for the dashboard
│   └── templates/       # Jinja2 HTML templates
│
├── sql/
│   └── schema.sql       # Table definitions + indexes
│
├── data/raw/            # Raw JSON files (gitignored)
├── config.py            # All settings in one place
├── docker-compose.yml   # PostgreSQL + Airflow
└── requirements.txt     # Local dependencies (Flask dashboard)
```

---

## Key Design Decisions

**Watermark (incremental load)**  
The `watermark` table stores which `anime_id`s are already in the database. Every scraper run reads this and skips anime already stored. No wasted requests, no duplicates.

**Token bucket rate limiter**  
A thread-safe token bucket in `scraper.py` limits requests to 5 per 10 seconds. Prevents IP bans and respects the source site.

**Idempotent upserts**  
`loader.py` uses `ON CONFLICT (anime_id) DO UPDATE`. Re-running the pipeline is always safe — no duplicate rows.

**SQL indexes**  
`schema.sql` creates indexes on `score`, `rank`, `type`, and `members`. Makes analytical queries significantly faster as the dataset grows.

**Data quality log**  
Every pipeline run logs how many records passed/failed validation to the `quality_log` table. Trackable over time from the dashboard.

**Config in one file**  
Everything — DB credentials, scrape limits, rate limiting — lives in `config.py`. One place to change, nothing scattered.
