"""
dags/anime_etl_dag.py
----------------------
The Airflow DAG for the Anime Analytics pipeline.

Schedule: daily at 6am UTC.
Tasks:    scrape → transform → load

How it works:
  - Airflow picks this file up automatically from the /dags folder.
  - On each run, the scraper checks the DB for existing anime_ids
    and only fetches NEW ones (watermark-based incremental load).
  - If any task fails, Airflow retries it up to 2 times before alerting.
  - You can also trigger a manual run from the Airflow UI.

To monitor: http://localhost:8080  (admin / admin)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# The project root is mounted at /opt/airflow in Docker
sys.path.insert(0, "/opt/airflow")


# ── Default settings applied to all tasks ────────────────────────────────────
default_args = {
    "owner":           "data-engineer",
    "retries":         2,                       # retry a failed task twice
    "retry_delay":     timedelta(minutes=5),    # wait 5 min between retries
    "email_on_failure": False,
}


# ── Task Functions ────────────────────────────────────────────────────────────
def task_scrape(**context):
    """
    Runs the scraper.
    Saves the raw JSON filepath into XCom so the next task can find it.
    """
    from pipeline.scraper import run
    filepath = run()
    # XCom lets tasks pass small values to each other
    context["ti"].xcom_push(key="raw_filepath", value=filepath)


def task_transform(**context):
    """
    Reads the raw JSON, cleans and validates it with Pandas.
    Passes clean + rejected DataFrames to XCom for the loader.
    """
    from pipeline.transform import run
    import pickle, os

    clean_df, rejected_df = run()

    # Serialize DataFrames temporarily so XCom can pass them
    tmp_dir = "/tmp/anime_etl"
    os.makedirs(tmp_dir, exist_ok=True)

    clean_path    = f"{tmp_dir}/clean.pkl"
    rejected_path = f"{tmp_dir}/rejected.pkl"

    clean_df.to_pickle(clean_path)
    rejected_df.to_pickle(rejected_path)

    context["ti"].xcom_push(key="clean_path",    value=clean_path)
    context["ti"].xcom_push(key="rejected_path", value=rejected_path)


def task_load(**context):
    """
    Loads clean records into PostgreSQL.
    Logs quality stats and updates the watermark.
    """
    import pandas as pd
    from pipeline.loader import run

    clean_path    = context["ti"].xcom_pull(key="clean_path",    task_ids="transform")
    rejected_path = context["ti"].xcom_pull(key="rejected_path", task_ids="transform")

    clean_df    = pd.read_pickle(clean_path)
    rejected_df = pd.read_pickle(rejected_path)

    run(clean_df=clean_df, rejected_df=rejected_df)


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id          = "anime_etl",
    description     = "Daily incremental ETL: scrape → transform → load anime data",
    schedule_interval = "0 6 * * *",        # every day at 6am UTC
    start_date      = datetime(2024, 1, 1),
    catchup         = False,                # don't backfill past runs
    default_args    = default_args,
    tags            = ["anime", "etl"],
) as dag:

    scrape = PythonOperator(
        task_id         = "scrape",
        python_callable = task_scrape,
    )

    transform = PythonOperator(
        task_id         = "transform",
        python_callable = task_transform,
    )

    load = PythonOperator(
        task_id         = "load",
        python_callable = task_load,
    )

    # ── Pipeline order ────────────────────────────────────────────────────────
    scrape >> transform >> load
