from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import psycopg2
import requests
import time
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'anime_etl_incremental_daily',
    default_args=default_args,
    description='Daily incremental anime ETL',
    schedule_interval='0 0 * * *',
    start_date=datetime(2026, 2, 15),
    catchup=False,
    tags=['anime', 'etl'],
)

DATA_DIR = '/opt/airflow/data/anime'
RAW_DIR = f'{DATA_DIR}/raw'
PROCESSED_DIR = f'{DATA_DIR}/processed'
LOGS_DIR = f'{DATA_DIR}/logs'

Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)
Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

raw_file = f'{RAW_DIR}/anime_raw_{{ ds }}.csv'
new_file = f'{PROCESSED_DIR}/anime_new_{{ ds }}.csv'
processed_file = f'{PROCESSED_DIR}/anime_processed_{{ ds }}.csv'

DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'anime_db',
    'user': 'postgres',
    'password': 'hutsun',
}

def scrape_task(**context):
    logger.info("Scraping anime data...")
    metadata_file = f'{LOGS_DIR}/metadata.json'
    
    try:
        with open(metadata_file) as f:
            meta = json.load(f)
            last_page = meta.get('last_page', 0)
    except:
        last_page = 0
    
    anime_list = []
    page = last_page + 1
    empty_count = 0
    
    class TokenBucket:
        def __init__(self):
            self.tokens = 10
            self.last_refill = time.time()
        
        def wait(self):
            while self.tokens < 1:
                elapsed = time.time() - self.last_refill
                self.tokens += elapsed * 5
                self.last_refill = time.time()
                if self.tokens >= 1:
                    break
                time.sleep(0.1)
            self.tokens -= 1
    
    bucket = TokenBucket()
    
    while True:
        try:
            bucket.wait()
            url = f"https://api.jikan.moe/v4/anime?page={page}"
            resp = requests.get(url, timeout=10)
            
            if resp.status_code == 429:
                time.sleep(5)
                continue
            
            data = resp.json()
            if 'data' in data and len(data['data']) > 0:
                empty_count = 0
                for anime in data['data']:
                    anime_list.append({
                        'mal_id': anime.get('mal_id'),
                        'title': anime.get('title'),
                        'score': anime.get('score'),
                        'episodes': anime.get('episodes'),
                        'status': anime.get('status'),
                        'genres': ', '.join([g.get('name', '') for g in anime.get('genres', [])]) if anime.get('genres') else None,
                        'favorite_count': anime.get('favorites', 0)
                    })
                page += 1
                time.sleep(0.2)
            else:
                empty_count += 1
                if empty_count >= 3:
                    break
                page += 1
        except Exception as e:
            logger.error(f"Error: {e}")
            break
    
    if len(anime_list) > 0:
        df = pd.DataFrame(anime_list)
        df.to_csv(raw_file, index=False)
    else:
        pd.DataFrame(columns=['mal_id', 'title', 'score', 'episodes', 'status', 'genres', 'favorite_count']).to_csv(raw_file, index=False)
    
    with open(metadata_file, 'w') as f:
        json.dump({'last_page': page - 1}, f)
    
    context['task_instance'].xcom_push(key='count', value=len(anime_list))
    logger.info(f"Scraped {len(anime_list)} records")

def compare_task(**context):
    logger.info("Comparing with database...")
    try:
        today_df = pd.read_csv(raw_file)
    except:
        context['task_instance'].xcom_push(key='new', value=0)
        return
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT mal_id FROM anime")
        db_ids = set([r[0] for r in cur.fetchall()])
        cur.close()
        conn.close()
    except:
        db_ids = set()
    
    today_ids = set(today_df['mal_id'].unique())
    new_ids = today_ids - db_ids
    new_df = today_df[today_df['mal_id'].isin(new_ids)]
    
    new_df.to_csv(new_file, index=False)
    context['task_instance'].xcom_push(key='new', value=len(new_df))
    logger.info(f"Found {len(new_df)} new records")

def validate_task(**context):
    logger.info("Validating...")
    df = pd.read_csv(new_file)
    df = df.dropna(subset=['score', 'episodes'])
    df.to_csv(new_file, index=False)
    context['task_instance'].xcom_push(key='valid', value=len(df))
    logger.info(f"Valid records: {len(df)}")

def process_task(**context):
    logger.info("Processing...")
    df = pd.read_csv(new_file)
    if len(df) == 0:
        df.to_csv(processed_file, index=False)
        return
    
    df = df[df['score'] >= 6.5]
    if len(df) > 0:
        df['rating'] = pd.cut(df['score'], bins=[0, 6.5, 7.5, 8.5, 10], labels=['Avg', 'Good', 'Great', 'Master'])
    
    df.to_csv(processed_file, index=False)
    context['task_instance'].xcom_push(key='processed', value=len(df))
    logger.info(f"Processed: {len(df)}")

def load_task(**context):
    logger.info("Loading to database...")
    df = pd.read_csv(processed_file)
    if len(df) == 0:
        logger.info("No records to load")
        return
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        for _, row in df.iterrows():
            try:
                cur.execute(
                    "INSERT INTO anime (mal_id, title, score, episodes, status, genres, favorite_count, rating_category) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (row['mal_id'], row['title'], row['score'], row['episodes'], row['status'], row['genres'], row['favorite_count'], row.get('rating'))
                )
            except psycopg2.errors.UniqueViolation:
                pass
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Loaded {len(df)} records")
    except Exception as e:
        logger.error(f"Load error: {e}")

scrape = PythonOperator(task_id='scrape', python_callable=scrape_task, provide_context=True, dag=dag)
compare = PythonOperator(task_id='compare', python_callable=compare_task, provide_context=True, dag=dag)
validate = PythonOperator(task_id='validate', python_callable=validate_task, provide_context=True, dag=dag)
process = PythonOperator(task_id='process', python_callable=process_task, provide_context=True, dag=dag)
load = PythonOperator(task_id='load', python_callable=load_task, provide_context=True, dag=dag)

scrape >> compare >> validate >> process >> load