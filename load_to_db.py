import pandas as pd
import psycopg2
from datetime import datetime
import os

print("=" * 60)
print("LOADING DATA TO POSTGRESQL")
print("=" * 60)
print(f"Started at: {datetime.now()}\n")

# Read processed CSV
print("Reading processed_anime_data.csv...")
df = pd.read_csv('processed_anime_data.csv')
print(f"✅ Loaded {len(df)} records\n")

# Clean data before loading
print("Cleaning data...")
df = df.fillna('Unknown')  # Replace NaN with 'Unknown'
df = df.drop_duplicates(subset=['mal_id'], keep='first')  # Remove duplicates
print(f"✅ After cleaning: {len(df)} records\n")

# Save cleaned CSV temporarily
temp_csv = 'temp_anime_clean.csv'
df.to_csv(temp_csv, index=False, sep=',')
print(f"Saved to temporary file: {temp_csv}\n")

# Connect to PostgreSQL
print("Connecting to PostgreSQL...")
try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="anime_db",
        user="postgres",
        password="hutsun"
    )
    cursor = conn.cursor()
    print("✅ Connected to anime_db\n")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit()

# Use COPY command (much faster and more reliable)
print(f"Inserting {len(df)} records using COPY command...\n")

try:
    with open(temp_csv, 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
            COPY anime (mal_id, title, score, episodes, status, aired_from, 
                       aired_to, genres, source, rating, favorite_count, 
                       rating_category, episode_range, popularity)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
        """, f)
    
    conn.commit()
    print(f"✅ Successfully inserted records!\n")
except Exception as e:
    print(f"❌ Error inserting data: {e}")
    conn.rollback()

# Verify insertion
cursor.execute("SELECT COUNT(*) FROM anime;")
count = cursor.fetchone()[0]
print(f"✅ Total records in database: {count}\n")

# Show sample data
if count > 0:
    print("Sample data from database:")
    cursor.execute("SELECT mal_id, title, score, rating_category FROM anime LIMIT 5;")
    for row in cursor.fetchall():
        print(f"  {row[0]} | {row[1][:30]} | Score: {row[2]} | {row[3]}")

# Close connection
cursor.close()
conn.close()

# Clean up temp file
os.remove(temp_csv)
print(f"\n✅ Done at {datetime.now()}")
print("=" * 60)