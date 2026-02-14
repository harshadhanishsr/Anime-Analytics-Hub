import psycopg2
from datetime import datetime

print("=" * 60)
print("ANIME ANALYTICS - SQL QUERIES")
print("=" * 60)
print(f"Started at: {datetime.now()}\n")

# Connect to database
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

# Check how many records exist
cursor.execute("SELECT COUNT(*) FROM anime;")
count = cursor.fetchone()[0]
print(f"Total records in database: {count}\n")

if count == 0:
    print("❌ No data in database!")
    cursor.close()
    conn.close()
    exit()

# Query 1: Top 10 anime by score
print("=" * 60)
print("QUERY 1: Top 10 Anime by Score")
print("=" * 60)
try:
    cursor.execute("""
        SELECT mal_id, title, score, episodes, rating_category 
        FROM anime 
        ORDER BY score DESC 
        LIMIT 10;
    """)
    results = cursor.fetchall()
    print(f"{'Rank':<5} {'Title':<30} {'Score':<8} {'Episodes':<10} {'Category':<15}")
    print("-" * 70)
    for i, row in enumerate(results, 1):
        print(f"{i:<5} {str(row[1])[:30]:<30} {str(row[2]):<8} {str(row[3]):<10} {str(row[4]):<15}")
except Exception as e:
    print(f"❌ Query 1 failed: {e}")

print("\n")

# Query 2: Count by category
print("=" * 60)
print("QUERY 2: Anime by Rating Category")
print("=" * 60)
try:
    cursor.execute("""
        SELECT rating_category, COUNT(*) as count
        FROM anime
        GROUP BY rating_category
        ORDER BY count DESC;
    """)
    results = cursor.fetchall()
    print(f"{'Category':<20} {'Count':<10}")
    print("-" * 30)
    for row in results:
        print(f"{str(row[0]):<20} {str(row[1]):<10}")
except Exception as e:
    print(f"❌ Query 2 failed: {e}")

print("\n")

# Query 3: Overall stats
print("=" * 60)
print("QUERY 3: Overall Statistics")
print("=" * 60)
try:
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            ROUND(AVG(score)::numeric, 2) as avg_score,
            MAX(score) as max_score
        FROM anime;
    """)
    results = cursor.fetchone()
    print(f"Total Anime: {results[0]}")
    print(f"Average Score: {results[1]}")
    print(f"Max Score: {results[2]}")
except Exception as e:
    print(f"❌ Query 3 failed: {e}")

# Close connection
cursor.close()
conn.close()

print(f"\n✅ Done at {datetime.now()}")
print("=" * 60)