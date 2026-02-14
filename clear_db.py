import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="anime_db",
    user="postgres",
    password="hutsun"
)
cursor = conn.cursor()

print("Clearing old data...")
cursor.execute("DELETE FROM anime;")
conn.commit()

cursor.execute("SELECT COUNT(*) FROM anime;")
count = cursor.fetchone()[0]
print(f"✅ Database cleared! Records remaining: {count}")

cursor.close()
conn.close()