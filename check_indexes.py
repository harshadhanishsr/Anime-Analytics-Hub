import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="anime_db",
    user="postgres",
    password="hutsun"
)
cursor = conn.cursor()

cursor.execute("SELECT indexname FROM pg_indexes WHERE tablename='anime'")
indexes = cursor.fetchall()

print("Current indexes:")
for idx in indexes:
    print(f"  - {idx[0]}")

cursor.close()
conn.close()