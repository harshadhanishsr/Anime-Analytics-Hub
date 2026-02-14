from flask import Flask, render_template, request, jsonify
import psycopg2
from datetime import datetime

app = Flask(__name__)

# Database connection function
def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="anime_db",
        user="postgres",
        password="hutsun"
    )
    return conn

@app.route('/')
def dashboard():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Query 1: Top 10 anime
    cursor.execute("""
        SELECT mal_id, title, score, episodes, rating_category 
        FROM anime 
        ORDER BY score DESC 
        LIMIT 10;
    """)
    top_anime = cursor.fetchall()
    
    # Query 2: Rating distribution
    cursor.execute("""
        SELECT rating_category, COUNT(*) as count
        FROM anime
        GROUP BY rating_category
        ORDER BY count DESC;
    """)
    rating_dist = cursor.fetchall()
    
    # Query 3: Episode range distribution
    cursor.execute("""
        SELECT episode_range, COUNT(*) as count
        FROM anime
        GROUP BY episode_range
        ORDER BY count DESC;
    """)
    episode_dist = cursor.fetchall()
    
    # Query 4: Overall stats
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            ROUND(AVG(score)::numeric, 2) as avg_score,
            MAX(score) as max_score,
            MIN(score) as min_score,
            ROUND(AVG(episodes)::numeric, 2) as avg_episodes
        FROM anime;
    """)
    stats = cursor.fetchone()
    
    # Query 5: Popularity distribution
    cursor.execute("""
        SELECT popularity, COUNT(*) as count
        FROM anime
        GROUP BY popularity
        ORDER BY count DESC;
    """)
    popularity_dist = cursor.fetchall()
    
    # Query 6: Top sources
    cursor.execute("""
        SELECT source, COUNT(*) as count, ROUND(AVG(score)::numeric, 2) as avg_score
        FROM anime
        GROUP BY source
        ORDER BY count DESC
        LIMIT 8;
    """)
    source_dist = cursor.fetchall()
    
    # Get unique genres
    cursor.execute("""
        SELECT DISTINCT genres FROM anime LIMIT 50;
    """)
    genres_raw = cursor.fetchall()
    genres_set = set()
    for g in genres_raw:
        if g[0] and g[0] != 'Unknown':
            genres_set.update([x.strip() for x in g[0].split(',')])
    genres = sorted(list(genres_set))[:20]
    
    cursor.close()
    conn.close()
    
    return render_template('dashboard.html',
        top_anime=top_anime,
        rating_dist=rating_dist,
        episode_dist=episode_dist,
        popularity_dist=popularity_dist,
        source_dist=source_dist,
        stats=stats,
        genres=genres
    )

@app.route('/api/search')
@app.route('/api/search')
def search_anime():
    query = request.args.get('q', '').strip().lower()
    limit = request.args.get('limit', 50, type=int)
    
    # Handle empty query
    if not query:
        return jsonify([])
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Case-insensitive search with better formatting
    cursor.execute("""
        SELECT mal_id, title, score, episodes, rating_category, popularity, source
        FROM anime
        WHERE LOWER(TRIM(title)) LIKE LOWER(%s)
           OR LOWER(TRIM(genres)) LIKE LOWER(%s)
        ORDER BY score DESC
        LIMIT %s;
    """, (f'%{query}%', f'%{query}%', limit))
    
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return jsonify([{
        'mal_id': r[0],
        'title': r[1].strip(),
        'score': float(r[2]),
        'episodes': int(r[3]) if r[3] else 0,
        'rating_category': r[4].strip() if r[4] else 'Unknown',
        'popularity': r[5].strip() if r[5] else 'Unknown',
        'source': r[6].strip() if r[6] else 'Unknown'
    } for r in results])
    
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return jsonify([{
        'mal_id': r[0],
        'title': r[1],
        'score': r[2],
        'episodes': r[3],
        'rating_category': r[4],
        'popularity': r[5],
        'source': r[6]
    } for r in results])

@app.route('/api/filter')
def filter_anime():
    score_min = request.args.get('score_min', 6.5, type=float)
    score_max = request.args.get('score_max', 10, type=float)
    rating_category = request.args.get('rating_category', '')
    episode_range = request.args.get('episode_range', '')
    popularity = request.args.get('popularity', '')
    source = request.args.get('source', '')
    limit = request.args.get('limit', 50, type=int)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = "SELECT mal_id, title, score, episodes, rating_category, popularity, source FROM anime WHERE score BETWEEN %s AND %s"
    params = [score_min, score_max]
    
    if rating_category and rating_category != 'all':
        query += " AND rating_category = %s"
        params.append(rating_category)
    
    if episode_range and episode_range != 'all':
        query += " AND episode_range = %s"
        params.append(episode_range)
    
    if popularity and popularity != 'all':
        query += " AND popularity = %s"
        params.append(popularity)
    
    if source and source != 'all':
        query += " AND source = %s"
        params.append(source)
    
    query += " ORDER BY score DESC LIMIT %s"
    params.append(limit)
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return jsonify([{
        'mal_id': r[0],
        'title': r[1],
        'score': r[2],
        'episodes': r[3],
        'rating_category': r[4],
        'popularity': r[5],
        'source': r[6]
    } for r in results])

@app.route('/api/genre')
def filter_by_genre():
    genre = request.args.get('genre', '')
    limit = request.args.get('limit', 50, type=int)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT mal_id, title, score, episodes, rating_category, popularity, genres
        FROM anime
        WHERE genres LIKE %s
        ORDER BY score DESC
        LIMIT %s;
    """, (f'%{genre}%', limit))
    
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return jsonify([{
        'mal_id': r[0],
        'title': r[1],
        'score': r[2],
        'episodes': r[3],
        'rating_category': r[4],
        'popularity': r[5]
    } for r in results])

if __name__ == '__main__':
    app.run(debug=True, port=5000)