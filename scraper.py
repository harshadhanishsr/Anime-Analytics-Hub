import requests
import pandas as pd
from datetime import datetime
import time

print("=" * 60)
print("ANIME DATA SCRAPER - COMPLETE DATASET (IMPROVED)")
print("=" * 60)
print(f"Started at: {datetime.now()}\n")

anime_list = []
page = 1
has_data = True
start_time = time.time()
retry_count = 0
max_retries = 3

print("Fetching ALL anime from Jikan API...")
print("With improved error handling and retries...\n")

while has_data:
    url = f"https://api.jikan.moe/v4/anime?page={page}&order_by=score&sort=desc"
    
    try:
        response = requests.get(url, timeout=15)
        
        # Handle rate limiting with exponential backoff
        if response.status_code == 429:
            wait_time = min(2 ** retry_count, 60)  # Exponential backoff
            print(f"⚠️  Rate limited on page {page}. Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            retry_count += 1
            
            if retry_count > max_retries:
                print(f"Max retries reached. Stopping at page {page}")
                break
            continue
        
        # Reset retry count on success
        retry_count = 0
        
        response.raise_for_status()
        data = response.json()
        
        if 'data' in data and len(data['data']) > 0:
            for anime in data['data']:
                anime_list.append({
                    'mal_id': anime.get('mal_id', 'Unknown'),
                    'title': anime.get('title', 'Unknown'),
                    'score': anime.get('score', None),
                    'episodes': anime.get('episodes', None),
                    'status': anime.get('status', 'Unknown'),
                    'aired_from': anime.get('aired', {}).get('from', 'Unknown'),
                    'aired_to': anime.get('aired', {}).get('to', 'Unknown'),
                    'genres': ', '.join([g.get('name', '') for g in anime.get('genres', [])]) if anime.get('genres') else 'Unknown',
                    'source': anime.get('source', 'Unknown'),
                    'rating': anime.get('rating', 'Unknown'),
                    'favorite_count': anime.get('favorites', 0)
                })
            
            elapsed = time.time() - start_time
            if page % 5 == 0:
                print(f"✓ Page {page}: {len(anime_list)} total records... ({elapsed/60:.1f} min elapsed)")
            
            page += 1
            time.sleep(0.5)  # Respectful delay between requests
        else:
            print(f"No data returned on page {page}. Stopping.")
            has_data = False
    
    except requests.exceptions.Timeout:
        print(f"⚠️  Timeout on page {page}. Retrying...")
        retry_count += 1
        time.sleep(5)
        
        if retry_count > max_retries:
            print("Max retries reached. Stopping.")
            break
        continue
    
    except requests.exceptions.ConnectionError:
        print(f"⚠️  Connection error on page {page}. Retrying...")
        retry_count += 1
        time.sleep(10)
        
        if retry_count > max_retries:
            print("Max retries reached. Stopping.")
            break
        continue
    
    except Exception as e:
        print(f"❌ Unexpected error on page {page}: {e}")
        has_data = False

elapsed_time = time.time() - start_time

print(f"\n✅ Successfully fetched {len(anime_list)} anime records")
print(f"Time taken: {elapsed_time/60:.1f} minutes\n")

# Create DataFrame
df = pd.DataFrame(anime_list)

print("Data Statistics:")
print(f"Total records: {len(df)}")
print(f"Columns: {list(df.columns)}")
print()

# Process data (clean + transform)
print("Processing data...")
df = df.dropna(subset=['score', 'episodes'])
df['score'] = df['score'].astype(float)
df['episodes'] = df['episodes'].astype(int)

print(f"After cleaning: {len(df)} records")

# Add calculated columns
df['rating_category'] = pd.cut(df['score'], 
    bins=[0, 6.5, 7.5, 8.5, 10],
    labels=['Average', 'Good', 'Great', 'Masterpiece'],
    right=False
)

df['episode_range'] = pd.cut(df['episodes'],
    bins=[0, 3, 12, 24, 1000],
    labels=['Very Short', 'Short', 'Standard', 'Long Series'],
    right=False
)

df['popularity'] = pd.cut(df['favorite_count'],
    bins=[0, 10000, 50000, 100000, 10000000],
    labels=['Less Popular', 'Moderately Popular', 'Popular', 'Very Popular'],
    right=False
)

# Filter
df_filtered = df

# Save processed data
output_file = 'processed_anime_data.csv'
df_filtered.to_csv(output_file, index=False)

print(f"✅ Processing complete!")
print(f"Final records after filtering: {len(df_filtered)}")
print(f"Saved to: {output_file}\n")

print(f"✅ Done at {datetime.now()}")
print("=" * 60)