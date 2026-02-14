import pandas as pd
from datetime import datetime

print("\n" + "=" * 60)
print("DATA PROCESSOR")
print("=" * 60)

# Read the CSV file
df = pd.read_csv('raw_anime_data.csv')
print(f"Loaded {len(df)} records")

# Data Cleaning
df = df.dropna(subset=['score', 'episodes'])
print(f"After cleaning: {len(df)} records")

df['score'] = df['score'].astype(float)
df['episodes'] = df['episodes'].astype(int)

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
df_filtered = df[df['score'] >= 6.5]

# Select final columns
df_final = df_filtered[[
    'mal_id', 'title', 'score', 'episodes', 'episode_range',
    'genres', 'source', 'rating', 'favorite_count', 'popularity',
    'rating_category', 'status', 'aired_from', 'aired_to'
]]

# Save to CSV
output_file = 'processed_anime_data.csv'
df_final.to_csv(output_file, index=False)

# Summary
print(f"\n✅ Processing Complete!")
print(f"Final records: {len(df_final)}")
print(f"Columns: {len(df_final.columns)}")
print(f"Saved to: {output_file}")
print("\n" + "=" * 60 + "\n")