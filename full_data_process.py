import pandas as pd
import psycopg2
import os
import time

# Database connection parameters
db_params = {
    'dbname': 'music_recommendation',
    'user': 'tdcooley',
    'password': 'appledogfish',
    'host': 'localhost',
    'port': '5432'
}

def load_all_processed_data():
    """Load all processed data from CSV files into the database"""
    print("Loading all processed MSD data...")
    
    # Check if processed data exists
    if not os.path.exists('data/processed/songs_processed.csv'):
        print("Error: Processed data files not found. Run process_msd_data.py first.")
        return
        
    start_time = time.time()
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Load artists
        print("Loading artists from CSV...")
        artists_df = pd.read_csv('data/processed/artists.csv')
        print(f"Found {len(artists_df)} artists in CSV file")
        
        # Process in small batches
        batch_size = 100
        total_artists = len(artists_df)
        artist_count = 0
        
        for i in range(0, total_artists, batch_size):
            end = min(i + batch_size, total_artists)
            batch = artists_df.iloc[i:end]
            
            for _, row in batch.iterrows():
                try:
                    cursor.execute(
                        'INSERT INTO artist (artist_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING',
                        (row['artist_id'], row['name'])
                    )
                    artist_count += 1
                except Exception as e:
                    print(f"Error inserting artist {row['artist_id']}: {e}")
            
            conn.commit()
            print(f"Loaded artists {i+1} to {end} of {total_artists}")
        
        # Load songs
        print("\nLoading songs from CSV...")
        songs_df = pd.read_csv('data/processed/songs_processed.csv')
        print(f"Found {len(songs_df)} songs in CSV file")
        
        batch_size = 100
        total_songs = len(songs_df)
        song_count = 0
        
        for i in range(0, total_songs, batch_size):
            end = min(i + batch_size, total_songs)
            batch = songs_df.iloc[i:end]
            
            for _, row in batch.iterrows():
                try:
                    cursor.execute(
                        '''
                        INSERT INTO song (
                            song_id, title, key_signature, mode, release_year, 
                            tempo, energy, danceability, loudness, cluster_id, artist_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        ''',
                        (
                            row['song_id'], row['title'], 
                            str(row['key_signature']) if not pd.isna(row['key_signature']) else None, 
                            str(row['mode']) if not pd.isna(row['mode']) else None, 
                            int(row['release_year']) if not pd.isna(row['release_year']) else None,
                            float(row['tempo']) if not pd.isna(row['tempo']) else None,
                            float(row['energy']) if not pd.isna(row['energy']) else None,
                            float(row['danceability']) if not pd.isna(row['danceability']) else None,
                            float(row['loudness']) if not pd.isna(row['loudness']) else None,
                            int(row['cluster_id']) if not pd.isna(row['cluster_id']) else None,
                            row['artist_id']
                        )
                    )
                    song_count += 1
                except Exception as e:
                    print(f"Error inserting song {row['song_id']}: {e}")
            
            conn.commit()
            elapsed = time.time() - start_time
            print(f"Loaded songs {i+1} to {end} of {total_songs} - {elapsed:.1f} seconds elapsed")
        
        # Add songs to sample playlists
        print("\nAdding songs to sample playlists...")
        
        # Get random song IDs
        cursor.execute('SELECT song_id FROM song ORDER BY RANDOM() LIMIT 30')
        song_ids = cursor.fetchall()
        
        if song_ids:
            playlist_songs = []
            for i, playlist_id in enumerate(['playlist1', 'playlist2', 'playlist3']):
                for j in range(10):  # 10 songs per playlist
                    idx = i * 10 + j
                    if idx < len(song_ids):
                        playlist_songs.append((playlist_id, song_ids[idx][0]))
            
            for ps in playlist_songs:
                try:
                    cursor.execute(
                        'INSERT INTO playlist_song (playlist_id, song_id) VALUES (%s, %s) ON CONFLICT DO NOTHING',
                        ps
                    )
                except Exception as e:
                    print(f"Error inserting playlist_song {ps}: {e}")
            
            conn.commit()
            print(f"Added {len(playlist_songs)} songs to playlists")
        
        total_time = time.time() - start_time
        print(f"\nLoading complete! Added {artist_count} artists and {song_count} songs in {total_time:.1f} seconds")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection closed.")

if __name__ == "__main__":
    load_all_processed_data()