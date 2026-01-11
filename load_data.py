import pandas as pd
import psycopg2
from psycopg2 import sql
import io

# Database connection parameters
db_params = {
    'dbname': 'music_recommendation',
    'user': 'tdcooley',
    'password': 'appledogfish',
    'host': 'localhost',
    'port': '5432'
}

def load_data_to_postgres():
    print("Loading data to PostgreSQL...")
    
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Clear existing data
        print("Clearing existing data...")
        cursor.execute("SET CONSTRAINTS ALL DEFERRED;")
        cursor.execute('TRUNCATE TABLE playlist_song, playlist, "User", song, artist CASCADE;')
        conn.commit()
        print("Database cleared")
        
        # Load artists
        print("Loading artists...")
        artists_df = pd.read_csv('data/processed/artists.csv')
        
        # Use StringIO for efficient loading
        artists_data = io.StringIO()
        artists_df.to_csv(artists_data, header=False, index=False)
        artists_data.seek(0)
        
        cursor.copy_from(artists_data, 'artist', sep=',', columns=('artist_id', 'name'))
        conn.commit()
        print(f"Loaded {len(artists_df)} artists")
        
        # Load songs
        print("Loading songs...")
        songs_df = pd.read_csv('data/processed/songs_processed.csv')
        
        # Process in batches to avoid memory issues
        batch_size = 1000
        total_songs = len(songs_df)
        
        for i in range(0, total_songs, batch_size):
            end = min(i + batch_size, total_songs)
            batch = songs_df.iloc[i:end]
            
            # Use StringIO for efficient loading
            songs_data = io.StringIO()
            batch.to_csv(songs_data, header=False, index=False, na_rep='NULL')
            songs_data.seek(0)
            
            cursor.copy_from(
                songs_data, 
                'song', 
                sep=',', 
                columns=('song_id', 'title', 'key_signature', 'mode', 'release_year', 
                        'tempo', 'energy', 'danceability', 'loudness', 'cluster_id', 'artist_id')
            )
            conn.commit()
            print(f"Loaded songs {i+1} to {end} of {total_songs}")
        
        # Create sample users
        sample_users = [
            ('user1', 'John Doe', 'john@example.com'),
            ('user2', 'Jane Smith', 'jane@example.com'),
            ('user3', 'Bob Johnson', 'bob@example.com')
        ]
        
        cursor.executemany(
            'INSERT INTO "User" (user_id, name, email) VALUES (%s, %s, %s)',
            sample_users
        )
        conn.commit()
        print(f"Created {len(sample_users)} sample users")
        
        # Create sample playlists
        sample_playlists = [
            ('playlist1', 'My Favorites', 'user1'),
            ('playlist2', 'Workout Mix', 'user1'),
            ('playlist3', 'Chill Vibes', 'user2')
        ]
        
        cursor.executemany(
            'INSERT INTO playlist (playlist_id, name, user_id) VALUES (%s, %s, %s)',
            sample_playlists
        )
        conn.commit()
        print(f"Created {len(sample_playlists)} sample playlists")
        
        # Add some songs to playlists
        # Get random song IDs for each playlist
        cursor.execute('SELECT song_id FROM song ORDER BY RANDOM() LIMIT 15')
        song_ids = cursor.fetchall()
        
        playlist_songs = []
        for i, playlist_id in enumerate(['playlist1', 'playlist2', 'playlist3']):
            for j in range(5):
                idx = i * 5 + j
                if idx < len(song_ids):
                    playlist_songs.append((playlist_id, song_ids[idx][0]))
        
        cursor.executemany(
            'INSERT INTO playlist_song (playlist_id, song_id) VALUES (%s, %s)',
            playlist_songs
        )
        conn.commit()
        print(f"Added {len(playlist_songs)} songs to playlists")
        
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
    load_data_to_postgres()