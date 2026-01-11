import pandas as pd
import psycopg2
import os

# Database connection parameters
db_params = {
    'dbname': 'music_recommendation',
    'user': 'tdcooley',
    'password': 'appledogfish',
    'host': 'localhost',
    'port': '5432'
}

def load_basic_data():
    """Load essential data to make the site functional"""
    conn = None
    try:
        # Check if processed data exists
        if not os.path.exists('data/processed/songs_processed.csv'):
            print("Error: Processed data files not found. Run process_msd_data.py first.")
            return
            
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Add a few sample artists
        artists = [
            ('AR1', 'The Beatles'),
            ('AR2', 'Queen'),
            ('AR3', 'Michael Jackson'),
            ('AR4', 'Madonna'),
            ('AR5', 'Beyonce')
        ]
        
        print("Adding sample artists...")
        for artist in artists:
            cursor.execute(
                'INSERT INTO artist (artist_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING',
                artist
            )
        
        # Add a few sample songs
        songs = [
            ('S1', 'Yesterday', 0, '1', 1965, 120.0, 0.5, 0.3, -8.0, 1, 'AR1'),
            ('S2', 'Bohemian Rhapsody', 1, '1', 1975, 140.0, 0.8, 0.4, -5.0, 2, 'AR2'),
            ('S3', 'Billie Jean', 2, '0', 1983, 130.0, 0.7, 0.8, -4.0, 3, 'AR3'),
            ('S4', 'Like a Prayer', 3, '1', 1989, 125.0, 0.6, 0.7, -6.0, 4, 'AR4'),
            ('S5', 'Halo', 4, '1', 2008, 135.0, 0.7, 0.5, -7.0, 5, 'AR5')
        ]
        
        print("Adding sample songs...")
        for song in songs:
            cursor.execute(
                '''INSERT INTO song 
                (song_id, title, key_signature, mode, release_year, tempo, 
                energy, danceability, loudness, cluster_id, artist_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING''',
                song
            )
        
        # Add sample users
        users = [
            ('user1', 'John Doe', 'john@example.com'),
            ('user2', 'Jane Smith', 'jane@example.com'),
            ('user3', 'Bob Johnson', 'bob@example.com')
        ]
        
        print("Adding sample users...")
        for user in users:
            cursor.execute(
                'INSERT INTO "User" (user_id, name, email) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING',
                user
            )
        
        # Add sample playlists
        playlists = [
            ('playlist1', 'My Favorites', 'user1'),
            ('playlist2', 'Workout Mix', 'user1'),
            ('playlist3', 'Chill Vibes', 'user2')
        ]
        
        print("Adding sample playlists...")
        for playlist in playlists:
            cursor.execute(
                'INSERT INTO playlist (playlist_id, name, user_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING',
                playlist
            )
        
        # Add songs to playlists
        playlist_songs = [
            ('playlist1', 'S1'),
            ('playlist1', 'S3'),
            ('playlist2', 'S2'),
            ('playlist2', 'S5'),
            ('playlist3', 'S4')
        ]
        
        print("Adding songs to playlists...")
        for ps in playlist_songs:
            cursor.execute(
                'INSERT INTO playlist_song (playlist_id, song_id) VALUES (%s, %s) ON CONFLICT DO NOTHING',
                ps
            )
        
        conn.commit()
        print("Basic data loaded successfully!")
        
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
    load_basic_data()