import psycopg2
from psycopg2 import sql

# Database connection parameters
db_params = {
    'dbname': 'music_recommendation',
    'user': 'tdcooley',  
    'password': 'appledogfish',  
    'host': 'localhost',
    'port': '5432'
}

# Connect to PostgreSQL
conn = None
try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    print("Connected to PostgreSQL successfully!")
    
    # Create tables
    tables = [
        """
        CREATE TABLE IF NOT EXISTS Artist (
            artist_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Song (
            song_id VARCHAR PRIMARY KEY,
            title VARCHAR NOT NULL,
            album VARCHAR,
            tempo FLOAT,
            key_signature VARCHAR,
            mode VARCHAR,
            release_year INTEGER,
            energy FLOAT,
            danceability FLOAT,
            loudness FLOAT,
            cluster_id INTEGER,
            artist_id VARCHAR,
            FOREIGN KEY (artist_id) REFERENCES Artist(artist_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS "User" (
            user_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            email VARCHAR NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Playlist (
            playlist_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            user_id VARCHAR,
            FOREIGN KEY (user_id) REFERENCES "User"(user_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Playlist_Song (
            playlist_id VARCHAR,
            song_id VARCHAR,
            PRIMARY KEY (playlist_id, song_id),
            FOREIGN KEY (playlist_id) REFERENCES Playlist(playlist_id),
            FOREIGN KEY (song_id) REFERENCES Song(song_id)
        )
        """
    ]
    
    for table in tables:
        cursor.execute(table)
    
    conn.commit()
    print("Tables created successfully!")
    
except Exception as e:
    print(f"Error: {e}")
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection closed.")