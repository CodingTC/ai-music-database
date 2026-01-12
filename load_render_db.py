import csv
import psycopg2
import psycopg2.extras
import os
import sys
import time

# =====================
# CONFIG
# =====================

DB_CONFIG = {
    "dbname": "music_recommendation_plub",
    "user": "tdcooley",
    "password": "4a3lT83imHIxReR3LNBVInUuwoB7Hpeb",
    "host": "dpg-d5i1m0npm1nc73edol00-a.oregon-postgres.render.com",
    "port": 5432,
}

ARTISTS_CSV = "data/processed/artists.csv"
SONGS_CSV = "data/processed/songs_processed.csv"

MAX_ARTISTS = 2000   # ⬅️ limit data to save storage
MAX_SONGS = 5000     # ⬅️ limit data to save storage

COMMIT_EVERY = 50    # commit frequently so it never stalls

# =====================
# CONNECT
# =====================

print("Connecting to Render Postgres...")
try:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()
    print("Connected successfully!\n")
except Exception as e:
    print("FAILED TO CONNECT TO DATABASE")
    print(e)
    sys.exit(1)

# =====================
# LOAD ARTISTS
# =====================

print("Loading artists...")
artist_count = 0
start_time = time.time()

with open(ARTISTS_CSV, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        try:
            cur.execute(
                """
                INSERT INTO artist (artist_id, name)
                VALUES (%s, %s)
                ON CONFLICT (artist_id) DO NOTHING
                """,
                (row["artist_id"], row["name"])
            )

            artist_count += 1
            print(f"[ARTIST] Inserted {row['artist_id']} — {row['name']}")

            if artist_count % COMMIT_EVERY == 0:
                conn.commit()
                print(f"Committed {artist_count} artists...\n")

            if artist_count >= MAX_ARTISTS:
                print("Artist limit reached.")
                break

        except Exception as e:
            print("ERROR inserting artist:", row)
            print(e)
            conn.rollback()

conn.commit()
print(f"Artists loaded: {artist_count}")
print(f"Artist load time: {time.time() - start_time:.2f}s\n")

# =====================
# LOAD SONGS
# =====================

print("Loading songs...")
song_count = 0
start_time = time.time()

with open(SONGS_CSV, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        try:
            cur.execute(
                """
                INSERT INTO song (
                    song_id, title, tempo, key_signature, mode,
                    release_year, energy, danceability, loudness,
                    cluster_id, artist_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (song_id) DO NOTHING
                """,
                (
                    row["song_id"],
                    row["title"],
                    float(row["tempo"]),
                    int(row["key_signature"]),
                    int(row["mode"]),
                    int(row["release_year"]),
                    float(row["energy"]),
                    float(row["danceability"]),
                    float(row["loudness"]),
                    int(row["cluster_id"]),
                    row["artist_id"],
                )
            )

            song_count += 1
            print(f"[SONG] Inserted {row['song_id']} — {row['title']}")

            if song_count % COMMIT_EVERY == 0:
                conn.commit()
                print(f"Committed {song_count} songs...\n")

            if song_count >= MAX_SONGS:
                print("Song limit reached.")
                break

        except Exception as e:
            print("ERROR inserting song:", row["song_id"])
            print(e)
            conn.rollback()

conn.commit()
print(f"Songs loaded: {song_count}")
print(f"Song load time: {time.time() - start_time:.2f}s\n")

# =====================
# CLEANUP
# =====================

cur.close()
conn.close()
print("Database load COMPLETE ✅")
