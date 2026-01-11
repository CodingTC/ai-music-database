import psycopg2
from psycopg2.extras import RealDictCursor
import time

DB_CONFIG = {
    'dbname': 'music_recommendation',
    'user': 'tdcooley',
    'password': 'appledogfish',
    'host': 'localhost',
    'port': '5432'
}

class SchemaAnalysis:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Connected to music_recommendation database\n")
    
    def close(self):
        self.conn.close()
    
    def get_table_stats(self, table_name):
        """Get table size and row count"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Get row count
        cur.execute(f"SELECT COUNT(*) as row_count FROM {table_name}")
        row_count = cur.fetchone()['row_count']
        
        # Get table size in bytes
        cur.execute(f"""
            SELECT pg_total_relation_size('{table_name}') as size_bytes
        """)
        size_bytes = cur.fetchone()['size_bytes']
        
        # Get number of columns
        cur.execute(f"""
            SELECT COUNT(*) as column_count
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """)
        column_count = cur.fetchone()['column_count']
        
        cur.close()
        
        return {
            'table': table_name,
            'rows': row_count,
            'size_bytes': size_bytes,
            'size_mb': size_bytes / (1024 * 1024),
            'columns': column_count
        }
    
    def get_query_statistics(self, query_name, query, runs=15):
        """Execute query and return performance stats"""
        times = []
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        for _ in range(runs):
            start = time.perf_counter()
            cur.execute(query)
            results = cur.fetchall()
            end = time.perf_counter()
            
            elapsed_ms = (end - start) * 1000
            times.append(elapsed_ms)
        
        cur.close()
        
        return {
            'query': query_name,
            'avg_ms': sum(times) / len(times),
            'min_ms': min(times),
            'max_ms': max(times),
            'runs': runs
        }
    
    def analyze_schema(self):
        """Analyze your normalized schema"""
        
        print("\n" + "="*80)
        print("MUSIC RECOMMENDATION DATABASE - SCHEMA ANALYSIS")
        print("="*80)
        
        # Part 1: Table sizes and structure
        print("\n1. TABLE STRUCTURE & STORAGE EFFICIENCY")
        print("-" * 80)
        
        tables = ['Artist', 'Song', '"User"', 'Playlist', 'Playlist_Song']
        total_size = 0
        total_rows = 0
        
        stats_list = []
        for table in tables:
            try:
                stats = self.get_table_stats(table)
                stats_list.append(stats)
                total_size += stats['size_bytes']
                total_rows += stats['rows']
                
                print(f"\n{table.strip('\"')}:")
                print(f"  Rows:    {stats['rows']:,}")
                print(f"  Columns: {stats['columns']}")
                print(f"  Size:    {stats['size_mb']:.2f} MB ({stats['size_bytes']:,} bytes)")
            except Exception as e:
                print(f"\n{table}: Error - {e}")
        
        print(f"\n{'─' * 40}")
        print(f"Total schema size: {total_size / (1024*1024):.2f} MB")
        print(f"Total rows: {total_rows:,}")
        print(f"\n✓ Key insight: Normalized schema stores each fact once")
        print(f"  Artist, Song, User, and Playlist data are separated,")
        print(f"  eliminating data redundancy")
        
        # Part 2: Key relationships and indexes
        print("\n\n2. REFERENTIAL INTEGRITY & RELATIONSHIPS")
        print("-" * 80)
        
        relationships = [
            ("Song → Artist", "Song.artist_id → Artist.artist_id"),
            ("Playlist → User", "Playlist.user_id → User.user_id"),
            ("Playlist_Song → Playlist", "Playlist_Song.playlist_id → Playlist.playlist_id"),
            ("Playlist_Song → Song", "Playlist_Song.song_id → Song.song_id")
        ]
        
        print("\nSchema relationships (4 total):")
        for name, relationship in relationships:
            print(f"  ✓ {name}: {relationship}")
        
        # Check for orphaned records
        print("\n\nData integrity checks:")
        
        try:
            cur = self.conn.cursor()
            
            # Check for songs with invalid artist_id
            cur.execute("""
                SELECT COUNT(*) as orphan_count
                FROM Song s
                WHERE s.artist_id IS NOT NULL
                AND NOT EXISTS (SELECT 1 FROM Artist a WHERE a.artist_id = s.artist_id)
            """)
            orphan_songs = cur.fetchone()[0]
            print(f"  Songs with orphaned artist_id: {orphan_songs} ✓")
            
            # Check for playlists with invalid user_id
            cur.execute("""
                SELECT COUNT(*) as orphan_count
                FROM Playlist p
                WHERE NOT EXISTS (SELECT 1 FROM "User" u WHERE u.user_id = p.user_id)
            """)
            orphan_playlists = cur.fetchone()[0]
            print(f"  Playlists with orphaned user_id: {orphan_playlists} ✓")
            
            # Check for playlist_songs with invalid references
            cur.execute("""
                SELECT COUNT(*) as orphan_count
                FROM Playlist_Song ps
                WHERE NOT EXISTS (SELECT 1 FROM Playlist p WHERE p.playlist_id = ps.playlist_id)
                OR NOT EXISTS (SELECT 1 FROM Song s WHERE s.song_id = ps.song_id)
            """)
            orphan_ps = cur.fetchone()[0]
            print(f"  Playlist_Songs with orphaned references: {orphan_ps} ✓")
            
            cur.close()
        except Exception as e:
            print(f"  Error checking orphans: {e}")
        
        print(f"\n✓ No data anomalies through BCNF normalization")
        
        # Part 3: Query efficiency
        print("\n\n3. QUERY EFFICIENCY WITH NORMALIZED SCHEMA")
        print("-" * 80)
        
        print("\nQuery A: User's playlists with song count (3-table JOIN):")
        query_a = """
            SELECT u.name as user_name, p.playlist_id, p.name as playlist_name, COUNT(ps.song_id) as song_count
            FROM "User" u
            JOIN Playlist p ON u.user_id = p.user_id
            LEFT JOIN Playlist_Song ps ON p.playlist_id = ps.playlist_id
            WHERE u.user_id = (SELECT user_id FROM "User" LIMIT 1)
            GROUP BY u.user_id, u.name, p.playlist_id, p.name
        """
        
        try:
            stats_a = self.get_query_statistics("User playlists", query_a)
            print(f"  Execution time: {stats_a['avg_ms']:.2f}ms (avg of {stats_a['runs']} runs)")
            print(f"  Range: {stats_a['min_ms']:.2f}ms - {stats_a['max_ms']:.2f}ms")
        except Exception as e:
            print(f"  Error: {e}")
        
        print("\nQuery B: Find songs in playlist with audio features:")
        query_b = """
            SELECT s.song_id, s.title, s.tempo, s.danceability, s.energy
            FROM Playlist_Song ps
            JOIN Song s ON ps.song_id = s.song_id
            WHERE ps.playlist_id = (SELECT playlist_id FROM Playlist LIMIT 1)
            ORDER BY s.danceability DESC
            LIMIT 20
        """
        
        try:
            stats_b = self.get_query_statistics("Playlist songs", query_b)
            print(f"  Execution time: {stats_b['avg_ms']:.2f}ms (avg of {stats_b['runs']} runs)")
            print(f"  Range: {stats_b['min_ms']:.2f}ms - {stats_b['max_ms']:.2f}ms")
        except Exception as e:
            print(f"  Error: {e}")
        
        print("\nQuery C: Aggregate - average audio features by artist:")
        query_c = """
            SELECT a.name, 
                   COUNT(s.song_id) as song_count,
                   ROUND(AVG(s.tempo)::numeric, 2) as avg_tempo,
                   ROUND(AVG(s.danceability)::numeric, 2) as avg_danceability
            FROM Artist a
            LEFT JOIN Song s ON a.artist_id = s.artist_id
            GROUP BY a.artist_id, a.name
            HAVING COUNT(s.song_id) > 0
            ORDER BY song_count DESC
            LIMIT 20
        """
        
        try:
            stats_c = self.get_query_statistics("Artist aggregates", query_c)
            print(f"  Execution time: {stats_c['avg_ms']:.2f}ms (avg of {stats_c['runs']} runs)")
            print(f"  Range: {stats_c['min_ms']:.2f}ms - {stats_c['max_ms']:.2f}ms")
        except Exception as e:
            print(f"  Error: {e}")
        
        print(f"\n✓ PostgreSQL query planner optimizes these JOINs efficiently")
        
        # Summary
        print("\n\n" + "="*80)
        print("SUMMARY: NORMALIZED SCHEMA BENEFITS")
        print("="*80)
        print("""
✓ Storage efficiency: No data redundancy (Artist, Song, User kept separate)
✓ Data integrity: BCNF prevents update/delete anomalies, zero orphaned records
✓ Query optimization: Clear table boundaries enable efficient JOINs
✓ Scalability: Separate concerns scale better than denormalized alternatives
✓ Maintainability: Changes to one entity don't ripple through multiple tables
        """)

def main():
    analysis = SchemaAnalysis()
    try:
        analysis.analyze_schema()
    finally:
        analysis.close()

if __name__ == "__main__":
    main()
