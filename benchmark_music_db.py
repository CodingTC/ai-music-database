import psycopg2
from psycopg2.extras import RealDictCursor
import time
import statistics

# Your actual database config
DB_CONFIG = {
    'dbname': 'music_recommendation',
    'user': 'tdcooley',
    'password': 'appledogfish',
    'host': 'localhost',
    'port': '5432'
}

class MusicDatabaseBenchmark:
    def __init__(self, num_runs=15):
        self.num_runs = num_runs
        self.results = {}
        self.conn = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            print("✓ Connected to music_recommendation database")
            return True
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def measure_query(self, name, query):
        """Measure query execution time across multiple runs"""
        times = []
        
        try:
            for _ in range(self.num_runs):
                cur = self.conn.cursor(cursor_factory=RealDictCursor)
                
                # Actual measurement
                start = time.perf_counter()
                cur.execute(query)
                results = cur.fetchall()
                end = time.perf_counter()
                
                elapsed_ms = (end - start) * 1000
                times.append(elapsed_ms)
                cur.close()
            
            self.results[name] = {
                'mean': statistics.mean(times),
                'median': statistics.median(times),
                'min': min(times),
                'max': max(times),
                'stdev': statistics.stdev(times) if len(times) > 1 else 0,
                'num_runs': self.num_runs,
                'rows_returned': len(results)
            }
            
            return self.results[name]
        except Exception as e:
            print(f"✗ Query failed: {e}")
            self.results[name] = {'error': str(e)}
            return None
    
    def print_report(self):
        """Print formatted benchmark report"""
        print("\n" + "="*80)
        print("MUSIC RECOMMENDATION DATABASE - PERFORMANCE REPORT")
        print("="*80 + "\n")
        
        for query_name, metrics in self.results.items():
            if 'error' in metrics:
                print(f"\n{query_name}")
                print("-" * 60)
                print(f"  ERROR: {metrics['error']}")
                continue
            
            print(f"\n{query_name}")
            print("-" * 60)
            print(f"  Runs:           {metrics['num_runs']}")
            print(f"  Mean:           {metrics['mean']:.2f}ms")
            print(f"  Median:         {metrics['median']:.2f}ms")
            print(f"  Min:            {metrics['min']:.2f}ms")
            print(f"  Max:            {metrics['max']:.2f}ms")
            print(f"  Std Dev:        {metrics['stdev']:.2f}ms")
            print(f"  Rows returned:  {metrics['rows_returned']}")

def run_benchmarks():
    """Run actual benchmark queries against your schema"""
    benchmark = MusicDatabaseBenchmark(num_runs=15)
    
    if not benchmark.connect():
        return
    
    try:
        print("\nRunning benchmarks...\n")
        
        # Query 1: Find songs by artist (uses FK index)
        benchmark.measure_query(
            "Find all songs by a specific artist (FK JOIN)",
            """
            SELECT s.song_id, s.title, a.name, s.tempo, s.danceability
            FROM Song s
            JOIN Artist a ON s.artist_id = a.artist_id
            WHERE s.artist_id = (SELECT artist_id FROM Artist LIMIT 1)
            LIMIT 100
            """
        )
        
        # Query 2: Get user's playlists with song count
        benchmark.measure_query(
            "User's playlists with song count (3-table JOIN)",
            """
            SELECT p.playlist_id, p.name, COUNT(ps.song_id) as song_count
            FROM "User" u
            JOIN Playlist p ON u.user_id = p.user_id
            LEFT JOIN Playlist_Song ps ON p.playlist_id = ps.playlist_id
            WHERE u.user_id = (SELECT user_id FROM "User" LIMIT 1)
            GROUP BY p.playlist_id, p.name
            """
        )
        
        # Query 3: Find songs in a playlist with audio features
        benchmark.measure_query(
            "Songs in playlist with audio features (sorted by danceability)",
            """
            SELECT s.song_id, s.title, s.tempo, s.danceability, s.energy
            FROM Playlist_Song ps
            JOIN Song s ON ps.song_id = s.song_id
            WHERE ps.playlist_id = (SELECT playlist_id FROM Playlist LIMIT 1)
            ORDER BY s.danceability DESC
            LIMIT 20
            """
        )
        
        # Query 4: Find similar songs (same artist, similar tempo)
        benchmark.measure_query(
            "Find similar songs (by artist and audio features)",
            """
            SELECT s.song_id, s.title, s.tempo, s.danceability, s.energy
            FROM Song s
            WHERE s.artist_id = (SELECT artist_id FROM Song LIMIT 1)
            AND s.tempo BETWEEN 
                (SELECT tempo FROM Song WHERE song_id = (SELECT song_id FROM Song LIMIT 1)) - 10
                AND
                (SELECT tempo FROM Song WHERE song_id = (SELECT song_id FROM Song LIMIT 1)) + 10
            LIMIT 20
            """
        )
        
        # Query 5: Aggregate query - average audio features per artist
        benchmark.measure_query(
            "Aggregate: average tempo/danceability per artist",
            """
            SELECT a.name, 
                   COUNT(s.song_id) as song_count,
                   ROUND(AVG(s.tempo)::numeric, 2) as avg_tempo,
                   ROUND(AVG(s.danceability)::numeric, 2) as avg_danceability,
                   ROUND(AVG(s.energy)::numeric, 2) as avg_energy
            FROM Artist a
            LEFT JOIN Song s ON a.artist_id = s.artist_id
            GROUP BY a.artist_id, a.name
            HAVING COUNT(s.song_id) > 0
            ORDER BY song_count DESC
            LIMIT 20
            """
        )
        
        # Query 6: User + Playlists + Songs (complex JOIN)
        benchmark.measure_query(
            "User's complete playlist data (4-table JOIN)",
            """
            SELECT u.name as user_name, 
                   p.name as playlist_name, 
                   s.title as song_title,
                   s.tempo, s.danceability
            FROM "User" u
            JOIN Playlist p ON u.user_id = p.user_id
            JOIN Playlist_Song ps ON p.playlist_id = ps.playlist_id
            JOIN Song s ON ps.song_id = s.song_id
            WHERE u.user_id = (SELECT user_id FROM "User" LIMIT 1)
            LIMIT 50
            """
        )
        
        # Query 7: Songs in cluster (for recommendation engine)
        benchmark.measure_query(
            "Songs in same ML cluster (for recommendations)",
            """
            SELECT s.song_id, s.title, s.tempo, s.danceability, s.energy, s.cluster_id
            FROM Song s
            WHERE s.cluster_id = (SELECT cluster_id FROM Song WHERE cluster_id IS NOT NULL LIMIT 1)
            ORDER BY s.danceability DESC
            LIMIT 30
            """
        )
        
        benchmark.print_report()
        
    finally:
        benchmark.close()

if __name__ == "__main__":
    run_benchmarks()
