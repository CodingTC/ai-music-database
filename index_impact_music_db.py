import psycopg2
import time

DB_CONFIG = {
    'dbname': 'music_recommendation',
    'user': 'tdcooley',
    'password': 'appledogfish',
    'host': 'localhost',
    'port': '5432'
}

class IndexImpactTest:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Connected to music_recommendation database\n")
    
    def close(self):
        self.conn.close()
    
    def run_query_timed(self, query, runs=10):
        """Run query multiple times and return avg execution time"""
        times = []
        cur = self.conn.cursor()
        
        for _ in range(runs):
            start = time.perf_counter()
            cur.execute(query)
            cur.fetchall()
            end = time.perf_counter()
            times.append((end - start) * 1000)  # Convert to ms
        
        cur.close()
        return {
            'avg_ms': sum(times) / len(times),
            'min_ms': min(times),
            'max_ms': max(times)
        }
    
    def create_index(self, index_name, table, column):
        """Create an index"""
        cur = self.conn.cursor()
        
        # Check if index already exists
        cur.execute(f"""
            SELECT 1 FROM pg_indexes 
            WHERE schemaname = 'public' 
            AND tablename = '{table}' 
            AND indexname = '{index_name}'
        """)
        
        if not cur.fetchone():
            print(f"✓ Creating index: {index_name}")
            cur.execute(f"CREATE INDEX {index_name} ON {table}({column})")
            self.conn.commit()
            return True
        else:
            print(f"⚠ Index {index_name} already exists")
            return False
        
        cur.close()
    
    def drop_index(self, index_name):
        """Drop an index"""
        cur = self.conn.cursor()
        try:
            cur.execute(f"DROP INDEX IF EXISTS {index_name}")
            self.conn.commit()
            print(f"✓ Dropped index: {index_name}")
        except Exception as e:
            print(f"✗ Failed to drop index: {e}")
        cur.close()
    
    def benchmark_index_impact(self, index_name, table, column, test_query, description):
        """
        Measure query performance before and after index creation
        """
        print("\n" + "="*70)
        print(f"INDEX IMPACT TEST: {description}")
        print("="*70)
        
        # Drop index if it exists (to test without it)
        cur = self.conn.cursor()
        cur.execute(f"DROP INDEX IF EXISTS {index_name}")
        self.conn.commit()
        cur.close()
        
        print("\n1. WITHOUT INDEX (sequential scan):")
        without_index = self.run_query_timed(test_query, runs=10)
        print(f"   Average: {without_index['avg_ms']:.2f}ms")
        print(f"   Range: {without_index['min_ms']:.2f}ms - {without_index['max_ms']:.2f}ms")
        
        # Create index
        self.create_index(index_name, table, column)
        
        # Allow time for index creation
        time.sleep(0.5)
        
        print("\n2. WITH INDEX:")
        with_index = self.run_query_timed(test_query, runs=10)
        print(f"   Average: {with_index['avg_ms']:.2f}ms")
        print(f"   Range: {with_index['min_ms']:.2f}ms - {with_index['max_ms']:.2f}ms")
        
        # Calculate improvement
        if with_index['avg_ms'] > 0:
            improvement = ((without_index['avg_ms'] - with_index['avg_ms']) / without_index['avg_ms']) * 100
            speedup = without_index['avg_ms'] / with_index['avg_ms']
        else:
            improvement = 0
            speedup = 1
        
        print(f"\n3. PERFORMANCE IMPROVEMENT:")
        print(f"   ✓ {improvement:.1f}% faster with index")
        print(f"   ✓ {speedup:.1f}x speedup")
        
        return {
            'index_name': index_name,
            'without_index_ms': without_index['avg_ms'],
            'with_index_ms': with_index['avg_ms'],
            'improvement_percent': improvement,
            'speedup': speedup
        }

def main():
    test = IndexImpactTest()
    results = []
    
    try:
        # TEST 1: Foreign key index on Song.artist_id
        result1 = test.benchmark_index_impact(
            index_name="idx_song_artist_id",
            table="Song",
            column="artist_id",
            test_query="""
                SELECT s.song_id, s.title, a.name, s.tempo
                FROM Song s
                JOIN Artist a ON s.artist_id = a.artist_id
                WHERE s.artist_id = (SELECT artist_id FROM Song WHERE artist_id IS NOT NULL LIMIT 1)
                LIMIT 100
            """,
            description="FK Index on Song.artist_id"
        )
        results.append(result1)
        
        # TEST 2: Foreign key index on Playlist.user_id
        result2 = test.benchmark_index_impact(
            index_name="idx_playlist_user_id",
            table="Playlist",
            column="user_id",
            test_query="""
                SELECT p.playlist_id, p.name
                FROM Playlist p
                WHERE p.user_id = (SELECT user_id FROM Playlist LIMIT 1)
            """,
            description="FK Index on Playlist.user_id"
        )
        results.append(result2)
        
        # TEST 3: Foreign key index on Playlist_Song.song_id
        result3 = test.benchmark_index_impact(
            index_name="idx_playlist_song_song_id",
            table="Playlist_Song",
            column="song_id",
            test_query="""
                SELECT ps.playlist_id, ps.song_id, s.title
                FROM Playlist_Song ps
                JOIN Song s ON ps.song_id = s.song_id
                WHERE ps.song_id = (SELECT song_id FROM Playlist_Song LIMIT 1)
            """,
            description="FK Index on Playlist_Song.song_id"
        )
        results.append(result3)
        
        # TEST 4: Index on Song.cluster_id (for recommendation queries)
        result4 = test.benchmark_index_impact(
            index_name="idx_song_cluster_id",
            table="Song",
            column="cluster_id",
            test_query="""
                SELECT s.song_id, s.title, s.tempo, s.danceability
                FROM Song s
                WHERE s.cluster_id = (SELECT cluster_id FROM Song WHERE cluster_id IS NOT NULL LIMIT 1)
                LIMIT 50
            """,
            description="Index on Song.cluster_id (for ML recommendations)"
        )
        results.append(result4)
        
        # SUMMARY
        print("\n" + "="*70)
        print("SUMMARY: INDEX OPTIMIZATION IMPACT")
        print("="*70)
        
        if results:
            total_improvement = sum(r['improvement_percent'] for r in results) / len(results)
            
            print(f"\nAverage improvement across all indexes: {total_improvement:.1f}%")
            print("\nDetailed results:")
            for r in results:
                print(f"\n  {r['index_name']}:")
                print(f"    • {r['improvement_percent']:.1f}% faster ({r['speedup']:.1f}x speedup)")
                print(f"    • {r['without_index_ms']:.2f}ms → {r['with_index_ms']:.2f}ms")
        
    finally:
        test.close()

if __name__ == "__main__":
    main()
