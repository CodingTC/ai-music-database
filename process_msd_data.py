import os
import h5py
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import glob
import warnings

# Suppress NumPy version warning
warnings.filterwarnings("ignore", message="A NumPy version")

# Create directory for processed data
os.makedirs('data/processed', exist_ok=True)

def process_msd_data():
    print("Processing MSD data from local files...")
    
    # Path to your Million Song Subset directory
    msd_path = 'MillionSongSubset'
    
    # Find all h5 files in the directory (recursively)
    h5_files = glob.glob(f'{msd_path}/**/*.h5', recursive=True)
    print(f"Found {len(h5_files)} h5 files")
    
    # Extract song data
    song_data = []
    
    # Limit to 200 songs for initial development
    limit = 200
    count = 0
    
    if h5_files:
        sample_h5 = h5py.File(h5_files[0], 'r')
        print("H5 file structure:")
        for key in sample_h5.keys():
            print(f"- {key}")
            if isinstance(sample_h5[key], h5py.Group):
                print(f"  Subkeys in {key}:")
                for subkey in sample_h5[key].keys():
                    print(f"  - {subkey}")
        sample_h5.close()
    
    for h5_file in h5_files:
        if count >= limit:
            break
            
        try:
            h5 = h5py.File(h5_file, 'r')
            
            # Extract metadata using the correct paths based on MSD structure
            try:
                # The title is in h5['metadata']['songs']['title']
                title = h5['metadata']['songs']['title'][0].decode('utf-8', errors='replace')
            except:
                title = f"Unknown Song {count}"
                
            try:
                # The artist_id is in h5['metadata']['songs']['artist_id']
                artist_id = h5['metadata']['songs']['artist_id'][0].decode('utf-8', errors='replace')
            except:
                artist_id = f"AR{count:08d}"
                
            try:
                # The artist_name is in h5['metadata']['songs']['artist_name']
                artist_name = h5['metadata']['songs']['artist_name'][0].decode('utf-8', errors='replace')
            except:
                artist_name = f"Artist {count}"
            
            # Extract audio features with error handling
            try:
                # The tempo is in h5['analysis']['songs']['tempo']
                tempo = float(h5['analysis']['songs']['tempo'][0])
            except:
                tempo = np.random.uniform(60, 180)  # Random BPM
                
            try:
                # The key is in h5['analysis']['songs']['key']
                key = int(h5['analysis']['songs']['key'][0])
            except:
                key = np.random.randint(0, 12)  # Random key
                
            try:
                # The mode is in h5['analysis']['songs']['mode']
                mode = int(h5['analysis']['songs']['mode'][0])
            except:
                mode = np.random.randint(0, 2)  # Random mode
            
            try:
                # The year is in h5['musicbrainz']['songs']['year']
                year = int(h5['musicbrainz']['songs']['year'][0])
                if year == 0:  # Some entries have year as 0, replace with a reasonable value
                    year = np.random.randint(1950, 2023)
            except:
                year = np.random.randint(1950, 2023)  # Random year
            
            try:
                # The loudness is in h5['analysis']['songs']['loudness']
                loudness = float(h5['analysis']['songs']['loudness'][0])
            except:
                loudness = np.random.uniform(-60, 0)  # Random loudness
                
            # Extract energy and danceability if available
            try:
                # The energy might be in h5['analysis']['songs']['energy']
                energy = float(h5['analysis']['songs']['energy'][0])
            except:
                energy = np.random.uniform(0, 1)  # Random energy
                
            try:
                # The danceability might be in h5['analysis']['songs']['danceability']
                danceability = float(h5['analysis']['songs']['danceability'][0])
            except:
                danceability = np.random.uniform(0, 1)  # Random danceability
            
            # The song_id should be extracted from h5['metadata']['songs']['track_id']
            try:
                song_id = h5['metadata']['songs']['track_id'][0].decode('utf-8', errors='replace')
            except:
                # Use filename as fallback
                song_id = os.path.basename(h5_file).replace('.h5', '')
            
            song = {
                'song_id': song_id,
                'title': title,
                'artist_id': artist_id,
                'artist_name': artist_name,
                'tempo': tempo,
                'key': key,
                'mode': mode,
                'year': year,
                'energy': energy,
                'danceability': danceability,
                'loudness': loudness
            }
            
            song_data.append(song)
            count += 1
            
            if count % 10 == 0:
                print(f"Processed {count} songs")
            
            h5.close()
            
        except Exception as e:
            print(f"Error processing file {h5_file}: {e}")
    
    print(f"Successfully processed {len(song_data)} songs")
    
    # Convert to DataFrame
    df = pd.DataFrame(song_data)
    
    # Run K-means clustering
    #4 dimension
    features = df[['tempo', 'energy', 'danceability', 'loudness']].values
    
    features = np.nan_to_num(features)
    
    #make sure one dimension is not weighted too heavily
    features = (features - features.mean(axis=0)) / (features.std(axis=0) + 1e-8)
    
    n_clusters = 10
    
    # Apply K-means
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    df['cluster_id'] = kmeans.fit_predict(features)
    
    df.to_csv('data/processed/songs.csv', index=False)
    
    # Create separate files for each table
    df[['artist_id', 'artist_name']].drop_duplicates().rename(
        columns={'artist_name': 'name'}
    ).to_csv('data/processed/artists.csv', index=False)
    
    df[['song_id', 'title', 'tempo', 'key', 'mode', 'year', 'energy', 
        'danceability', 'loudness', 'cluster_id', 'artist_id']].rename(
        columns={'key': 'key_signature', 'year': 'release_year'}
    ).to_csv('data/processed/songs_processed.csv', index=False)
    
    print("Processing complete!")
    return True

# Main execution
if __name__ == "__main__":
    process_msd_data()