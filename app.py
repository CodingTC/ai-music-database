from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import psycopg2
import psycopg2.extras

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # for flash messages

# Database connection
def get_db_connection():
    conn = psycopg2.connect(
        dbname='music_recommendation',
        user='tdcooley',  # Update to your username
        password='appledogfish',  # Update to your password
        host='localhost',
        port='5432'
    )
    conn.cursor_factory = psycopg2.extras.DictCursor
    return conn

@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # display songs
    cur.execute('SELECT s.song_id, s.title, a.name as artist, s.cluster_id FROM song s JOIN artist a ON s.artist_id = a.artist_id LIMIT 20')
    songs = cur.fetchall()
    
    # show playlists
    cur.execute('SELECT playlist_id, name FROM playlist')
    playlists = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return render_template('index.html', songs=songs, playlists=playlists)

@app.route('/song/<song_id>')
def song_detail(song_id):
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get song details
    cur.execute('''
        SELECT s.song_id, s.title, a.name as artist, s.tempo, s.energy, 
        s.danceability, s.loudness, s.cluster_id
        FROM song s 
        JOIN artist a ON s.artist_id = a.artist_id
        WHERE s.song_id = %s
    ''', (song_id,))
    song = cur.fetchone()
    
    if not song:
        cur.close()
        conn.close()
        flash('Song not found!')
        return redirect(url_for('index'))
    
    # Get similar songs (same cluster)
    cur.execute('''
        SELECT s.song_id, s.title, a.name as artist
        FROM song s 
        JOIN artist a ON s.artist_id = a.artist_id
        WHERE s.cluster_id = %s AND s.song_id != %s
        LIMIT 5
    ''', (song['cluster_id'], song_id))
    similar_songs = cur.fetchall()
    
    # Get playlists for the "Add to Playlist" functionality
    cur.execute('SELECT playlist_id, name FROM playlist')
    playlists = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return render_template('song_detail.html', song=song, similar_songs=similar_songs, playlists=playlists)

@app.route('/search')
def search():
    query = request.args.get('query', '')
    
    if not query:
        return redirect(url_for('index'))
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Search for songs or artists
    cur.execute('''
        SELECT s.song_id, s.title, a.name as artist, s.cluster_id
        FROM song s 
        JOIN artist a ON s.artist_id = a.artist_id
        WHERE s.title ILIKE %s OR a.name ILIKE %s
        LIMIT 50
    ''', (f'%{query}%', f'%{query}%'))
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return render_template('search_results.html', results=results, query=query)

@app.route('/playlists')
def playlists():
    conn = get_db_connection()
    cur = conn.cursor()
    #get all playlists with their creator names, only showing playlists with at least 1 song
    cur.execute('''
        SELECT p.playlist_id, p.name, u.name as creator, COUNT(ps.song_id) as song_count
        FROM playlist p
        JOIN "User" u ON p.user_id = u.user_id
        LEFT JOIN playlist_song ps ON p.playlist_id = ps.playlist_id
        GROUP BY p.playlist_id, p.name, u.name
        HAVING COUNT(ps.song_id) > 0
        ORDER BY p.name
    ''')
    
    playlists = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return render_template('playlists.html', playlists=playlists)

@app.route('/create_playlist', methods=['GET', 'POST'])
def create_playlist():
    if request.method == 'POST':
        name = request.form['name']
        user_id = request.form['user_id']
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Generate a simple playlist ID using timestamp
        import time
        playlist_id = f"playlist{int(time.time())}"
        
        cursor.execute(
            'INSERT INTO playlist (playlist_id, name, user_id) VALUES (%s, %s, %s)',
            (playlist_id, name, user_id)
        )
        conn.commit()
        
        cursor.close()
        conn.close()
        
        flash(f'Playlist "{name}" created successfully!')
        return redirect(url_for('playlist_detail', playlist_id=playlist_id))
    
    # GET request - show the form
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get users for dropdown
    cursor.execute('SELECT user_id, name FROM "User"')
    users = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('create_playlist.html', users=users)

@app.route('/create_user', methods=['GET', 'POST'])
def create_user():
    if request.method == 'POST':
        name = request.form['name']
        email = request.form['email']
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Find the next available user ID
        cursor.execute('SELECT MAX(SUBSTRING(user_id FROM 5)::int) FROM "User" WHERE user_id LIKE \'user%\'')
        result = cursor.fetchone()[0]
        next_id = 1 if result is None else result + 1
        user_id = f"user{next_id}"
        
        cursor.execute(
            'INSERT INTO "User" (user_id, name, email) VALUES (%s, %s, %s)',
            (user_id, name, email)
        )
        conn.commit()
        
        cursor.close()
        conn.close()
        
        flash(f'User "{name}" created successfully!')
        return redirect(url_for('users'))
    
    # GET request - show the form
    return render_template('create_user.html')

@app.route('/edit_user/<user_id>', methods=['GET', 'POST'])
def edit_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if request.method == 'POST':
        name = request.form['name']
        email = request.form['email']
        
        cursor.execute(
            'UPDATE "User" SET name = %s, email = %s WHERE user_id = %s',
            (name, email, user_id)
        )
        conn.commit()
        
        flash("User updated successfully!")
        return redirect(url_for('users'))
    
    # GET request - show the form with current values
    cursor.execute('SELECT * FROM "User" WHERE user_id = %s', (user_id,))
    user = cursor.fetchone()
    
    if user is None:
        cursor.close()
        conn.close()
        flash("User not found!")
        return redirect(url_for('users'))
    
    cursor.close()
    conn.close()
    
    return render_template('edit_user.html', user=user)

@app.route('/delete_user/<user_id>', methods=['POST'])
def delete_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # First check if user has any playlists
    cursor.execute('SELECT COUNT(*) FROM playlist WHERE user_id = %s', (user_id,))
    playlist_count = cursor.fetchone()[0]
    
    if playlist_count > 0:
        flash("Cannot delete user - they have playlists. Delete their playlists first.")
        cursor.close()
        conn.close()
        return redirect(url_for('users'))
    
    # Delete the user
    cursor.execute('DELETE FROM "User" WHERE user_id = %s', (user_id,))
    conn.commit()
    
    cursor.close()
    conn.close()
    
    flash("User deleted successfully!")
    return redirect(url_for('users'))

# Add a route to display all users
@app.route('/users')
def users():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all users
    cursor.execute('SELECT user_id, name, email FROM "User"')
    users = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('users.html', users=users)

# Add this route for adding a song to a playlist
@app.route('/add_to_playlist', methods=['POST'])
def add_to_playlist():
    song_id = request.form['song_id']
    playlist_id = request.form['playlist_id']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if song already in playlist
    cursor.execute(
        'SELECT * FROM playlist_song WHERE playlist_id = %s AND song_id = %s',
        (playlist_id, song_id)
    )
    
    if cursor.fetchone() is None:
        # Add song to playlist
        cursor.execute(
            'INSERT INTO playlist_song (playlist_id, song_id) VALUES (%s, %s)',
            (playlist_id, song_id)
        )
        conn.commit()
        message = "Song added to playlist successfully!"
    else:
        message = "Song is already in this playlist!"
    
    cursor.close()
    conn.close()
    
    # If it's an AJAX request, return JSON
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return jsonify({'success': True, 'message': message})
    
    # Otherwise redirect
    flash(message)
    return redirect(url_for('song_detail', song_id=song_id))

# Add this route for removing a song from a playlist
@app.route('/remove_from_playlist', methods=['POST'])
def remove_from_playlist():
    song_id = request.form['song_id']
    playlist_id = request.form['playlist_id']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute(
        'DELETE FROM playlist_song WHERE playlist_id = %s AND song_id = %s',
        (playlist_id, song_id)
    )
    conn.commit()
    
    cursor.close()
    conn.close()
    
    flash("Song removed from playlist!")
    return redirect(url_for('playlist_detail', playlist_id=playlist_id))

# Add this route for updating a playlist's details
@app.route('/edit_playlist/<playlist_id>', methods=['GET', 'POST'])
def edit_playlist(playlist_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if request.method == 'POST':
        name = request.form['name']
        
        cursor.execute(
            'UPDATE playlist SET name = %s WHERE playlist_id = %s',
            (name, playlist_id)
        )
        conn.commit()
        
        flash("Playlist updated successfully!")
        return redirect(url_for('playlist_detail', playlist_id=playlist_id))
    
    # GET request - show the form with current values
    cursor.execute('SELECT * FROM playlist WHERE playlist_id = %s', (playlist_id,))
    playlist = cursor.fetchone()
    
    if playlist is None:
        cursor.close()
        conn.close()
        flash("Playlist not found!")
        return redirect(url_for('playlists'))
    
    cursor.close()
    conn.close()
    
    return render_template('edit_playlist.html', playlist=playlist)

# Add this route for deleting a playlist
@app.route('/delete_playlist/<playlist_id>', methods=['POST'])
def delete_playlist(playlist_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # First delete all songs in the playlist (due to foreign key constraint)
    cursor.execute('DELETE FROM playlist_song WHERE playlist_id = %s', (playlist_id,))
    
    # Then delete the playlist
    cursor.execute('DELETE FROM playlist WHERE playlist_id = %s', (playlist_id,))
    conn.commit()
    
    cursor.close()
    conn.close()
    
    flash("Playlist deleted successfully!")
    return redirect(url_for('playlists'))

@app.route('/playlist/<playlist_id>')
def playlist_detail(playlist_id):
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get playlist info
    cur.execute('''
        SELECT p.playlist_id, p.name, u.name as creator
        FROM playlist p
        JOIN "User" u ON p.user_id = u.user_id
        WHERE p.playlist_id = %s
    ''', (playlist_id,))
    playlist = cur.fetchone()
    
    if not playlist:
        cur.close()
        conn.close()
        flash('Playlist not found!')
        return redirect(url_for('playlists'))
    
    # Get songs in this playlist
    cur.execute('''
        SELECT s.song_id, s.title, a.name as artist, s.cluster_id
        FROM song s
        JOIN artist a ON s.artist_id = a.artist_id
        JOIN playlist_song ps ON s.song_id = ps.song_id
        WHERE ps.playlist_id = %s
        ORDER BY s.title
    ''', (playlist_id,))
    songs = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return render_template('playlist_detail.html', playlist=playlist, songs=songs)


def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create the playlist ID sequence
    cursor.execute("CREATE SEQUENCE IF NOT EXISTS playlist_id_seq START 4")
    conn.commit()
    
    cursor.close()
    conn.close()
    print("Database initialized successfully!")

if __name__ == '__main__':
    initialize_database()
    app.run(debug=True)
