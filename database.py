# database.py
import sqlite3
import csv

def get_db_connection():
    conn = sqlite3.connect("database.sqlite")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        content TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        post_id INTEGER NOT NULL,
        content TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS likes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        post_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
        UNIQUE (user_id, post_id)
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event TEXT NOT NULL,
        details TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_likes_post_id ON likes(post_id);")

    conn.commit()
    conn.close()

def add_user(username, email):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO users (username, email) VALUES (?, ?)", (username, email))
        conn.commit()
    except sqlite3.IntegrityError as e:
        conn.rollback()
        print(f"⚠ Błąd: Użytkownik {username} lub email {email} już istnieje! ({e})")
    conn.close()

def add_post(user_id, content):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        user_id = int(user_id)
        cursor.execute("SELECT id FROM users WHERE id = ?", (user_id,))
        if not cursor.fetchone():
            raise ValueError(f"Użytkownik o ID {user_id} nie istnieje!")
        cursor.execute("INSERT INTO posts (user_id, content) VALUES (?, ?)", (user_id, content))
        conn.commit()
        log_event("Dodano post", f"Użytkownik {user_id} dodał post: '{content}'")
    except (sqlite3.IntegrityError, ValueError) as e:
        conn.rollback()
        print(f"⚠ Błąd: Nie można dodać postu dla użytkownika {user_id}! ({e})")
    conn.close()

def add_comment(user_id, post_id, content):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        user_id = int(user_id)
        post_id = int(post_id)
        cursor.execute("SELECT id FROM users WHERE id = ?", (user_id,))
        if not cursor.fetchone():
            raise ValueError(f"Użytkownik o ID {user_id} nie istnieje!")
        cursor.execute("SELECT id FROM posts WHERE id = ?", (post_id,))
        if not cursor.fetchone():
            raise ValueError(f"Post o ID {post_id} nie istnieje!")
        cursor.execute("INSERT INTO comments (user_id, post_id, content) VALUES (?, ?, ?)", (user_id, post_id, content))
        conn.commit()
        log_event("Dodano komentarz", f"Użytkownik {user_id} skomentował post {post_id}: '{content}'")
    except (sqlite3.IntegrityError, ValueError) as e:
        conn.rollback()
        print(f"⚠ Błąd: Nie można dodać komentarza! ({e})")
    conn.close()

def add_like(user_id, post_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        user_id = int(user_id)
        post_id = int(post_id)
        cursor.execute("INSERT INTO likes (user_id, post_id) VALUES (?, ?)", (user_id, post_id))
        conn.commit()
        log_event("Dodano polubienie", f"Użytkownik {user_id} polubił post {post_id}")
    except sqlite3.IntegrityError as e:
        conn.rollback()
        print(f"⚠ Błąd: Nie można dodać polubienia! ({e})")
    conn.close()

def log_event(event, details):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO logs (event, details) VALUES (?, ?)", (event, details))
        conn.commit()
    except sqlite3.Error as e:
        print(f"⚠ Błąd podczas logowania zdarzenia: {e}")
    conn.close()

def get_users():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    users = cursor.fetchall()
    conn.close()
    return users

def get_posts():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT posts.id, users.username, posts.content, posts.created_at
    FROM posts
    JOIN users ON posts.user_id = users.id
    ORDER BY posts.created_at DESC
    """)
    posts = cursor.fetchall()
    conn.close()
    return posts

def get_comments():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT c.id, u.username, p.content AS post_content, c.content, c.created_at
    FROM comments c
    JOIN users u ON c.user_id = u.id
    JOIN posts p ON c.post_id = p.id
    ORDER BY c.created_at DESC
    """)
    comments = cursor.fetchall()
    conn.close()
    return comments

def get_likes():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT likes.id, users.username, posts.content, likes.created_at
    FROM likes
    JOIN users ON likes.user_id = users.id
    JOIN posts ON likes.post_id = posts.id
    ORDER BY likes.created_at DESC
    """)
    likes = cursor.fetchall()
    conn.close()
    return likes

def get_user_posts(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT posts.id, posts.content, posts.created_at
    FROM posts
    WHERE posts.user_id = ?
    ORDER BY posts.created_at DESC
    """, (user_id,))
    posts = cursor.fetchall()
    conn.close()
    return posts

def get_post_comments(post_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT c.id, u.username, c.content, c.created_at
    FROM comments c
    JOIN users u ON c.user_id = u.id
    WHERE c.post_id = ?
    ORDER BY c.created_at DESC
    """, (post_id,))
    comments = cursor.fetchall()
    conn.close()
    return comments

def get_post_likes(post_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM likes WHERE post_id = ?", (post_id,))
    likes_count = cursor.fetchone()[0]
    conn.close()
    return likes_count

def get_user_post_counts():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT users.username, COUNT(posts.id) AS post_count
    FROM users
    LEFT JOIN posts ON users.id = posts.user_id
    GROUP BY users.id
    ORDER BY post_count DESC
    """)
    results = cursor.fetchall()
    conn.close()
    return results

def get_most_commented_posts():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT posts.id, posts.content, users.username, COUNT(comments.id) AS comment_count
    FROM posts
    JOIN users ON posts.user_id = users.id
    LEFT JOIN comments ON posts.id = comments.post_id
    GROUP BY posts.id
    HAVING comment_count > 0
    ORDER BY comment_count DESC
    """)
    results = cursor.fetchall()
    conn.close()
    return results

def get_top_likers():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT users.username, COUNT(likes.id) AS like_count
    FROM users
    JOIN likes ON users.id = likes.user_id
    GROUP BY users.id
    HAVING like_count > 0
    ORDER BY like_count DESC
    """)
    results = cursor.fetchall()
    conn.close()
    return results

def get_logs():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, event, details, created_at FROM logs ORDER BY created_at DESC")
    logs = cursor.fetchall()
    conn.close()
    return logs

def delete_inactive_users():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE id NOT IN (SELECT DISTINCT user_id FROM posts)")
    conn.commit()
    conn.close()
    print("🗑 Usunięto nieaktywnych użytkowników (bez postów).")

def delete_old_posts():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM posts WHERE created_at < DATETIME('now', '-30 days')")
    conn.commit()
    conn.close()
    print("🗑 Usunięto stare posty (starsze niż 30 dni).")

def delete_orphan_comments():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM comments WHERE post_id NOT IN (SELECT id FROM posts WHERE id IS NOT NULL)")
    conn.commit()
    conn.close()
    print("🗑 Usunięto osierocone komentarze.")

def delete_orphan_likes():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM likes WHERE post_id NOT IN (SELECT id FROM posts)")
    conn.commit()
    conn.close()
    print("🗑 Usunięto osierocone polubienia.")

def optimize_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("VACUUM")
    conn.commit()
    conn.close()
    print("🛠 Wykonano optymalizację bazy danych (VACUUM).")

def export_users():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, email, created_at FROM users")
    rows = cursor.fetchall()
    with open("users.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["ID", "Username", "Email", "Created At"])
        writer.writerows([(row['id'], row['username'], row['email'], row['created_at']) for row in rows])
    conn.close()
    print("📁 Eksportowano dane do users.csv!")

def export_posts():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT posts.id, users.username, posts.content, posts.created_at 
    FROM posts 
    JOIN users ON posts.user_id = users.id
    """)
    rows = cursor.fetchall()
    with open("posts.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Post ID", "Username", "Content", "Created At"])
        writer.writerows([(row['id'], row['username'], row['content'], row['created_at']) for row in rows])
    conn.close()
    print("📁 Eksportowano dane do posts.csv!")

def export_comments():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT comments.id, users.username, posts.content AS post_content, comments.content, comments.created_at 
    FROM comments 
    JOIN users ON comments.user_id = users.id 
    JOIN posts ON comments.post_id = posts.id
    """)
    rows = cursor.fetchall()
    with open("comments.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Comment ID", "Username", "Post Content", "Comment Content", "Created At"])
        writer.writerows([(row['id'], row['username'], row['post_content'], row['content'], row['created_at']) for row in rows])
    conn.close()
    print("📁 Eksportowano dane do comments.csv!")

def export_likes():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT likes.id, users.username, posts.content, likes.created_at 
    FROM likes 
    JOIN users ON likes.user_id = users.id 
    JOIN posts ON likes.post_id = posts.id
    """)
    rows = cursor.fetchall()
    with open("likes.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Like ID", "Username", "Post Content", "Created At"])
        writer.writerows([(row['id'], row['username'], row['content'], row['created_at']) for row in rows])
    conn.close()
    print("📁 Eksportowano dane do likes.csv!")

def export_logs():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, event, details, created_at FROM logs")
    rows = cursor.fetchall()
    with open("logs.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Log ID", "Event", "Details", "Created At"])
        writer.writerows([(row['id'], row['event'], row['details'], row['created_at']) for row in rows])
    conn.close()
    print("📁 Eksportowano dane do logs.csv!")