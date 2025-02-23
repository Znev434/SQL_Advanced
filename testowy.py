import sqlite3
import csv

# Połączenie z bazą danych
conn = sqlite3.connect("database.sqlite")
cursor = conn.cursor()

# Tworzenie tabeli 'users' (jeśli nie istnieje)
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# Tworzenie tabeli 'posts' (jeśli nie istnieje)
cursor.execute("""
CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
)
""")

# Tworzenie tabeli 'comments' (jeśli nie istnieje)
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

# Tworzenie tabeli 'likes' (jeśli nie istnieje)
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

# Tworzenie tabeli 'logs' (jeśli nie istnieje)
cursor.execute("""
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event TEXT NOT NULL,
    details TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

print("✅ Wszystkie tabele zostały utworzone: users, posts, comments, likes, logs.")

# Dodanie indeksów dla optymalizacji zapytań
cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_likes_post_id ON likes(post_id);")

print("✅ Indeksy zostały utworzone!")


# Funkcja do dodawania użytkownika
def add_user(username, email):
    try:
        cursor.execute("BEGIN")
        cursor.execute("INSERT INTO users (username, email) VALUES (?, ?)", (username, email))
        conn.commit()
        print(f"✅ Dodano użytkownika: {username}")
        cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
        print(f"📝 Użytkownicy po dodaniu: {cursor.fetchall()}")
    except sqlite3.IntegrityError as e:
        conn.rollback()
        print(f"⚠ Błąd: Użytkownik {username} lub email {email} już istnieje! ({e})")


# Funkcja do dodawania postu użytkownika + logowanie
def add_post(user_id, content):
    try:
        user_id = int(user_id)
        cursor.execute("SELECT id FROM users WHERE id = ?", (user_id,))
        if not cursor.fetchone():
            raise ValueError(f"Użytkownik o ID {user_id} nie istnieje!")
        cursor.execute("BEGIN")
        cursor.execute("INSERT INTO posts (user_id, content) VALUES (?, ?)", (user_id, content))
        conn.commit()
        log_event("Dodano post", f"Użytkownik {user_id} dodał post: '{content}'")
        print(f"✅ Dodano post użytkownika {user_id}!")
        cursor.execute("SELECT * FROM posts WHERE user_id = ?", (user_id,))
        print(f"📝 Posty użytkownika {user_id} po dodaniu: {cursor.fetchall()}")
    except (sqlite3.IntegrityError, ValueError) as e:
        conn.rollback()
        if "FOREIGN KEY" in str(e):
            print(f"⚠ Błąd: Użytkownik o ID {user_id} nie istnieje!")
        else:
            print(f"⚠ Błąd: Nie można dodać postu dla użytkownika {user_id}! ({e})")


# Funkcja do dodawania komentarza
def add_comment(user_id, post_id, content):
    try:
        user_id = int(user_id)
        post_id = int(post_id)
        # Sprawdzenie, czy użytkownik i post istnieją
        cursor.execute("SELECT id FROM users WHERE id = ?", (user_id,))
        if not cursor.fetchone():
            raise ValueError(f"Użytkownik o ID {user_id} nie istnieje!")
        cursor.execute("SELECT id FROM posts WHERE id = ?", (post_id,))
        if not cursor.fetchone():
            raise ValueError(f"Post o ID {post_id} nie istnieje!")

        cursor.execute("BEGIN")
        cursor.execute("INSERT INTO comments (user_id, post_id, content) VALUES (?, ?, ?)", (user_id, post_id, content))
        conn.commit()
        log_event("Dodano komentarz", f"Użytkownik {user_id} skomentował post {post_id}: '{content}'")
        print(f"✅ Dodano komentarz użytkownika {user_id} do postu {post_id}!")
        # Diagnostyka: sprawdzenie wszystkich komentarzy po dodaniu
        cursor.execute("SELECT * FROM comments WHERE post_id = ?", (post_id,))
        print(f"📝 Komentarze do postu {post_id} zaraz po dodaniu: {cursor.fetchall()}")
        # Dodatkowe sprawdzenie całej tabeli
        cursor.execute("SELECT * FROM comments")
        print(f"📝 Wszystkie komentarze w bazie po dodaniu: {cursor.fetchall()}")
    except (sqlite3.IntegrityError, ValueError) as e:
        conn.rollback()
        if "FOREIGN KEY" in str(e):
            print(f"⚠ Błąd: Niewłaściwy user_id ({user_id}) lub post_id ({post_id})!")
        else:
            print(f"⚠ Błąd: Nie można dodać komentarza dla użytkownika {user_id} do postu {post_id}! ({e})")


# Funkcja do dodawania polubienia + logowanie
def add_like(user_id, post_id):
    try:
        user_id = int(user_id)
        post_id = int(post_id)
        cursor.execute("BEGIN")
        cursor.execute("INSERT INTO likes (user_id, post_id) VALUES (?, ?)", (user_id, post_id))
        conn.commit()
        log_event("Dodano polubienie", f"Użytkownik {user_id} polubił post {post_id}")
        print(f"❤️ Użytkownik {user_id} polubił post {post_id}!")
    except sqlite3.IntegrityError as e:
        conn.rollback()
        if "FOREIGN KEY" in str(e):
            print(f"⚠ Błąd: Niewłaściwy user_id ({user_id}) lub post_id ({post_id})!")
        else:
            print(f"⚠ Błąd: Użytkownik {user_id} już polubił post {post_id}! ({e})")


# Funkcja do logowania zdarzeń
def log_event(event, details):
    try:
        cursor.execute("INSERT INTO logs (event, details) VALUES (?, ?)", (event, details))
        conn.commit()
    except sqlite3.Error as e:
        print(f"⚠ Błąd podczas logowania zdarzenia: {e}")


# Dodanie przykładowych użytkowników
add_user("user1", "user1@example.com")
add_user("user2", "user2@example.com")


# Funkcja do interaktywnego dodawania użytkownika
def add_user_interactively():
    username = input("\n📝 Podaj nazwę użytkownika: ")
    email = input("📧 Podaj email użytkownika: ")
    if "@" not in email:
        print("⚠ Błąd: Email musi zawierać znak '@'!")
        return
    add_user(username, email)


# Funkcja do interaktywnego dodawania postu
def add_post_interactively():
    try:
        user_id = int(input("\n🆔 Podaj ID użytkownika, który pisze post: "))
        content = input("✏ Podaj treść postu: ")
        add_post(user_id, content)
    except ValueError:
        print("⚠ Błąd: Wpisz poprawne ID jako liczbę!")


# Funkcja do interaktywnego dodawania komentarza
def add_comment_interactively():
    try:
        user_id = int(input("\n🆔 Podaj ID użytkownika, który dodaje komentarz: "))
        post_id = int(input("📝 Podaj ID postu, który komentujesz: "))
        content = input("💬 Podaj treść komentarza: ")
        add_comment(user_id, post_id, content)
    except ValueError:
        print("⚠ Błąd: Wpisz poprawne ID jako liczbę!")


# Funkcja do interaktywnego dodawania polubienia
def add_like_interactively():
    try:
        user_id = int(input("\n🆔 Podaj ID użytkownika, który lajkuje post: "))
        post_id = int(input("👍 Podaj ID postu, który chcesz polubić: "))
        add_like(user_id, post_id)
    except ValueError:
        print("⚠ Błąd: Wpisz poprawne ID jako liczbę!")


# "Procedura składowana" do pobierania postów użytkownika
def get_user_posts(user_id):
    cursor.execute("""
    SELECT posts.id, posts.content, posts.created_at
    FROM posts
    WHERE posts.user_id = ?
    ORDER BY posts.created_at DESC
    """, (user_id,))
    posts = cursor.fetchall()
    print(f"\n📝 Posty użytkownika {user_id}:")
    for post in posts:
        print(post)


# "Procedura składowana" do pobierania komentarzy pod postem
def get_post_comments(post_id):
    cursor.execute("""
    SELECT c.id, u.username, c.content, c.created_at
    FROM comments c
    JOIN users u ON c.user_id = u.id
    WHERE c.post_id = ?
    ORDER BY c.created_at DESC
    """, (post_id,))
    comments = cursor.fetchall()
    print(f"\n💬 Komentarze do postu {post_id}:")
    print("=" * 50)
    if not comments:
        print("⚠ Brak komentarzy dla tego postu!")
    else:
        for comment in comments:
            print(comment)


# "Procedura składowana" do liczenia polubień postu
def get_post_likes(post_id):
    cursor.execute("""
    SELECT COUNT(*) FROM likes WHERE post_id = ?
    """, (post_id,))
    likes_count = cursor.fetchone()[0]
    print(f"\n👍 Liczba polubień dla postu {post_id}: {likes_count}")


# Pobranie liczby postów każdego użytkownika
def get_user_post_counts():
    cursor.execute("""
    SELECT users.username, COUNT(posts.id) AS post_count
    FROM users
    LEFT JOIN posts ON users.id = posts.user_id
    GROUP BY users.id
    ORDER BY post_count DESC;
    """)
    results = cursor.fetchall()
    print("\n📊 Liczba postów każdego użytkownika:")
    for row in results:
        print(row)


# Pobranie postów z największą liczbą komentarzy
def get_most_commented_posts():
    cursor.execute("""
    SELECT posts.id, posts.content, users.username, COUNT(comments.id) AS comment_count
    FROM posts
    JOIN users ON posts.user_id = users.id
    LEFT JOIN comments ON posts.id = comments.post_id
    GROUP BY posts.id
    HAVING comment_count > 0
    ORDER BY comment_count DESC;
    """)
    results = cursor.fetchall()
    print("\n💬 Posty z największą liczbą komentarzy:")
    for row in results:
        print(row)


# Pobranie użytkowników, którzy polubili najwięcej postów
def get_top_likers():
    cursor.execute("""
    SELECT users.username, COUNT(likes.id) AS like_count
    FROM users
    JOIN likes ON users.id = likes.user_id
    GROUP BY users.id
    HAVING like_count > 0
    ORDER BY like_count DESC;
    """)
    results = cursor.fetchall()
    print("\n👍 Użytkownicy, którzy polubili najwięcej postów:")
    for row in results:
        print(row)


# Pobranie logów operacji
def get_logs():
    cursor.execute("SELECT id, event, details, created_at FROM logs ORDER BY created_at DESC")
    logs = cursor.fetchall()
    print("\n📜 Historia operacji (logi):")
    print("=" * 50)
    for log in logs:
        print(log)


# Usuwanie użytkowników bez postów
def delete_inactive_users():
    cursor.execute("""
    DELETE FROM users 
    WHERE id NOT IN (SELECT DISTINCT user_id FROM posts);
    """)
    conn.commit()
    print("🗑 Usunięto nieaktywnych użytkowników (bez postów).")


# Usuwanie postów starszych niż 30 dni
def delete_old_posts():
    cursor.execute("""
    DELETE FROM posts 
    WHERE created_at < DATETIME('now', '-30 days');
    """)
    conn.commit()
    print("🗑 Usunięto stare posty (starsze niż 30 dni).")


# Usuwanie komentarzy do nieistniejących postów
def delete_orphan_comments():
    cursor.execute("""
    DELETE FROM comments
    WHERE post_id NOT IN (SELECT id FROM posts WHERE id IS NOT NULL);
    """)
    conn.commit()
    print("🗑 Usunięto osierocone komentarze (dotyczące nieistniejących postów).")


# Usuwanie polubień do nieistniejących postów
def delete_orphan_likes():
    cursor.execute("""
    DELETE FROM likes 
    WHERE post_id NOT IN (SELECT id FROM posts);
    """)
    conn.commit()
    print("🗑 Usunięto polubienia do usuniętych postów.")


# Defragmentacja bazy danych
def optimize_database():
    cursor.execute("VACUUM;")
    conn.commit()
    print("🛠 Wykonano optymalizację bazy danych (VACUUM).")


# Funkcja do eksportu danych do CSV
def export_to_csv(query, filename, headers):
    cursor.execute(query)
    rows = cursor.fetchall()
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"📁 Eksportowano dane do {filename}!")


# Eksport użytkowników
def export_users():
    export_to_csv(
        "SELECT id, username, email, created_at FROM users",
        "users.csv",
        ["ID", "Username", "Email", "Created At"]
    )


# Eksport postów
def export_posts():
    export_to_csv(
        "SELECT posts.id, users.username, posts.content, posts.created_at FROM posts JOIN users ON posts.user_id = users.id",
        "posts.csv",
        ["Post ID", "Username", "Content", "Created At"]
    )


# Eksport komentarzy
def export_comments():
    export_to_csv(
        "SELECT comments.id, users.username, posts.content, comments.content, comments.created_at FROM comments JOIN users ON comments.user_id = users.id JOIN posts ON comments.post_id = posts.id",
        "comments.csv",
        ["Comment ID", "Username", "Post Content", "Comment Content", "Created At"]
    )


# Eksport polubień
def export_likes():
    export_to_csv(
        "SELECT likes.id, users.username, posts.content, likes.created_at FROM likes JOIN users ON likes.user_id = users.id JOIN posts ON likes.post_id = posts.id",
        "likes.csv",
        ["Like ID", "Username", "Post Content", "Created At"]
    )


# Zapytanie użytkownika, czy chce dodać polubienie
while True:
    choice = input("\n❤️ Czy chcesz polubić post? (tak/nie): ").strip().lower()
    if choice == "tak":
        add_like_interactively()
    elif choice == "nie":
        print("👋 Zakończono dodawanie polubień.")
        break
    else:
        print("⚠ Wpisz 'tak' lub 'nie'.")

# Zapytanie użytkownika o dodanie użytkowników
while True:
    choice = input("\n➕ Czy chcesz dodać nowego użytkownika? (tak/nie): ").strip().lower()
    if choice == "tak":
        add_user_interactively()
    elif choice == "nie":
        print("👋 Zakończono dodawanie użytkowników.")
        break
    else:
        print("⚠ Wpisz 'tak' lub 'nie'.")

# Zapytanie użytkownika o dodanie postów
while True:
    choice = input("\n📝 Czy chcesz dodać nowy post? (tak/nie): ").strip().lower()
    if choice == "tak":
        add_post_interactively()
    elif choice == "nie":
        print("👋 Zakończono dodawanie postów.")
        break
    else:
        print("⚠ Wpisz 'tak' lub 'nie'.")

# Zapytanie użytkownika o dodanie komentarzy
while True:
    choice = input("\n💬 Czy chcesz dodać nowy komentarz? (tak/nie): ").strip().lower()
    if choice == "tak":
        add_comment_interactively()
    elif choice == "nie":
        print("👋 Zakończono dodawanie komentarzy.")
        break
    else:
        print("⚠ Wpisz 'tak' lub 'nie'.")

# Wyświetlenie aktualnej listy użytkowników
cursor.execute("SELECT * FROM users")
users = cursor.fetchall()
print("\n📜 Zaktualizowana lista użytkowników:")
print("=" * 50)
for user in users:
    print(user)

# Pobranie wszystkich postów
cursor.execute("""
SELECT posts.id, users.username, posts.content, posts.created_at
FROM posts
JOIN users ON posts.user_id = users.id
""")
posts = cursor.fetchall()
print("\n📰 Lista postów:")
print("=" * 50)
for post in posts:
    print(post)

# Pobranie wszystkich komentarzy
cursor.execute("""
SELECT c.id, u.username, p.content AS post_content, c.content AS comment_content, c.created_at
FROM comments c
JOIN users u ON c.user_id = u.id
JOIN posts p ON c.post_id = p.id
""")
comments = cursor.fetchall()
print("\n💬 Lista komentarzy:")
print("=" * 50)
for comment in comments:
    print(comment)

# Dodatkowa diagnostyka: pełna tabela comments
cursor.execute("SELECT * FROM comments")
print("\n📝 Pełna tabela komentarzy przed zakończeniem:")
print(cursor.fetchall())

# Pobranie wszystkich polubień
cursor.execute("""
SELECT likes.id, users.username, posts.content, likes.created_at
FROM likes
JOIN users ON likes.user_id = users.id
JOIN posts ON likes.post_id = posts.id
""")
likes = cursor.fetchall()
print("\n👍 Lista polubień:")
print("=" * 50)
for like in likes:
    print(like)

# Testowanie wydajności zapytania - EXPLAIN QUERY PLAN
print("\n📊 Testowanie wydajności zapytań...")
cursor.execute("EXPLAIN QUERY PLAN SELECT * FROM users WHERE username = 'user1';")
print("🔍 Plan zapytania dla wyszukiwania użytkownika:", cursor.fetchall())
cursor.execute("EXPLAIN QUERY PLAN SELECT * FROM posts WHERE user_id = 1 ORDER BY created_at DESC;")
print("🔍 Plan zapytania dla wyszukiwania postów:", cursor.fetchall())
cursor.execute("EXPLAIN QUERY PLAN SELECT * FROM comments WHERE post_id = 1;")
print("🔍 Plan zapytania dla wyszukiwania komentarzy:", cursor.fetchall())

print("\n🛠 Testowanie transakcji...")
try:
    cursor.execute("BEGIN")
    cursor.execute("INSERT INTO users (username, email) VALUES ('test_user', 'test@example.com')")
    cursor.execute("INSERT INTO users (username, email) VALUES ('test_user', 'test@example.com')")
    conn.commit()
    print("✅ Testowa transakcja zakończona sukcesem!")
except sqlite3.IntegrityError:
    conn.rollback()
    print("⚠ Błąd w transakcji! Zmiany zostały cofnięte.")

print("\n📌 Testowanie procedur składowanych...")
get_user_posts(2)
get_post_comments(1)
get_post_likes(1)

print("\n📌 Testowanie zaawansowanej analizy SQL...")
get_user_post_counts()
get_most_commented_posts()
get_top_likers()

# Diagnostyka przed optymalizacją
print("\n📌 TEST: Lista postów przed optymalizacją:")
cursor.execute("SELECT * FROM posts;")
print(cursor.fetchall())
print("\n📌 TEST: Lista komentarzy przed optymalizacją:")
cursor.execute("SELECT * FROM comments;")
print(cursor.fetchall())

# Opcjonalna optymalizacja z pytaniem
choice = input("\n🛠 Czy chcesz przeprowadzić optymalizację bazy (usuwanie starych danych)? (tak/nie): ").strip().lower()
if choice == "tak":
    print("\n📌 Wykonywanie optymalizacji bazy danych...")
    delete_inactive_users()
    delete_old_posts()
    delete_orphan_comments()
    delete_orphan_likes()
    optimize_database()
    # Diagnostyka po optymalizacji
    print("\n📌 TEST: Lista postów po optymalizacji:")
    cursor.execute("SELECT * FROM posts;")
    print(cursor.fetchall())
    print("\n📌 TEST: Lista komentarzy po optymalizacji:")
    cursor.execute("SELECT * FROM comments;")
    print(cursor.fetchall())
else:
    print("👋 Pominięto optymalizację – dane pozostają bez zmian.")

print("\n📌 Eksportowanie danych do CSV...")
export_users()
export_posts()
export_comments()
export_likes()


# Eksport logów do CSV
def export_logs():
    export_to_csv(
        "SELECT id, event, details, created_at FROM logs",
        "logs.csv",
        ["Log ID", "Event", "Details", "Created At"]
    )


print("\n📌 Eksportowanie logów do CSV...")
export_logs()

# Zamykamy połączenie z bazą
conn.commit()
conn.close()
print("✅ Proces zakończony!")