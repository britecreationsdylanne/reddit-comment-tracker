import sqlite3
import os
from datetime import datetime, timezone

from config.settings import DATABASE_PATH


def get_db():
    """Get a database connection with row factory enabled."""
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_db()
    cursor = conn.cursor()

    # Create core tables
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            subreddit TEXT NOT NULL,
            url TEXT NOT NULL,
            created_utc REAL NOT NULL,
            first_seen_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            post_id TEXT NOT NULL,
            author TEXT,
            body TEXT NOT NULL,
            created_utc REAL NOT NULL,
            parent_id TEXT,
            score INTEGER DEFAULT 0,
            first_seen_at TEXT NOT NULL,
            FOREIGN KEY (post_id) REFERENCES posts(id)
        );

        CREATE TABLE IF NOT EXISTS email_recipients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            name TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            posts_found INTEGER DEFAULT 0,
            new_comments_found INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running',
            error_message TEXT,
            email_sent INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id);
        CREATE INDEX IF NOT EXISTS idx_comments_created_utc ON comments(created_utc);
        CREATE INDEX IF NOT EXISTS idx_comments_first_seen_at ON comments(first_seen_at);
    """)

    # Migrate existing databases — add new columns if missing
    cursor.execute("PRAGMA table_info(comments)")
    columns = {row[1] for row in cursor.fetchall()}
    if 'sentiment' not in columns:
        cursor.execute("ALTER TABLE comments ADD COLUMN sentiment TEXT DEFAULT 'neutral'")
    if 'reply_status' not in columns:
        cursor.execute("ALTER TABLE comments ADD COLUMN reply_status TEXT DEFAULT 'needs_reply'")

    # Create indexes on new columns (after migration ensures they exist)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_sentiment ON comments(sentiment)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_reply_status ON comments(reply_status)")

    conn.commit()
    conn.close()


# --- Posts ---

def insert_post(post_data):
    """Insert a post if it doesn't already exist. Returns True if new."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO posts (id, title, subreddit, url, created_utc, first_seen_at) VALUES (?, ?, ?, ?, ?, ?)",
        (
            post_data['id'],
            post_data['title'],
            post_data['subreddit'],
            post_data['url'],
            post_data['created_utc'],
            datetime.now(timezone.utc).isoformat()
        )
    )
    is_new = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return is_new


def get_posts_with_counts():
    """Return all posts with their comment counts, newest first."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT p.*, COUNT(c.id) as comment_count
        FROM posts p
        LEFT JOIN comments c ON c.post_id = p.id
        GROUP BY p.id
        ORDER BY p.created_utc DESC
    """)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


# --- Comments ---

def insert_comment(comment_data):
    """Insert a comment if it doesn't already exist. Returns True if new."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO comments (id, post_id, author, body, created_utc, parent_id, score, first_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            comment_data['id'],
            comment_data['post_id'],
            comment_data.get('author', '[deleted]'),
            comment_data['body'],
            comment_data['created_utc'],
            comment_data.get('parent_id'),
            comment_data.get('score', 0),
            datetime.now(timezone.utc).isoformat()
        )
    )
    is_new = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return is_new


def get_comments(post_id=None, date_from=None, date_to=None, sentiment=None, reply_status=None, page=1, per_page=50):
    """Get comments with optional filters and pagination."""
    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT c.*, p.title as post_title, p.url as post_url, p.subreddit
        FROM comments c
        JOIN posts p ON c.post_id = p.id
        WHERE 1=1
    """
    params = []

    if post_id:
        query += " AND c.post_id = ?"
        params.append(post_id)

    if date_from:
        query += " AND c.created_utc >= ?"
        params.append(date_from)

    if date_to:
        query += " AND c.created_utc <= ?"
        params.append(date_to)

    if sentiment:
        query += " AND c.sentiment = ?"
        params.append(sentiment)

    if reply_status:
        query += " AND c.reply_status = ?"
        params.append(reply_status)

    # Get total count
    count_query = query.replace("SELECT c.*, p.title as post_title, p.url as post_url, p.subreddit", "SELECT COUNT(*)")
    cursor.execute(count_query, params)
    total = cursor.fetchone()[0]

    # Add ordering and pagination
    query += " ORDER BY c.created_utc DESC LIMIT ? OFFSET ?"
    params.extend([per_page, (page - 1) * per_page])

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return {
        'comments': rows,
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': max(1, (total + per_page - 1) // per_page)
    }


def update_comment_sentiment(comment_id, sentiment):
    """Update the sentiment tag for a comment."""
    conn = get_db()
    conn.execute("UPDATE comments SET sentiment = ? WHERE id = ?", (sentiment, comment_id))
    conn.commit()
    conn.close()


def update_comment_reply_status(comment_id, reply_status):
    """Update the reply status for a comment. Valid: needs_reply, replied, ignored."""
    conn = get_db()
    conn.execute("UPDATE comments SET reply_status = ? WHERE id = ?", (reply_status, comment_id))
    conn.commit()
    conn.close()


def get_comments_without_sentiment(limit=50):
    """Get comments that haven't been sentiment-analyzed yet."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, body FROM comments WHERE sentiment = 'neutral' ORDER BY first_seen_at DESC LIMIT ?",
        (limit,)
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_all_comments_for_export(post_id=None, date_from=None, date_to=None, sentiment=None, reply_status=None):
    """Get all comments matching filters (no pagination) for CSV export."""
    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT c.id, c.author, c.body, c.created_utc, c.score, c.sentiment, c.reply_status,
               p.title as post_title, p.url as post_url, p.subreddit
        FROM comments c
        JOIN posts p ON c.post_id = p.id
        WHERE 1=1
    """
    params = []

    if post_id:
        query += " AND c.post_id = ?"
        params.append(post_id)
    if date_from:
        query += " AND c.created_utc >= ?"
        params.append(date_from)
    if date_to:
        query += " AND c.created_utc <= ?"
        params.append(date_to)
    if sentiment:
        query += " AND c.sentiment = ?"
        params.append(sentiment)
    if reply_status:
        query += " AND c.reply_status = ?"
        params.append(reply_status)

    query += " ORDER BY c.created_utc DESC"
    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_all_posts():
    """Get all posts as dicts (for sync upload)."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, title, subreddit, url, created_utc FROM posts")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_all_comments_raw():
    """Get all comments as dicts (for sync upload)."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, post_id, author, body, created_utc, parent_id, score FROM comments")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_new_comments_since(timestamp):
    """Get comments first seen after the given timestamp."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.*, p.title as post_title, p.url as post_url, p.subreddit
        FROM comments c
        JOIN posts p ON c.post_id = p.id
        WHERE c.first_seen_at > ?
        ORDER BY c.created_utc DESC
    """, (timestamp,))
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_stats():
    """Get dashboard summary stats."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM posts")
    total_posts = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM comments")
    total_comments = cursor.fetchone()[0]

    # Comments first seen today
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    cursor.execute("SELECT COUNT(*) FROM comments WHERE first_seen_at >= ?", (today_start,))
    new_today = cursor.fetchone()[0]

    # Last successful scrape
    cursor.execute("SELECT * FROM scrape_log WHERE status = 'success' ORDER BY completed_at DESC LIMIT 1")
    last_scrape = cursor.fetchone()
    last_scrape_dict = dict(last_scrape) if last_scrape else None

    conn.close()
    return {
        'total_posts': total_posts,
        'total_comments': total_comments,
        'new_today': new_today,
        'last_scrape': last_scrape_dict
    }


# --- Scrape Log ---

def log_scrape_start():
    """Log the start of a scrape run. Returns the log ID."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO scrape_log (started_at, status) VALUES (?, 'running')",
        (datetime.now(timezone.utc).isoformat(),)
    )
    log_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return log_id


def log_scrape_end(log_id, posts_found=0, new_comments_found=0, status='success', error_message=None, email_sent=False):
    """Update a scrape log entry with results."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE scrape_log
        SET completed_at = ?, posts_found = ?, new_comments_found = ?, status = ?, error_message = ?, email_sent = ?
        WHERE id = ?
    """, (
        datetime.now(timezone.utc).isoformat(),
        posts_found,
        new_comments_found,
        status,
        error_message,
        1 if email_sent else 0,
        log_id
    ))
    conn.commit()
    conn.close()


def get_last_successful_scrape():
    """Get the most recent successful scrape log entry."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM scrape_log WHERE status = 'success' ORDER BY completed_at DESC LIMIT 1")
    row = cursor.fetchone()
    result = dict(row) if row else None
    conn.close()
    return result


def get_scrape_log(limit=20):
    """Get recent scrape log entries."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM scrape_log ORDER BY started_at DESC LIMIT ?", (limit,))
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


# --- Email Recipients ---

def get_recipients(active_only=False):
    """Get email recipients."""
    conn = get_db()
    cursor = conn.cursor()
    if active_only:
        cursor.execute("SELECT * FROM email_recipients WHERE is_active = 1 ORDER BY created_at")
    else:
        cursor.execute("SELECT * FROM email_recipients ORDER BY created_at")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def add_recipient(email, name=None):
    """Add a new email recipient. Returns the new ID or None if duplicate."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO email_recipients (email, name, created_at) VALUES (?, ?, ?)",
            (email, name, datetime.now(timezone.utc).isoformat())
        )
        new_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return new_id
    except sqlite3.IntegrityError:
        conn.close()
        return None


def update_recipient(recipient_id, is_active=None, name=None):
    """Update a recipient's name or active status."""
    conn = get_db()
    cursor = conn.cursor()
    if is_active is not None:
        cursor.execute("UPDATE email_recipients SET is_active = ? WHERE id = ?", (1 if is_active else 0, recipient_id))
    if name is not None:
        cursor.execute("UPDATE email_recipients SET name = ? WHERE id = ?", (name, recipient_id))
    conn.commit()
    conn.close()


def delete_recipient(recipient_id):
    """Delete an email recipient."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM email_recipients WHERE id = ?", (recipient_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted
