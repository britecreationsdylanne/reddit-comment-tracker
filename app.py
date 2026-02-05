import os
import sys
import csv
import io
import json
import secrets
from datetime import datetime, timezone
from flask import Flask, request, jsonify, render_template, Response, redirect, url_for, session
from flask_cors import CORS
from authlib.integrations.flask_client import OAuth
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(__file__))

from config.settings import (
    FLASK_PORT, FLASK_DEBUG, REDDIT_USERNAME, TEST_MODE,
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, SCRAPE_HOUR, SCRAPE_MINUTE,
    SYNC_API_KEY
)
from backend.database import (
    init_db, get_comments, get_posts_with_counts, get_authors_with_counts, get_stats,
    get_scrape_log, get_recipients, add_recipient, update_recipient,
    delete_recipient, get_new_comments_since, get_last_successful_scrape,
    update_comment_sentiment, update_comment_reply_status,
    get_comments_without_sentiment, get_all_comments_for_export,
    insert_post, insert_comment, log_scrape_start, log_scrape_end
)
from backend.reddit_scraper import run_scrape
from backend.email_notifier import send_notification, send_test_email
from backend.ai_helper import suggest_reply, batch_analyze_sentiment
from backend.gcs_backup import restore_db, backup_db

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
CORS(app)

# Session configuration for OAuth (Cloud Run compatible)
app.secret_key = os.environ.get('FLASK_SECRET_KEY') or secrets.token_hex(32)
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# Google OAuth
ALLOWED_DOMAIN = 'brite.co'
oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=os.environ.get('GOOGLE_CLIENT_ID'),
    client_secret=os.environ.get('GOOGLE_CLIENT_SECRET'),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)


def get_current_user():
    """Get current authenticated user from session."""
    return session.get('user')


# Restore database from GCS (if configured) before init
restore_db()

# Initialize database
init_db()

# Initialize scheduler (works with both gunicorn and direct python execution)
from backend.scheduler import init_scheduler
init_scheduler(app)


# ========================================
# Auth Routes
# ========================================

@app.route('/auth/login')
def auth_login():
    """Redirect to Google OAuth."""
    if get_current_user():
        return redirect('/')
    redirect_uri = url_for('auth_callback', _external=True)
    return google.authorize_redirect(redirect_uri)


@app.route('/auth/callback')
def auth_callback():
    """Handle OAuth callback from Google."""
    try:
        token = google.authorize_access_token()
        user_info = token.get('userinfo')

        if not user_info:
            return 'Failed to get user info', 400

        email = user_info.get('email', '')

        if not email.endswith(f'@{ALLOWED_DOMAIN}'):
            return (
                '<div style="font-family:system-ui;max-width:400px;margin:80px auto;text-align:center;">'
                '<h2>Access Denied</h2>'
                f'<p>Only @{ALLOWED_DOMAIN} accounts are allowed.</p>'
                f'<p style="color:#888;">You signed in as {email}</p>'
                '<a href="/auth/login">Try again</a></div>'
            ), 403

        session['user'] = {
            'email': email,
            'name': user_info.get('name', ''),
            'picture': user_info.get('picture', '')
        }

        return redirect('/')
    except Exception as e:
        print(f"[AUTH ERROR] OAuth callback failed: {e}")
        return f'Authentication failed: {str(e)}', 500


@app.route('/auth/logout')
def auth_logout():
    """Clear session and redirect to login."""
    session.pop('user', None)
    return redirect('/auth/login')


# ========================================
# Page Routes (protected)
# ========================================

@app.route('/')
def dashboard():
    user = get_current_user()
    if not user:
        return redirect('/auth/login')
    return render_template('dashboard.html', active_page='dashboard', user=user)


@app.route('/settings')
def settings_page():
    user = get_current_user()
    if not user:
        return redirect('/auth/login')
    schedule_time = f"{SCRAPE_HOUR:02d}:{SCRAPE_MINUTE:02d}"
    has_api_creds = bool(REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET)
    return render_template(
        'settings.html',
        active_page='settings',
        schedule_time=schedule_time,
        reddit_username=REDDIT_USERNAME,
        test_mode=TEST_MODE,
        has_api_creds=has_api_creds,
        user=user
    )


# ========================================
# API Routes — Data
# ========================================

@app.route('/api/stats')
def api_get_stats():
    return jsonify(get_stats())


@app.route('/api/posts')
def api_get_posts():
    return jsonify(get_posts_with_counts())


@app.route('/api/authors')
def api_get_authors():
    return jsonify(get_authors_with_counts())


@app.route('/api/comments/ids')
def api_get_comment_ids():
    """Get all comment IDs matching the current filter (for select-all-in-filter)."""
    post_id = request.args.get('post_id')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    sentiment = request.args.get('sentiment')
    reply_status = request.args.get('reply_status')
    author = request.args.get('author')

    date_from_ts = None
    date_to_ts = None
    if date_from:
        try:
            date_from_ts = datetime.strptime(date_from, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass
    if date_to:
        try:
            date_to_ts = datetime.strptime(date_to, '%Y-%m-%d').replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            ).timestamp()
        except ValueError:
            pass

    result = get_comments(
        post_id=post_id, date_from=date_from_ts, date_to=date_to_ts,
        sentiment=sentiment, reply_status=reply_status, author=author,
        page=1, per_page=10000
    )
    return jsonify({'ids': [c['id'] for c in result['comments']], 'total': result['total']})


@app.route('/api/comments')
def api_get_comments():
    post_id = request.args.get('post_id')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    sentiment = request.args.get('sentiment')
    reply_status = request.args.get('reply_status')
    author = request.args.get('author')
    sort_by = request.args.get('sort_by', 'date_desc')
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 25, type=int)

    # Convert date strings to timestamps
    date_from_ts = None
    date_to_ts = None
    if date_from:
        try:
            date_from_ts = datetime.strptime(date_from, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass
    if date_to:
        try:
            date_to_ts = datetime.strptime(date_to, '%Y-%m-%d').replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            ).timestamp()
        except ValueError:
            pass

    result = get_comments(
        post_id=post_id,
        date_from=date_from_ts,
        date_to=date_to_ts,
        sentiment=sentiment,
        reply_status=reply_status,
        author=author,
        sort_by=sort_by,
        page=page,
        per_page=per_page
    )
    return jsonify(result)


# ========================================
# API Routes — Scraper
# ========================================

@app.route('/api/scrape/run', methods=['POST'])
def api_trigger_scrape():
    try:
        posts_found, new_comments = run_scrape()
        return jsonify({
            'success': True,
            'posts_found': posts_found,
            'new_comments': new_comments
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scrape-log')
def api_get_scrape_log():
    limit = request.args.get('limit', 20, type=int)
    return jsonify(get_scrape_log(limit=limit))


# ========================================
# API Routes — Email Recipients
# ========================================

@app.route('/api/recipients', methods=['GET'])
def api_get_recipients():
    return jsonify(get_recipients())


@app.route('/api/recipients', methods=['POST'])
def api_add_recipient():
    data = request.get_json()
    if not data or not data.get('email'):
        return jsonify({'success': False, 'error': 'Email is required'}), 400

    email = data['email'].strip()
    name = data.get('name', '').strip() or None

    new_id = add_recipient(email, name)
    if new_id:
        return jsonify({'success': True, 'id': new_id})
    else:
        return jsonify({'success': False, 'error': 'Email already exists'}), 409


@app.route('/api/recipients/<int:recipient_id>', methods=['PUT'])
def api_update_recipient(recipient_id):
    data = request.get_json()
    is_active = data.get('is_active')
    name = data.get('name')
    update_recipient(recipient_id, is_active=is_active, name=name)
    return jsonify({'success': True})


@app.route('/api/recipients/<int:recipient_id>', methods=['DELETE'])
def api_delete_recipient(recipient_id):
    deleted = delete_recipient(recipient_id)
    if deleted:
        return jsonify({'success': True})
    else:
        return jsonify({'success': False, 'error': 'Recipient not found'}), 404


# ========================================
# API Routes — Email
# ========================================

@app.route('/api/test-email', methods=['POST'])
def api_send_test_email():
    data = request.get_json()
    if not data or not data.get('email'):
        return jsonify({'success': False, 'error': 'Email is required'}), 400

    success, message = send_test_email(data['email'].strip())
    return jsonify({'success': success, 'message': message})


# ========================================
# API Routes — AI (Claude)
# ========================================

@app.route('/api/ai/suggest-reply', methods=['POST'])
def api_suggest_reply():
    data = request.get_json()
    if not data or not data.get('comment_body'):
        return jsonify({'success': False, 'error': 'comment_body is required'}), 400

    result = suggest_reply(
        comment_body=data['comment_body'],
        post_title=data.get('post_title', ''),
        author=data.get('author', 'unknown')
    )
    return jsonify(result)


@app.route('/api/ai/analyze-sentiment', methods=['POST'])
def api_analyze_sentiment():
    """Analyze sentiment for untagged comments and store results."""
    comments = get_comments_without_sentiment(limit=20)
    if not comments:
        return jsonify({'success': True, 'analyzed': 0, 'message': 'No comments to analyze'})

    results = batch_analyze_sentiment(comments)

    updated = 0
    for comment_id, sentiment in results.items():
        update_comment_sentiment(comment_id, sentiment)
        updated += 1

    return jsonify({'success': True, 'analyzed': updated})


# ========================================
# API Routes — Reply Status
# ========================================

@app.route('/api/comments/<comment_id>/reply-status', methods=['PUT'])
def api_update_reply_status(comment_id):
    data = request.get_json()
    status = data.get('reply_status')
    if status not in ('needs_reply', 'replied', 'ignored'):
        return jsonify({'success': False, 'error': 'Invalid status'}), 400
    update_comment_reply_status(comment_id, status)
    return jsonify({'success': True})


@app.route('/api/comments/bulk-status', methods=['PUT'])
def api_bulk_update_reply_status():
    data = request.get_json()
    comment_ids = data.get('comment_ids', [])
    status = data.get('reply_status')
    if status not in ('needs_reply', 'replied', 'ignored'):
        return jsonify({'success': False, 'error': 'Invalid status'}), 400
    if not comment_ids:
        return jsonify({'success': False, 'error': 'No comments selected'}), 400
    for cid in comment_ids:
        update_comment_reply_status(cid, status)
    return jsonify({'success': True, 'updated': len(comment_ids)})


@app.route('/api/comments/<comment_id>/sentiment', methods=['PUT'])
def api_update_sentiment(comment_id):
    data = request.get_json()
    sentiment = data.get('sentiment')
    if sentiment not in ('positive', 'negative', 'question', 'neutral'):
        return jsonify({'success': False, 'error': 'Invalid sentiment'}), 400
    update_comment_sentiment(comment_id, sentiment)
    return jsonify({'success': True})


# ========================================
# API Routes — CSV Export
# ========================================

@app.route('/api/export/csv')
def api_export_csv():
    post_id = request.args.get('post_id')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    sentiment = request.args.get('sentiment')
    reply_status = request.args.get('reply_status')

    date_from_ts = None
    date_to_ts = None
    if date_from:
        try:
            date_from_ts = datetime.strptime(date_from, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass
    if date_to:
        try:
            date_to_ts = datetime.strptime(date_to, '%Y-%m-%d').replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            ).timestamp()
        except ValueError:
            pass

    comments = get_all_comments_for_export(
        post_id=post_id,
        date_from=date_from_ts,
        date_to=date_to_ts,
        sentiment=sentiment,
        reply_status=reply_status
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Date', 'Author', 'Comment', 'Post Title', 'Subreddit', 'Sentiment', 'Reply Status', 'Score', 'Post URL'])

    for c in comments:
        date_str = datetime.fromtimestamp(c['created_utc'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M')
        writer.writerow([
            date_str,
            c.get('author', ''),
            c.get('body', ''),
            c.get('post_title', ''),
            c.get('subreddit', ''),
            c.get('sentiment', 'neutral'),
            c.get('reply_status', 'needs_reply'),
            c.get('score', 0),
            c.get('post_url', '')
        ])

    output.seek(0)
    timestamp = datetime.now().strftime('%Y%m%d')
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=briteco_comments_{timestamp}.csv'}
    )


# ========================================
# API Routes — Sync (local scraper → cloud)
# ========================================

@app.route('/api/sync/upload', methods=['POST'])
def api_sync_upload():
    """Receive scraped data from a local machine.

    Used when Reddit blocks cloud IPs — scrape runs locally,
    then uploads results here.
    """
    # Verify API key
    auth = request.headers.get('X-Sync-Key', '')
    if not SYNC_API_KEY or auth != SYNC_API_KEY:
        return jsonify({'success': False, 'error': 'Invalid or missing sync key'}), 401

    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'}), 400

    posts = data.get('posts', [])
    comments = data.get('comments', [])

    # Log the sync as a scrape
    log_id = log_scrape_start()

    new_comments = 0
    for post in posts:
        insert_post(post)
    for comment in comments:
        if insert_comment(comment):
            new_comments += 1
        # Sync reply_status if present
        if comment.get('reply_status') and comment['reply_status'] != 'needs_reply':
            update_comment_reply_status(comment['id'], comment['reply_status'])

    log_scrape_end(
        log_id,
        posts_found=len(posts),
        new_comments_found=new_comments,
        status='success'
    )

    # Back up to GCS after sync
    backup_db()

    return jsonify({
        'success': True,
        'posts_synced': len(posts),
        'new_comments': new_comments
    })


# ========================================
# Health Check
# ========================================

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'ok',
        'reddit_username': REDDIT_USERNAME,
        'test_mode': TEST_MODE,
        'scrape_method': 'mock' if TEST_MODE else ('praw' if REDDIT_CLIENT_ID else 'json')
    })


# ========================================
# Main
# ========================================

if __name__ == '__main__':
    mode = 'TEST MODE' if TEST_MODE else ('PRAW' if REDDIT_CLIENT_ID else 'Public JSON')

    print(f"\n{'='*60}")
    print(f"  BriteCo Reddit Comment Tracker")
    print(f"  Running on http://localhost:{FLASK_PORT}")
    print(f"  Reddit user: u/{REDDIT_USERNAME}")
    print(f"  Scrape mode: {mode}")
    print(f"  Schedule: Daily at {SCRAPE_HOUR:02d}:{SCRAPE_MINUTE:02d}")
    print(f"{'='*60}\n")

    app.run(host='0.0.0.0', port=FLASK_PORT, debug=FLASK_DEBUG, use_reloader=False)
