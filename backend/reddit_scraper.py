import time
import random
import requests
from datetime import datetime, timezone

from config.settings import (
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT, REDDIT_USERNAME, TEST_MODE
)
from backend.database import insert_post, insert_comment, log_scrape_start, log_scrape_end


# --- Public JSON Scraper (no API key needed) ---

_session = None

def _get_session():
    """Get or create a requests session with browser-like headers."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        })
    return _session


def _fetch_json(url, max_retries=3):
    """Fetch a Reddit JSON endpoint with rate-limit handling and retries.

    Tries old.reddit.com first (less aggressive blocking),
    then falls back to www.reddit.com.
    """
    session = _get_session()

    # Try old.reddit.com first
    urls_to_try = [url]
    if 'www.reddit.com' in url:
        urls_to_try.insert(0, url.replace('www.reddit.com', 'old.reddit.com'))

    last_error = None
    for try_url in urls_to_try:
        for attempt in range(max_retries):
            try:
                # Add a small random delay to look more human
                time.sleep(random.uniform(1.5, 3.5))
                response = session.get(try_url, timeout=30)

                if response.status_code == 429:
                    wait = 10 * (attempt + 1)
                    print(f"[Scraper] Rate limited, waiting {wait}s before retry...")
                    time.sleep(wait)
                    continue

                if response.status_code == 403:
                    print(f"[Scraper] 403 Blocked for {try_url}, trying next option...")
                    last_error = requests.exceptions.HTTPError(
                        f"403 Client Error: Blocked for url: {try_url}", response=response
                    )
                    break  # try next URL variant

                response.raise_for_status()
                return response.json()
            except requests.exceptions.ConnectionError as e:
                last_error = e
                break  # try next URL variant

    # If all URLs failed, raise the last error
    if last_error:
        raise last_error
    raise requests.exceptions.HTTPError("All Reddit endpoints returned errors")


def _discover_posts_from_comments():
    """Discover BriteCo's posts by looking at which posts they've commented on.

    Reddit promoted/profile posts don't appear in the submitted.json endpoint,
    but BriteCo's own comments on those posts do appear in comments.json.
    We extract the unique post IDs (link_id) from those comments.
    """
    discovered = {}  # post_id -> permalink
    after = None

    # Paginate through BriteCo's comments to find all unique posts
    for _ in range(5):  # max 5 pages (500 comments)
        url = f"https://www.reddit.com/user/{REDDIT_USERNAME}/comments.json?limit=100&raw_json=1"
        if after:
            url += f"&after={after}"

        data = _fetch_json(url)
        children = data.get('data', {}).get('children', [])
        if not children:
            break

        for item in children:
            comment = item['data']
            link_id = comment.get('link_id', '')  # e.g. t3_abc123
            permalink = comment.get('permalink', '')
            link_title = comment.get('link_title', '')
            subreddit = comment.get('subreddit', '')

            if link_id and link_id not in discovered:
                # Build the post URL from the comment permalink
                # Comment permalink: /r/sub/comments/POST_ID/slug/COMMENT_ID/
                # Post permalink: /r/sub/comments/POST_ID/slug/
                parts = permalink.strip('/').split('/')
                if len(parts) >= 5:
                    post_permalink = '/' + '/'.join(parts[:5]) + '/'
                    discovered[link_id] = {
                        'permalink': post_permalink,
                        'title': link_title,
                        'subreddit': subreddit
                    }

        after = data.get('data', {}).get('after')
        if not after:
            break

    return discovered


def _scrape_with_json():
    """Scrape using Reddit's public JSON endpoints. No auth required.

    For accounts with promoted/profile posts (like BriteCo), posts don't appear
    in the submitted.json endpoint. Instead, we discover posts by looking at
    which posts the account has commented on, then fetch comments for each.
    """
    posts_found = 0
    new_comments = 0

    # First try the normal submitted endpoint
    url = f"https://www.reddit.com/user/{REDDIT_USERNAME}/submitted.json?limit=100&raw_json=1"
    data = _fetch_json(url)
    submitted_posts = data.get('data', {}).get('children', [])

    # If no submitted posts found, discover posts from comments
    if not submitted_posts:
        discovered = _discover_posts_from_comments()
        posts_found = len(discovered)

        for post_id, post_info in discovered.items():
            # Fetch the full post + comments
            post_url = f"https://www.reddit.com{post_info['permalink']}.json?limit=500&raw_json=1"
            try:
                post_data = _fetch_json(post_url)
            except Exception:
                continue

            if not post_data or len(post_data) < 2:
                continue

            # Extract post details from the response
            post_listing = post_data[0].get('data', {}).get('children', [])
            if post_listing:
                actual_post = post_listing[0]['data']
                insert_post({
                    'id': actual_post['name'],
                    'title': actual_post.get('title', post_info['title']),
                    'subreddit': actual_post.get('subreddit', post_info['subreddit']),
                    'url': f"https://www.reddit.com{actual_post['permalink']}",
                    'created_utc': actual_post['created_utc']
                })
                target_post_id = actual_post['name']
            else:
                insert_post({
                    'id': post_id,
                    'title': post_info['title'],
                    'subreddit': post_info['subreddit'],
                    'url': f"https://www.reddit.com{post_info['permalink']}",
                    'created_utc': 0
                })
                target_post_id = post_id

            # Process comments
            comment_listing = post_data[1].get('data', {}).get('children', [])
            new_comments += _process_comments(comment_listing, target_post_id)
    else:
        # Normal path for accounts with regular submitted posts
        posts_found = len(submitted_posts)
        for post_item in submitted_posts:
            post = post_item['data']
            post_id = post['name']

            insert_post({
                'id': post_id,
                'title': post['title'],
                'subreddit': post['subreddit'],
                'url': f"https://www.reddit.com{post['permalink']}",
                'created_utc': post['created_utc']
            })

            comments_url = f"https://www.reddit.com{post['permalink']}.json?limit=500&raw_json=1"
            try:
                comments_data = _fetch_json(comments_url)
            except Exception:
                continue

            if len(comments_data) < 2:
                continue

            comment_listing = comments_data[1].get('data', {}).get('children', [])
            new_comments += _process_comments(comment_listing, post_id)

    return posts_found, new_comments


def _process_comments(comment_listing, post_id):
    """Recursively process a comment listing. Returns count of new comments."""
    new_count = 0

    for item in comment_listing:
        if item['kind'] != 't1':  # skip "more" items
            continue

        comment = item['data']
        author = comment.get('author', '[deleted]')

        is_new = insert_comment({
            'id': comment['name'],
            'post_id': post_id,
            'author': author,
            'body': comment.get('body', ''),
            'created_utc': comment['created_utc'],
            'parent_id': comment.get('parent_id'),
            'score': comment.get('score', 0)
        })

        if is_new:
            new_count += 1

        # Process replies
        replies = comment.get('replies')
        if replies and isinstance(replies, dict):
            reply_children = replies.get('data', {}).get('children', [])
            new_count += _process_comments(reply_children, post_id)

    return new_count


# --- PRAW Scraper (used when API credentials are available) ---

def _scrape_with_praw():
    """Scrape using PRAW (official Reddit API). Requires credentials.

    BriteCo's posts are promoted/profile posts that don't appear in
    user.submissions. Instead, we discover posts by looking at which posts
    BriteCo has commented on, then fetch comments for each.
    """
    import praw

    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )

    posts_found = 0
    new_comments = 0

    user = reddit.redditor(REDDIT_USERNAME)

    # First try normal submissions
    submissions = list(user.submissions.new(limit=100))

    if not submissions:
        # Discover posts from BriteCo's comments (promoted/profile posts)
        print("[Scraper] No submissions found, discovering posts from comments...")
        seen_posts = {}
        for comment in user.comments.new(limit=500):
            post_id = comment.link_id  # e.g. t3_abc123
            if post_id not in seen_posts:
                seen_posts[post_id] = comment.submission

        submissions = list(seen_posts.values())
        print(f"[Scraper] Discovered {len(submissions)} posts from comments")

    for submission in submissions:
        posts_found += 1

        insert_post({
            'id': submission.fullname,
            'title': submission.title,
            'subreddit': str(submission.subreddit),
            'url': f"https://www.reddit.com{submission.permalink}",
            'created_utc': submission.created_utc
        })

        submission.comments.replace_more(limit=10)
        for comment in submission.comments.list():
            author = str(comment.author) if comment.author else '[deleted]'

            is_new = insert_comment({
                'id': comment.fullname,
                'post_id': submission.fullname,
                'author': author,
                'body': comment.body,
                'created_utc': comment.created_utc,
                'parent_id': comment.parent_id,
                'score': comment.score
            })

            if is_new:
                new_comments += 1

    return posts_found, new_comments


# --- Mock Scraper (for testing without Reddit) ---

MOCK_SUBREDDITS = ['jewelry', 'engagementrings', 'Insurance', 'jewelers', 'diamonds']
MOCK_TITLES = [
    "Why You Need Jewelry Insurance in 2026",
    "BriteCo vs Traditional Jewelry Insurance - Our Experience",
    "PSA: Get Your Engagement Ring Appraised and Insured",
    "How to Choose the Right Jewelry Insurance",
    "We just launched updated coverage options!",
    "AMA: Ask us anything about jewelry insurance",
]
MOCK_AUTHORS = ['ring_lover_22', 'diamondgirl', 'insurancequestion', 'engaged_2026', 'jewelry_collector', 'sparkle_fan']
MOCK_COMMENTS = [
    "This is really helpful, thanks for sharing!",
    "How does the claims process work? I had a bad experience with another company.",
    "Just signed up last week. The process was super easy.",
    "Does this cover watches too or just jewelry?",
    "What's the difference between replacement value and actual value coverage?",
    "My jeweler recommended BriteCo. Glad to see you're active here!",
    "How long does an appraisal take?",
    "Is there a deductible?",
    "Can I add items to my policy later?",
    "Great info! Sharing this with my fiancée.",
]


def _scrape_mock():
    """Generate mock data for testing."""
    posts_found = random.randint(3, 6)
    new_comments = 0
    base_time = datetime.now(timezone.utc).timestamp()

    for i in range(posts_found):
        post_id = f"t3_mock_{int(base_time)}_{i}"
        subreddit = random.choice(MOCK_SUBREDDITS)

        insert_post({
            'id': post_id,
            'title': random.choice(MOCK_TITLES),
            'subreddit': subreddit,
            'url': f"https://www.reddit.com/r/{subreddit}/comments/mock{i}/mock_post/",
            'created_utc': base_time - random.randint(86400, 604800)  # 1-7 days ago
        })

        num_comments = random.randint(0, 5)
        for j in range(num_comments):
            comment_id = f"t1_mock_{int(base_time)}_{i}_{j}"
            is_new = insert_comment({
                'id': comment_id,
                'post_id': post_id,
                'author': random.choice(MOCK_AUTHORS),
                'body': random.choice(MOCK_COMMENTS),
                'created_utc': base_time - random.randint(0, 86400),
                'parent_id': post_id,
                'score': random.randint(1, 50)
            })
            if is_new:
                new_comments += 1

    return posts_found, new_comments


# --- Main entry point ---

def run_scrape():
    """Run the scraper. Returns (posts_found, new_comments_found).

    Uses mock mode if TEST_MODE is on, PRAW if credentials are set,
    otherwise falls back to public JSON endpoints.
    """
    log_id = log_scrape_start()

    try:
        if TEST_MODE:
            posts_found, new_comments = _scrape_mock()
        elif REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET:
            posts_found, new_comments = _scrape_with_praw()
        else:
            posts_found, new_comments = _scrape_with_json()

        log_scrape_end(
            log_id,
            posts_found=posts_found,
            new_comments_found=new_comments,
            status='success'
        )
        return posts_found, new_comments

    except Exception as e:
        error_msg = str(e)
        if '403' in error_msg:
            error_msg = (
                f"{error_msg} — Reddit may be blocking requests from cloud IPs. "
                "Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET to use the official API instead."
            )
        log_scrape_end(log_id, status='error', error_message=error_msg)
        raise
