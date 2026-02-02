"""
Local Sync Script — Scrapes Reddit locally and uploads data to Cloud Run.

Reddit blocks requests from cloud IPs, so this script runs on your local
machine where Reddit works, then pushes the data to the cloud dashboard.

Usage:
    python sync_to_cloud.py

Set these in your .env file:
    CLOUD_URL=https://britepulse-reddit-279545860595.us-central1.run.app
    SYNC_API_KEY=your-secret-key-here
"""

import os
import sys
import json
import requests
from dotenv import load_dotenv

# Ensure working directory is the project root (needed for Task Scheduler)
os.chdir(os.path.dirname(os.path.abspath(__file__)))

load_dotenv()

sys.path.insert(0, os.path.dirname(__file__))

from backend.database import init_db, get_all_posts, get_all_comments_raw
from backend.reddit_scraper import run_scrape

CLOUD_URL = os.getenv('CLOUD_URL', '').rstrip('/')
SYNC_API_KEY = os.getenv('SYNC_API_KEY', '')


def sync():
    if not CLOUD_URL:
        print("[Sync] ERROR: Set CLOUD_URL in .env (e.g. https://britepulse-reddit-xxx.us-central1.run.app)")
        return
    if not SYNC_API_KEY:
        print("[Sync] ERROR: Set SYNC_API_KEY in .env (must match the cloud app's SYNC_API_KEY)")
        return

    # Step 1: Run scrape locally (this works from residential IPs)
    print("[Sync] Running local scrape...")
    init_db()
    try:
        posts_found, new_comments = run_scrape()
        print(f"[Sync] Scraped {posts_found} posts, {new_comments} new comments")
    except Exception as e:
        print(f"[Sync] Scrape failed: {e}")
        print("[Sync] Continuing with existing local data...")

    # Step 2: Read all data from local DB
    print("[Sync] Reading local database...")
    posts = get_all_posts()
    comments = get_all_comments_raw()
    print(f"[Sync] Found {len(posts)} posts and {len(comments)} comments to sync")

    # Step 3: Upload to cloud
    print(f"[Sync] Uploading to {CLOUD_URL}...")
    try:
        response = requests.post(
            f"{CLOUD_URL}/api/sync/upload",
            json={'posts': posts, 'comments': comments},
            headers={
                'X-Sync-Key': SYNC_API_KEY,
                'Content-Type': 'application/json'
            },
            timeout=120
        )

        if response.status_code == 200:
            result = response.json()
            print(f"[Sync] Success! Synced {result.get('posts_synced', 0)} posts, "
                  f"{result.get('new_comments', 0)} new comments to cloud")
        else:
            print(f"[Sync] Upload failed: {response.status_code} — {response.text}")
    except Exception as e:
        print(f"[Sync] Upload failed: {e}")


if __name__ == '__main__':
    sync()
