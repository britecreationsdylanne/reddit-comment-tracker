"""GCS Backup — Persist SQLite database across Cloud Run deploys.

Cloud Run containers are ephemeral. This module backs up the SQLite DB
to a GCS bucket after data changes, and restores it on startup.
"""

import os
from config.settings import DATABASE_PATH, GCS_BUCKET_NAME

DB_BLOB_NAME = 'reddit_tracker.db'


def _get_client():
    """Get GCS client. Returns None if bucket not configured."""
    if not GCS_BUCKET_NAME:
        return None
    try:
        from google.cloud import storage
        return storage.Client()
    except Exception as e:
        print(f"[GCS] Failed to create client: {e}")
        return None


def restore_db():
    """Download the SQLite DB from GCS on startup.

    Called before init_db() so existing data is preserved across deploys.
    Silently skips if GCS is not configured or the file doesn't exist yet.
    """
    client = _get_client()
    if not client:
        return

    try:
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(DB_BLOB_NAME)

        if not blob.exists():
            print("[GCS] No backup found in bucket — starting fresh")
            return

        os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
        blob.download_to_filename(DATABASE_PATH)
        print(f"[GCS] Restored database from gs://{GCS_BUCKET_NAME}/{DB_BLOB_NAME}")
    except Exception as e:
        print(f"[GCS] Restore failed (will start fresh): {e}")


def backup_db():
    """Upload the SQLite DB to GCS after data changes.

    Called after sync uploads and scrapes to persist new data.
    """
    client = _get_client()
    if not client:
        return

    if not os.path.exists(DATABASE_PATH):
        print("[GCS] No database file to back up")
        return

    try:
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(DB_BLOB_NAME)
        blob.upload_from_filename(DATABASE_PATH)
        print(f"[GCS] Backed up database to gs://{GCS_BUCKET_NAME}/{DB_BLOB_NAME}")
    except Exception as e:
        print(f"[GCS] Backup failed: {e}")
