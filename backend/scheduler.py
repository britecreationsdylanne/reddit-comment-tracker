from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from config.settings import SCRAPE_HOUR, SCRAPE_MINUTE

scheduler = BackgroundScheduler()


def init_scheduler(app):
    """Initialize and start the background scheduler for daily scraping."""
    from backend.reddit_scraper import run_scrape
    from backend.email_notifier import send_notification
    from backend.database import get_new_comments_since, get_last_successful_scrape, log_scrape_end

    def daily_job():
        with app.app_context():
            print(f"[Scheduler] Starting daily Reddit scrape...")

            # Get the timestamp of the last successful scrape (before this one runs)
            last_scrape = get_last_successful_scrape()
            last_timestamp = last_scrape['started_at'] if last_scrape else '1970-01-01T00:00:00'

            try:
                posts_found, new_count = run_scrape()
                print(f"[Scheduler] Scrape complete: {posts_found} posts, {new_count} new comments")

                if new_count > 0:
                    new_comments = get_new_comments_since(last_timestamp)
                    sent = send_notification(new_comments)

                    # Update the scrape log with email status
                    from backend.database import get_db
                    conn = get_db()
                    conn.execute(
                        "UPDATE scrape_log SET email_sent = ? WHERE id = (SELECT MAX(id) FROM scrape_log WHERE status = 'success')",
                        (1 if sent > 0 else 0,)
                    )
                    conn.commit()
                    conn.close()
                else:
                    print("[Scheduler] No new comments — skipping email notification")

            except Exception as e:
                print(f"[Scheduler] Scrape failed: {e}")

    scheduler.add_job(
        daily_job,
        trigger=CronTrigger(hour=SCRAPE_HOUR, minute=SCRAPE_MINUTE),
        id='daily_reddit_scrape',
        name='Daily Reddit comment scrape',
        replace_existing=True,
        misfire_grace_time=3600
    )

    scheduler.start()
    print(f"[Scheduler] Started — daily scrape scheduled at {SCRAPE_HOUR:02d}:{SCRAPE_MINUTE:02d}")
