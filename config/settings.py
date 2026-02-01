import os
from dotenv import load_dotenv

load_dotenv()

# Reddit API credentials (optional — public JSON endpoints used if these are empty)
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID', '')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET', '')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'BriteCo Comment Tracker v1.0')
REDDIT_USERNAME = os.getenv('REDDIT_USERNAME', '') or 'BriteCo_Insurance'

# Scraper schedule (24-hour format)
SCRAPE_HOUR = int(os.getenv('SCRAPE_HOUR', '8'))
SCRAPE_MINUTE = int(os.getenv('SCRAPE_MINUTE', '0'))

# SendGrid
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY', '')
SENDGRID_FROM_EMAIL = os.getenv('SENDGRID_FROM_EMAIL', 'notifications@brite.co')
SENDGRID_FROM_NAME = os.getenv('SENDGRID_FROM_NAME', 'BriteCo Reddit Tracker')

# Database
DATABASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'reddit_tracker.db')

# Flask
FLASK_PORT = int(os.getenv('PORT', '8080'))
FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'

# Anthropic API (Claude) — for suggested replies and sentiment tagging
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')

# Sync API key — protects the /api/sync/upload endpoint
SYNC_API_KEY = os.getenv('SYNC_API_KEY', '')

# Test mode — uses mock data instead of hitting Reddit
TEST_MODE = os.getenv('TEST_MODE', 'false').lower() == 'true'
