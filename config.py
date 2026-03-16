"""
Platform configuration — loaded from environment variables or .env file.

All sensitive values (API keys, MongoDB URI) must be provided via
environment variables. Never commit real credentials to source control.
"""

import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────
#  IST TIMEZONE — All market operations must use IST
#  (VPS servers typically run in UTC; datetime.now() would
#   return UTC which breaks market-hour checks, EOD snapshots,
#   and date-based MongoDB lookups)
# ──────────────────────────────────────────────────────────────

IST = timezone(timedelta(hours=5, minutes=30))


def now_ist() -> datetime:
    """Return the current datetime in IST (Indian Standard Time).

    ALWAYS use this instead of datetime.now() for any market-related
    time checks (EOD snapshots, trading day calculations, token dates, etc.).
    """
    return datetime.now(IST)


def today_ist() -> str:
    """Return today's date string 'YYYY-MM-DD' in IST."""
    return now_ist().strftime("%Y-%m-%d")

# ──────────────────────────────────────────────────────────────
#  KITE CONNECT API CREDENTIALS (up to 3 API keys for workers)
# ──────────────────────────────────────────────────────────────

KITE_CREDENTIALS = {
    "api_key": os.getenv("KITE_API_KEY", ""),
    "api_secret": os.getenv("KITE_API_SECRET", ""),
}

# ──────────────────────────────────────────────────────────────
#  MONGODB
# ──────────────────────────────────────────────────────────────

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017"
)
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "options_analytics")

# ──────────────────────────────────────────────────────────────
#  APPLICATION
# ──────────────────────────────────────────────────────────────

DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1", "yes")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
FLASK_PORT = int(os.getenv("FLASK_PORT", "5000"))
