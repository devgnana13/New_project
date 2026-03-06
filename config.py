"""
Platform configuration — loaded from environment variables or .env file.

All sensitive values (API keys, MongoDB URI) must be provided via
environment variables. Never commit real credentials to source control.
"""

import os
from dotenv import load_dotenv

load_dotenv()

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
