"""
Token Manager — Handles Kite access token generation and storage in MongoDB.

Flow:
    1. User opens the app → check MongoDB for a valid access token (created today)
    2. If no valid token → show token setup page
    3. User clicks "Open Kite Login" → opens Kite login URL in browser
    4. After login, user gets a request_token from the redirect URL
    5. User pastes the request_token → this module converts it to access_token
    6. Access token is stored in MongoDB with today's date
    7. Token is valid for the entire day (Kite tokens expire daily)

Collections:
    kite_tokens — Stores access tokens with creation date.
        Schema:
        {
            "api_key": "...",
            "access_token": "...",
            "date": "2026-03-06",
            "created_at": ISODate(...)
        }
"""

import logging
from datetime import datetime
from typing import Optional
from kiteconnect import KiteConnect

logger = logging.getLogger(__name__)


class TokenManager:
    """
    Manages Kite Connect access tokens with MongoDB persistence.

    - Stores tokens per day (Kite tokens expire daily at ~6AM)
    - Converts request_token → access_token via KiteConnect API
    - Provides quick lookup for today's valid token
    """

    def __init__(self, db_manager, api_key: str, api_secret: str):
        """
        Args:
            db_manager:  DatabaseManager instance (must be connected).
            api_key:     Kite API key.
            api_secret:  Kite API secret.
        """
        self._db = db_manager
        self._api_key = api_key
        self._api_secret = api_secret
        self._collection = None
        self._access_token = None  # cached in-memory

        if db_manager and db_manager.is_connected:
            self._collection = db_manager._db["kite_tokens"]

    def get_login_url(self) -> str:
        """Return the Kite Connect login URL for the configured API key."""
        return f"https://kite.zerodha.com/connect/login?v=3&api_key={self._api_key}"

    def get_today_token(self) -> str | None:
        """
        Check MongoDB for a valid access token created today.

        Returns:
            Access token string if found, None otherwise.
        """
        if self._access_token:
            return self._access_token

        if self._collection is None:
            logger.warning("Token collection not initialized (DB not connected)")
            return None

        today = datetime.now().strftime("%Y-%m-%d")

        try:
            doc = self._collection.find_one(
                {"api_key": self._api_key, "date": today},
                {"_id": 0, "access_token": 1},
            )

            if doc and doc.get("access_token"):
                self._access_token = doc["access_token"]
                logger.info("Found valid access token for today (%s) in MongoDB.", today)
                return self._access_token

        except Exception as e:
            logger.error("Error fetching today's token from MongoDB: %s", e)

        return None

    def generate_and_store_token(self, request_token: str) -> dict:
        """
        Convert a request_token to an access_token and store it in MongoDB.

        Args:
            request_token: The request token obtained after Kite login.

        Returns:
            Dict with 'success', 'access_token' (or 'error' on failure).
        """
        try:
            kite = KiteConnect(api_key=self._api_key)
            data = kite.generate_session(request_token, api_secret=self._api_secret)
            access_token = data["access_token"]

            # Store in MongoDB
            today = datetime.now().strftime("%Y-%m-%d")
            token_doc = {
                "api_key": self._api_key,
                "access_token": access_token,
                "date": today,
                "created_at": datetime.utcnow(),
            }

            if self._collection is not None:
                self._collection.update_one(
                    {"api_key": self._api_key, "date": today},
                    {"$set": token_doc},
                    upsert=True,
                )
                logger.info("✅ Access token generated and stored in MongoDB for %s", today)
            else:
                logger.warning("DB not connected — token generated but NOT persisted!")

            # Cache in memory
            self._access_token = access_token

            return {"success": True, "access_token": access_token}

        except Exception as e:
            error_msg = str(e)
            logger.error("❌ Failed to generate access token: %s", error_msg)
            return {"success": False, "error": error_msg}

    def invalidate_token(self):
        """Clear the cached token (forces re-fetch from DB on next call)."""
        self._access_token = None

    def validate_token(self, access_token: Optional[str] = None) -> bool:
        """
        Quick validation — tries a simple Kite API call to check if token works.

        Args:
            access_token: Token to validate. Uses cached/DB token if not provided.

        Returns:
            True if the token is valid and working.
        """
        token = access_token or self.get_today_token()
        if not token:
            return False

        try:
            kite = KiteConnect(api_key=self._api_key)
            kite.set_access_token(token)
            # A lightweight call to verify the token works
            kite.profile()
            return True
        except Exception as e:
            logger.warning("Token validation failed: %s", e)
            return False

    def cleanup_old_tokens(self):
        """Remove tokens older than today to keep collection clean."""
        if self._collection is None:
            return

        today = datetime.now().strftime("%Y-%m-%d")
        try:
            result = self._collection.delete_many({"date": {"$ne": today}})
            if result.deleted_count > 0:
                logger.info("Cleaned up %d old token(s).", result.deleted_count)
        except Exception as e:
            logger.error("Error cleaning up old tokens: %s", e)
