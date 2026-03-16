"""
Database Module — MongoDB integration for storing historical EOD
volume data and alerts.

Collections:
    eod_volumes  — End-of-day CE/PE volume totals per symbol per date.
    alerts       — Trading alerts (volume spikes, PCR thresholds, etc.).

Architecture:
    ┌────────────────────────────────────────────────────────┐
    │                   DATABASE MODULE                      │
    │                                                        │
    │  ┌──────────────────────────────────────────────────┐  │
    │  │ MongoClient (pymongo)                             │  │
    │  │   └─ Database: options_analytics                   │  │
    │  │       ├─ Collection: eod_volumes                   │  │
    │  │       │   Index: (symbol, date) UNIQUE             │  │
    │  │       │   Schema:                                  │  │
    │  │       │   {                                        │  │
    │  │       │     symbol: "RELIANCE",                    │  │
    │  │       │     date: "2026-03-05",                    │  │
    │  │       │     call_volume_total: 150000,             │  │
    │  │       │     put_volume_total: 120000,              │  │
    │  │       │     created_at: ISODate(...)               │  │
    │  │       │   }                                        │  │
    │  │       │                                            │  │
    │  │       └─ Collection: alerts                        │  │
    │  │           Index: (symbol, date, alert_type) UNIQUE │  │
    │  │           Schema:                                  │  │
    │  │           {                                        │  │
    │  │             symbol: "RELIANCE",                    │  │
    │  │             date: "2026-03-05",                    │  │
    │  │             alert_type: "volume_spike",            │  │
    │  │             message: "CE volume surged 3x ...",    │  │
    │  │             data: {...},                           │  │
    │  │             created_at: ISODate(...)               │  │
    │  │           }                                        │  │
    │  └──────────────────────────────────────────────────┘  │
    │                                                        │
    │  Public API:                                           │
    │    store_eod_volumes(volumes_dict)                     │
    │    get_yesterday_volume(symbol)                        │
    │    get_eod_volume(symbol, date)                        │
    │    get_volume_history(symbol, days)                    │
    │    store_alert(symbol, alert_type, message, data)      │
    │    get_alerts(symbol, date)                            │
    └────────────────────────────────────────────────────────┘

Usage:
    from core.database import DatabaseManager

    db = DatabaseManager()   # Uses MONGO_URI from config
    db.connect()

    # At market close — store today's final volumes
    volumes = volume_aggregator.get_volumes()
    db.store_eod_volumes(volumes)

    # Next day — get yesterday's data
    yesterday = db.get_yesterday_volume("RELIANCE")
    # {"symbol": "RELIANCE", "date": "2026-03-04",
    #  "call_volume_total": 150000, "put_volume_total": 120000}
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, ConnectionFailure
from config import now_ist, today_ist

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    MongoDB client wrapper for the options analytics platform.

    Manages two collections:
        - eod_volumes: Historical end-of-day CE/PE volume totals
        - alerts: Trading alerts and notifications
    """

    def __init__(
        self,
        mongo_uri: str = None,
        db_name: str = None,
        client: MongoClient = None,
    ):
        """
        Args:
            mongo_uri:  MongoDB connection string. Defaults to config.MONGO_URI.
            db_name:    Database name. Defaults to config.MONGO_DB_NAME.
            client:     Optional pre-configured MongoClient (for testing).
        """
        if client is not None:
            # Use injected client (for testing)
            self._client = client
            self._db_name = db_name or "test_db"
        else:
            # Lazy import config to avoid circular imports
            from config import MONGO_URI, MONGO_DB_NAME
            self._mongo_uri = mongo_uri or MONGO_URI
            self._db_name = db_name or MONGO_DB_NAME
            self._client = None

        self._db = None
        self._eod_volumes = None
        self._alerts = None
        self._connected = False

        # If client was injected, connect immediately
        if client is not None:
            self._client = client
            self._db = self._client[self._db_name]
            self._eod_volumes = self._db["eod_volumes"]
            self._eod_oi = self._db["eod_oi"]
            self._alerts = self._db["alerts"]
            self._connected = True
            self._ensure_indexes()

    # ──────────────────────────────────────────────────────────
    #  CONNECTION
    # ──────────────────────────────────────────────────────────

    def connect(self) -> bool:
        """
        Establish the MongoDB connection and set up indexes.

        Returns:
            True if connection succeeded.
        """
        if self._connected:
            return True

        try:
            self._client = MongoClient(
                self._mongo_uri,
                serverSelectionTimeoutMS=5000,
            )
            # Verify connection
            self._client.admin.command("ping")

            self._db = self._client[self._db_name]
            self._eod_volumes = self._db["eod_volumes"]
            self._eod_oi = self._db["eod_oi"]
            self._alerts = self._db["alerts"]
            self._connected = True

            self._ensure_indexes()

            logger.info(
                "Connected to MongoDB: %s / %s",
                self._mongo_uri, self._db_name,
            )
            return True

        except ConnectionFailure as e:
            logger.error("MongoDB connection failed: %s", e)
            return False

    def disconnect(self) -> None:
        """Close the MongoDB connection."""
        if self._client:
            self._client.close()
            self._connected = False
            logger.info("Disconnected from MongoDB.")

    @property
    def is_connected(self) -> bool:
        return self._connected

    def _ensure_indexes(self) -> None:
        """Create unique compound indexes for efficient lookups."""
        if self._eod_volumes is not None:
            self._eod_volumes.create_index(
                [("symbol", ASCENDING), ("date", ASCENDING)],
                unique=True,
                name="idx_symbol_date",
            )
        if hasattr(self, '_eod_oi') and self._eod_oi is not None:
            self._eod_oi.create_index(
                [("symbol", ASCENDING), ("date", ASCENDING)],
                unique=True,
                name="idx_symbol_date",
            )
        if self._alerts is not None:
            self._alerts.create_index(
                [("symbol", ASCENDING), ("date", ASCENDING),
                 ("alert_type", ASCENDING)],
                unique=True,
                name="idx_symbol_date_type",
            )

    def _ensure_connected(self) -> None:
        """Raise if not connected."""
        if not self._connected:
            raise RuntimeError(
                "Database not connected. Call connect() first."
            )

    # ──────────────────────────────────────────────────────────
    #  EOD VOLUMES — Write
    # ──────────────────────────────────────────────────────────

    def store_eod_volumes(
        self,
        volumes: dict[str, dict],
        date: str = None,
    ) -> int:
        """
        Store end-of-day volume totals for all symbols.

        Called at market close (15:30 IST) with the final volume
        snapshot from the VolumeAggregator.

        Args:
            volumes:  Dict from VolumeAggregator.get_volumes():
                      {"RELIANCE": {"call_volume": X, "put_volume": Y}, ...}
            date:     Date string "YYYY-MM-DD". Defaults to today.

        Returns:
            Number of symbols stored/updated.
        """
        self._ensure_connected()

        if date is None:
            date = today_ist()

        stored = 0
        now = now_ist()

        for symbol, vol_data in volumes.items():
            doc = {
                "symbol": symbol.upper(),
                "date": date,
                "call_volume_total": vol_data.get("call_volume", 0),
                "put_volume_total": vol_data.get("put_volume", 0),
                "created_at": now,
            }

            try:
                self._eod_volumes.update_one(
                    {"symbol": doc["symbol"], "date": date},
                    {"$set": doc},
                    upsert=True,
                )
                stored += 1
            except Exception as e:
                logger.error(
                    "Failed to store EOD volume for %s: %s", symbol, e,
                )

        logger.info(
            "Stored EOD volumes for %d/%d symbols (date=%s).",
            stored, len(volumes), date,
        )
        return stored

    def delete_old_eod_volumes(self, keep_date: str) -> int:
        """
        Delete all EOD volume records for dates OTHER than keep_date.

        Called right after the daily snapshot to keep the DB lean —
        only the most recent day's data is needed.

        Args:
            keep_date: Date string \"YYYY-MM-DD\" to preserve.
                       All other dates will be deleted.

        Returns:
            Number of documents deleted.
        """
        self._ensure_connected()
        try:
            result = self._eod_volumes.delete_many(
                {"date": {"$ne": keep_date}}
            )
            deleted = result.deleted_count
            logger.info(
                "Cleaned up EOD volumes: deleted %d records (kept date=%s).",
                deleted, keep_date,
            )
            return deleted
        except Exception as e:
            logger.error("Failed to delete old EOD volumes: %s", e)
            return 0

    def delete_old_alerts(self, keep_date: str) -> int:
        """
        Delete all alert records for dates OTHER than keep_date.

        Args:
            keep_date: Date string \"YYYY-MM-DD\" to preserve.

        Returns:
            Number of documents deleted.
        """
        self._ensure_connected()
        try:
            result = self._alerts.delete_many(
                {"date": {"$ne": keep_date}}
            )
            deleted = result.deleted_count
            logger.info(
                "Cleaned up alerts: deleted %d records (kept date=%s).",
                deleted, keep_date,
            )
            return deleted
        except Exception as e:
            logger.error("Failed to delete old alerts: %s", e)
            return 0


    def store_single_eod(
        self,
        symbol: str,
        call_volume_total: int,
        put_volume_total: int,
        date: str = None,
    ) -> bool:
        """
        Store EOD volume for a single symbol.

        Args:
            symbol:            Stock symbol.
            call_volume_total: Total CE volume for the day.
            put_volume_total:  Total PE volume for the day.
            date:              Date string. Defaults to today.

        Returns:
            True if stored successfully.
        """
        self._ensure_connected()

        if date is None:
            date = today_ist()

        doc = {
            "symbol": symbol.upper(),
            "date": date,
            "call_volume_total": call_volume_total,
            "put_volume_total": put_volume_total,
            "created_at": now_ist(),
        }

        try:
            self._eod_volumes.update_one(
                {"symbol": doc["symbol"], "date": date},
                {"$set": doc},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("Failed to store EOD for %s: %s", symbol, e)
            return False

    # ──────────────────────────────────────────────────────────
    #  EOD VOLUMES — Read
    # ──────────────────────────────────────────────────────────

    def get_yesterday_volume(self, symbol: str) -> Optional[dict]:
        """
        Get the last trading day's EOD volume for a symbol.
        Skips weekends (Sat/Sun) and falls back up to 7 days.

        Returns:
            Dict with symbol, date, call_volume_total, put_volume_total.
            None if no data found.
        """
        self._ensure_connected()

        check_date = now_ist() - timedelta(days=1)
        while check_date.weekday() in (5, 6):  # Sat=5, Sun=6
            check_date -= timedelta(days=1)

        # Try the calculated date first
        result = self.get_eod_volume(symbol, check_date.strftime("%Y-%m-%d"))
        if result:
            return result

        # Fallback: search back up to 7 days for data (handles holidays)
        for days_back in range(2, 8):
            fallback = now_ist() - timedelta(days=days_back)
            if fallback.weekday() in (5, 6):
                continue
            result = self.get_eod_volume(symbol, fallback.strftime("%Y-%m-%d"))
            if result:
                return result

        return None

    def get_eod_volume(self, symbol: str, date: str) -> Optional[dict]:
        """
        Get EOD volume for a specific symbol and date.

        Args:
            symbol: Stock symbol (case-insensitive).
            date:   Date string "YYYY-MM-DD".

        Returns:
            Dict with symbol, date, call_volume_total, put_volume_total.
            None if no data found.
        """
        self._ensure_connected()

        doc = self._eod_volumes.find_one(
            {"symbol": symbol.upper(), "date": date},
            {"_id": 0, "symbol": 1, "date": 1,
             "call_volume_total": 1, "put_volume_total": 1},
        )
        return doc

    def get_volume_history(
        self,
        symbol: str,
        days: int = 30,
    ) -> list[dict]:
        """
        Get volume history for a symbol over the last N days.

        Args:
            symbol: Stock symbol.
            days:   Number of days to look back (default: 30).

        Returns:
            List of dicts sorted by date (oldest first).
        """
        self._ensure_connected()

        cutoff = (now_ist() - timedelta(days=days)).strftime("%Y-%m-%d")
        cursor = self._eod_volumes.find(
            {"symbol": symbol.upper(), "date": {"$gte": cutoff}},
            {"_id": 0, "symbol": 1, "date": 1,
             "call_volume_total": 1, "put_volume_total": 1},
        ).sort("date", ASCENDING)

        return list(cursor)

    def get_all_eod_volumes(self, date: str = None) -> list[dict]:
        """
        Get EOD volumes for all symbols on a given date.

        Args:
            date: Date string. Defaults to today.

        Returns:
            List of volume dicts.
        """
        self._ensure_connected()

        if date is None:
            date = today_ist()

        cursor = self._eod_volumes.find(
            {"date": date},
            {"_id": 0},
        ).sort("symbol", ASCENDING)

        return list(cursor)

    # ──────────────────────────────────────────────────────────
    #  EOD OI — Write & Read
    # ──────────────────────────────────────────────────────────

    def store_eod_oi(
        self,
        oi_data: dict[str, dict],
        date: str = None,
    ) -> int:
        self._ensure_connected()
        if date is None:
            date = today_ist()

        stored = 0
        now = now_ist()

        for symbol, data in oi_data.items():
            doc = {
                "symbol": symbol.upper(),
                "date": date,
                "strikes": data.get("strikes", {}),
                "created_at": now,
            }
            try:
                self._eod_oi.update_one(
                    {"symbol": doc["symbol"], "date": date},
                    {"$set": doc},
                    upsert=True,
                )
                stored += 1
            except Exception as e:
                logger.error("Failed to store EOD OI for %s: %s", symbol, e)

        return stored

    def delete_old_eod_oi(self, keep_date: str) -> int:
        self._ensure_connected()
        try:
            result = self._eod_oi.delete_many(
                {"date": {"$ne": keep_date}}
            )
            return result.deleted_count
        except Exception as e:
            logger.error("Failed to delete old EOD OI: %s", e)
            return 0

    def get_eod_oi(self, symbol: str, date: str) -> Optional[dict]:
        self._ensure_connected()
        doc = self._eod_oi.find_one(
            {"symbol": symbol.upper(), "date": date},
            {"_id": 0, "symbol": 1, "date": 1, "strikes": 1},
        )
        return doc
        
    def get_yesterday_oi(self, symbol: str) -> Optional[dict]:
        self._ensure_connected()
        check_date = now_ist() - timedelta(days=1)
        while check_date.weekday() in (5, 6):
            check_date -= timedelta(days=1)

        result = self.get_eod_oi(symbol, check_date.strftime("%Y-%m-%d"))
        if result:
            return result

        for days_back in range(2, 8):
            fallback = now_ist() - timedelta(days=days_back)
            if fallback.weekday() in (5, 6):
                continue
            result = self.get_eod_oi(symbol, fallback.strftime("%Y-%m-%d"))
            if result:
                return result

        return None

    def get_all_eod_ois(self, date: str = None) -> list[dict]:
        self._ensure_connected()
        if date is None:
            date = today_ist()
        cursor = self._eod_oi.find(
            {"date": date},
            {"_id": 0},
        ).sort("symbol", ASCENDING)
        return list(cursor)

    # ──────────────────────────────────────────────────────────
    #  ALERTS — Write
    # ──────────────────────────────────────────────────────────

    def store_alert(
        self,
        symbol: str,
        alert_type: str,
        message: str,
        data: dict = None,
        date: str = None,
    ) -> bool:
        """
        Store a trading alert.

        The (symbol, date, alert_type) combination is unique — storing
        the same alert twice updates the existing record.

        Args:
            symbol:     Stock symbol.
            alert_type: e.g., "volume_spike", "pcr_extreme", "oi_buildup".
            message:    Human-readable alert message.
            data:       Optional additional data dict.
            date:       Date string. Defaults to today.

        Returns:
            True if stored successfully.
        """
        self._ensure_connected()

        if date is None:
            date = today_ist()

        doc = {
            "symbol": symbol.upper(),
            "date": date,
            "alert_type": alert_type,
            "message": message,
            "data": data or {},
            "created_at": now_ist(),
        }

        try:
            self._alerts.update_one(
                {
                    "symbol": doc["symbol"],
                    "date": date,
                    "alert_type": alert_type,
                },
                {"$set": doc},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error(
                "Failed to store alert for %s: %s", symbol, e,
            )
            return False

    # ──────────────────────────────────────────────────────────
    #  ALERTS — Read
    # ──────────────────────────────────────────────────────────

    def get_alerts(
        self,
        symbol: str = None,
        date: str = None,
        alert_type: str = None,
    ) -> list[dict]:
        """
        Get alerts, optionally filtered by symbol, date, and type.

        Args:
            symbol:     Filter by symbol (optional).
            date:       Filter by date (optional, defaults to today).
            alert_type: Filter by alert type (optional).

        Returns:
            List of alert dicts.
        """
        self._ensure_connected()

        query = {}
        if symbol:
            query["symbol"] = symbol.upper()
        if date:
            query["date"] = date
        if alert_type:
            query["alert_type"] = alert_type

        cursor = self._alerts.find(
            query,
            {"_id": 0},
        ).sort("created_at", DESCENDING)

        return list(cursor)

    def get_recent_alerts(self, limit: int = 50) -> list[dict]:
        """Get the most recent alerts across all symbols."""
        self._ensure_connected()

        cursor = self._alerts.find(
            {},
            {"_id": 0},
        ).sort("created_at", DESCENDING).limit(limit)

        return list(cursor)

    # ──────────────────────────────────────────────────────────
    #  STATS
    # ──────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Return database statistics."""
        if not self._connected:
            return {"connected": False}

        return {
            "connected": True,
            "database": self._db_name,
            "eod_volumes_count": self._eod_volumes.count_documents({}),
            "alerts_count": self._alerts.count_documents({}),
            "eod_symbols": len(
                self._eod_volumes.distinct("symbol")
            ),
        }
