"""
Tests for the DatabaseManager module.

Uses a mock MongoClient to verify EOD volume storage/retrieval,
alert management, and the get_yesterday_volume() function without
requiring a running MongoDB server.

Run:  python -m pytest tests/test_database.py -v
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import DatabaseManager


# ──────────────────────────────────────────────────────────────
#  MOCK MONGODB HELPER
# ──────────────────────────────────────────────────────────────


class MockCollection:
    """
    In-memory mock of a pymongo Collection.

    Supports:
        - insert_one / update_one (with upsert)
        - find_one / find (with projection and sort)
        - create_index
        - count_documents
        - distinct
    """

    def __init__(self):
        self._docs: list[dict] = []
        self._indexes: list[dict] = []

    def create_index(self, keys, **kwargs):
        self._indexes.append({"keys": keys, **kwargs})

    def insert_one(self, doc):
        self._docs.append(dict(doc))
        return MagicMock(inserted_id="mock_id")

    def update_one(self, filter_doc, update, upsert=False):
        # Find existing
        existing = None
        for i, doc in enumerate(self._docs):
            if all(doc.get(k) == v for k, v in filter_doc.items()):
                existing = i
                break

        if existing is not None:
            # Update existing
            if "$set" in update:
                self._docs[existing].update(update["$set"])
            return MagicMock(modified_count=1, upserted_id=None)
        elif upsert:
            # Insert new
            new_doc = dict(filter_doc)
            if "$set" in update:
                new_doc.update(update["$set"])
            self._docs.append(new_doc)
            return MagicMock(modified_count=0, upserted_id="mock_id")
        return MagicMock(modified_count=0, upserted_id=None)

    def find_one(self, filter_doc=None, projection=None):
        filter_doc = filter_doc or {}
        for doc in self._docs:
            if all(doc.get(k) == v for k, v in filter_doc.items()):
                return self._apply_projection(doc, projection)
        return None

    def find(self, filter_doc=None, projection=None):
        filter_doc = filter_doc or {}
        results = []
        for doc in self._docs:
            if self._matches(doc, filter_doc):
                results.append(self._apply_projection(doc, projection))
        return MockCursor(results)

    def count_documents(self, filter_doc=None):
        if not filter_doc:
            return len(self._docs)
        return sum(1 for d in self._docs if self._matches(d, filter_doc))

    def distinct(self, field):
        return list(set(d.get(field) for d in self._docs if field in d))

    def _matches(self, doc, filter_doc):
        for k, v in filter_doc.items():
            if isinstance(v, dict) and "$gte" in v:
                if doc.get(k, "") < v["$gte"]:
                    return False
            elif doc.get(k) != v:
                return False
        return True

    def _apply_projection(self, doc, projection):
        if not projection:
            return dict(doc)
        result = {}
        for key, include in projection.items():
            if key == "_id" and not include:
                continue
            if include and key in doc:
                result[key] = doc[key]
        # If projection only excludes _id, return all other fields
        if set(projection.keys()) == {"_id"} and projection["_id"] == 0:
            return {k: v for k, v in doc.items() if k != "_id"}
        return result


class MockCursor:
    """Mock pymongo cursor with sort and limit."""

    def __init__(self, docs):
        self._docs = docs

    def sort(self, key, direction=1):
        if isinstance(key, str):
            self._docs.sort(
                key=lambda d: d.get(key, ""),
                reverse=(direction == -1),
            )
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        return iter(self._docs)

    def __list__(self):
        return list(self._docs)


class MockDatabase:
    """Mock pymongo Database."""

    def __init__(self):
        self._collections = {}

    def __getitem__(self, name):
        if name not in self._collections:
            self._collections[name] = MockCollection()
        return self._collections[name]


class MockMongoClient:
    """Mock pymongo MongoClient."""

    def __init__(self, *args, **kwargs):
        self._dbs = {}
        self.admin = MagicMock()
        self.admin.command = MagicMock(return_value={"ok": 1})

    def __getitem__(self, name):
        if name not in self._dbs:
            self._dbs[name] = MockDatabase()
        return self._dbs[name]

    def close(self):
        pass


# ──────────────────────────────────────────────────────────────
#  FIXTURES
# ──────────────────────────────────────────────────────────────


@pytest.fixture
def mock_client():
    return MockMongoClient()


@pytest.fixture
def db(mock_client):
    """Create a DatabaseManager with injected mock client."""
    return DatabaseManager(
        db_name="test_options",
        client=mock_client,
    )


@pytest.fixture
def db_with_data(db):
    """Pre-populate with some EOD volume data."""
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

    db.store_single_eod("RELIANCE", 150000, 120000, date=yesterday)
    db.store_single_eod("RELIANCE", 140000, 130000, date=two_days_ago)
    db.store_single_eod("TCS", 80000, 75000, date=yesterday)
    db.store_single_eod("NIFTY", 500000, 450000, date=yesterday)
    db.store_single_eod("RELIANCE", 160000, 110000, date=today)

    return db


# ──────────────────────────────────────────────────────────────
#  TEST: Connection
# ──────────────────────────────────────────────────────────────


class TestConnection:
    """Tests for database connection management."""

    def test_injected_client_is_connected(self, db):
        """Injected client should be connected immediately."""
        assert db.is_connected

    def test_disconnect(self, db):
        """disconnect() should set connected to False."""
        db.disconnect()
        assert not db.is_connected

    def test_operations_fail_when_disconnected(self, db):
        """Operations should raise when not connected."""
        db.disconnect()
        with pytest.raises(RuntimeError, match="not connected"):
            db.get_yesterday_volume("RELIANCE")


# ──────────────────────────────────────────────────────────────
#  TEST: EOD Volume Storage
# ──────────────────────────────────────────────────────────────


class TestEODStorage:
    """Tests for storing EOD volume data."""

    def test_store_single_eod(self, db):
        """Should store a single symbol's EOD data."""
        result = db.store_single_eod(
            "RELIANCE", 150000, 120000, date="2026-03-05",
        )
        assert result is True

        vol = db.get_eod_volume("RELIANCE", "2026-03-05")
        assert vol is not None
        assert vol["call_volume_total"] == 150000
        assert vol["put_volume_total"] == 120000

    def test_store_bulk_eod(self, db):
        """Should store volumes for multiple symbols."""
        volumes = {
            "RELIANCE": {"call_volume": 150000, "put_volume": 120000},
            "TCS": {"call_volume": 80000, "put_volume": 75000},
            "SBIN": {"call_volume": 60000, "put_volume": 55000},
        }
        count = db.store_eod_volumes(volumes, date="2026-03-05")
        assert count == 3

    def test_upsert_overwrites(self, db):
        """Storing same symbol+date twice should update, not duplicate."""
        db.store_single_eod("RELIANCE", 100000, 90000, date="2026-03-05")
        db.store_single_eod("RELIANCE", 200000, 180000, date="2026-03-05")

        vol = db.get_eod_volume("RELIANCE", "2026-03-05")
        assert vol["call_volume_total"] == 200000
        assert vol["put_volume_total"] == 180000

    def test_different_dates_separate(self, db):
        """Same symbol on different dates should be stored separately."""
        db.store_single_eod("RELIANCE", 100000, 90000, date="2026-03-04")
        db.store_single_eod("RELIANCE", 200000, 180000, date="2026-03-05")

        vol1 = db.get_eod_volume("RELIANCE", "2026-03-04")
        vol2 = db.get_eod_volume("RELIANCE", "2026-03-05")
        assert vol1["call_volume_total"] == 100000
        assert vol2["call_volume_total"] == 200000

    def test_case_insensitive_symbol(self, db):
        """Should uppercase the symbol for consistency."""
        db.store_single_eod("reliance", 100000, 90000, date="2026-03-05")
        vol = db.get_eod_volume("Reliance", "2026-03-05")
        assert vol is not None
        assert vol["symbol"] == "RELIANCE"

    def test_default_date_is_today(self, db):
        """If no date provided, should use today."""
        db.store_single_eod("SBIN", 50000, 45000)
        today = datetime.now().strftime("%Y-%m-%d")
        vol = db.get_eod_volume("SBIN", today)
        assert vol is not None


# ──────────────────────────────────────────────────────────────
#  TEST: get_yesterday_volume
# ──────────────────────────────────────────────────────────────


class TestGetYesterdayVolume:
    """Tests for the get_yesterday_volume() function."""

    def test_basic_retrieval(self, db_with_data):
        """Should return yesterday's EOD volume."""
        result = db_with_data.get_yesterday_volume("RELIANCE")
        assert result is not None
        assert result["symbol"] == "RELIANCE"
        assert result["call_volume_total"] == 150000
        assert result["put_volume_total"] == 120000

    def test_correct_date(self, db_with_data):
        """Returned date should be yesterday."""
        result = db_with_data.get_yesterday_volume("RELIANCE")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        assert result["date"] == yesterday

    def test_different_symbols(self, db_with_data):
        """Should return correct data for different symbols."""
        tcs = db_with_data.get_yesterday_volume("TCS")
        assert tcs is not None
        assert tcs["call_volume_total"] == 80000

        nifty = db_with_data.get_yesterday_volume("NIFTY")
        assert nifty is not None
        assert nifty["call_volume_total"] == 500000

    def test_missing_symbol_returns_none(self, db_with_data):
        """Missing symbol should return None."""
        result = db_with_data.get_yesterday_volume("FAKESTOCK")
        assert result is None

    def test_case_insensitive(self, db_with_data):
        """Should handle case-insensitive lookups."""
        result = db_with_data.get_yesterday_volume("reliance")
        assert result is not None
        assert result["symbol"] == "RELIANCE"

    def test_no_id_field(self, db_with_data):
        """Returned dict should not contain MongoDB _id field."""
        result = db_with_data.get_yesterday_volume("RELIANCE")
        assert "_id" not in result


# ──────────────────────────────────────────────────────────────
#  TEST: EOD Volume Read
# ──────────────────────────────────────────────────────────────


class TestEODRead:
    """Tests for reading EOD volume data."""

    def test_get_eod_volume_specific_date(self, db_with_data):
        """Should return volume for a specific date."""
        two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        vol = db_with_data.get_eod_volume("RELIANCE", two_days_ago)
        assert vol is not None
        assert vol["call_volume_total"] == 140000

    def test_get_eod_volume_missing(self, db_with_data):
        """Should return None for missing date."""
        vol = db_with_data.get_eod_volume("RELIANCE", "2020-01-01")
        assert vol is None

    def test_get_volume_history(self, db_with_data):
        """Should return sorted history."""
        history = db_with_data.get_volume_history("RELIANCE", days=7)
        assert len(history) >= 2
        # Should be sorted by date ascending
        dates = [h["date"] for h in history]
        assert dates == sorted(dates)

    def test_get_all_eod_volumes(self, db_with_data):
        """Should return all symbols for a given date."""
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        vols = db_with_data.get_all_eod_volumes(yesterday)
        symbols = [v["symbol"] for v in vols]
        assert "RELIANCE" in symbols
        assert "TCS" in symbols
        assert "NIFTY" in symbols


# ──────────────────────────────────────────────────────────────
#  TEST: Alerts
# ──────────────────────────────────────────────────────────────


class TestAlerts:
    """Tests for the alerts collection."""

    def test_store_alert(self, db):
        """Should store an alert."""
        result = db.store_alert(
            symbol="RELIANCE",
            alert_type="volume_spike",
            message="CE volume surged 3x in 5 minutes",
            data={"ce_volume": 500000, "threshold": 150000},
            date="2026-03-05",
        )
        assert result is True

    def test_get_alerts_by_symbol(self, db):
        """Should filter alerts by symbol."""
        db.store_alert("RELIANCE", "volume_spike", "msg1", date="2026-03-05")
        db.store_alert("TCS", "pcr_extreme", "msg2", date="2026-03-05")

        rel_alerts = db.get_alerts(symbol="RELIANCE")
        assert len(rel_alerts) == 1
        assert rel_alerts[0]["symbol"] == "RELIANCE"

    def test_get_alerts_by_date(self, db):
        """Should filter alerts by date."""
        db.store_alert("RELIANCE", "vol1", "msg1", date="2026-03-04")
        db.store_alert("RELIANCE", "vol2", "msg2", date="2026-03-05")

        alerts = db.get_alerts(date="2026-03-05")
        assert len(alerts) == 1
        assert alerts[0]["date"] == "2026-03-05"

    def test_get_alerts_by_type(self, db):
        """Should filter by alert type."""
        db.store_alert("RELIANCE", "volume_spike", "a", date="2026-03-05")
        db.store_alert("RELIANCE", "pcr_extreme", "b", date="2026-03-05")

        alerts = db.get_alerts(
            symbol="RELIANCE", alert_type="pcr_extreme",
        )
        assert len(alerts) == 1
        assert alerts[0]["alert_type"] == "pcr_extreme"

    def test_alert_upsert(self, db):
        """Same symbol+date+type should update, not duplicate."""
        db.store_alert("RELIANCE", "volume_spike", "old msg", date="2026-03-05")
        db.store_alert("RELIANCE", "volume_spike", "new msg", date="2026-03-05")

        alerts = db.get_alerts(symbol="RELIANCE", date="2026-03-05")
        assert len(alerts) == 1
        assert alerts[0]["message"] == "new msg"

    def test_get_recent_alerts(self, db):
        """Should return most recent alerts."""
        for i in range(5):
            db.store_alert(
                f"SYM{i}", "test", f"msg{i}",
                date=f"2026-03-0{i + 1}",
            )

        recent = db.get_recent_alerts(limit=3)
        assert len(recent) == 3

    def test_alert_data_stored(self, db):
        """Alert data dict should be preserved."""
        db.store_alert(
            "RELIANCE", "volume_spike", "msg",
            data={"volume": 500000, "change_pct": 250.0},
            date="2026-03-05",
        )
        alerts = db.get_alerts(symbol="RELIANCE")
        assert alerts[0]["data"]["volume"] == 500000
        assert alerts[0]["data"]["change_pct"] == 250.0


# ──────────────────────────────────────────────────────────────
#  TEST: Stats
# ──────────────────────────────────────────────────────────────


class TestStats:
    """Tests for database statistics."""

    def test_stats_connected(self, db_with_data):
        """Stats should show connected status and counts."""
        stats = db_with_data.get_stats()
        assert stats["connected"] is True
        assert stats["database"] == "test_options"
        assert stats["eod_volumes_count"] >= 4
        assert stats["eod_symbols"] >= 2

    def test_stats_disconnected(self, db):
        """Stats should show disconnected when not connected."""
        db.disconnect()
        stats = db.get_stats()
        assert stats["connected"] is False


# ──────────────────────────────────────────────────────────────
#  TEST: Integration with VolumeAggregator output
# ──────────────────────────────────────────────────────────────


class TestIntegration:
    """Test storing the exact output format from VolumeAggregator."""

    def test_store_volume_aggregator_output(self, db):
        """Should handle the exact dict format from VolumeAggregator."""
        # This is the exact format from VolumeAggregator.get_volumes()
        volumes = {
            "RELIANCE": {"call_volume": 150000, "put_volume": 120000},
            "TCS": {"call_volume": 80000, "put_volume": 75000},
            "NIFTY": {"call_volume": 500000, "put_volume": 450000},
            "SBIN": {"call_volume": 60000, "put_volume": 55000},
            "SENSEX": {"call_volume": 200000, "put_volume": 190000},
        }

        count = db.store_eod_volumes(volumes, date="2026-03-05")
        assert count == 5

        # Verify each
        for sym, vd in volumes.items():
            stored = db.get_eod_volume(sym, "2026-03-05")
            assert stored is not None
            assert stored["call_volume_total"] == vd["call_volume"]
            assert stored["put_volume_total"] == vd["put_volume"]

    def test_end_of_day_workflow(self, db):
        """Simulate complete EOD workflow."""
        # 1. Market closes — store today's volumes
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Store yesterday's data (simulating previous run)
        db.store_eod_volumes(
            {"RELIANCE": {"call_volume": 140000, "put_volume": 130000}},
            date=yesterday,
        )

        # Store today's data
        db.store_eod_volumes(
            {"RELIANCE": {"call_volume": 160000, "put_volume": 110000}},
            date=today,
        )

        # 2. Next day — retrieve yesterday's volume
        # (Shift perspective: "today" becomes "yesterday")
        yd = db.get_eod_volume("RELIANCE", today)
        assert yd is not None
        assert yd["call_volume_total"] == 160000
        assert yd["put_volume_total"] == 110000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
