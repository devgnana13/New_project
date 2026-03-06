"""
Tests for the AlertEngine module.

Verifies volume spike detection, dedup logic (once per symbol per day),
MongoDB alert storage, yesterday's EOD caching, and the background
check loop — all using mock objects without real MongoDB or live data.

Run:  python -m pytest tests/test_alert_engine.py -v
"""

import os
import sys
import time
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.alert_engine import (
    AlertEngine,
    ALERT_TYPE_CALL,
    ALERT_TYPE_PUT,
    DEFAULT_MULTIPLIER,
)


# ──────────────────────────────────────────────────────────────
#  MOCK HELPERS
# ──────────────────────────────────────────────────────────────


def _make_mock_vol_agg(volumes: dict[str, dict]):
    """Create a mock VolumeAggregator with given volumes."""
    mock = MagicMock()
    mock.get_volumes.return_value = volumes
    mock.get_symbol_volume.side_effect = lambda s: volumes.get(s.upper())
    return mock


def _make_mock_db(yesterday_data: dict[str, dict]):
    """Create a mock DatabaseManager with yesterday's EOD data."""
    mock = MagicMock()

    def get_yesterday(symbol):
        return yesterday_data.get(symbol.upper())

    mock.get_yesterday_volume.side_effect = get_yesterday
    mock.store_alert.return_value = True
    return mock


# ──────────────────────────────────────────────────────────────
#  FIXTURES
# ──────────────────────────────────────────────────────────────


@pytest.fixture
def live_volumes():
    """Live volumes from VolumeAggregator."""
    return {
        "RELIANCE": {"call_volume": 350000, "put_volume": 100000},
        "TCS": {"call_volume": 80000, "put_volume": 200000},
        "SBIN": {"call_volume": 50000, "put_volume": 40000},
        "NIFTY": {"call_volume": 600000, "put_volume": 400000},
    }


@pytest.fixture
def yesterday_eod():
    """Yesterday's EOD volumes from MongoDB."""
    return {
        "RELIANCE": {
            "symbol": "RELIANCE",
            "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "call_volume_total": 150000,
            "put_volume_total": 120000,
        },
        "TCS": {
            "symbol": "TCS",
            "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "call_volume_total": 80000,
            "put_volume_total": 75000,
        },
        "SBIN": {
            "symbol": "SBIN",
            "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "call_volume_total": 60000,
            "put_volume_total": 55000,
        },
        "NIFTY": {
            "symbol": "NIFTY",
            "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "call_volume_total": 500000,
            "put_volume_total": 450000,
        },
    }


@pytest.fixture
def engine(live_volumes, yesterday_eod):
    """Create an AlertEngine with mock dependencies."""
    vol_agg = _make_mock_vol_agg(live_volumes)
    db = _make_mock_db(yesterday_eod)
    eng = AlertEngine(
        volume_aggregator=vol_agg,
        database=db,
        multiplier=2.0,
        check_interval=1.0,
    )
    yield eng
    eng.stop()


# ──────────────────────────────────────────────────────────────
#  TEST: Alert Detection
# ──────────────────────────────────────────────────────────────


class TestAlertDetection:
    """Tests for volume spike detection logic."""

    def test_call_spike_detected(self, engine):
        """RELIANCE live CE 350k >= 2× yesterday 150k → alert."""
        alerts = engine.check_now()
        call_alerts = [a for a in alerts if a["alert_type"] == ALERT_TYPE_CALL]

        reliance_call = [a for a in call_alerts if a["symbol"] == "RELIANCE"]
        assert len(reliance_call) == 1
        assert reliance_call[0]["live_volume"] == 350000
        assert reliance_call[0]["yesterday_volume"] == 150000

    def test_put_spike_detected(self, engine):
        """TCS live PE 200k >= 2× yesterday 75k → alert."""
        alerts = engine.check_now()
        put_alerts = [a for a in alerts if a["alert_type"] == ALERT_TYPE_PUT]

        tcs_put = [a for a in put_alerts if a["symbol"] == "TCS"]
        assert len(tcs_put) == 1
        assert tcs_put[0]["live_volume"] == 200000
        assert tcs_put[0]["yesterday_volume"] == 75000

    def test_no_alert_below_threshold(self, engine):
        """SBIN live CE 50k < 2× yesterday 60k → no alert."""
        alerts = engine.check_now()
        sbin_calls = [
            a for a in alerts
            if a["symbol"] == "SBIN" and a["alert_type"] == ALERT_TYPE_CALL
        ]
        assert len(sbin_calls) == 0

    def test_no_alert_below_threshold_put(self, engine):
        """SBIN live PE 40k < 2× yesterday 55k → no alert."""
        alerts = engine.check_now()
        sbin_puts = [
            a for a in alerts
            if a["symbol"] == "SBIN" and a["alert_type"] == ALERT_TYPE_PUT
        ]
        assert len(sbin_puts) == 0

    def test_exact_threshold_triggers(self):
        """Live volume == exactly 2× yesterday should trigger."""
        vol_agg = _make_mock_vol_agg({
            "AAA": {"call_volume": 200, "put_volume": 50},
        })
        db = _make_mock_db({
            "AAA": {"call_volume_total": 100, "put_volume_total": 50},
        })
        engine = AlertEngine(vol_agg, db, multiplier=2.0)
        alerts = engine.check_now()

        call_alerts = [a for a in alerts if a["alert_type"] == ALERT_TYPE_CALL]
        assert len(call_alerts) == 1
        assert call_alerts[0]["symbol"] == "AAA"

    def test_multiplier_calculation(self, engine):
        """Alert should include correct multiplier value."""
        alerts = engine.check_now()
        rel_call = [
            a for a in alerts
            if a["symbol"] == "RELIANCE" and a["alert_type"] == ALERT_TYPE_CALL
        ][0]
        # 350000 / 150000 = 2.333...
        assert rel_call["multiplier"] == 2.33

    def test_timestamp_is_set(self, engine):
        """Alert should have a timestamp."""
        alerts = engine.check_now()
        assert all("timestamp" in a for a in alerts)
        assert all(isinstance(a["timestamp"], datetime) for a in alerts)

    def test_custom_multiplier(self):
        """Should use custom multiplier threshold."""
        vol_agg = _make_mock_vol_agg({
            "X": {"call_volume": 450, "put_volume": 50},
        })
        db = _make_mock_db({
            "X": {"call_volume_total": 100, "put_volume_total": 100},
        })
        # With 3× multiplier, 450 >= 3×100=300 → triggers
        engine = AlertEngine(vol_agg, db, multiplier=3.0)
        alerts = engine.check_now()

        call_alerts = [a for a in alerts if a["alert_type"] == ALERT_TYPE_CALL]
        assert len(call_alerts) == 1

    def test_custom_multiplier_below(self):
        """Should NOT trigger when below custom multiplier."""
        vol_agg = _make_mock_vol_agg({
            "X": {"call_volume": 250, "put_volume": 50},
        })
        db = _make_mock_db({
            "X": {"call_volume_total": 100, "put_volume_total": 100},
        })
        # 250 < 3×100=300 → no alert
        engine = AlertEngine(vol_agg, db, multiplier=3.0)
        alerts = engine.check_now()

        call_alerts = [a for a in alerts if a["alert_type"] == ALERT_TYPE_CALL]
        assert len(call_alerts) == 0


# ──────────────────────────────────────────────────────────────
#  TEST: Dedup — Once Per Symbol Per Day
# ──────────────────────────────────────────────────────────────


class TestDedup:
    """Tests for the once-per-symbol-per-day dedup logic."""

    def test_alert_fires_only_once(self, engine):
        """Second check_now() should NOT re-fire same alert."""
        alerts1 = engine.check_now()
        alerts2 = engine.check_now()

        # First check should fire alerts
        assert len(alerts1) > 0
        # Second check should return empty
        assert len(alerts2) == 0

    def test_different_types_fire_independently(self):
        """CALL and PUT alerts for same symbol are independent."""
        vol_agg = _make_mock_vol_agg({
            "AAA": {"call_volume": 300, "put_volume": 300},
        })
        db = _make_mock_db({
            "AAA": {"call_volume_total": 100, "put_volume_total": 100},
        })
        engine = AlertEngine(vol_agg, db)
        alerts = engine.check_now()

        types = {a["alert_type"] for a in alerts}
        assert ALERT_TYPE_CALL in types
        assert ALERT_TYPE_PUT in types

    def test_get_fired_alerts(self, engine):
        """get_fired_alerts() should return today's fired alerts."""
        engine.check_now()
        fired = engine.get_fired_alerts()

        symbols_fired = {f["symbol"] for f in fired}
        assert "RELIANCE" in symbols_fired

    def test_reset_daily_clears_dedup(self, engine):
        """reset_daily() should allow alerts to fire again."""
        alerts1 = engine.check_now()
        assert len(alerts1) > 0

        engine.reset_daily()

        alerts2 = engine.check_now()
        assert len(alerts2) > 0
        assert len(alerts2) == len(alerts1)

    def test_different_symbols_independent(self, engine):
        """Firing for RELIANCE should not affect TCS."""
        alerts = engine.check_now()
        symbols = {a["symbol"] for a in alerts}
        # RELIANCE (call spike) and TCS (put spike) should both fire
        assert "RELIANCE" in symbols
        assert "TCS" in symbols


# ──────────────────────────────────────────────────────────────
#  TEST: MongoDB Storage
# ──────────────────────────────────────────────────────────────


class TestMongoStorage:
    """Tests for alert persistence to MongoDB."""

    def test_alert_stored_in_db(self, engine):
        """store_alert should be called for each triggered alert."""
        engine.check_now()
        engine._db.store_alert.assert_called()

    def test_store_alert_params(self, engine):
        """store_alert should receive correct parameters."""
        engine.check_now()

        # Find the RELIANCE CALL_VOLUME_SPIKE call
        calls = engine._db.store_alert.call_args_list
        rel_call = None
        for c in calls:
            kwargs = c.kwargs if c.kwargs else {}
            args = c.args if c.args else ()
            # store_alert uses keyword args
            if kwargs.get("symbol") == "RELIANCE" and \
               kwargs.get("alert_type") == ALERT_TYPE_CALL:
                rel_call = kwargs
                break

        assert rel_call is not None
        assert "message" in rel_call
        assert "data" in rel_call
        assert rel_call["data"]["live_volume"] == 350000
        assert rel_call["data"]["yesterday_volume"] == 150000

    def test_store_alert_date_is_today(self, engine):
        """Alert date should be today."""
        engine.check_now()
        today = datetime.now().strftime("%Y-%m-%d")

        calls = engine._db.store_alert.call_args_list
        for c in calls:
            kwargs = c.kwargs if c.kwargs else {}
            assert kwargs.get("date") == today


# ──────────────────────────────────────────────────────────────
#  TEST: Yesterday's EOD Cache
# ──────────────────────────────────────────────────────────────


class TestYesterdayCache:
    """Tests for yesterday's EOD volume caching."""

    def test_loads_once_per_day(self, engine):
        """Yesterday's data should be fetched from DB only once."""
        engine.check_now()
        call_count_1 = engine._db.get_yesterday_volume.call_count

        engine.reset_daily()
        engine._fired_today.clear()  # Reset dedup so check actually runs

        engine.check_now()
        call_count_2 = engine._db.get_yesterday_volume.call_count

        # First call fetches, second call after reset fetches again
        assert call_count_2 > call_count_1

    def test_cache_avoids_db_calls(self, engine):
        """Second check_now should use cached yesterday data."""
        engine.check_now()
        count_after_first = engine._db.get_yesterday_volume.call_count

        # Reset dedup only (not the yesterday cache)
        engine._fired_today.clear()
        engine.check_now()
        count_after_second = engine._db.get_yesterday_volume.call_count

        # No additional DB calls for yesterday volume
        assert count_after_second == count_after_first

    def test_no_yesterday_data_skips(self):
        """No yesterday data → no alerts fired."""
        vol_agg = _make_mock_vol_agg({
            "AAA": {"call_volume": 99999, "put_volume": 99999},
        })
        db = _make_mock_db({})  # No yesterday data
        engine = AlertEngine(vol_agg, db)
        alerts = engine.check_now()
        assert len(alerts) == 0


# ──────────────────────────────────────────────────────────────
#  TEST: Edge Cases
# ──────────────────────────────────────────────────────────────


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_yesterday_zero_volume_no_alert(self):
        """Zero yesterday volume should not trigger (avoid div by zero)."""
        vol_agg = _make_mock_vol_agg({
            "AAA": {"call_volume": 99999, "put_volume": 99999},
        })
        db = _make_mock_db({
            "AAA": {"call_volume_total": 0, "put_volume_total": 0},
        })
        engine = AlertEngine(vol_agg, db)
        alerts = engine.check_now()
        assert len(alerts) == 0

    def test_live_zero_volume_no_alert(self):
        """Zero live volume should not trigger."""
        vol_agg = _make_mock_vol_agg({
            "AAA": {"call_volume": 0, "put_volume": 0},
        })
        db = _make_mock_db({
            "AAA": {"call_volume_total": 100000, "put_volume_total": 100000},
        })
        engine = AlertEngine(vol_agg, db)
        alerts = engine.check_now()
        assert len(alerts) == 0

    def test_symbol_filter(self, live_volumes, yesterday_eod):
        """Should only check specified symbols."""
        vol_agg = _make_mock_vol_agg(live_volumes)
        db = _make_mock_db(yesterday_eod)
        engine = AlertEngine(vol_agg, db, symbols=["RELIANCE"])
        alerts = engine.check_now()

        symbols = {a["symbol"] for a in alerts}
        assert "RELIANCE" in symbols
        assert "TCS" not in symbols

    def test_missing_live_data_skips(self, yesterday_eod):
        """Symbol with no live data should be skipped."""
        vol_agg = _make_mock_vol_agg({})  # No live data
        db = _make_mock_db(yesterday_eod)
        engine = AlertEngine(vol_agg, db)
        alerts = engine.check_now()
        assert len(alerts) == 0

    def test_nifty_spike(self, engine):
        """NIFTY live CE 600k >= 2× yesterday 500k → alert."""
        alerts = engine.check_now()
        nifty_calls = [
            a for a in alerts
            if a["symbol"] == "NIFTY" and a["alert_type"] == ALERT_TYPE_CALL
        ]
        # 600000 >= 2 * 500000 = 1000000 → NOT a spike
        assert len(nifty_calls) == 0


# ──────────────────────────────────────────────────────────────
#  TEST: Callbacks
# ──────────────────────────────────────────────────────────────


class TestCallbacks:
    """Tests for the on_alert callback."""

    def test_callback_fires_per_alert(self, live_volumes, yesterday_eod):
        """on_alert should be called for each new alert."""
        received = []
        vol_agg = _make_mock_vol_agg(live_volumes)
        db = _make_mock_db(yesterday_eod)
        engine = AlertEngine(
            vol_agg, db,
            on_alert=lambda a: received.append(a),
        )
        alerts = engine.check_now()

        assert len(received) == len(alerts)
        assert all("symbol" in r for r in received)

    def test_callback_not_on_dedup(self, live_volumes, yesterday_eod):
        """Callback should NOT fire on second check (dedup)."""
        received = []
        vol_agg = _make_mock_vol_agg(live_volumes)
        db = _make_mock_db(yesterday_eod)
        engine = AlertEngine(
            vol_agg, db,
            on_alert=lambda a: received.append(a),
        )
        engine.check_now()
        count_after_first = len(received)

        engine.check_now()
        # No new callbacks
        assert len(received) == count_after_first


# ──────────────────────────────────────────────────────────────
#  TEST: Background Thread
# ──────────────────────────────────────────────────────────────


class TestBackgroundThread:
    """Tests for the background check loop."""

    def test_start_and_stop(self, engine):
        """Should start and stop cleanly."""
        engine.start()
        assert engine.is_running
        engine.stop()
        assert not engine.is_running

    def test_auto_check(self, live_volumes, yesterday_eod):
        """Background thread should run checks automatically."""
        vol_agg = _make_mock_vol_agg(live_volumes)
        db = _make_mock_db(yesterday_eod)
        engine = AlertEngine(vol_agg, db, check_interval=0.1)
        engine.start()

        time.sleep(0.35)
        engine.stop()

        assert engine._total_checks >= 1

    def test_stats(self, engine):
        """get_stats should return current state."""
        engine.check_now()
        stats = engine.get_stats()

        assert stats["multiplier"] == 2.0
        assert stats["total_checks"] == 1
        assert stats["total_alerts_fired"] >= 1
        assert stats["alerts_today"] >= 1
        assert stats["yesterday_symbols_cached"] >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
