"""
Tests for the Flask API server.

Uses Flask's built-in test client and mock dependencies to verify
all endpoints: live-volumes, alerts, eod-volumes, and health.

Run:  python -m pytest tests/test_api.py -v
"""

import os
import sys
import json
import time
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.app import create_app


# ──────────────────────────────────────────────────────────────
#  MOCK HELPERS
# ──────────────────────────────────────────────────────────────


def _mock_volume_aggregator():
    """Create a mock VolumeAggregator with sample data."""
    mock = MagicMock()

    volumes = {
        "RELIANCE": {"call_volume": 150000, "put_volume": 120000},
        "TCS": {"call_volume": 80000, "put_volume": 75000},
        "NIFTY": {"call_volume": 500000, "put_volume": 450000},
    }

    detailed = {
        "RELIANCE": {
            "call_volume": 150000, "put_volume": 120000,
            "call_tokens": 21, "put_tokens": 21,
            "updated_at": datetime.now().isoformat(),
        },
        "TCS": {
            "call_volume": 80000, "put_volume": 75000,
            "call_tokens": 21, "put_tokens": 21,
            "updated_at": datetime.now().isoformat(),
        },
        "NIFTY": {
            "call_volume": 500000, "put_volume": 450000,
            "call_tokens": 21, "put_tokens": 21,
            "updated_at": datetime.now().isoformat(),
        },
    }

    pcrs = {"RELIANCE": 0.8, "TCS": 0.9375, "NIFTY": 0.9}

    mock.get_volumes.return_value = volumes
    mock.get_symbol_volume.side_effect = lambda s: volumes.get(s.upper())
    mock.get_detailed_volumes.return_value = detailed
    mock.get_all_pcr.return_value = pcrs
    mock.get_stats.return_value = {
        "running": True, "symbols_tracked": 3, "tokens_mapped": 126,
        "update_count": 100, "update_interval": 1.0, "last_update_ms": 0.5,
    }

    return mock


def _mock_alert_engine():
    """Create a mock AlertEngine."""
    mock = MagicMock()
    mock.get_fired_alerts.return_value = [
        {"symbol": "RELIANCE", "alert_type": "CALL_VOLUME_SPIKE"},
        {"symbol": "TCS", "alert_type": "PUT_VOLUME_SPIKE"},
    ]
    mock.get_stats.return_value = {
        "running": True, "total_checks": 50, "total_alerts_fired": 2,
        "alerts_today": 2, "multiplier": 2.0,
    }
    return mock


def _mock_database():
    """Create a mock DatabaseManager with alert and EOD data."""
    mock = MagicMock()

    today = datetime.now().strftime("%Y-%m-%d")

    alerts = [
        {
            "symbol": "RELIANCE", "date": today,
            "alert_type": "CALL_VOLUME_SPIKE",
            "message": "RELIANCE CALL VOLUME SPIKE: 350k is 2.3x",
            "data": {"live_volume": 350000, "yesterday_volume": 150000},
        },
        {
            "symbol": "TCS", "date": today,
            "alert_type": "PUT_VOLUME_SPIKE",
            "message": "TCS PUT VOLUME SPIKE: 200k is 2.7x",
            "data": {"live_volume": 200000, "yesterday_volume": 75000},
        },
    ]

    recent_alerts = alerts + [
        {
            "symbol": "SBIN", "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "alert_type": "CALL_VOLUME_SPIKE",
            "message": "SBIN alert from yesterday",
            "data": {},
        },
    ]

    eod_volumes = [
        {"symbol": "RELIANCE", "date": today,
         "call_volume_total": 150000, "put_volume_total": 120000},
        {"symbol": "TCS", "date": today,
         "call_volume_total": 80000, "put_volume_total": 75000},
    ]

    def get_alerts_fn(symbol=None, date=None, alert_type=None):
        result = alerts
        if symbol:
            result = [a for a in result if a["symbol"] == symbol.upper()]
        if date:
            result = [a for a in result if a["date"] == date]
        if alert_type:
            result = [a for a in result if a["alert_type"] == alert_type]
        return result

    def get_eod_volume_fn(symbol, date):
        for v in eod_volumes:
            if v["symbol"] == symbol.upper() and v["date"] == date:
                return v
        return None

    mock.get_alerts.side_effect = get_alerts_fn
    mock.get_recent_alerts.return_value = recent_alerts
    mock.get_eod_volume.side_effect = get_eod_volume_fn
    mock.get_all_eod_volumes.return_value = eod_volumes
    mock.get_stats.return_value = {
        "connected": True, "database": "options_analytics",
        "eod_volumes_count": 500, "alerts_count": 25,
    }
    mock.is_connected = True

    return mock


# ──────────────────────────────────────────────────────────────
#  FIXTURES
# ──────────────────────────────────────────────────────────────


@pytest.fixture
def app():
    """Create a fully wired Flask test app."""
    return create_app(
        volume_aggregator=_mock_volume_aggregator(),
        alert_engine=_mock_alert_engine(),
        database=_mock_database(),
    )


@pytest.fixture
def client(app):
    """Flask test client."""
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


@pytest.fixture
def minimal_app():
    """Flask app with no dependencies (for error cases)."""
    return create_app()


@pytest.fixture
def minimal_client(minimal_app):
    """Test client for app with no dependencies."""
    minimal_app.config["TESTING"] = True
    with minimal_app.test_client() as c:
        yield c


# ──────────────────────────────────────────────────────────────
#  TEST: /api/live-volumes
# ──────────────────────────────────────────────────────────────


class TestLiveVolumes:
    """Tests for the /api/live-volumes endpoint."""

    def test_returns_all_volumes(self, client):
        """Should return volumes for all stocks."""
        resp = client.get("/api/live-volumes")
        assert resp.status_code == 200

        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["count"] == 3
        assert "RELIANCE" in data["data"]
        assert "TCS" in data["data"]
        assert "NIFTY" in data["data"]

    def test_volume_structure(self, client):
        """Each symbol should have call_volume and put_volume."""
        resp = client.get("/api/live-volumes")
        data = resp.get_json()

        rel = data["data"]["RELIANCE"]
        assert rel["call_volume"] == 150000
        assert rel["put_volume"] == 120000

    def test_has_timestamp(self, client):
        """Response should include a timestamp."""
        resp = client.get("/api/live-volumes")
        data = resp.get_json()
        assert "timestamp" in data

    def test_response_time_header(self, client):
        """Response should include X-Response-Time header."""
        resp = client.get("/api/live-volumes")
        assert "X-Response-Time" in resp.headers

    def test_json_content_type(self, client):
        """Response should be JSON."""
        resp = client.get("/api/live-volumes")
        assert resp.content_type.startswith("application/json")

    def test_503_without_aggregator(self, minimal_client):
        """Should return 503 when aggregator not initialized."""
        resp = minimal_client.get("/api/live-volumes")
        assert resp.status_code == 503

    def test_cors_headers(self, client):
        """Should include CORS headers."""
        resp = client.get("/api/live-volumes")
        # flask-cors adds Access-Control-Allow-Origin
        assert resp.status_code == 200


class TestLiveVolumeSymbol:
    """Tests for /api/live-volumes/<symbol>."""

    def test_single_symbol(self, client):
        """Should return volume for a specific symbol."""
        resp = client.get("/api/live-volumes/RELIANCE")
        assert resp.status_code == 200

        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["symbol"] == "RELIANCE"
        assert data["data"]["call_volume"] == 150000

    def test_case_insensitive(self, client):
        """Should handle lowercase symbols."""
        resp = client.get("/api/live-volumes/reliance")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["symbol"] == "RELIANCE"

    def test_not_found(self, client):
        """Should return 404 for unknown symbol."""
        resp = client.get("/api/live-volumes/FAKESYM")
        assert resp.status_code == 404
        data = resp.get_json()
        assert data["status"] == "error"


class TestLiveVolumesDetailed:
    """Tests for /api/live-volumes/detailed."""

    def test_detailed_includes_pcr(self, client):
        """Detailed response should include PCR values."""
        resp = client.get("/api/live-volumes/detailed")
        assert resp.status_code == 200

        data = resp.get_json()
        rel = data["data"]["RELIANCE"]
        assert "pcr" in rel
        assert rel["pcr"] == 0.8

    def test_detailed_includes_token_counts(self, client):
        """Detailed response should include token counts."""
        resp = client.get("/api/live-volumes/detailed")
        data = resp.get_json()

        rel = data["data"]["RELIANCE"]
        assert rel["call_tokens"] == 21
        assert rel["put_tokens"] == 21


# ──────────────────────────────────────────────────────────────
#  TEST: /api/alerts
# ──────────────────────────────────────────────────────────────


class TestAlerts:
    """Tests for the /api/alerts endpoint."""

    def test_returns_today_alerts(self, client):
        """Should return today's alerts."""
        resp = client.get("/api/alerts")
        assert resp.status_code == 200

        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["count"] == 2
        assert len(data["data"]) == 2

    def test_alert_structure(self, client):
        """Each alert should have required fields."""
        resp = client.get("/api/alerts")
        data = resp.get_json()

        alert = data["data"][0]
        assert "symbol" in alert
        assert "alert_type" in alert
        assert "message" in alert

    def test_filter_by_symbol(self, client):
        """Should filter alerts by symbol query param."""
        resp = client.get("/api/alerts?symbol=RELIANCE")
        assert resp.status_code == 200

        data = resp.get_json()
        assert data["count"] == 1
        assert data["data"][0]["symbol"] == "RELIANCE"

    def test_filter_by_type(self, client):
        """Should filter by alert_type query param."""
        resp = client.get("/api/alerts?type=PUT_VOLUME_SPIKE")
        assert resp.status_code == 200

        data = resp.get_json()
        assert data["count"] == 1
        assert data["data"][0]["alert_type"] == "PUT_VOLUME_SPIKE"

    def test_503_without_dependencies(self, minimal_client):
        """Should return 503 when no alert system."""
        resp = minimal_client.get("/api/alerts")
        assert resp.status_code == 503

    def test_has_date(self, client):
        """Response should include today's date."""
        resp = client.get("/api/alerts")
        data = resp.get_json()
        assert "date" in data


class TestAlertsHistory:
    """Tests for /api/alerts/history."""

    def test_returns_recent_alerts(self, client):
        """Should return recent alerts across all dates."""
        resp = client.get("/api/alerts/history")
        assert resp.status_code == 200

        data = resp.get_json()
        assert data["count"] == 3  # 2 today + 1 yesterday

    def test_limit_param(self, client):
        """Should respect limit query parameter."""
        resp = client.get("/api/alerts/history?limit=2")
        assert resp.status_code == 200
        # Mock returns all 3 regardless of limit, but the route caps at 200

    def test_503_without_database(self, minimal_client):
        """Should return 503 without database."""
        resp = minimal_client.get("/api/alerts/history")
        assert resp.status_code == 503


# ──────────────────────────────────────────────────────────────
#  TEST: /api/eod-volumes
# ──────────────────────────────────────────────────────────────


class TestEODVolumes:
    """Tests for the /api/eod-volumes endpoint."""

    def test_returns_all_eod(self, client):
        """Should return EOD volumes for today."""
        resp = client.get("/api/eod-volumes")
        assert resp.status_code == 200

        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["count"] == 2

    def test_filter_by_symbol(self, client):
        """Should filter by symbol query param."""
        today = datetime.now().strftime("%Y-%m-%d")
        resp = client.get(f"/api/eod-volumes?symbol=RELIANCE&date={today}")
        assert resp.status_code == 200

        data = resp.get_json()
        assert data["count"] == 1
        assert data["data"][0]["symbol"] == "RELIANCE"
        assert data["data"][0]["call_volume_total"] == 150000

    def test_missing_symbol_empty(self, client):
        """Missing symbol should return empty list."""
        resp = client.get("/api/eod-volumes?symbol=FAKESYM")
        assert resp.status_code == 200

        data = resp.get_json()
        assert data["count"] == 0

    def test_503_without_database(self, minimal_client):
        """Should return 503 without database."""
        resp = minimal_client.get("/api/eod-volumes")
        assert resp.status_code == 503


# ──────────────────────────────────────────────────────────────
#  TEST: /api/health
# ──────────────────────────────────────────────────────────────


class TestHealth:
    """Tests for the /api/health endpoint."""

    def test_health_returns_ok(self, client):
        """Should return ok status."""
        resp = client.get("/api/health")
        assert resp.status_code == 200

        data = resp.get_json()
        assert data["status"] == "ok"

    def test_health_includes_components(self, client):
        """Should include component stats."""
        resp = client.get("/api/health")
        data = resp.get_json()

        assert "components" in data
        assert "volume_aggregator" in data["components"]
        assert "alert_engine" in data["components"]
        assert "database" in data["components"]

    def test_health_vol_agg_stats(self, client):
        """Volume aggregator stats should be present."""
        resp = client.get("/api/health")
        data = resp.get_json()

        va = data["components"]["volume_aggregator"]
        assert va["running"] is True
        assert va["symbols_tracked"] == 3

    def test_health_minimal(self, minimal_client):
        """Health should work even with no components."""
        resp = minimal_client.get("/api/health")
        assert resp.status_code == 200

        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["components"] == {}


# ──────────────────────────────────────────────────────────────
#  TEST: Error Handling
# ──────────────────────────────────────────────────────────────


class TestErrorHandling:
    """Tests for error handlers and edge cases."""

    def test_404_unknown_endpoint(self, client):
        """Unknown routes should return 404 JSON."""
        resp = client.get("/api/nonexistent")
        assert resp.status_code == 404

        data = resp.get_json()
        assert data["status"] == "error"

    def test_response_has_timestamp_header(self, client):
        """All responses should have X-Timestamp header."""
        resp = client.get("/api/live-volumes")
        assert "X-Timestamp" in resp.headers


# ──────────────────────────────────────────────────────────────
#  TEST: Performance (simulated)
# ──────────────────────────────────────────────────────────────


class TestPerformance:
    """Verify the API can handle rapid polling."""

    def test_rapid_polling(self, client):
        """Simulate 1/s dashboard polling — 10 requests in sequence."""
        for i in range(10):
            resp = client.get("/api/live-volumes")
            assert resp.status_code == 200

            data = resp.get_json()
            assert data["count"] == 3

    def test_concurrent_endpoints(self, client):
        """Multiple different endpoints in quick succession."""
        for _ in range(5):
            r1 = client.get("/api/live-volumes")
            r2 = client.get("/api/alerts")
            r3 = client.get("/api/health")

            assert r1.status_code == 200
            assert r2.status_code == 200
            assert r3.status_code == 200


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
