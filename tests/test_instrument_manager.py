"""
Tests for the InstrumentManager module.

These tests use mock data to verify the instrument manager's caching,
filtering, indexing, lookup, and batching functionality without needing
actual Kite Connect API credentials.

Run:  python -m pytest tests/test_instrument_manager.py -v
"""

import os
import json
import time
import shutil
import tempfile
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

# ── Adjust path for imports ──
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.instrument_manager import InstrumentManager
from core.constants import DEFAULT_STRIKE_INTERVAL


# ──────────────────────────────────────────────────────────────
#  TEST FIXTURES
# ──────────────────────────────────────────────────────────────


def _make_instrument(
    symbol, strike, opt_type, expiry="2026-03-26", token=None, exchange="NFO"
):
    """Create a mock instrument dict matching Kite API format."""
    if token is None:
        # Generate a deterministic token from attributes
        token = hash((symbol, strike, opt_type, expiry)) % 10_000_000

    return {
        "instrument_token": abs(token),
        "exchange_token": abs(token) // 100,
        "tradingsymbol": f"{symbol}26MAR{int(strike)}{opt_type}",
        "name": symbol,
        "last_price": 0.0,
        "tick_size": 0.05,
        "lot_size": 25,
        "instrument_type": opt_type,
        "segment": "NFO-OPT",
        "exchange": exchange,
        "expiry": expiry,
        "strike": float(strike),
    }


def _make_instrument_set(symbol, atm, interval, num_strikes=15, expiry="2026-03-26"):
    """
    Create a full set of instruments for a symbol centered around ATM.
    Generates num_strikes strikes above and below ATM.
    """
    instruments = []
    for i in range(-num_strikes, num_strikes + 1):
        strike = atm + (i * interval)
        for opt_type in ("CE", "PE"):
            instruments.append(
                _make_instrument(symbol, strike, opt_type, expiry)
            )
    return instruments


def _make_mock_dump():
    """
    Create a full mock instruments dump with multiple stocks and some
    non-option instruments (futures, equity) that should be filtered out.
    """
    instruments = []

    # RELIANCE options: ATM 2850, interval 20, 15 strikes each side
    instruments.extend(
        _make_instrument_set("RELIANCE", 2850, 20, num_strikes=15)
    )

    # TCS options: ATM 3500, interval 25
    instruments.extend(
        _make_instrument_set("TCS", 3500, 25, num_strikes=15)
    )

    # INFY options: ATM 1500, interval 25
    instruments.extend(
        _make_instrument_set("INFY", 1500, 25, num_strikes=15)
    )

    # Second expiry for RELIANCE
    instruments.extend(
        _make_instrument_set("RELIANCE", 2850, 20, num_strikes=15, expiry="2026-04-30")
    )

    # ── Non-option instruments (should be filtered out) ──
    instruments.append({
        "instrument_token": 9999999,
        "exchange_token": 99999,
        "tradingsymbol": "RELIANCE26MARFUT",
        "name": "RELIANCE",
        "last_price": 2850.0,
        "tick_size": 0.05,
        "lot_size": 250,
        "instrument_type": "FUT",
        "segment": "NFO-FUT",
        "exchange": "NFO",
        "expiry": "2026-03-26",
        "strike": 0.0,
    })
    instruments.append({
        "instrument_token": 8888888,
        "exchange_token": 88888,
        "tradingsymbol": "RELIANCE",
        "name": "RELIANCE",
        "last_price": 2850.0,
        "tick_size": 0.05,
        "lot_size": 1,
        "instrument_type": "EQ",
        "segment": "NSE",
        "exchange": "NSE",
        "expiry": "",
        "strike": 0.0,
    })

    return instruments


@pytest.fixture
def temp_cache_dir():
    """Create and cleanup a temp directory for cache files."""
    tmpdir = tempfile.mkdtemp(prefix="test_instruments_")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def mock_kite():
    """Patch KiteConnect so no real API calls are made."""
    with patch("core.instrument_manager.KiteConnect") as MockKite:
        instance = MagicMock()
        instance.instruments.return_value = _make_mock_dump()
        MockKite.return_value = instance
        yield instance


@pytest.fixture
def manager(mock_kite, temp_cache_dir):
    """Create an InstrumentManager with mocked API and temp cache."""
    # Patch STOCK_SYMBOLS to only include test symbols (no SENSEX = no BFO download)
    # and DERIVATIVE_EXCHANGE_MAP to empty so only NFO is downloaded once.
    with patch("core.instrument_manager.STOCK_SYMBOLS", ["RELIANCE", "TCS", "INFY"]), \
         patch("core.instrument_manager.DERIVATIVE_EXCHANGE_MAP", {}):
        mgr = InstrumentManager(
            api_key="test_key",
            access_token="test_token",
            cache_dir=temp_cache_dir,
        )
        yield mgr


# ──────────────────────────────────────────────────────────────
#  TEST: LOADING AND CACHING
# ──────────────────────────────────────────────────────────────


class TestLoadAndCache:
    """Tests for instrument loading, caching, and refresh logic."""

    def test_load_from_api_on_first_call(self, manager, mock_kite):
        """First load should download from API (no cache exists)."""
        count = manager.load_instruments()
        assert count > 0
        # Should have been called with at least NFO
        mock_kite.instruments.assert_any_call("NFO")

    def test_cache_file_created(self, manager, temp_cache_dir):
        """Cache file should be created after first load."""
        manager.load_instruments()
        cache_path = os.path.join(temp_cache_dir, "instruments_nfo.json")
        assert os.path.exists(cache_path)

    def test_load_from_cache_on_second_call(self, manager, mock_kite):
        """Second load should use cache (no API call)."""
        manager.load_instruments()
        mock_kite.instruments.reset_mock()

        # Second load — should use cache
        manager.load_instruments()
        mock_kite.instruments.assert_not_called()

    def test_force_refresh_ignores_cache(self, manager, mock_kite):
        """force_refresh=True should re-download even with valid cache."""
        manager.load_instruments()
        mock_kite.instruments.reset_mock()

        manager.load_instruments(force_refresh=True)
        mock_kite.instruments.assert_any_call("NFO")

    def test_expired_cache_triggers_redownload(self, manager, mock_kite, temp_cache_dir):
        """Expired cache should trigger a fresh API download."""
        manager.load_instruments()
        mock_kite.instruments.reset_mock()

        # Simulate expired cache by backdating the file modification time
        cache_path = os.path.join(temp_cache_dir, "instruments_nfo.json")
        old_time = time.time() - (20 * 3600)  # 20 hours ago
        os.utime(cache_path, (old_time, old_time))

        manager.load_instruments()
        mock_kite.instruments.assert_any_call("NFO")

    def test_only_options_loaded(self, manager):
        """Only NFO-OPT instruments (CE/PE) should be loaded, not FUT/EQ."""
        count = manager.load_instruments()
        symbols = manager.get_all_symbols()
        # Should have RELIANCE, TCS, INFY — not futures or equity
        assert "RELIANCE" in symbols
        assert "TCS" in symbols
        assert "INFY" in symbols
        # Each stock has 31 strikes × 2 sides = 62 options
        # RELIANCE has two expiries so 124, TCS has 62, INFY has 62 = 248
        assert count == 248


# ──────────────────────────────────────────────────────────────
#  TEST: LOOKUP OPERATIONS
# ──────────────────────────────────────────────────────────────


class TestLookup:
    """Tests for instrument lookup by symbol, strike, expiry, option type."""

    def test_exact_lookup(self, manager):
        """Look up a specific instrument by all attributes."""
        manager.load_instruments()
        inst = manager.get_instrument("RELIANCE", "2026-03-26", 2850.0, "CE")
        assert inst is not None
        assert inst["name"] == "RELIANCE"
        assert inst["strike"] == 2850.0
        assert inst["instrument_type"] == "CE"

    def test_lookup_returns_none_for_missing(self, manager):
        """Lookup should return None for non-existent instruments."""
        manager.load_instruments()
        inst = manager.get_instrument("RELIANCE", "2026-03-26", 9999.0, "CE")
        assert inst is None

    def test_get_instrument_token(self, manager):
        """get_instrument_token should return just the token integer."""
        manager.load_instruments()
        token = manager.get_instrument_token("RELIANCE", "2026-03-26", 2850.0, "CE")
        assert isinstance(token, int)
        assert token > 0

    def test_get_instrument_token_missing_returns_none(self, manager):
        """Missing instrument should return None, not raise."""
        manager.load_instruments()
        token = manager.get_instrument_token("FAKESTOCK", "2026-03-26", 100.0, "CE")
        assert token is None

    def test_case_insensitive_symbol(self, manager):
        """Lookups should be case-insensitive for the symbol."""
        manager.load_instruments()
        upper = manager.get_instrument("RELIANCE", "2026-03-26", 2850.0, "CE")
        lower = manager.get_instrument("reliance", "2026-03-26", 2850.0, "CE")
        mixed = manager.get_instrument("Reliance", "2026-03-26", 2850.0, "ce")
        assert upper == lower == mixed

    def test_get_expiries(self, manager):
        """Should return sorted list of expiries for a symbol."""
        manager.load_instruments()
        expiries = manager.get_expiries("RELIANCE")
        assert len(expiries) == 2
        assert expiries == ["2026-03-26", "2026-04-30"]

    def test_get_nearest_expiry(self, manager):
        """Should return the nearest non-expired expiry."""
        manager.load_instruments()
        nearest = manager.get_nearest_expiry("RELIANCE")
        assert nearest == "2026-03-26"

    def test_get_strikes(self, manager):
        """Should return sorted strike list for (symbol, expiry)."""
        manager.load_instruments()
        strikes = manager.get_strikes("RELIANCE", "2026-03-26")
        assert len(strikes) == 31  # -15 to +15 strikes
        assert strikes == sorted(strikes)
        assert 2850.0 in strikes  # ATM


# ──────────────────────────────────────────────────────────────
#  TEST: ATM & STRIKE RESOLUTION
# ──────────────────────────────────────────────────────────────


class TestATMResolution:
    """Tests for ATM calculation and strike token resolution."""

    def test_atm_calculation_exact(self, manager):
        """Spot price exactly on a strike should return that strike."""
        manager.load_instruments()
        # RELIANCE interval = 20, exact multiple → no rounding needed
        atm = manager.get_atm_strike("RELIANCE", 2840.0)
        assert atm == 2840.0  # 2840 / 20 = 142 → 142 * 20 = 2840

    def test_atm_calculation_round_up(self, manager):
        """Spot price closer to next strike should round up."""
        manager.load_instruments()
        atm = manager.get_atm_strike("RELIANCE", 2865.0)
        assert atm == 2860.0  # 2865 / 20 = 143.25 → round = 143 → 143 * 20 = 2860

    def test_atm_calculation_round_down(self, manager):
        """Spot price closer to previous strike should round down."""
        manager.load_instruments()
        atm = manager.get_atm_strike("RELIANCE", 2834.0)
        assert atm == 2840.0  # 2834 / 20 = 141.7 → round = 142 → 142 * 20 = 2840

    def test_atm_uses_stock_specific_interval(self, manager):
        """ATM calculation should use the stock's specific interval."""
        manager.load_instruments()
        # NIFTY has interval 50
        atm = manager.get_atm_strike("NIFTY", 24523.0)
        assert atm % 50 == 0

        # MRF has interval 100
        atm = manager.get_atm_strike("MRF", 123456.0)
        assert atm % 100 == 0

        # BANKNIFTY has interval 100
        atm = manager.get_atm_strike("BANKNIFTY", 51234.0)
        assert atm % 100 == 0

    def test_get_strike_tokens_count(self, manager):
        """ATM ± 10 should yield 21 strikes × 2 (CE+PE) = 42 tokens."""
        manager.load_instruments()
        # Use TCS (interval=25) which has 15 strikes each side → range 10 fits
        tokens = manager.get_strike_tokens("TCS", spot_price=3500.0, strike_range=10)
        assert len(tokens) == 42

    def test_get_strike_tokens_structure(self, manager):
        """Each returned token should have the expected keys."""
        manager.load_instruments()
        tokens = manager.get_strike_tokens("TCS", spot_price=3500.0, strike_range=5)
        for t in tokens:
            assert "instrument_token" in t
            assert "tradingsymbol" in t
            assert "strike" in t
            assert "option_type" in t
            assert "expiry" in t
            assert "symbol" in t
            assert t["option_type"] in ("CE", "PE")
            assert t["symbol"] == "TCS"

    def test_get_strike_tokens_covers_range(self, manager):
        """Returned strikes should cover ATM - range to ATM + range."""
        manager.load_instruments()
        tokens = manager.get_strike_tokens("TCS", spot_price=3500.0, strike_range=5)
        strikes = sorted(set(t["strike"] for t in tokens))
        expected = [3500.0 + (i * 25) for i in range(-5, 6)]
        assert strikes == expected

    def test_get_strike_tokens_respects_expiry(self, manager):
        """Specific expiry should be used when provided."""
        manager.load_instruments()
        tokens = manager.get_strike_tokens(
            "RELIANCE", spot_price=2850.0, strike_range=5, expiry="2026-04-30"
        )
        for t in tokens:
            assert t["expiry"] == "2026-04-30"

    def test_raises_on_invalid_symbol(self, manager):
        """Should raise ValueError for a symbol with no instruments."""
        manager.load_instruments()
        with pytest.raises(ValueError, match="No expiry found"):
            manager.get_strike_tokens("FAKESTOCK", spot_price=100.0)

    def test_raises_before_load(self, manager):
        """Should raise RuntimeError if load_instruments() not called."""
        with pytest.raises(RuntimeError, match="not loaded"):
            manager.get_strike_tokens("RELIANCE", spot_price=2850.0)


# ──────────────────────────────────────────────────────────────
#  TEST: BULK RESOLUTION & BATCHING
# ──────────────────────────────────────────────────────────────


class TestBulkAndBatching:
    """Tests for resolve_all_stocks() and partition_into_batches()."""

    def test_resolve_all_stocks(self, manager):
        """Should resolve instruments for all stocks with spot prices."""
        manager.load_instruments()
        spot_prices = {
            "RELIANCE": 2850.0,
            "TCS": 3500.0,
            "INFY": 1500.0,
        }
        result = manager.resolve_all_stocks(spot_prices, strike_range=5)

        assert "RELIANCE" in result
        assert "TCS" in result
        assert "INFY" in result
        # TCS: 11 strikes × 2 = 22
        assert len(result["TCS"]) == 22

    def test_resolve_skips_missing_spot_prices(self, manager):
        """Stocks without spot prices should be skipped with a warning."""
        manager.load_instruments()
        spot_prices = {"RELIANCE": 2850.0}  # No TCS or INFY
        result = manager.resolve_all_stocks(
            spot_prices, strike_range=5, stock_list=["RELIANCE", "TCS"]
        )
        assert "RELIANCE" in result
        assert "TCS" not in result

    def test_partition_into_batches_respects_limit(self, manager):
        """Each batch should not exceed max_per_batch."""
        manager.load_instruments()
        spot_prices = {
            "RELIANCE": 2850.0,
            "TCS": 3500.0,
            "INFY": 1500.0,
        }
        resolved = manager.resolve_all_stocks(spot_prices, strike_range=10)
        batches = manager.partition_into_batches(resolved, max_per_batch=50)

        for batch_name, tokens in batches.items():
            assert len(tokens) <= 50, (
                f"{batch_name} has {len(tokens)} tokens, exceeds limit of 50"
            )

    def test_partition_total_equals_resolved(self, manager):
        """Total tokens across all batches should equal total resolved."""
        manager.load_instruments()
        spot_prices = {
            "RELIANCE": 2850.0,
            "TCS": 3500.0,
            "INFY": 1500.0,
        }
        resolved = manager.resolve_all_stocks(spot_prices, strike_range=10)
        batches = manager.partition_into_batches(resolved, max_per_batch=100)

        total_resolved = sum(len(v) for v in resolved.values())
        total_batched = sum(len(v) for v in batches.values())
        assert total_batched == total_resolved

    def test_partition_batch_names(self, manager):
        """Batch keys should follow 'worker_N' naming."""
        manager.load_instruments()
        spot_prices = {"RELIANCE": 2850.0, "TCS": 3500.0}
        resolved = manager.resolve_all_stocks(spot_prices, strike_range=10)
        batches = manager.partition_into_batches(resolved, max_per_batch=50)

        for key in batches:
            assert key.startswith("worker_")


# ──────────────────────────────────────────────────────────────
#  TEST: STATS & UTILITY
# ──────────────────────────────────────────────────────────────


class TestStats:
    """Tests for utility methods."""

    def test_get_stats_unloaded(self, manager):
        """Stats should show loaded=False before loading."""
        stats = manager.get_stats()
        assert stats["loaded"] is False
        assert stats["total_instruments"] == 0

    def test_get_stats_loaded(self, manager):
        """Stats should update after loading."""
        manager.load_instruments()
        stats = manager.get_stats()
        assert stats["loaded"] is True
        assert stats["total_instruments"] > 0
        assert stats["unique_symbols"] == 3
        assert stats["load_timestamp"] is not None

    def test_get_all_symbols(self, manager):
        """Should return sorted unique symbols."""
        manager.load_instruments()
        symbols = manager.get_all_symbols()
        assert symbols == sorted(symbols)
        assert "RELIANCE" in symbols


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
