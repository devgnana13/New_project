"""
Tests for the ATMResolver module.

Tests use mock data to verify LTP fetching, ATM calculation, token
resolution, caching, and worker batching without needing real API keys.

Run:  python -m pytest tests/test_atm_resolver.py -v
"""

import os
import sys
import time
import tempfile
import shutil
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.atm_resolver import ATMResolver, ATMResolution
from core.instrument_manager import InstrumentManager


# ──────────────────────────────────────────────────────────────
#  TEST HELPERS
# ──────────────────────────────────────────────────────────────


def _make_instrument(
    symbol, strike, opt_type, expiry="2026-03-26", token=None, exchange="NFO"
):
    """Create a mock instrument dict matching Kite API format."""
    if token is None:
        token = abs(hash((symbol, strike, opt_type, expiry))) % 10_000_000
    return {
        "instrument_token": token,
        "exchange_token": token // 100,
        "tradingsymbol": f"{symbol}26MAR{int(strike)}{opt_type}",
        "name": symbol,
        "last_price": 0.0,
        "tick_size": 0.05,
        "lot_size": 25,
        "instrument_type": opt_type,
        "segment": f"{exchange}-OPT",
        "exchange": exchange,
        "expiry": expiry,
        "strike": float(strike),
    }


def _make_instrument_set(symbol, center, interval, num_strikes=20, expiry="2026-03-26", exchange="NFO"):
    """
    Generate instruments at absolute multiples of the interval, 
    centered around `center`. This mirrors how real exchanges list
    strikes (e.g., 2800, 2820, 2840... for interval=20).
    """
    instruments = []
    # Round center to nearest multiple of interval
    base = round(center / interval) * interval
    for i in range(-num_strikes, num_strikes + 1):
        strike = base + (i * interval)
        for opt_type in ("CE", "PE"):
            instruments.append(
                _make_instrument(symbol, strike, opt_type, expiry, exchange=exchange)
            )
    return instruments


def _make_full_dump():
    """Create a mock instruments dump with multiple stocks and an index."""
    instruments = []
    # Use 20 strikes each side to ensure ATM ± 10 always fits
    instruments.extend(_make_instrument_set("RELIANCE", 2860, 20, num_strikes=20))
    instruments.extend(_make_instrument_set("TCS", 3500, 25, num_strikes=20))
    instruments.extend(_make_instrument_set("NIFTY", 24500, 50, num_strikes=20))
    instruments.extend(_make_instrument_set("SBIN", 750, 10, num_strikes=20))
    instruments.extend(
        _make_instrument_set("SENSEX", 80000, 100, num_strikes=20, exchange="BFO")
    )

    # Add some non-option instruments that should be filtered
    instruments.append({
        "instrument_token": 9999999, "exchange_token": 99999,
        "tradingsymbol": "RELIANCE26MARFUT", "name": "RELIANCE",
        "last_price": 2850.0, "tick_size": 0.05, "lot_size": 250,
        "instrument_type": "FUT", "segment": "NFO-FUT",
        "exchange": "NFO", "expiry": "2026-03-26", "strike": 0.0,
    })
    return instruments


def _make_mock_quotes():
    """Mock quote API response matching Kite format."""
    return {
        "NSE:RELIANCE": {"last_price": 2853.50, "ohlc": {}, "depth": {}},
        "NSE:TCS": {"last_price": 3512.75, "ohlc": {}, "depth": {}},
        "NSE:NIFTY 50": {"last_price": 24523.40, "ohlc": {}, "depth": {}},
        "NSE:SBIN": {"last_price": 748.60, "ohlc": {}, "depth": {}},
        "BSE:SENSEX": {"last_price": 80125.30, "ohlc": {}, "depth": {}},
    }


# ──────────────────────────────────────────────────────────────
#  FIXTURES
# ──────────────────────────────────────────────────────────────


@pytest.fixture
def temp_cache_dir():
    tmpdir = tempfile.mkdtemp(prefix="test_atm_")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def mock_kite():
    with patch("core.instrument_manager.KiteConnect") as MockKite:
        instance = MagicMock()

        # Build exchange-specific dumps
        full_dump = _make_full_dump()
        nfo_instruments = [i for i in full_dump if i.get("exchange") != "BFO"]
        bfo_instruments = [i for i in full_dump if i.get("exchange") == "BFO"]

        def instruments_side_effect(exchange):
            if exchange == "BFO":
                return bfo_instruments
            return nfo_instruments  # NFO

        instance.instruments.side_effect = instruments_side_effect
        instance.quote.return_value = _make_mock_quotes()
        MockKite.return_value = instance
        yield instance


@pytest.fixture
def ins_mgr(mock_kite, temp_cache_dir):
    test_symbols = ["RELIANCE", "TCS", "NIFTY", "SBIN", "SENSEX"]
    with patch("core.instrument_manager.STOCK_SYMBOLS", test_symbols), \
         patch("core.instrument_manager.DERIVATIVE_EXCHANGE_MAP", {"SENSEX": "BFO"}):
        mgr = InstrumentManager(
            api_key="test", access_token="test", cache_dir=temp_cache_dir,
        )
        mgr.load_instruments()
        yield mgr


@pytest.fixture
def resolver(ins_mgr, mock_kite):
    with patch("core.atm_resolver.STOCK_SYMBOLS", ["RELIANCE", "TCS", "NIFTY", "SBIN", "SENSEX"]), \
         patch("core.atm_resolver.QUOTE_EXCHANGE_MAP", {
            "NIFTY": "NSE:NIFTY 50",
            "SENSEX": "BSE:SENSEX",
         }), \
         patch("core.atm_resolver.INDEX_SYMBOLS", {"NIFTY", "SENSEX"}):
        r = ATMResolver(
            instrument_manager=ins_mgr,
            kite=mock_kite,
            symbols=["RELIANCE", "TCS", "NIFTY", "SBIN", "SENSEX"],
            strike_range=10,
            update_interval=30,
        )
        yield r


# ──────────────────────────────────────────────────────────────
#  TEST: LTP FETCHING
# ──────────────────────────────────────────────────────────────


class TestLTPFetching:
    """Tests for the LTP fetch mechanism."""

    def test_fetches_all_symbols(self, resolver, mock_kite):
        """Should fetch LTP for all configured symbols."""
        result = resolver.resolve_all()
        assert len(result) == 5  # RELIANCE, TCS, NIFTY, SBIN, SENSEX

    def test_spot_prices_populated(self, resolver):
        """Spot prices should be populated after resolve."""
        resolver.resolve_all()
        prices = resolver.get_spot_prices()
        assert prices["RELIANCE"] == 2853.50
        assert prices["TCS"] == 3512.75
        assert prices["SBIN"] == 748.60

    def test_index_uses_special_quote_key(self, resolver, mock_kite):
        """Indices should use exchange-prefixed keys for the quote API."""
        resolver.resolve_all()
        # Verify the mock was called with the right keys
        call_args = mock_kite.quote.call_args
        quote_keys = call_args[0][0]
        assert "NSE:NIFTY 50" in quote_keys
        assert "BSE:SENSEX" in quote_keys

    def test_stock_uses_nse_prefix(self, resolver, mock_kite):
        """Stocks should use NSE:SYMBOL format."""
        resolver.resolve_all()
        call_args = mock_kite.quote.call_args
        quote_keys = call_args[0][0]
        assert "NSE:RELIANCE" in quote_keys
        assert "NSE:TCS" in quote_keys
        assert "NSE:SBIN" in quote_keys

    def test_handles_quote_api_failure(self, resolver, mock_kite):
        """Should handle quote API errors gracefully."""
        mock_kite.quote.side_effect = Exception("Network error")
        result = resolver.resolve_all()
        assert len(result) == 0  # No resolutions if LTP fetch fails

    def test_skips_zero_ltp(self, resolver, mock_kite):
        """Should skip symbols with LTP = 0."""
        mock_kite.quote.return_value = {
            "NSE:RELIANCE": {"last_price": 0.0},
            "NSE:TCS": {"last_price": 3500.0},
            "NSE:NIFTY 50": {"last_price": 24500.0},
            "NSE:SBIN": {"last_price": 750.0},
            "BSE:SENSEX": {"last_price": 80000.0},
        }
        result = resolver.resolve_all()
        assert "RELIANCE" not in result
        assert "TCS" in result


# ──────────────────────────────────────────────────────────────
#  TEST: ATM CALCULATION
# ──────────────────────────────────────────────────────────────


class TestATMCalculation:
    """Tests for ATM strike computation using stock-specific intervals."""

    def test_reliance_atm(self, resolver):
        """RELIANCE (interval=20): spot 2853.50 → ATM 2860."""
        result = resolver.resolve_all()
        # 2853.50 / 20 = 142.675 → round = 143 → 143 * 20 = 2860
        assert result["RELIANCE"].atm_strike == 2860.0

    def test_tcs_atm(self, resolver):
        """TCS (interval=25): spot 3512.75 → ATM 3525."""
        result = resolver.resolve_all()
        # 3512.75 / 25 = 140.51 → round = 141 → 141 * 25 = 3525
        assert result["TCS"].atm_strike == 3525.0

    def test_nifty_atm(self, resolver):
        """NIFTY (interval=50): spot 24523.40 → ATM 24500 or 24550."""
        result = resolver.resolve_all()
        # 24523.40 / 50 = 490.468 → round = 490 → 490 * 50 = 24500
        assert result["NIFTY"].atm_strike == 24500.0

    def test_sbin_atm(self, resolver):
        """SBIN (interval=10): spot 748.60 → ATM 750."""
        result = resolver.resolve_all()
        # 748.60 / 10 = 74.86 → round = 75 → 75 * 10 = 750
        assert result["SBIN"].atm_strike == 750.0

    def test_sensex_atm(self, resolver):
        """SENSEX (interval=100): spot 80125.30 → ATM 80100."""
        result = resolver.resolve_all()
        # 80125.30 / 100 = 801.253 → round = 801 → 801 * 100 = 80100
        assert result["SENSEX"].atm_strike == 80100.0

    def test_strike_interval_stored(self, resolver):
        """ATM resolution should store the strike interval used."""
        result = resolver.resolve_all()
        assert result["RELIANCE"].strike_interval == 20.0
        assert result["NIFTY"].strike_interval == 50.0
        assert result["SENSEX"].strike_interval == 100.0


# ──────────────────────────────────────────────────────────────
#  TEST: TOKEN RESOLUTION
# ──────────────────────────────────────────────────────────────


class TestTokenResolution:
    """Tests for CE/PE instrument token resolution."""

    def test_token_count_per_symbol(self, resolver):
        """Each symbol should have 42 tokens (21 strikes × CE + PE)."""
        result = resolver.resolve_all()
        for sym, res in result.items():
            assert len(res.tokens) == 42, (
                f"{sym} has {len(res.tokens)} tokens, expected 42"
            )

    def test_token_structure(self, resolver):
        """Each token dict should have required fields."""
        result = resolver.resolve_all()
        for token in result["RELIANCE"].tokens:
            assert "instrument_token" in token
            assert "tradingsymbol" in token
            assert "strike" in token
            assert "option_type" in token
            assert "expiry" in token
            assert "symbol" in token
            assert token["option_type"] in ("CE", "PE")
            assert token["symbol"] == "RELIANCE"

    def test_ce_pe_balance(self, resolver):
        """Should have equal CE and PE tokens per symbol."""
        result = resolver.resolve_all()
        for sym, res in result.items():
            ce = [t for t in res.tokens if t["option_type"] == "CE"]
            pe = [t for t in res.tokens if t["option_type"] == "PE"]
            assert len(ce) == len(pe) == 21, (
                f"{sym}: CE={len(ce)}, PE={len(pe)}, expected 21 each"
            )

    def test_strikes_cover_range(self, resolver):
        """Strikes should span ATM ± 10 × interval."""
        result = resolver.resolve_all()
        res = result["RELIANCE"]
        expected_min = res.atm_strike - 10 * res.strike_interval
        expected_max = res.atm_strike + 10 * res.strike_interval
        assert min(res.strikes) == expected_min
        assert max(res.strikes) == expected_max

    def test_get_all_tokens_flat(self, resolver):
        """get_all_tokens() should return flat list of all token ints."""
        resolver.resolve_all()
        all_tokens = resolver.get_all_tokens()
        assert isinstance(all_tokens, list)
        assert all(isinstance(t, int) for t in all_tokens)
        # 5 symbols × 42 tokens = 210
        assert len(all_tokens) == 210

    def test_get_all_tokens_with_details(self, resolver):
        """get_all_tokens_with_details() should return list of dicts."""
        resolver.resolve_all()
        details = resolver.get_all_tokens_with_details()
        assert isinstance(details, list)
        assert len(details) == 210
        assert all("instrument_token" in d for d in details)


# ──────────────────────────────────────────────────────────────
#  TEST: CACHING (30s TTL)
# ──────────────────────────────────────────────────────────────


class TestCaching:
    """Tests for the 30-second cache mechanism."""

    def test_second_call_uses_cache(self, resolver, mock_kite):
        """Second resolve_all() within 30s should not re-fetch."""
        resolver.resolve_all()
        mock_kite.quote.reset_mock()

        resolver.resolve_all()
        mock_kite.quote.assert_not_called()

    def test_force_bypasses_cache(self, resolver, mock_kite):
        """force=True should re-fetch even within cache TTL."""
        resolver.resolve_all()
        mock_kite.quote.reset_mock()

        resolver.resolve_all(force=True)
        mock_kite.quote.assert_called()

    def test_expired_cache_triggers_refresh(self, resolver, mock_kite):
        """After cache TTL expires, next call should re-fetch."""
        resolver.resolve_all()
        mock_kite.quote.reset_mock()

        # Simulate cache expiry by backdating the timestamp
        resolver._cache_timestamp = time.time() - 31  # 31s ago

        resolver.resolve_all()
        mock_kite.quote.assert_called()

    def test_cache_preserves_data(self, resolver):
        """Cached data should be identical between calls."""
        result1 = resolver.resolve_all()
        result2 = resolver.resolve_all()  # From cache

        for sym in result1:
            assert result1[sym].atm_strike == result2[sym].atm_strike
            assert result1[sym].spot_price == result2[sym].spot_price
            assert len(result1[sym].tokens) == len(result2[sym].tokens)


# ──────────────────────────────────────────────────────────────
#  TEST: WORKER BATCHING
# ──────────────────────────────────────────────────────────────


class TestWorkerBatching:
    """Tests for partition into worker batches."""

    def test_batches_created(self, resolver):
        """Should create at least one worker batch."""
        resolver.resolve_all()
        batches = resolver.get_worker_batches()
        assert len(batches) >= 1

    def test_total_tokens_match(self, resolver):
        """Total tokens in batches should match total resolved."""
        resolver.resolve_all()
        batches = resolver.get_worker_batches()
        total_in_batches = sum(len(b) for b in batches.values())
        assert total_in_batches == 210  # 5 × 42

    def test_batch_keys_are_worker_names(self, resolver):
        """Batch keys should follow 'worker_N' pattern."""
        resolver.resolve_all()
        batches = resolver.get_worker_batches()
        for key in batches:
            assert key.startswith("worker_")


# ──────────────────────────────────────────────────────────────
#  TEST: ATMResolution DATA CLASS
# ──────────────────────────────────────────────────────────────


class TestATMResolutionClass:
    """Tests for the ATMResolution data class."""

    def test_to_dict(self, resolver):
        """to_dict() should return serializable output."""
        result = resolver.resolve_all()
        d = result["RELIANCE"].to_dict()
        assert d["symbol"] == "RELIANCE"
        assert "spot_price" in d
        assert "atm_strike" in d
        assert "num_tokens" in d
        assert d["ce_tokens"] == 21
        assert d["pe_tokens"] == 21

    def test_repr(self, resolver):
        """__repr__ should return readable string."""
        result = resolver.resolve_all()
        r = repr(result["RELIANCE"])
        assert "RELIANCE" in r
        assert "atm=" in r

    def test_token_list_is_ints(self, resolver):
        """token_list should be list of ints."""
        result = resolver.resolve_all()
        for sym, res in result.items():
            assert all(isinstance(t, int) for t in res.token_list)


# ──────────────────────────────────────────────────────────────
#  TEST: STATS & MONITORING
# ──────────────────────────────────────────────────────────────


class TestStats:
    """Tests for stats/monitoring methods."""

    def test_stats_before_resolve(self, resolver):
        """Stats should show zero before any resolution."""
        stats = resolver.get_stats()
        assert stats["symbols_resolved"] == 0
        assert stats["total_tokens"] == 0
        assert stats["refresh_count"] == 0

    def test_stats_after_resolve(self, resolver):
        """Stats should update after resolution."""
        resolver.resolve_all()
        stats = resolver.get_stats()
        assert stats["symbols_resolved"] == 5
        assert stats["total_tokens"] == 210
        assert stats["refresh_count"] == 1
        assert stats["cache_age_seconds"] is not None
        assert stats["last_ltp_fetch_ms"] is not None

    def test_summary_returns_list(self, resolver):
        """get_summary() should return list of dicts."""
        resolver.resolve_all()
        summary = resolver.get_summary()
        assert isinstance(summary, list)
        assert len(summary) == 5
        assert all("symbol" in s for s in summary)

    def test_resolve_symbol(self, resolver):
        """resolve_symbol() should return a single ATMResolution."""
        resolver.resolve_all()
        res = resolver.resolve_symbol("RELIANCE")
        assert res is not None
        assert res.symbol == "RELIANCE"
        assert len(res.tokens) == 42


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
