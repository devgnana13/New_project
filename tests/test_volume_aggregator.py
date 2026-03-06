"""
Tests for the VolumeAggregator module.

Verifies CE/PE volume summation, token mapping, periodic updates,
PCR calculation, and integration with the TickAggregator.

Run:  python -m pytest tests/test_volume_aggregator.py -v
"""

import os
import sys
import time
import multiprocessing as mp
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.aggregator import TickAggregator, TickData
from core.volume_aggregator import VolumeAggregator, SymbolVolume


# ──────────────────────────────────────────────────────────────
#  TEST HELPERS
# ──────────────────────────────────────────────────────────────


def _make_resolution(symbol, ce_tokens, pe_tokens):
    """
    Create a mock ATMResolution-like object with token dicts.

    Args:
        symbol:     e.g., "RELIANCE"
        ce_tokens:  list of CE instrument tokens (ints)
        pe_tokens:  list of PE instrument tokens (ints)
    """
    tokens = []
    for t in ce_tokens:
        tokens.append({
            "instrument_token": t,
            "symbol": symbol,
            "option_type": "CE",
            "strike": 2850.0,
            "tradingsymbol": f"{symbol}CE{t}",
            "expiry": "2026-03-26",
        })
    for t in pe_tokens:
        tokens.append({
            "instrument_token": t,
            "symbol": symbol,
            "option_type": "PE",
            "strike": 2850.0,
            "tradingsymbol": f"{symbol}PE{t}",
            "expiry": "2026-03-26",
        })

    res = MagicMock()
    res.symbol = symbol
    res.tokens = tokens
    res.token_list = [td["instrument_token"] for td in tokens]
    return res


def _put_tick(queue, token, volume, ltp=100.0):
    """Push a single tick with the given volume into the queue."""
    queue.put({
        "instrument_token": token,
        "last_price": ltp,
        "volume": volume,
        "timestamp": "2026-03-05 10:00:00",
        "worker_id": "test",
    })


# ──────────────────────────────────────────────────────────────
#  FIXTURES
# ──────────────────────────────────────────────────────────────


@pytest.fixture
def queue():
    return mp.Queue()


@pytest.fixture
def tick_agg(queue):
    agg = TickAggregator(queue)
    agg.start()
    yield agg
    agg.stop()


@pytest.fixture
def resolutions():
    """Create mock ATM resolutions for 3 stocks."""
    return {
        "RELIANCE": _make_resolution(
            "RELIANCE",
            ce_tokens=[1001, 1002, 1003],
            pe_tokens=[2001, 2002, 2003],
        ),
        "TCS": _make_resolution(
            "TCS",
            ce_tokens=[3001, 3002],
            pe_tokens=[4001, 4002],
        ),
        "SBIN": _make_resolution(
            "SBIN",
            ce_tokens=[5001],
            pe_tokens=[6001],
        ),
    }


@pytest.fixture
def vol_agg(tick_agg):
    agg = VolumeAggregator(tick_agg, update_interval=0.1)
    yield agg
    agg.stop()


# ──────────────────────────────────────────────────────────────
#  TEST: Token Mapping
# ──────────────────────────────────────────────────────────────


class TestTokenMapping:
    """Tests for building the token → (symbol, opt_type) map."""

    def test_build_from_resolutions(self, vol_agg, resolutions):
        """Should build correct token map from ATM resolutions."""
        vol_agg.build_token_map(resolutions)

        assert len(vol_agg._token_map) == 12  # 6 + 4 + 2
        assert vol_agg._token_map[1001] == ("RELIANCE", "CE")
        assert vol_agg._token_map[2001] == ("RELIANCE", "PE")
        assert vol_agg._token_map[3001] == ("TCS", "CE")
        assert vol_agg._token_map[4001] == ("TCS", "PE")

    def test_build_from_list(self, vol_agg):
        """Should build from flat token detail list."""
        details = [
            {"instrument_token": 1001, "symbol": "REL", "option_type": "CE"},
            {"instrument_token": 2001, "symbol": "REL", "option_type": "PE"},
            {"instrument_token": 3001, "symbol": "TCS", "option_type": "CE"},
        ]
        vol_agg.build_token_map_from_list(details)

        assert len(vol_agg._token_map) == 3
        assert vol_agg._token_map[1001] == ("REL", "CE")
        assert "REL" in vol_agg._symbol_tokens
        assert "TCS" in vol_agg._symbol_tokens

    def test_symbol_tokens_grouped(self, vol_agg, resolutions):
        """Symbol tokens should be grouped by CE/PE."""
        vol_agg.build_token_map(resolutions)

        assert set(vol_agg._symbol_tokens["RELIANCE"]["CE"]) == {1001, 1002, 1003}
        assert set(vol_agg._symbol_tokens["RELIANCE"]["PE"]) == {2001, 2002, 2003}

    def test_rebuild_clears_old(self, vol_agg, resolutions):
        """Rebuilding map should clear old entries."""
        vol_agg.build_token_map(resolutions)
        assert "SBIN" in vol_agg._symbol_tokens

        # Rebuild without SBIN
        new_res = {k: v for k, v in resolutions.items() if k != "SBIN"}
        vol_agg.build_token_map(new_res)
        assert "SBIN" not in vol_agg._symbol_tokens
        assert "SBIN" not in vol_agg._volumes

    def test_initializes_volume_entries(self, vol_agg, resolutions):
        """Building map should create SymbolVolume entries."""
        vol_agg.build_token_map(resolutions)
        assert "RELIANCE" in vol_agg._volumes
        assert "TCS" in vol_agg._volumes
        assert "SBIN" in vol_agg._volumes


# ──────────────────────────────────────────────────────────────
#  TEST: Volume Computation
# ──────────────────────────────────────────────────────────────


class TestVolumeComputation:
    """Tests for CE/PE volume summation."""

    def test_basic_volume_sum(self, vol_agg, tick_agg, queue, resolutions):
        """Should sum CE and PE volumes correctly."""
        vol_agg.build_token_map(resolutions)

        # Push CE ticks for RELIANCE
        _put_tick(queue, 1001, volume=5000)
        _put_tick(queue, 1002, volume=3000)
        _put_tick(queue, 1003, volume=2000)

        # Push PE ticks for RELIANCE
        _put_tick(queue, 2001, volume=4000)
        _put_tick(queue, 2002, volume=6000)
        _put_tick(queue, 2003, volume=1000)

        time.sleep(0.15)  # Let tick_agg drain

        result = vol_agg.update_now()
        assert result["RELIANCE"]["call_volume"] == 10000  # 5k + 3k + 2k
        assert result["RELIANCE"]["put_volume"] == 11000   # 4k + 6k + 1k

    def test_multiple_symbols(self, vol_agg, tick_agg, queue, resolutions):
        """Should compute volumes independently per symbol."""
        vol_agg.build_token_map(resolutions)

        # RELIANCE
        _put_tick(queue, 1001, volume=1000)
        _put_tick(queue, 2001, volume=2000)

        # TCS
        _put_tick(queue, 3001, volume=500)
        _put_tick(queue, 4001, volume=800)

        time.sleep(0.15)

        result = vol_agg.update_now()
        assert result["RELIANCE"]["call_volume"] == 1000
        assert result["RELIANCE"]["put_volume"] == 2000
        assert result["TCS"]["call_volume"] == 500
        assert result["TCS"]["put_volume"] == 800

    def test_missing_ticks_count_as_zero(self, vol_agg, tick_agg, queue, resolutions):
        """Tokens without tick data should contribute 0 volume."""
        vol_agg.build_token_map(resolutions)

        # Only push 1 of 3 CE ticks
        _put_tick(queue, 1001, volume=5000)
        time.sleep(0.15)

        result = vol_agg.update_now()
        assert result["RELIANCE"]["call_volume"] == 5000  # Only 1001

    def test_volume_overwrites_on_update(self, vol_agg, tick_agg, queue, resolutions):
        """New tick for same token should update the volume."""
        vol_agg.build_token_map(resolutions)

        _put_tick(queue, 1001, volume=1000)
        time.sleep(0.1)
        vol_agg.update_now()

        # Update with new volume
        _put_tick(queue, 1001, volume=9999)
        time.sleep(0.1)

        result = vol_agg.update_now()
        assert result["RELIANCE"]["call_volume"] == 9999

    def test_empty_map_no_crash(self, vol_agg):
        """Should handle empty token map gracefully."""
        result = vol_agg.update_now()
        assert result == {}

    def test_no_ticks_returns_zeros(self, vol_agg, resolutions):
        """Before any ticks, volumes should be zero."""
        vol_agg.build_token_map(resolutions)
        result = vol_agg.update_now()
        assert result["RELIANCE"]["call_volume"] == 0
        assert result["RELIANCE"]["put_volume"] == 0


# ──────────────────────────────────────────────────────────────
#  TEST: Read API
# ──────────────────────────────────────────────────────────────


class TestReadAPI:
    """Tests for the volume read methods."""

    def test_get_volumes(self, vol_agg, tick_agg, queue, resolutions):
        """get_volumes() should return all symbol volumes."""
        vol_agg.build_token_map(resolutions)
        _put_tick(queue, 1001, volume=100)
        _put_tick(queue, 2001, volume=200)
        time.sleep(0.15)
        vol_agg.update_now()

        vols = vol_agg.get_volumes()
        assert "RELIANCE" in vols
        assert "TCS" in vols
        assert "SBIN" in vols
        assert vols["RELIANCE"]["call_volume"] == 100
        assert vols["RELIANCE"]["put_volume"] == 200

    def test_get_symbol_volume(self, vol_agg, tick_agg, queue, resolutions):
        """get_symbol_volume() should return one symbol's data."""
        vol_agg.build_token_map(resolutions)
        _put_tick(queue, 3001, volume=777)
        _put_tick(queue, 4001, volume=888)
        time.sleep(0.15)
        vol_agg.update_now()

        tcs = vol_agg.get_symbol_volume("TCS")
        assert tcs is not None
        assert tcs["call_volume"] == 777
        assert tcs["put_volume"] == 888

    def test_get_symbol_volume_missing(self, vol_agg, resolutions):
        """Missing symbol should return None."""
        vol_agg.build_token_map(resolutions)
        assert vol_agg.get_symbol_volume("FAKESYM") is None

    def test_get_symbol_volume_case_insensitive(self, vol_agg, resolutions):
        """Should be case-insensitive."""
        vol_agg.build_token_map(resolutions)
        vol_agg.update_now()
        assert vol_agg.get_symbol_volume("reliance") is not None
        assert vol_agg.get_symbol_volume("Tcs") is not None

    def test_get_detailed_volumes(self, vol_agg, tick_agg, queue, resolutions):
        """get_detailed_volumes() should include token counts."""
        vol_agg.build_token_map(resolutions)
        _put_tick(queue, 1001, volume=100)
        _put_tick(queue, 1002, volume=200)
        _put_tick(queue, 2001, volume=300)
        time.sleep(0.15)
        vol_agg.update_now()

        details = vol_agg.get_detailed_volumes()
        rel = details["RELIANCE"]
        assert rel["call_volume"] == 300  # 100 + 200
        assert rel["put_volume"] == 300
        assert rel["call_tokens"] == 2   # 2 of 3 CE tokens have data
        assert rel["put_tokens"] == 1    # 1 of 3 PE tokens have data
        assert rel["updated_at"] is not None


# ──────────────────────────────────────────────────────────────
#  TEST: Put-Call Ratio
# ──────────────────────────────────────────────────────────────


class TestPCR:
    """Tests for Put-Call Ratio computation."""

    def test_basic_pcr(self, vol_agg, tick_agg, queue, resolutions):
        """PCR = put_volume / call_volume."""
        vol_agg.build_token_map(resolutions)
        _put_tick(queue, 1001, volume=10000)  # CE
        _put_tick(queue, 2001, volume=8000)   # PE
        time.sleep(0.15)
        vol_agg.update_now()

        pcr = vol_agg.get_pcr("RELIANCE")
        assert pcr == 0.8  # 8000 / 10000

    def test_pcr_zero_calls(self, vol_agg, resolutions):
        """PCR should be None when call_volume is 0."""
        vol_agg.build_token_map(resolutions)
        vol_agg.update_now()
        pcr = vol_agg.get_pcr("RELIANCE")
        assert pcr is None

    def test_pcr_missing_symbol(self, vol_agg, resolutions):
        """PCR for unknown symbol should be None."""
        vol_agg.build_token_map(resolutions)
        assert vol_agg.get_pcr("FAKESYM") is None

    def test_get_all_pcr(self, vol_agg, tick_agg, queue, resolutions):
        """get_all_pcr() should return PCR for all symbols."""
        vol_agg.build_token_map(resolutions)
        _put_tick(queue, 1001, volume=1000)
        _put_tick(queue, 2001, volume=1500)
        _put_tick(queue, 3001, volume=500)
        _put_tick(queue, 4001, volume=250)
        time.sleep(0.15)
        vol_agg.update_now()

        pcrs = vol_agg.get_all_pcr()
        assert pcrs["RELIANCE"] == 1.5    # 1500/1000
        assert pcrs["TCS"] == 0.5         # 250/500
        assert pcrs["SBIN"] is None       # No data


# ──────────────────────────────────────────────────────────────
#  TEST: Background Updates
# ──────────────────────────────────────────────────────────────


class TestBackgroundUpdates:
    """Tests for the periodic update mechanism."""

    def test_start_and_stop(self, vol_agg):
        """Should start and stop cleanly."""
        vol_agg.start()
        assert vol_agg.is_running
        vol_agg.stop()
        assert not vol_agg.is_running

    def test_periodic_update(self, vol_agg, tick_agg, queue, resolutions):
        """Background thread should auto-update volumes."""
        vol_agg.build_token_map(resolutions)
        vol_agg.start()

        _put_tick(queue, 1001, volume=5000)
        _put_tick(queue, 2001, volume=3000)
        time.sleep(0.4)  # Wait for at least 1 update cycle

        vols = vol_agg.get_volumes()
        assert vols["RELIANCE"]["call_volume"] == 5000
        assert vols["RELIANCE"]["put_volume"] == 3000

    def test_on_update_callback(self, tick_agg, queue, resolutions):
        """on_update callback should fire after each update."""
        updates = []
        vol_agg = VolumeAggregator(
            tick_agg, update_interval=0.1,
            on_update=lambda v: updates.append(v),
        )
        vol_agg.build_token_map(resolutions)
        vol_agg.start()

        _put_tick(queue, 1001, volume=1000)
        time.sleep(0.4)
        vol_agg.stop()

        assert len(updates) >= 1
        assert "RELIANCE" in updates[-1]

    def test_stats(self, vol_agg, resolutions):
        """get_stats() should return current state."""
        vol_agg.build_token_map(resolutions)
        vol_agg.start()
        time.sleep(0.3)

        stats = vol_agg.get_stats()
        assert stats["running"] is True
        assert stats["symbols_tracked"] == 3
        assert stats["tokens_mapped"] == 12
        assert stats["update_count"] >= 1
        assert stats["update_interval"] == 0.1
        assert stats["last_update_ms"] is not None

        vol_agg.stop()


# ──────────────────────────────────────────────────────────────
#  TEST: SymbolVolume Data Class
# ──────────────────────────────────────────────────────────────


class TestSymbolVolume:
    """Tests for the SymbolVolume data class."""

    def test_defaults(self):
        """Should initialize with zero volumes."""
        sv = SymbolVolume("RELIANCE")
        assert sv.symbol == "RELIANCE"
        assert sv.call_volume == 0
        assert sv.put_volume == 0
        assert sv.updated_at is None

    def test_to_dict(self):
        """to_dict() should return serializable output."""
        sv = SymbolVolume("TCS")
        sv.call_volume = 5000
        sv.put_volume = 3000
        sv.call_tokens = 21
        sv.put_tokens = 21
        sv.updated_at = datetime(2026, 3, 5, 10, 0, 0)

        d = sv.to_dict()
        assert d["call_volume"] == 5000
        assert d["put_volume"] == 3000
        assert d["call_tokens"] == 21
        assert d["put_tokens"] == 21
        assert "2026-03-05" in d["updated_at"]

    def test_repr(self):
        """__repr__ should be readable."""
        sv = SymbolVolume("NIFTY")
        sv.call_volume = 150000
        sv.put_volume = 120000
        r = repr(sv)
        assert "NIFTY" in r
        assert "150,000" in r


# ──────────────────────────────────────────────────────────────
#  TEST: Integration
# ──────────────────────────────────────────────────────────────


class TestIntegration:
    """End-to-end: ticks → tick_agg → volume_agg → volumes."""

    def test_full_pipeline(self):
        """Complete flow from tick to volume output."""
        queue = mp.Queue()
        tick_agg = TickAggregator(queue)
        tick_agg.start()

        vol_agg = VolumeAggregator(tick_agg, update_interval=0.1)
        resolutions = {
            "HDFC": _make_resolution(
                "HDFC",
                ce_tokens=[7001, 7002, 7003, 7004, 7005],
                pe_tokens=[8001, 8002, 8003, 8004, 8005],
            ),
        }
        vol_agg.build_token_map(resolutions)
        vol_agg.start()

        # Simulate CE ticks
        for i in range(5):
            _put_tick(queue, 7001 + i, volume=1000 * (i + 1))

        # Simulate PE ticks
        for i in range(5):
            _put_tick(queue, 8001 + i, volume=500 * (i + 1))

        time.sleep(0.4)

        vols = vol_agg.get_volumes()
        # CE: 1000 + 2000 + 3000 + 4000 + 5000 = 15000
        assert vols["HDFC"]["call_volume"] == 15000
        # PE: 500 + 1000 + 1500 + 2000 + 2500 = 7500
        assert vols["HDFC"]["put_volume"] == 7500

        pcr = vol_agg.get_pcr("HDFC")
        assert pcr == 0.5  # 7500 / 15000

        vol_agg.stop()
        tick_agg.stop()

    def test_dynamic_remap(self):
        """Token map rebuild should update volumes correctly."""
        queue = mp.Queue()
        tick_agg = TickAggregator(queue)
        tick_agg.start()

        vol_agg = VolumeAggregator(tick_agg, update_interval=0.1)

        # Initial map
        res1 = {
            "AAA": _make_resolution("AAA", ce_tokens=[9001], pe_tokens=[9002]),
        }
        vol_agg.build_token_map(res1)
        _put_tick(queue, 9001, volume=100)
        _put_tick(queue, 9002, volume=200)
        time.sleep(0.15)
        vol_agg.update_now()

        assert vol_agg.get_symbol_volume("AAA")["call_volume"] == 100

        # Rebuild with different tokens (simulating ATM shift)
        res2 = {
            "AAA": _make_resolution("AAA", ce_tokens=[9003], pe_tokens=[9004]),
        }
        vol_agg.build_token_map(res2)
        _put_tick(queue, 9003, volume=500)
        _put_tick(queue, 9004, volume=600)
        time.sleep(0.15)
        vol_agg.update_now()

        vols = vol_agg.get_symbol_volume("AAA")
        assert vols["call_volume"] == 500
        assert vols["put_volume"] == 600

        tick_agg.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
