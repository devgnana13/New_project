"""
Tests for the streaming system: StreamWorker, TickAggregator, and
WorkerSupervisor.

Uses mock data to verify tick processing, queue publishing, aggregation,
caching behavior, heartbeats, and worker lifecycle management without
needing real WebSocket connections or API keys.

Run:  python -m pytest tests/test_streaming.py -v
"""

import os
import sys
import time
import threading
import multiprocessing as mp
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from queue import Empty

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.aggregator import TickData, TickAggregator
from workers.stream_worker import StreamWorker


# ──────────────────────────────────────────────────────────────
#  TEST HELPERS
# ──────────────────────────────────────────────────────────────


def _make_raw_tick(token=12345, ltp=150.50, volume=10000, ts="2026-03-05 09:30:01"):
    """Create a raw tick dict matching KiteTicker format."""
    return {
        "instrument_token": token,
        "last_price": ltp,
        "volume": volume,
        "timestamp": ts,
        "change": 1.5,
        "oi": 5000,
        "tradingsymbol": f"SYM{token}",
        "worker_id": "worker_1",
    }


def _make_tick_batch(worker_id="worker_1", count=5, base_token=10000):
    """Create a batch tick message."""
    ticks = []
    for i in range(count):
        ticks.append({
            "instrument_token": base_token + i,
            "last_price": 100.0 + i * 0.5,
            "volume": 1000 * (i + 1),
            "timestamp": "2026-03-05 09:30:01",
        })
    return {
        "type": "tick_batch",
        "worker_id": worker_id,
        "ticks": ticks,
        "count": count,
        "timestamp": time.time(),
    }


def _make_heartbeat(worker_id="worker_1"):
    """Create a heartbeat message."""
    return {
        "type": "heartbeat",
        "worker_id": worker_id,
        "connected": True,
        "total_ticks": 100,
        "unique_instruments": 50,
        "timestamp": time.time(),
    }


# ──────────────────────────────────────────────────────────────
#  TEST: TickData
# ──────────────────────────────────────────────────────────────


class TestTickData:
    """Tests for the TickData data class."""

    def test_basic_construction(self):
        """Should extract fields from raw dict."""
        raw = _make_raw_tick(token=99999, ltp=250.75, volume=5000)
        tick = TickData(raw)
        assert tick.instrument_token == 99999
        assert tick.last_price == 250.75
        assert tick.volume == 5000
        assert tick.worker_id == "worker_1"

    def test_defaults_for_missing_fields(self):
        """Missing fields should default to zero/empty."""
        tick = TickData({})
        assert tick.instrument_token == 0
        assert tick.last_price == 0.0
        assert tick.volume == 0
        assert tick.worker_id == "unknown"

    def test_to_dict(self):
        """to_dict() should return serializable output."""
        raw = _make_raw_tick()
        tick = TickData(raw)
        d = tick.to_dict()
        assert d["instrument_token"] == 12345
        assert d["last_price"] == 150.50
        assert d["volume"] == 10000
        assert "received_at" in d
        assert isinstance(d["received_at"], float)

    def test_repr(self):
        """__repr__ should be readable."""
        tick = TickData(_make_raw_tick())
        r = repr(tick)
        assert "12345" in r
        assert "150.5" in r

    def test_received_at_is_set(self):
        """received_at should be set to current time."""
        before = time.time()
        tick = TickData(_make_raw_tick())
        after = time.time()
        assert before <= tick.received_at <= after


# ──────────────────────────────────────────────────────────────
#  TEST: TickAggregator
# ──────────────────────────────────────────────────────────────


class TestTickAggregator:
    """Tests for the TickAggregator."""

    @pytest.fixture
    def queue(self):
        return mp.Queue()

    @pytest.fixture
    def aggregator(self, queue):
        agg = TickAggregator(queue)
        yield agg
        agg.stop()

    def test_start_and_stop(self, aggregator):
        """Should start and stop cleanly."""
        aggregator.start()
        assert aggregator.is_running
        aggregator.stop()
        assert not aggregator.is_running

    def test_process_single_tick(self, aggregator, queue):
        """Should process a single tick from the queue."""
        aggregator.start()
        raw = _make_raw_tick(token=11111, ltp=200.0, volume=7500)
        queue.put(raw)
        time.sleep(0.1)  # Let drain thread process

        tick = aggregator.get_tick(11111)
        assert tick is not None
        assert tick.last_price == 200.0
        assert tick.volume == 7500

    def test_process_tick_batch(self, aggregator, queue):
        """Should process a batch tick message."""
        aggregator.start()
        batch = _make_tick_batch(count=5, base_token=20000)
        queue.put(batch)
        time.sleep(0.1)

        # Should have 5 ticks
        all_ticks = aggregator.get_all_ticks()
        batch_ticks = {k: v for k, v in all_ticks.items()
                       if 20000 <= k < 20005}
        assert len(batch_ticks) == 5

    def test_latest_tick_overwrites(self, aggregator, queue):
        """New tick for same token should overwrite old one."""
        aggregator.start()
        queue.put(_make_raw_tick(token=33333, ltp=100.0))
        time.sleep(0.05)
        queue.put(_make_raw_tick(token=33333, ltp=105.0))
        time.sleep(0.1)

        tick = aggregator.get_tick(33333)
        assert tick.last_price == 105.0

    def test_get_ltp(self, aggregator, queue):
        """get_ltp() should return just the price."""
        aggregator.start()
        queue.put(_make_raw_tick(token=44444, ltp=999.99))
        time.sleep(0.1)

        ltp = aggregator.get_ltp(44444)
        assert ltp == 999.99

    def test_get_ltp_missing(self, aggregator):
        """get_ltp() should return None for unknown token."""
        assert aggregator.get_ltp(99999) is None

    def test_get_ticks_subset(self, aggregator, queue):
        """get_ticks() should return only requested tokens."""
        aggregator.start()
        for tok in [1001, 1002, 1003]:
            queue.put(_make_raw_tick(token=tok, ltp=float(tok)))
        time.sleep(0.1)

        result = aggregator.get_ticks([1001, 1003, 9999])
        assert 1001 in result
        assert 1003 in result
        assert 9999 not in result

    def test_heartbeat_tracking(self, aggregator, queue):
        """Heartbeat messages should update worker heartbeats."""
        aggregator.start()
        queue.put(_make_heartbeat("worker_1"))
        queue.put(_make_heartbeat("worker_2"))
        time.sleep(0.1)

        stats = aggregator.get_stats()
        assert "worker_1" in stats["worker_heartbeats"]
        assert "worker_2" in stats["worker_heartbeats"]

    def test_stats_update(self, aggregator, queue):
        """Stats should reflect processed ticks."""
        aggregator.start()
        for i in range(10):
            queue.put(_make_raw_tick(token=5000 + i))
        time.sleep(0.2)

        stats = aggregator.get_stats()
        assert stats["total_ticks_processed"] >= 10
        assert stats["unique_instruments"] >= 10
        assert stats["running"] is True

    def test_on_tick_callback(self, queue):
        """on_tick callback should fire for each tick."""
        received = []
        agg = TickAggregator(queue, on_tick=lambda t: received.append(t))
        agg.start()

        queue.put(_make_raw_tick(token=6001))
        queue.put(_make_raw_tick(token=6002))
        time.sleep(0.1)
        agg.stop()

        assert len(received) == 2
        assert received[0].instrument_token == 6001
        assert received[1].instrument_token == 6002

    def test_on_batch_callback(self, queue):
        """on_batch callback should fire with list of ticks."""
        batches = []
        agg = TickAggregator(queue, on_batch=lambda b: batches.append(b))
        agg.start()

        batch_msg = _make_tick_batch(count=3, base_token=7000)
        queue.put(batch_msg)
        time.sleep(0.1)
        agg.stop()

        assert len(batches) >= 1
        total = sum(len(b) for b in batches)
        assert total == 3

    def test_get_all_ltps(self, aggregator, queue):
        """get_all_ltps() should return all LTPs."""
        aggregator.start()
        queue.put(_make_raw_tick(token=8001, ltp=100.0))
        queue.put(_make_raw_tick(token=8002, ltp=200.0))
        time.sleep(0.1)

        ltps = aggregator.get_all_ltps()
        assert ltps[8001] == 100.0
        assert ltps[8002] == 200.0


# ──────────────────────────────────────────────────────────────
#  TEST: StreamWorker (unit-level, no real WebSocket)
# ──────────────────────────────────────────────────────────────


class TestStreamWorker:
    """Tests for the StreamWorker internals (no real WS connection)."""

    @pytest.fixture
    def queue(self):
        return mp.Queue()

    @pytest.fixture
    def worker(self, queue):
        return StreamWorker(
            worker_id="test_worker",
            api_key="test_key",
            access_token="test_token",
            tokens=[10001, 10002, 10003, 10004, 10005],
            tick_queue=queue,
            mode="full",
        )

    def test_init_state(self, worker):
        """Worker should initialize with correct state."""
        assert worker.worker_id == "test_worker"
        assert len(worker._tokens) == 5
        assert not worker._running
        assert not worker._connected

    def test_on_ticks_updates_local_memory(self, worker):
        """_on_ticks should update local tick store."""
        mock_ticks = [
            {"instrument_token": 10001, "last_price": 100.0,
             "volume_traded": 5000, "exchange_timestamp": "2026-03-05 09:30:01"},
            {"instrument_token": 10002, "last_price": 200.0,
             "volume_traded": 3000, "exchange_timestamp": "2026-03-05 09:30:02"},
        ]
        worker._on_ticks(None, mock_ticks)

        assert 10001 in worker._latest_ticks
        assert worker._latest_ticks[10001]["last_price"] == 100.0
        assert worker._latest_ticks[10001]["volume"] == 5000
        assert 10002 in worker._latest_ticks
        assert worker._latest_ticks[10002]["last_price"] == 200.0

    def test_on_ticks_increments_counter(self, worker):
        """Each tick should increment the total counter."""
        ticks = [{"instrument_token": 10001 + i, "last_price": 100.0 + i}
                 for i in range(5)]
        worker._on_ticks(None, ticks)
        assert worker._total_ticks == 5

    def test_tick_buffer_fills(self, worker):
        """Ticks should be buffered before flushing."""
        worker._running = True
        ticks = [{"instrument_token": 10001, "last_price": 100.0}]
        worker._on_ticks(None, ticks)
        assert len(worker._tick_buffer) == 1

    def test_flush_publishes_to_queue(self, worker, queue):
        """Flushing buffer should publish batch to queue."""
        worker._running = True
        ticks = [
            {"instrument_token": 10001 + i, "last_price": 100.0 + i}
            for i in range(3)
        ]
        worker._on_ticks(None, ticks)
        worker._flush_tick_buffer()

        # Check queue received the batch
        msg = queue.get(timeout=1.0)
        assert msg["type"] == "tick_batch"
        assert msg["worker_id"] == "test_worker"
        assert msg["count"] == 3

    def test_flush_clears_buffer(self, worker):
        """After flush, buffer should be empty."""
        worker._running = True
        ticks = [{"instrument_token": 10001, "last_price": 100.0}]
        worker._on_ticks(None, ticks)
        worker._flush_tick_buffer()
        assert len(worker._tick_buffer) == 0

    def test_get_latest_tick(self, worker):
        """get_latest_tick() should return the stored tick."""
        ticks = [{"instrument_token": 10001, "last_price": 150.0,
                   "volume_traded": 9999}]
        worker._on_ticks(None, ticks)

        tick = worker.get_latest_tick(10001)
        assert tick is not None
        assert tick["last_price"] == 150.0
        assert tick["volume"] == 9999

    def test_get_latest_tick_missing(self, worker):
        """Missing token should return None."""
        assert worker.get_latest_tick(99999) is None

    def test_get_all_latest_ticks(self, worker):
        """get_all_latest_ticks() should return copy of all ticks."""
        ticks = [
            {"instrument_token": 10001, "last_price": 100.0},
            {"instrument_token": 10002, "last_price": 200.0},
        ]
        worker._on_ticks(None, ticks)
        all_ticks = worker.get_all_latest_ticks()
        assert len(all_ticks) == 2
        assert 10001 in all_ticks
        assert 10002 in all_ticks

    def test_get_stats(self, worker):
        """get_stats() should return worker statistics."""
        worker._started_at = time.time()
        worker._running = True
        worker._connected = True
        ticks = [{"instrument_token": 10001 + i, "last_price": 100.0}
                 for i in range(3)]
        worker._on_ticks(None, ticks)

        stats = worker.get_stats()
        assert stats["worker_id"] == "test_worker"
        assert stats["running"] is True
        assert stats["connected"] is True
        assert stats["subscribed_tokens"] == 5
        assert stats["total_ticks"] == 3
        assert stats["unique_instruments_seen"] == 3

    def test_on_connect_subscribes(self, worker):
        """_on_connect should subscribe tokens and set mode."""
        mock_ws = MagicMock()
        worker._on_connect(mock_ws, {})

        assert worker._connected is True
        assert worker._connect_count == 1
        mock_ws.subscribe.assert_called()
        mock_ws.set_mode.assert_called()

    def test_on_close_updates_state(self, worker):
        """_on_close should mark disconnected."""
        worker._connected = True
        worker._on_close(None, 1000, "Normal closure")

        assert worker._connected is False
        assert worker._disconnect_count == 1

    def test_on_error_is_handled(self, worker):
        """_on_error should not raise."""
        worker._on_error(None, 1001, "Test error")

    def test_volume_from_volume_traded(self, worker):
        """Should extract volume from volume_traded field."""
        ticks = [{"instrument_token": 10001, "last_price": 100.0,
                   "volume_traded": 12345}]
        worker._on_ticks(None, ticks)
        tick = worker.get_latest_tick(10001)
        assert tick["volume"] == 12345

    def test_volume_fallback_to_volume(self, worker):
        """Should fall back to volume field if volume_traded is absent."""
        ticks = [{"instrument_token": 10001, "last_price": 100.0,
                   "volume": 6789}]
        worker._on_ticks(None, ticks)
        tick = worker.get_latest_tick(10001)
        assert tick["volume"] == 6789

    def test_timestamp_from_exchange_timestamp(self, worker):
        """Should prefer exchange_timestamp over timestamp."""
        ts = "2026-03-05 10:00:00"
        ticks = [{"instrument_token": 10001, "last_price": 100.0,
                   "exchange_timestamp": ts, "timestamp": "other"}]
        worker._on_ticks(None, ticks)
        tick = worker.get_latest_tick(10001)
        assert tick["timestamp"] == ts


# ──────────────────────────────────────────────────────────────
#  TEST: Integration (Worker → Queue → Aggregator)
# ──────────────────────────────────────────────────────────────


class TestIntegration:
    """Integration tests: worker publishes ticks → aggregator reads them."""

    def test_worker_to_aggregator_flow(self):
        """Ticks from worker should appear in aggregator."""
        queue = mp.Queue()
        agg = TickAggregator(queue)
        agg.start()

        worker = StreamWorker(
            worker_id="int_worker",
            api_key="test", access_token="test",
            tokens=[50001, 50002], tick_queue=queue,
        )

        # Simulate tick reception
        ticks = [
            {"instrument_token": 50001, "last_price": 500.0,
             "volume_traded": 10000, "exchange_timestamp": "2026-03-05 10:00:00"},
            {"instrument_token": 50002, "last_price": 600.0,
             "volume_traded": 20000, "exchange_timestamp": "2026-03-05 10:00:01"},
        ]
        worker._on_ticks(None, ticks)
        worker._flush_tick_buffer()

        time.sleep(0.2)

        # Aggregator should have the ticks
        tick1 = agg.get_tick(50001)
        tick2 = agg.get_tick(50002)
        assert tick1 is not None
        assert tick1.last_price == 500.0
        assert tick1.volume == 10000
        assert tick2 is not None
        assert tick2.last_price == 600.0
        assert tick2.volume == 20000

        agg.stop()

    def test_multiple_workers_to_aggregator(self):
        """Ticks from multiple workers should all appear in aggregator."""
        queue = mp.Queue()
        agg = TickAggregator(queue)
        agg.start()

        # Worker 1
        w1 = StreamWorker(
            worker_id="w1", api_key="t", access_token="t",
            tokens=[60001], tick_queue=queue,
        )
        w1._on_ticks(None, [{"instrument_token": 60001, "last_price": 100.0}])
        w1._flush_tick_buffer()

        # Worker 2
        w2 = StreamWorker(
            worker_id="w2", api_key="t", access_token="t",
            tokens=[60002], tick_queue=queue,
        )
        w2._on_ticks(None, [{"instrument_token": 60002, "last_price": 200.0}])
        w2._flush_tick_buffer()

        time.sleep(0.2)

        # Both ticks should be in aggregator
        assert agg.get_ltp(60001) == 100.0
        assert agg.get_ltp(60002) == 200.0
        assert agg.get_stats()["unique_instruments"] >= 2

        agg.stop()

    def test_heartbeat_flow(self):
        """Worker heartbeats should be tracked by aggregator."""
        queue = mp.Queue()
        agg = TickAggregator(queue)
        agg.start()

        queue.put(_make_heartbeat("hb_worker"))
        time.sleep(0.1)

        stats = agg.get_stats()
        assert "hb_worker" in stats["worker_heartbeats"]
        # Heartbeat age should be very recent
        assert stats["worker_heartbeats"]["hb_worker"] < 2.0

        agg.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
