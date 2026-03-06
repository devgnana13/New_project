"""
Tick Aggregator — Collects real-time tick data from multiple WebSocket
worker processes via a shared multiprocessing Queue.

Architecture:
    ┌──────────┐   ┌──────────┐   ┌──────────┐
    │ Worker 1 │   │ Worker 2 │   │ Worker 3 │
    │ (≤3000)  │   │ (≤3000)  │   │ (≤3000)  │
    └────┬─────┘   └────┬─────┘   └────┬─────┘
         │              │              │
         └──────────┬───┘──────────────┘
                    │
              ┌─────▼─────┐
              │   Queue    │   (multiprocessing.Queue)
              └─────┬─────┘
                    │
              ┌─────▼─────┐
              │ Aggregator │   Merges ticks into a unified store
              │            │   Maintains latest tick per instrument
              └────────────┘

Design Goals:
    1. Lock-free reads — last tick data is stored in a dict that can be
       read without blocking worker writes.
    2. Each tick contains: instrument_token, last_price, volume, timestamp.
    3. The aggregator runs in the main process and drains the queue in
       a background thread.
    4. Supports optional callbacks for downstream consumers (MongoDB writer,
       analytics engine, etc.).

Usage:
    from core.aggregator import TickAggregator
    import multiprocessing as mp

    tick_queue = mp.Queue()
    aggregator = TickAggregator(tick_queue)
    aggregator.start()   # Starts background drain thread

    # In worker processes:
    tick_queue.put({
        "instrument_token": 12345,
        "last_price": 150.50,
        "volume": 10000,
        "timestamp": "2026-03-05 09:30:01",
        "worker_id": "worker_1",
    })

    # Read latest ticks:
    tick = aggregator.get_tick(12345)
    all_ticks = aggregator.get_all_ticks()
"""

import time
import logging
import threading
from datetime import datetime
from typing import Optional, Callable
from multiprocessing import Queue
from queue import Empty

logger = logging.getLogger(__name__)


class TickData:
    """
    Immutable data class representing a single tick snapshot.
    """
    __slots__ = (
        "instrument_token", "last_price", "volume",
        "timestamp", "worker_id", "received_at",
        "change", "oi", "tradingsymbol",
    )

    def __init__(self, raw: dict):
        self.instrument_token: int = raw.get("instrument_token", 0)
        self.last_price: float = raw.get("last_price", 0.0)
        self.volume: int = raw.get("volume", 0)
        self.timestamp = raw.get("timestamp")
        self.worker_id: str = raw.get("worker_id", "unknown")
        self.received_at: float = time.time()
        self.change: float = raw.get("change", 0.0)
        self.oi: int = raw.get("oi", 0)
        self.tradingsymbol: str = raw.get("tradingsymbol", "")

    def to_dict(self) -> dict:
        return {
            "instrument_token": self.instrument_token,
            "last_price": self.last_price,
            "volume": self.volume,
            "timestamp": str(self.timestamp) if self.timestamp else None,
            "worker_id": self.worker_id,
            "received_at": self.received_at,
            "change": self.change,
            "oi": self.oi,
            "tradingsymbol": self.tradingsymbol,
        }

    def __repr__(self) -> str:
        return (
            f"TickData(token={self.instrument_token}, "
            f"ltp={self.last_price}, vol={self.volume})"
        )


class TickAggregator:
    """
    Consumes tick messages from a multiprocessing Queue and maintains
    the latest tick data for each instrument token in memory.

    Runs a background drain thread that continuously reads from the
    queue and updates the internal store.
    """

    def __init__(
        self,
        tick_queue: Queue,
        on_tick: Optional[Callable[[TickData], None]] = None,
        on_batch: Optional[Callable[[list[TickData]], None]] = None,
        drain_interval: float = 0.01,
        batch_size: int = 500,
    ):
        """
        Args:
            tick_queue:      Multiprocessing queue that workers write to.
            on_tick:         Optional callback invoked for each tick.
            on_batch:        Optional callback invoked with batches of ticks.
            drain_interval:  Seconds between queue drain cycles (default 10ms).
            batch_size:      Max ticks to process per drain cycle.
        """
        self._queue = tick_queue
        self._on_tick = on_tick
        self._on_batch = on_batch
        self._drain_interval = drain_interval
        self._batch_size = batch_size

        # ── Tick store: token → TickData ──
        self._ticks: dict[int, TickData] = {}

        # ── Drain thread ──
        self._drain_thread: Optional[threading.Thread] = None
        self._running = False

        # ── Stats ──
        self._total_ticks_processed = 0
        self._ticks_per_second = 0.0
        self._last_stats_time = time.time()
        self._ticks_since_last_stats = 0
        self._worker_heartbeats: dict[str, float] = {}
        self._started_at: Optional[float] = None

    # ──────────────────────────────────────────────────────────
    #  LIFECYCLE
    # ──────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Start the background drain thread.

        The thread continuously reads ticks from the queue and
        updates the in-memory store. Call stop() to terminate.
        """
        if self._running:
            logger.warning("Aggregator is already running.")
            return

        self._running = True
        self._started_at = time.time()
        self._drain_thread = threading.Thread(
            target=self._drain_loop,
            name="TickAggregator-Drain",
            daemon=True,
        )
        self._drain_thread.start()
        logger.info("TickAggregator started (drain_interval=%.3fs).", self._drain_interval)

    def stop(self) -> None:
        """Stop the background drain thread."""
        self._running = False
        if self._drain_thread and self._drain_thread.is_alive():
            self._drain_thread.join(timeout=5.0)
            logger.info("TickAggregator stopped.")

    @property
    def is_running(self) -> bool:
        return self._running

    # ──────────────────────────────────────────────────────────
    #  PUBLIC API — Read ticks
    # ──────────────────────────────────────────────────────────

    def get_tick(self, instrument_token: int) -> Optional[TickData]:
        """
        Get the latest tick for a specific instrument.

        Returns:
            TickData or None if no tick has been received.
        """
        return self._ticks.get(instrument_token)

    def get_ticks(self, tokens: list[int]) -> dict[int, TickData]:
        """
        Get latest ticks for multiple instruments.

        Returns:
            Dict of {token: TickData} for tokens that have data.
        """
        return {t: self._ticks[t] for t in tokens if t in self._ticks}

    def get_all_ticks(self) -> dict[int, TickData]:
        """
        Get all latest ticks.

        Returns:
            Dict of {instrument_token: TickData}.
        """
        return dict(self._ticks)

    def get_ltp(self, instrument_token: int) -> Optional[float]:
        """Get just the last traded price for an instrument."""
        tick = self._ticks.get(instrument_token)
        return tick.last_price if tick else None

    def get_all_ltps(self) -> dict[int, float]:
        """Get LTP for all instruments."""
        return {
            token: tick.last_price
            for token, tick in self._ticks.items()
        }

    def get_stats(self) -> dict:
        """Return aggregator statistics."""
        now = time.time()
        uptime = now - self._started_at if self._started_at else 0

        return {
            "running": self._running,
            "uptime_seconds": round(uptime, 1),
            "unique_instruments": len(self._ticks),
            "total_ticks_processed": self._total_ticks_processed,
            "ticks_per_second": round(self._ticks_per_second, 1),
            "queue_size": self._queue.qsize() if hasattr(self._queue, 'qsize') else -1,
            "worker_heartbeats": {
                wid: round(now - ts, 1)
                for wid, ts in self._worker_heartbeats.items()
            },
        }

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — Drain loop
    # ──────────────────────────────────────────────────────────

    def _drain_loop(self) -> None:
        """
        Main drain loop — runs in a background thread.

        Continuously reads messages from the multiprocessing Queue,
        converts them to TickData, and updates the in-memory store.
        """
        logger.info("Drain loop started.")

        while self._running:
            try:
                batch = self._drain_batch()
                if batch:
                    self._process_batch(batch)
                else:
                    # No data — sleep briefly to avoid busy spin
                    time.sleep(self._drain_interval)
            except Exception as e:
                logger.error("Drain loop error: %s", e, exc_info=True)
                time.sleep(0.1)

        logger.info("Drain loop stopped.")

    def _drain_batch(self) -> list[dict]:
        """
        Read up to batch_size messages from the queue.

        Non-blocking: returns immediately with whatever is available.
        """
        batch = []
        for _ in range(self._batch_size):
            try:
                msg = self._queue.get_nowait()
                batch.append(msg)
            except Empty:
                break
        return batch

    def _process_batch(self, raw_ticks: list[dict]) -> None:
        """
        Process a batch of raw tick dicts from the queue.

        Updates the in-memory store and invokes callbacks.
        """
        tick_objects = []

        for raw in raw_ticks:
            # Handle heartbeat messages
            if raw.get("type") == "heartbeat":
                worker_id = raw.get("worker_id", "unknown")
                self._worker_heartbeats[worker_id] = time.time()
                continue

            # Handle tick batch messages (worker sends ticks in batches)
            if raw.get("type") == "tick_batch":
                ticks = raw.get("ticks", [])
                worker_id = raw.get("worker_id", "unknown")
                for tick_raw in ticks:
                    tick_raw["worker_id"] = worker_id
                    tick = TickData(tick_raw)
                    self._ticks[tick.instrument_token] = tick
                    tick_objects.append(tick)
                    if self._on_tick:
                        self._on_tick(tick)
                self._worker_heartbeats[worker_id] = time.time()
                continue

            # Handle individual tick messages
            tick = TickData(raw)
            self._ticks[tick.instrument_token] = tick
            tick_objects.append(tick)

            if self._on_tick:
                self._on_tick(tick)

        # Fire batch callback
        if tick_objects and self._on_batch:
            self._on_batch(tick_objects)

        # Update stats
        count = len(tick_objects)
        self._total_ticks_processed += count
        self._ticks_since_last_stats += count
        self._update_tps()

    def _update_tps(self) -> None:
        """Update ticks-per-second metric every second."""
        now = time.time()
        elapsed = now - self._last_stats_time
        if elapsed >= 1.0:
            self._ticks_per_second = self._ticks_since_last_stats / elapsed
            self._ticks_since_last_stats = 0
            self._last_stats_time = now
