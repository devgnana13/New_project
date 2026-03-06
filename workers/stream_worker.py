"""
Stream Worker — A standalone process that connects to Kite WebSocket
(KiteTicker), subscribes to instruments, and publishes ticks to a
shared aggregator via a multiprocessing Queue.

Architecture:
    ┌─────────────────────────────────────────────────┐
    │              STREAM WORKER PROCESS               │
    │                                                   │
    │  ┌────────────┐     ┌──────────┐     ┌────────┐ │
    │  │ KiteTicker  │────▶│ on_ticks │────▶│ Queue  │ │
    │  │ WebSocket   │     │ handler  │     │ (IPC)  │ │
    │  └──────┬─────┘     └──────────┘     └────────┘ │
    │         │                                        │
    │    Auto-reconnect                                │
    │    on disconnect                                 │
    │                                                   │
    │  In-memory: {token → latest_tick}                │
    └─────────────────────────────────────────────────┘

Each worker:
    - Runs as a separate OS process (via multiprocessing)
    - Subscribes to ≤3000 instrument tokens
    - Maintains latest tick data in local memory
    - Publishes ticks to the shared Queue for the aggregator
    - Auto-reconnects on WebSocket disconnect with exponential backoff
    - Sends periodic heartbeat messages to the aggregator

Usage (called by the supervisor, not directly):
    from workers.stream_worker import StreamWorker

    worker = StreamWorker(
        worker_id="worker_1",
        api_key="xxx",
        access_token="yyy",
        tokens=[12345, 67890, ...],
        tick_queue=multiprocessing.Queue(),
    )
    worker.start()   # Blocking — runs the WebSocket event loop
"""

import os
import time
import signal
import logging
import threading
from datetime import datetime
from typing import Optional
from multiprocessing import Queue

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
#  CONSTANTS
# ──────────────────────────────────────────────────────────────

# Reconnect backoff
INITIAL_RECONNECT_DELAY = 1.0     # seconds
MAX_RECONNECT_DELAY = 60.0        # seconds
RECONNECT_BACKOFF_FACTOR = 2.0

# Heartbeat
HEARTBEAT_INTERVAL = 10.0         # seconds

# Tick batching for queue efficiency
TICK_BATCH_SIZE = 100             # Flush to queue every N ticks
TICK_BATCH_TIMEOUT = 0.5          # ... or every 0.5 seconds


class StreamWorker:
    """
    A WebSocket streaming worker that connects to KiteTicker,
    subscribes to instrument tokens, and forwards ticks to a
    shared aggregator queue.

    Designed to run as a standalone process spawned by the supervisor.
    """

    def __init__(
        self,
        worker_id: str,
        api_key: str,
        access_token: str,
        tokens: list[int],
        tick_queue: Queue,
        mode: str = "full",
    ):
        """
        Args:
            worker_id:     Unique identifier (e.g., "worker_1").
            api_key:       Kite Connect API key.
            access_token:  Kite Connect access token.
            tokens:        List of instrument tokens to subscribe.
            tick_queue:    Multiprocessing Queue for publishing ticks.
            mode:          KiteTicker subscription mode:
                           "ltp" | "quote" | "full" (default: "full").
        """
        self.worker_id = worker_id
        self._api_key = api_key
        self._access_token = access_token
        self._tokens = tokens
        self._tick_queue = tick_queue
        self._mode = mode

        # ── In-memory tick store ──
        self._latest_ticks: dict[int, dict] = {}

        # ── KiteTicker instance (created on start) ──
        self._ticker = None

        # ── State ──
        self._running = False
        self._connected = False
        self._reconnect_delay = INITIAL_RECONNECT_DELAY
        self._connect_count = 0
        self._disconnect_count = 0
        self._total_ticks = 0
        self._started_at: Optional[float] = None

        # ── Tick batching ──
        self._tick_buffer: list[dict] = []
        self._last_flush_time = time.time()
        self._flush_lock = threading.Lock()

        # ── Heartbeat ──
        self._heartbeat_thread: Optional[threading.Thread] = None

    # ──────────────────────────────────────────────────────────
    #  PUBLIC API
    # ──────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Start the WebSocket worker. This is BLOCKING — it runs the
        KiteTicker event loop until stop() is called or the process
        is terminated.

        Called by the supervisor in a child process.
        """
        self._running = True
        self._started_at = time.time()

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        # Configure logging for this worker process
        logging.basicConfig(
            level=logging.INFO,
            format=f"%(asctime)s [{self.worker_id}] %(levelname)s %(message)s",
        )

        logger.info(
            "Starting %s with %d tokens (mode=%s)",
            self.worker_id, len(self._tokens), self._mode,
        )

        # Start heartbeat thread
        self._start_heartbeat()

        # Connect to KiteTicker
        self._connect()

    def stop(self) -> None:
        """Stop the worker gracefully."""
        logger.info("Stopping %s...", self.worker_id)
        self._running = False

        # Flush remaining ticks
        self._flush_tick_buffer()

        if self._ticker:
            try:
                self._ticker.close()
            except Exception:
                pass

        self._connected = False
        logger.info("%s stopped.", self.worker_id)

    def get_latest_tick(self, token: int) -> Optional[dict]:
        """Get the latest tick data for a specific instrument."""
        return self._latest_ticks.get(token)

    def get_all_latest_ticks(self) -> dict[int, dict]:
        """Get all latest tick data."""
        return dict(self._latest_ticks)

    def get_stats(self) -> dict:
        """Return worker statistics."""
        uptime = time.time() - self._started_at if self._started_at else 0
        return {
            "worker_id": self.worker_id,
            "running": self._running,
            "connected": self._connected,
            "uptime_seconds": round(uptime, 1),
            "subscribed_tokens": len(self._tokens),
            "unique_instruments_seen": len(self._latest_ticks),
            "total_ticks": self._total_ticks,
            "connect_count": self._connect_count,
            "disconnect_count": self._disconnect_count,
            "mode": self._mode,
        }

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — WebSocket Connection
    # ──────────────────────────────────────────────────────────

    def _connect(self) -> None:
        """
        Create a KiteTicker instance and connect.
        """
        from kiteconnect import KiteTicker

        try:
            self._ticker = KiteTicker(
                api_key=self._api_key,
                access_token=self._access_token,
            )

            # Assign callbacks
            self._ticker.on_ticks = self._on_ticks
            self._ticker.on_connect = self._on_connect
            self._ticker.on_close = self._on_close
            self._ticker.on_error = self._on_error
            self._ticker.on_reconnect = self._on_reconnect
            self._ticker.on_noreconnect = self._on_noreconnect
            self._ticker.on_order_update = self._on_order_update

            logger.info("Connecting to KiteTicker...")

            # connect() is blocking — it runs the WebSocket event loop
            # threaded=True lets it run its own event loop and auto-reconnects
            self._ticker.connect(threaded=True)

            # Keep the main thread alive while the worker is running
            while self._running:
                time.sleep(1)
                self._check_and_flush()

        except Exception as e:
            logger.error("KiteTicker connection error: %s", e, exc_info=True)

    def _wait_before_reconnect(self) -> None:
        pass  # deprecated

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — KiteTicker Callbacks
    # ──────────────────────────────────────────────────────────

    def _on_connect(self, ws, response) -> None:
        """Called when WebSocket connection is established."""
        self._connected = True
        self._connect_count += 1
        self._reconnect_delay = INITIAL_RECONNECT_DELAY  # Reset backoff

        logger.info(
            "Connected! Subscribing to %d tokens...", len(self._tokens),
        )

        # Subscribe in chunks (KiteTicker has a per-message limit)
        chunk_size = 500
        for i in range(0, len(self._tokens), chunk_size):
            chunk = self._tokens[i:i + chunk_size]
            ws.subscribe(chunk)
            ws.set_mode(ws.MODE_FULL if self._mode == "full"
                       else ws.MODE_QUOTE if self._mode == "quote"
                       else ws.MODE_LTP, chunk)

        logger.info(
            "Subscribed to %d tokens in %s mode.",
            len(self._tokens), self._mode,
        )

    def _on_ticks(self, ws, ticks: list[dict]) -> None:
        """
        Called when ticks are received from the WebSocket.

        Each tick dict from KiteTicker contains (in full mode):
            instrument_token, last_price, volume, timestamp,
            change, oi, ohlc, depth, etc.

        We extract the essential fields, update local memory,
        and buffer for publishing to the aggregator queue.
        """
        for tick in ticks:
            token = tick.get("instrument_token")
            if token is None:
                continue

            # Extract essential fields
            processed = {
                "instrument_token": token,
                "last_price": tick.get("last_price", 0.0),
                "volume": tick.get("volume_traded", 0) or tick.get("volume", 0),
                "timestamp": tick.get("exchange_timestamp") or tick.get("timestamp"),
                "change": tick.get("change", 0.0),
                "oi": tick.get("oi", 0),
                "tradingsymbol": tick.get("tradingsymbol", ""),
            }

            # Update local memory
            self._latest_ticks[token] = processed
            self._total_ticks += 1

            # Add to buffer for queue publishing
            with self._flush_lock:
                self._tick_buffer.append(processed)

        # Check if buffer should be flushed
        self._check_and_flush()

    def _on_close(self, ws, code, reason) -> None:
        """Called when WebSocket connection closes."""
        self._connected = False
        self._disconnect_count += 1
        logger.warning(
            "WebSocket closed (code=%s, reason=%s). Disconnects: %d",
            code, reason, self._disconnect_count,
        )

    def _on_error(self, ws, code, reason) -> None:
        """Called on WebSocket error."""
        logger.error(
            "WebSocket error (code=%s, reason=%s)", code, reason,
        )

    def _on_reconnect(self, ws, attempts_count) -> None:
        """Called when KiteTicker attempts to reconnect."""
        logger.info("Reconnect attempt %d...", attempts_count)

    def _on_noreconnect(self, ws) -> None:
        """Called when KiteTicker gives up reconnecting."""
        logger.error(
            "KiteTicker gave up reconnecting. Will retry manually."
        )

    def _on_order_update(self, ws, data) -> None:
        """Called on order updates (not used, but required callback)."""
        pass

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — Tick Batching & Queue Publishing
    # ──────────────────────────────────────────────────────────

    def _check_and_flush(self) -> None:
        """Flush tick buffer if batch size or timeout is reached."""
        with self._flush_lock:
            should_flush = (
                len(self._tick_buffer) >= TICK_BATCH_SIZE
                or (
                    self._tick_buffer
                    and time.time() - self._last_flush_time >= TICK_BATCH_TIMEOUT
                )
            )

        if should_flush:
            self._flush_tick_buffer()

    def _flush_tick_buffer(self) -> None:
        """
        Send buffered ticks to the aggregator queue as a batch.

        Uses a batch message format to reduce queue overhead.
        """
        with self._flush_lock:
            if not self._tick_buffer:
                return
            batch = self._tick_buffer.copy()
            self._tick_buffer.clear()
            self._last_flush_time = time.time()

        try:
            self._tick_queue.put_nowait({
                "type": "tick_batch",
                "worker_id": self.worker_id,
                "ticks": batch,
                "count": len(batch),
                "timestamp": time.time(),
            })
        except Exception as e:
            logger.error("Failed to publish tick batch to queue: %s", e)

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — Heartbeat
    # ──────────────────────────────────────────────────────────

    def _start_heartbeat(self) -> None:
        """Start a background thread that sends periodic heartbeats."""
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self.worker_id}-Heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        """Send heartbeat messages to the aggregator queue."""
        while self._running:
            try:
                self._tick_queue.put_nowait({
                    "type": "heartbeat",
                    "worker_id": self.worker_id,
                    "connected": self._connected,
                    "total_ticks": self._total_ticks,
                    "unique_instruments": len(self._latest_ticks),
                    "timestamp": time.time(),
                })
            except Exception:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — Signal handling
    # ──────────────────────────────────────────────────────────

    def _handle_signal(self, signum, frame) -> None:
        """Handle SIGTERM/SIGINT for graceful shutdown."""
        logger.info(
            "Received signal %s. Shutting down %s...",
            signal.Signals(signum).name, self.worker_id,
        )
        self.stop()


# ──────────────────────────────────────────────────────────────
#  ENTRY POINT — Called by supervisor via multiprocessing
# ──────────────────────────────────────────────────────────────


def run_worker(
    worker_id: str,
    api_key: str,
    access_token: str,
    tokens: list[int],
    tick_queue: Queue,
    mode: str = "full",
) -> None:
    """
    Entry point for a worker process.

    This function is called by the supervisor via multiprocessing.Process.
    It creates a StreamWorker and runs it until stopped.

    Args:
        worker_id:     Unique worker identifier.
        api_key:       Kite API key.
        access_token:  Kite access token.
        tokens:        Instrument tokens to subscribe.
        tick_queue:    Shared multiprocessing Queue.
        mode:          Subscription mode ("ltp", "quote", "full").
    """
    worker = StreamWorker(
        worker_id=worker_id,
        api_key=api_key,
        access_token=access_token,
        tokens=tokens,
        tick_queue=tick_queue,
        mode=mode,
    )
    worker.start()
