"""
Worker Supervisor — Spawns, monitors, and manages WebSocket worker
processes that stream live market data from KiteTicker.

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                      SUPERVISOR (Main Process)               │
    │                                                               │
    │  ┌─────────────┐  ┌─────────────┐  ┌────────────────────┐   │
    │  │ ATM Resolver │  │ Instrument  │  │  Worker Manager    │   │
    │  │ (LTP → ATM)  │  │ Manager     │  │  (spawn/monitor)   │   │
    │  └──────┬──────┘  └──────┬──────┘  └────────┬───────────┘   │
    │         │                │                   │               │
    │         └────────┬───────┘                   │               │
    │                  │                           │               │
    │           Token Batches                      │               │
    │                  │                           │               │
    │         ┌────────▼────────┐                  │               │
    │         │  Split tokens   │                  │               │
    │         │  into batches   │                  │               │
    │         └────────┬────────┘                  │               │
    │                  │                           │               │
    │    ┌─────────────▼──────────────┐            │               │
    │    │                            │            │               │
    │   ┌▼──────┐  ┌───────┐  ┌─────▼┐           │               │
    │   │Worker1 │  │Worker2│  │Worker3│  ◄────────┘               │
    │   │Process │  │Process│  │Process│  (auto-restart on crash)  │
    │   └───┬────┘  └───┬───┘  └───┬───┘                          │
    │       │           │          │                                │
    │       └─────┬─────┘──────────┘                               │
    │             │                                                 │
    │       ┌─────▼─────┐                                          │
    │       │  Tick Queue │  (multiprocessing.Queue)                │
    │       └─────┬─────┘                                          │
    │             │                                                 │
    │       ┌─────▼──────┐                                         │
    │       │ Aggregator  │  (drains queue, maintains tick store)   │
    │       └────────────┘                                         │
    └─────────────────────────────────────────────────────────────┘

Usage:
    from workers.supervisor import WorkerSupervisor

    supervisor = WorkerSupervisor(credentials={...}, token_batches={...})
    supervisor.start()    # Spawns workers + aggregator
    supervisor.monitor()  # Blocking — monitors health, restarts crashed workers
    supervisor.stop()     # Graceful shutdown
"""

import os
import time
import signal
import logging
import multiprocessing as mp
from datetime import datetime
from typing import Optional

from core.aggregator import TickAggregator
from workers.stream_worker import run_worker

logger = logging.getLogger(__name__)

# How often to check worker health
MONITOR_INTERVAL = 5.0

# Max restarts before giving up on a worker
MAX_RESTARTS = 50


class WorkerSupervisor:
    """
    Manages the lifecycle of WebSocket streaming workers.

    Responsibilities:
        1. Spawn worker processes with their assigned token batches
        2. Monitor worker health (detect crashes, stalls)
        3. Auto-restart crashed workers
        4. Manage the shared tick queue and aggregator
        5. Graceful shutdown on SIGTERM/SIGINT
    """

    def __init__(
        self,
        credentials: dict,
        token_batches: dict[str, list[int]],
        mode: str = "full",
        on_tick=None,
        on_batch=None,
    ):
        """
        Args:
            credentials:   Dict containing single Kite API credential:
                           {"api_key": "...", "access_token": "..."}
            token_batches: Dict of {worker_id: [token_list]} from
                           ATMResolver.get_worker_batches().
            mode:          KiteTicker subscription mode ("ltp"/"quote"/"full").
            on_tick:       Optional per-tick callback for the aggregator.
            on_batch:      Optional batch callback for the aggregator.
        """
        self._credentials = credentials
        self._token_batches = token_batches
        self._mode = mode

        # ── Shared IPC ──
        self._tick_queue: mp.Queue = mp.Queue(maxsize=100_000)

        # ── Aggregator ──
        self._aggregator = TickAggregator(
            tick_queue=self._tick_queue,
            on_tick=on_tick,
            on_batch=on_batch,
        )

        # ── Worker processes ──
        self._workers: dict[str, mp.Process] = {}
        self._worker_restart_counts: dict[str, int] = {}

        # ── State ──
        self._running = False
        self._started_at: Optional[float] = None

    # ──────────────────────────────────────────────────────────
    #  PUBLIC API
    # ──────────────────────────────────────────────────────────

    @property
    def aggregator(self) -> TickAggregator:
        """Access the tick aggregator."""
        return self._aggregator

    @property
    def tick_queue(self) -> mp.Queue:
        """Access the shared tick queue."""
        return self._tick_queue

    def start(self) -> None:
        """
        Start the supervisor:
          1. Start the tick aggregator (background drain thread)
          2. Spawn a worker process for each token batch
        """
        self._running = True
        self._started_at = time.time()

        # Set up signal handlers
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        # Start aggregator
        self._aggregator.start()
        logger.info("Aggregator started.")

        # Spawn workers
        for worker_id, tokens in self._token_batches.items():
            self._spawn_worker(worker_id, tokens)

        logger.info(
            "Supervisor started: %d workers, %d total tokens.",
            len(self._workers),
            sum(len(t) for t in self._token_batches.values()),
        )

    def monitor(self) -> None:
        """
        Monitor worker health in a blocking loop.

        Checks each worker process periodically and restarts
        any that have crashed. Call stop() from a signal handler
        or another thread to terminate.
        """
        logger.info("Monitoring workers (interval=%.1fs)...", MONITOR_INTERVAL)

        while self._running:
            try:
                self._check_workers()
                time.sleep(MONITOR_INTERVAL)
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt — shutting down.")
                self.stop()
                break

    def stop(self) -> None:
        """
        Gracefully stop all workers and the aggregator.
        """
        logger.info("Stopping supervisor...")
        self._running = False

        # Terminate worker processes
        for worker_id, proc in self._workers.items():
            if proc.is_alive():
                logger.info("Terminating %s (pid=%d)...", worker_id, proc.pid)
                proc.terminate()
                proc.join(timeout=5.0)
                if proc.is_alive():
                    logger.warning("Force-killing %s...", worker_id)
                    proc.kill()

        # Stop aggregator
        self._aggregator.stop()

        # Cleanup queue
        try:
            self._tick_queue.close()
            self._tick_queue.join_thread()
        except Exception:
            pass

        logger.info("Supervisor stopped.")

    def get_stats(self) -> dict:
        """Return supervisor + aggregator + worker stats."""
        uptime = time.time() - self._started_at if self._started_at else 0

        worker_stats = {}
        for worker_id, proc in self._workers.items():
            worker_stats[worker_id] = {
                "alive": proc.is_alive(),
                "pid": proc.pid,
                "restart_count": self._worker_restart_counts.get(worker_id, 0),
                "tokens": len(self._token_batches.get(worker_id, [])),
            }

        return {
            "supervisor": {
                "running": self._running,
                "uptime_seconds": round(uptime, 1),
                "total_workers": len(self._workers),
                "active_workers": sum(
                    1 for p in self._workers.values() if p.is_alive()
                ),
            },
            "aggregator": self._aggregator.get_stats(),
            "workers": worker_stats,
        }

    def update_tokens(self, new_batches: dict[str, list[int]]) -> None:
        """
        Update token assignments and restart workers.

        This is called when ATM resolution changes (every 30s).
        Only restarts workers whose token list has actually changed.
        """
        for worker_id, new_tokens in new_batches.items():
            old_tokens = self._token_batches.get(worker_id, [])

            # Only restart if tokens actually changed
            if set(new_tokens) != set(old_tokens):
                logger.info(
                    "Token update for %s: %d → %d tokens",
                    worker_id, len(old_tokens), len(new_tokens),
                )
                self._token_batches[worker_id] = new_tokens
                self._restart_worker(worker_id)

        # Handle removed workers
        for worker_id in list(self._workers.keys()):
            if worker_id not in new_batches:
                logger.info("Removing worker %s (no longer needed).", worker_id)
                self._stop_worker(worker_id)
                del self._token_batches[worker_id]

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — Worker Management
    # ──────────────────────────────────────────────────────────

    def _spawn_worker(self, worker_id: str, tokens: list[int]) -> None:
        """Spawn a new worker process."""
        cred = self._credentials

        proc = mp.Process(
            target=run_worker,
            kwargs={
                "worker_id": worker_id,
                "api_key": cred["api_key"],
                "access_token": cred["access_token"],
                "tokens": tokens,
                "tick_queue": self._tick_queue,
                "mode": self._mode,
            },
            name=worker_id,
            daemon=True,
        )
        proc.start()
        self._workers[worker_id] = proc
        self._worker_restart_counts.setdefault(worker_id, 0)

        logger.info(
            "Spawned %s (pid=%d, tokens=%d)",
            worker_id, proc.pid, len(tokens)
        )

    def _stop_worker(self, worker_id: str) -> None:
        """Stop a specific worker process."""
        proc = self._workers.get(worker_id)
        if proc and proc.is_alive():
            proc.terminate()
            proc.join(timeout=5.0)
            if proc.is_alive():
                proc.kill()

    def _restart_worker(self, worker_id: str) -> None:
        """Restart a specific worker."""
        self._stop_worker(worker_id)
        tokens = self._token_batches.get(worker_id, [])
        if tokens:
            self._worker_restart_counts[worker_id] = (
                self._worker_restart_counts.get(worker_id, 0) + 1
            )
            self._spawn_worker(worker_id, tokens)

    def _check_workers(self) -> None:
        """
        Check all worker processes and restart crashed ones.
        """
        for worker_id in list(self._workers.keys()):
            proc = self._workers[worker_id]

            if not proc.is_alive() and self._running:
                restart_count = self._worker_restart_counts.get(worker_id, 0)

                if restart_count >= MAX_RESTARTS:
                    logger.error(
                        "%s has crashed %d times. Not restarting.",
                        worker_id, restart_count,
                    )
                    continue

                exit_code = proc.exitcode
                logger.warning(
                    "%s died (exit_code=%s, restarts=%d). Restarting...",
                    worker_id, exit_code, restart_count,
                )
                self._restart_worker(worker_id)

    def _handle_signal(self, signum, frame) -> None:
        """Handle SIGTERM/SIGINT for graceful shutdown."""
        sig_name = signal.Signals(signum).name
        logger.info("Received %s. Initiating graceful shutdown...", sig_name)
        self.stop()
