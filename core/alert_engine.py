"""
Alert Engine — Monitors live option volumes and triggers alerts
when CE or PE volume exceeds 2× yesterday's EOD total.

Architecture:
    ┌────────────────────────────────────────────────────────────┐
    │                      ALERT ENGINE                          │
    │                                                            │
    │  ┌────────────────┐    ┌─────────────────┐                │
    │  │ VolumeAggregator│    │ DatabaseManager  │               │
    │  │ (live volumes)  │    │ (yesterday EOD)  │               │
    │  └───────┬────────┘    └───────┬─────────┘               │
    │          │                     │                           │
    │          └──────────┬──────────┘                           │
    │                     │                                      │
    │            ┌────────▼─────────┐                            │
    │            │ Compare per sym  │                            │
    │            │                  │                            │
    │            │ live_CE ≥ 2×yd_CE│  → CALL_VOLUME_SPIKE      │
    │            │ live_PE ≥ 2×yd_PE│  → PUT_VOLUME_SPIKE       │
    │            └────────┬─────────┘                            │
    │                     │                                      │
    │            ┌────────▼─────────┐                            │
    │            │ Dedup check      │                            │
    │            │ (1 alert / sym   │                            │
    │            │  / type / day)   │                            │
    │            └────────┬─────────┘                            │
    │                     │                                      │
    │            ┌────────▼─────────┐                            │
    │            │ Store in MongoDB │                            │
    │            │ alerts collection│                            │
    │            └──────────────────┘                            │
    └────────────────────────────────────────────────────────────┘

Alert Schema (in MongoDB):
    {
        symbol: "RELIANCE",
        alert_type: "CALL_VOLUME_SPIKE",
        live_volume: 350000,
        yesterday_volume: 150000,
        multiplier: 2.33,
        timestamp: ISODate("2026-03-05T10:30:00Z"),
        date: "2026-03-05",
        message: "RELIANCE CALL volume spiked to 2.3x ...",
        data: { ... }
    }

Usage:
    from core.alert_engine import AlertEngine

    engine = AlertEngine(
        volume_aggregator=vol_agg,
        database=db,
    )
    engine.start()   # Checks every 60 seconds

    # Or check manually:
    alerts = engine.check_now()
    # [{"symbol": "RELIANCE", "alert_type": "CALL_VOLUME_SPIKE", ...}]
"""

import time
import logging
import threading
from datetime import datetime
from typing import Optional, Callable

from core.volume_aggregator import VolumeAggregator
from core.database import DatabaseManager
from config import now_ist, today_ist

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
#  CONSTANTS
# ──────────────────────────────────────────────────────────────

ALERT_TYPE_CALL = "CALL_VOLUME_SPIKE"
ALERT_TYPE_PUT = "PUT_VOLUME_SPIKE"

DEFAULT_CHECK_INTERVAL = 60.0   # seconds
DEFAULT_MULTIPLIER = 2.0        # alert when live >= 2× yesterday


class AlertEngine:
    """
    Compares live option volumes against yesterday's EOD totals
    and triggers alerts when volume exceeds a configured multiplier.

    Alerts fire at most once per symbol per alert type per day.
    Results are stored in MongoDB and optionally forwarded to a
    callback for real-time notification.
    """

    def __init__(
        self,
        volume_aggregator: VolumeAggregator,
        database: DatabaseManager,
        multiplier: float = DEFAULT_MULTIPLIER,
        check_interval: float = DEFAULT_CHECK_INTERVAL,
        on_alert: Optional[Callable[[dict], None]] = None,
        symbols: list[str] = None,
    ):
        """
        Args:
            volume_aggregator:  Live volume source.
            database:           MongoDB manager for EOD data + alert storage.
            multiplier:         Alert threshold multiplier (default: 2.0).
            check_interval:     Seconds between checks (default: 60).
            on_alert:           Optional callback on each new alert.
            symbols:            Symbols to monitor. If None, monitors all
                                symbols tracked by the VolumeAggregator.
        """
        self._vol_agg = volume_aggregator
        self._db = database
        self._multiplier = multiplier
        self._check_interval = check_interval
        self._on_alert = on_alert
        self._symbols = symbols

        # ── In-memory dedup: tracks which alerts have fired today ──
        # Key: (symbol, alert_type), Value: date string
        self._fired_today: dict[tuple[str, str], str] = {}

        # ── Yesterday's EOD cache (refreshed once per day) ──
        self._yesterday_cache: dict[str, dict] = {}
        self._yesterday_cache_date: Optional[str] = None

        # ── Background thread ──
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._started_at: Optional[float] = None

        # ── Stats ──
        self._total_checks = 0
        self._total_alerts_fired = 0
        self._last_check_time: Optional[float] = None

    # ──────────────────────────────────────────────────────────
    #  LIFECYCLE
    # ──────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Start the background alert-checking thread.

        Checks volumes every `check_interval` seconds.
        """
        if self._running:
            logger.warning("AlertEngine is already running.")
            return

        self._running = True
        self._started_at = time.time()
        self._thread = threading.Thread(
            target=self._check_loop,
            name="AlertEngine-Check",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "AlertEngine started (interval=%.0fs, multiplier=%.1fx).",
            self._check_interval, self._multiplier,
        )

    def stop(self) -> None:
        """Stop the background thread."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
            logger.info("AlertEngine stopped.")

    @property
    def is_running(self) -> bool:
        return self._running

    # ──────────────────────────────────────────────────────────
    #  PUBLIC API
    # ──────────────────────────────────────────────────────────

    def check_now(self) -> list[dict]:
        """
        Manually trigger an alert check.

        Compares current live volumes against yesterday's EOD for
        every monitored symbol. Returns list of newly fired alerts.

        Returns:
            List of alert dicts that were triggered.
        """
        return self._run_check()

    def get_fired_alerts(self) -> list[dict]:
        """
        Get all alerts fired today (from in-memory cache).

        Returns:
            List of (symbol, alert_type) pairs fired today.
        """
        today = today_ist()
        return [
            {"symbol": sym, "alert_type": atype}
            for (sym, atype), date in self._fired_today.items()
            if date == today
        ]

    def reset_daily(self) -> None:
        """
        Reset the daily dedup cache.

        Call this at the start of each trading day (e.g., 09:15 IST)
        to allow alerts to fire again.
        """
        self._fired_today.clear()
        self._yesterday_cache.clear()
        self._yesterday_cache_date = None
        logger.info("AlertEngine daily state reset.")

    def get_stats(self) -> dict:
        """Return engine statistics."""
        uptime = time.time() - self._started_at if self._started_at else 0
        today = today_ist()
        alerts_today = sum(
            1 for d in self._fired_today.values() if d == today
        )
        return {
            "running": self._running,
            "uptime_seconds": round(uptime, 1),
            "multiplier": self._multiplier,
            "check_interval": self._check_interval,
            "total_checks": self._total_checks,
            "total_alerts_fired": self._total_alerts_fired,
            "alerts_today": alerts_today,
            "yesterday_symbols_cached": len(self._yesterday_cache),
        }

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — Check loop
    # ──────────────────────────────────────────────────────────

    def _check_loop(self) -> None:
        """Background loop that runs checks at the configured interval."""
        logger.info("Alert check loop started.")

        while self._running:
            try:
                self._run_check()
            except Exception as e:
                logger.error("Alert check error: %s", e, exc_info=True)

            time.sleep(self._check_interval)

        logger.info("Alert check loop stopped.")

    def _run_check(self) -> list[dict]:
        """
        Execute one full alert check cycle.

        Steps:
            1. Load yesterday's EOD volumes (cached per day)
            2. Get current live volumes from VolumeAggregator
            3. Compare each symbol: live >= multiplier × yesterday
            4. Fire alerts for new spikes (dedup per day)
        """
        today = today_ist()
        self._total_checks += 1
        self._last_check_time = time.time()

        # Reset dedup cache if a new day started
        self._reset_if_new_day(today)

        # Step 1: Load yesterday's EOD volumes
        yesterday_volumes = self._get_yesterday_volumes()

        if not yesterday_volumes:
            logger.debug("No yesterday EOD data available. Skipping check.")
            return []

        # Step 2: Get live volumes
        live_volumes = self._vol_agg.get_volumes()

        if not live_volumes:
            return []

        # Step 3 + 4: Compare and fire alerts
        new_alerts = []
        symbols = self._symbols or list(live_volumes.keys())

        for symbol in symbols:
            symbol = symbol.upper()
            live = live_volumes.get(symbol)
            yesterday = yesterday_volumes.get(symbol)

            if not live or not yesterday:
                continue

            # Check CALL volume spike
            call_alert = self._check_spike(
                symbol=symbol,
                alert_type=ALERT_TYPE_CALL,
                live_volume=live["call_volume"],
                yesterday_volume=yesterday.get("call_volume_total", 0),
                today=today,
            )
            if call_alert:
                new_alerts.append(call_alert)

            # Check PUT volume spike
            put_alert = self._check_spike(
                symbol=symbol,
                alert_type=ALERT_TYPE_PUT,
                live_volume=live["put_volume"],
                yesterday_volume=yesterday.get("put_volume_total", 0),
                today=today,
            )
            if put_alert:
                new_alerts.append(put_alert)

        if new_alerts:
            logger.info(
                "Alert check fired %d new alerts.", len(new_alerts),
            )

        return new_alerts

    def _check_spike(
        self,
        symbol: str,
        alert_type: str,
        live_volume: int,
        yesterday_volume: int,
        today: str,
    ) -> Optional[dict]:
        """
        Check if live volume exceeds multiplier × yesterday's volume.

        Returns:
            Alert dict if triggered and not already fired today.
            None otherwise.
        """
        # Skip if yesterday had no volume (avoid false alerts)
        if yesterday_volume <= 0:
            return None

        # Skip if live volume hasn't reached threshold
        threshold = self._multiplier * yesterday_volume
        if live_volume < threshold:
            return None

        # Dedup: already fired today?
        dedup_key = (symbol, alert_type)
        if self._fired_today.get(dedup_key) == today:
            return None

        # ── Fire alert! ──
        multiplier = round(live_volume / yesterday_volume, 2)
        timestamp = now_ist()

        alert = {
            "symbol": symbol,
            "alert_type": alert_type,
            "live_volume": live_volume,
            "yesterday_volume": yesterday_volume,
            "multiplier": multiplier,
            "timestamp": timestamp,
        }

        message = (
            f"{symbol} {alert_type.replace('_', ' ')}: "
            f"Live volume {live_volume:,} is {multiplier}x "
            f"yesterday's {yesterday_volume:,}"
        )

        # Store in MongoDB
        self._db.store_alert(
            symbol=symbol,
            alert_type=alert_type,
            message=message,
            data={
                "live_volume": live_volume,
                "yesterday_volume": yesterday_volume,
                "multiplier": multiplier,
                "timestamp": timestamp.isoformat(),
            },
            date=today,
        )

        # Mark as fired
        self._fired_today[dedup_key] = today
        self._total_alerts_fired += 1

        logger.warning("🚨 ALERT: %s", message)

        # Fire callback
        if self._on_alert:
            try:
                self._on_alert(alert)
            except Exception as e:
                logger.error("on_alert callback error: %s", e)

        return alert

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — Yesterday's EOD cache
    # ──────────────────────────────────────────────────────────

    def _get_yesterday_volumes(self) -> dict[str, dict]:
        """
        Get yesterday's EOD volumes, cached for the current day.

        Fetches from MongoDB once per day and caches in memory.
        """
        today = today_ist()

        # Return cached if already loaded today
        if self._yesterday_cache_date == today and self._yesterday_cache:
            return self._yesterday_cache

        # Fetch from MongoDB
        symbols = self._symbols or list(self._vol_agg.get_volumes().keys())
        cache = {}

        for symbol in symbols:
            symbol = symbol.upper()
            vol = self._db.get_yesterday_volume(symbol)
            if vol:
                cache[symbol] = vol

        self._yesterday_cache = cache
        self._yesterday_cache_date = today

        logger.info(
            "Loaded yesterday's EOD data for %d/%d symbols.",
            len(cache), len(symbols),
        )
        return cache

    def _reset_if_new_day(self, today: str) -> None:
        """Reset dedup cache if a new trading day has started."""
        if self._yesterday_cache_date and self._yesterday_cache_date != today:
            logger.info("New day detected. Resetting alert state.")
            self._fired_today.clear()
            self._yesterday_cache.clear()
            self._yesterday_cache_date = None
