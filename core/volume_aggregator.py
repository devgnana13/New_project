"""
Volume Aggregator — Computes real-time CE and PE volume totals
for each stock across ATM ±10 strikes.

Architecture:
    ┌──────────────────────────────────────────────────────────┐
    │                  VOLUME AGGREGATOR                        │
    │                                                          │
    │  ┌──────────────┐        ┌────────────────────────────┐  │
    │  │ ATM Resolver  │───────▶│ Token Map                  │  │
    │  │ (token→symbol │        │ {token: (symbol, CE/PE)}   │  │
    │  │  + CE/PE)     │        └────────────┬───────────────┘  │
    │  └──────────────┘                     │                  │
    │                                        │                  │
    │  ┌──────────────┐        ┌────────────▼───────────────┐  │
    │  │ Tick          │───────▶│ Volume Summer              │  │
    │  │ Aggregator    │        │ ∑ CE volumes per symbol    │  │
    │  │ (live ticks)  │        │ ∑ PE volumes per symbol    │  │
    │  └──────────────┘        └────────────┬───────────────┘  │
    │                                        │                  │
    │                           ┌────────────▼───────────────┐  │
    │                           │ Volume Store (updated 1/s)  │  │
    │                           │ {"RELIANCE": {              │  │
    │                           │   "call_volume": 150000,    │  │
    │                           │   "put_volume": 120000      │  │
    │                           │ }}                          │  │
    │                           └────────────────────────────┘  │
    └──────────────────────────────────────────────────────────┘

Design Goals:
    1. Build a token → (symbol, option_type) mapping from ATM resolution.
    2. Every second, read latest tick volumes from TickAggregator.
    3. Sum CE and PE volumes per symbol across ATM ±10 strikes.
    4. Maintain a thread-safe shared dictionary with results.
    5. Support dynamic re-mapping when ATM resolution changes.

Usage:
    from core.volume_aggregator import VolumeAggregator

    vol_agg = VolumeAggregator(
        tick_aggregator=tick_agg,
        atm_resolver=resolver,
    )
    vol_agg.start()

    # Read volumes:
    volumes = vol_agg.get_volumes()
    # {"RELIANCE": {"call_volume": 150000, "put_volume": 120000}, ...}

    reliance = vol_agg.get_symbol_volume("RELIANCE")
    # {"call_volume": 150000, "put_volume": 120000}
"""

import time
import logging
import threading
from datetime import datetime
from typing import Optional, Callable

from core.aggregator import TickAggregator

logger = logging.getLogger(__name__)

# Default update interval
DEFAULT_UPDATE_INTERVAL = 1.0  # seconds


class SymbolVolume:
    """
    Volume snapshot for a single symbol.

    Attributes:
        symbol:         Underlying stock/index name.
        call_volume:    Total volume across all CE strikes (ATM ±10).
        put_volume:     Total volume across all PE strikes (ATM ±10).
        call_tokens:    Number of CE tokens with data.
        put_tokens:     Number of PE tokens with data.
        updated_at:     Timestamp of last update.
    """
    __slots__ = (
        "symbol", "call_volume", "put_volume",
        "call_tokens", "put_tokens", "updated_at",
    )

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.call_volume: int = 0
        self.put_volume: int = 0
        self.call_tokens: int = 0
        self.put_tokens: int = 0
        self.updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "call_volume": self.call_volume,
            "put_volume": self.put_volume,
            "call_tokens": self.call_tokens,
            "put_tokens": self.put_tokens,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self) -> str:
        return (
            f"SymbolVolume({self.symbol}: "
            f"CE={self.call_volume:,}, PE={self.put_volume:,})"
        )


class VolumeAggregator:
    """
    Aggregates CE and PE volumes per stock from live tick data.

    Reads tick volumes from the TickAggregator every second and
    sums them by symbol and option type using a mapping built
    from the ATM resolution.
    """

    def __init__(
        self,
        tick_aggregator: TickAggregator,
        update_interval: float = DEFAULT_UPDATE_INTERVAL,
        on_update: Optional[Callable[[dict[str, dict]], None]] = None,
    ):
        """
        Args:
            tick_aggregator:  The TickAggregator that holds live tick data.
            update_interval:  How often to re-compute volumes (default: 1s).
            on_update:        Optional callback invoked after each update
                              with the full volume dict.
        """
        self._tick_agg = tick_aggregator
        self._update_interval = update_interval
        self._on_update = on_update

        # ── Token mapping: instrument_token → (symbol, option_type, lot_size, strike) ──
        self._token_map: dict[int, tuple[str, str, int, float]] = {}

        # ── Per-symbol token groups for efficient lookup ──
        # {symbol: {"CE": [token1, token2, ...], "PE": [...]}}
        self._symbol_tokens: dict[str, dict[str, list[int]]] = {}

        # ── Volume store ──
        self._volumes: dict[str, SymbolVolume] = {}
        self._lock = threading.Lock()

        # ── Background thread ──
        self._thread: Optional[threading.Thread] = None
        self._running = False

        # ── Stats ──
        self._update_count = 0
        self._last_update_time: Optional[float] = None
        self._last_update_duration: Optional[float] = None
        self._started_at: Optional[float] = None

    # ──────────────────────────────────────────────────────────
    #  TOKEN MAPPING
    # ──────────────────────────────────────────────────────────

    def build_token_map(self, atm_resolutions: dict) -> None:
        """
        Build the token → (symbol, option_type, lot_size) mapping from
        ATM resolution results.

        This should be called:
          1. After initial ATM resolution
          2. Whenever ATM resolution is refreshed (every 30s)

        Args:
            atm_resolutions: Dict of {symbol: ATMResolution} from
                             ATMResolver.resolve_all().
        """
        new_token_map: dict[int, tuple[str, str, int]] = {}
        new_symbol_tokens: dict[str, dict[str, list[int]]] = {}

        for symbol, resolution in atm_resolutions.items():
            ce_tokens = []
            pe_tokens = []

            for token_dict in resolution.tokens:
                token = token_dict["instrument_token"]
                opt_type = token_dict["option_type"]
                lot_size = token_dict.get("lot_size", 1) or 1
                strike = token_dict.get("strike", 0.0)
                
                new_token_map[token] = (symbol, opt_type, lot_size, strike)

                if opt_type == "CE":
                    ce_tokens.append(token)
                elif opt_type == "PE":
                    pe_tokens.append(token)

            new_symbol_tokens[symbol] = {
                "CE": ce_tokens,
                "PE": pe_tokens,
            }

        # Atomic swap
        with self._lock:
            self._token_map = new_token_map
            self._symbol_tokens = new_symbol_tokens

            # Initialize volume entries for new symbols
            for symbol in new_symbol_tokens:
                if symbol not in self._volumes:
                    self._volumes[symbol] = SymbolVolume(symbol)

            # Remove symbols no longer in resolution
            for symbol in list(self._volumes.keys()):
                if symbol not in new_symbol_tokens:
                    del self._volumes[symbol]

        logger.info(
            "Token map rebuilt: %d tokens across %d symbols.",
            len(new_token_map), len(new_symbol_tokens),
        )

    def build_token_map_from_list(self, token_details: list[dict]) -> None:
        """
        Build the token map from a flat list of instrument dicts.

        Alternative to build_token_map() — accepts the output of
        ATMResolver.get_all_tokens_with_details().

        Args:
            token_details: List of dicts with instrument_token,
                           symbol, option_type, lot_size.
        """
        new_token_map: dict[int, tuple[str, str, int]] = {}
        new_symbol_tokens: dict[str, dict[str, list[int]]] = {}

        for td in token_details:
            token = td["instrument_token"]
            symbol = td["symbol"]
            opt_type = td["option_type"]
            lot_size = td.get("lot_size", 1) or 1
            strike = td.get("strike", 0.0)

            new_token_map[token] = (symbol, opt_type, lot_size, strike)
            if symbol not in new_symbol_tokens:
                new_symbol_tokens[symbol] = {"CE": [], "PE": []}
            new_symbol_tokens[symbol][opt_type].append(token)

        with self._lock:
            self._token_map = new_token_map
            self._symbol_tokens = new_symbol_tokens

            for symbol in new_symbol_tokens:
                if symbol not in self._volumes:
                    self._volumes[symbol] = SymbolVolume(symbol)

            for symbol in list(self._volumes.keys()):
                if symbol not in new_symbol_tokens:
                    del self._volumes[symbol]

        logger.info(
            "Token map rebuilt from list: %d tokens across %d symbols.",
            len(new_token_map), len(new_symbol_tokens),
        )

    # ──────────────────────────────────────────────────────────
    #  LIFECYCLE
    # ──────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Start the background volume update thread.

        The thread wakes every `update_interval` seconds, reads
        tick volumes from the TickAggregator, and recomputes
        CE/PE totals per symbol.
        """
        if self._running:
            logger.warning("VolumeAggregator is already running.")
            return

        self._running = True
        self._started_at = time.time()
        self._thread = threading.Thread(
            target=self._update_loop,
            name="VolumeAggregator-Update",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "VolumeAggregator started (interval=%.1fs).",
            self._update_interval,
        )

    def stop(self) -> None:
        """Stop the background update thread."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
            logger.info("VolumeAggregator stopped.")

    @property
    def is_running(self) -> bool:
        return self._running

    # ──────────────────────────────────────────────────────────
    #  PUBLIC API — Read volumes
    # ──────────────────────────────────────────────────────────

    def get_volumes(self) -> dict[str, dict]:
        """
        Get volume totals for all symbols.

        Returns:
            Dict of {symbol: {"call_volume": int, "put_volume": int}}
        """
        with self._lock:
            return {
                symbol: {
                    "call_volume": sv.call_volume,
                    "put_volume": sv.put_volume,
                }
                for symbol, sv in self._volumes.items()
            }

    def get_symbol_volume(self, symbol: str) -> Optional[dict]:
        """
        Get volume totals for a specific symbol.

        Returns:
            {"call_volume": int, "put_volume": int} or None.
        """
        symbol = symbol.upper()
        with self._lock:
            sv = self._volumes.get(symbol)
            if sv is None:
                return None
            return {
                "call_volume": sv.call_volume,
                "put_volume": sv.put_volume,
            }

    def get_detailed_volumes(self) -> dict[str, dict]:
        """
        Get detailed volume data for all symbols.

        Returns:
            Dict of {symbol: SymbolVolume.to_dict()}.
        """
        with self._lock:
            return {
                symbol: sv.to_dict()
                for symbol, sv in self._volumes.items()
            }

    def get_symbol_detail(self, symbol: str) -> Optional[dict]:
        """Get detailed volume for a single symbol."""
        symbol = symbol.upper()
        with self._lock:
            sv = self._volumes.get(symbol)
            return sv.to_dict() if sv else None

    def get_pcr(self, symbol: str) -> Optional[float]:
        """
        Get Put-Call Ratio (volume-based) for a symbol.

        Returns:
            put_volume / call_volume, or None if no data.
        """
        symbol = symbol.upper()
        with self._lock:
            sv = self._volumes.get(symbol)
            if sv is None or sv.call_volume == 0:
                return None
            return round(sv.put_volume / sv.call_volume, 4)

    def get_all_pcr(self) -> dict[str, Optional[float]]:
        """Get PCR for all symbols."""
        with self._lock:
            result = {}
            for symbol, sv in self._volumes.items():
                if sv.call_volume > 0:
                    result[symbol] = round(sv.put_volume / sv.call_volume, 4)
                else:
                    result[symbol] = None
            return result

    def get_stats(self) -> dict:
        """Return aggregator statistics."""
        uptime = time.time() - self._started_at if self._started_at else 0
        return {
            "running": self._running,
            "uptime_seconds": round(uptime, 1),
            "symbols_tracked": len(self._volumes),
            "tokens_mapped": len(self._token_map),
            "update_count": self._update_count,
            "update_interval": self._update_interval,
            "last_update_ms": (
                round(self._last_update_duration * 1000, 2)
                if self._last_update_duration is not None else None
            ),
        }

    # ──────────────────────────────────────────────────────────
    #  PUBLIC API — Manual update (for testing or on-demand)
    # ──────────────────────────────────────────────────────────

    def update_now(self) -> dict[str, dict]:
        """
        Manually trigger a volume update.

        Returns:
            The updated volume dict.
        """
        self._compute_volumes()
        return self.get_volumes()

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — Update loop
    # ──────────────────────────────────────────────────────────

    def _update_loop(self) -> None:
        """Background loop that recomputes volumes every interval."""
        logger.info("Volume update loop started.")

        while self._running:
            try:
                self._compute_volumes()
            except Exception as e:
                logger.error("Volume computation error: %s", e, exc_info=True)

            time.sleep(self._update_interval)

        logger.info("Volume update loop stopped.")

    def _compute_volumes(self) -> None:
        """
        Read latest tick volumes from TickAggregator and sum by
        symbol and option type.

        For each symbol:
            call_volume = ∑ volume of all CE tokens (ATM ±10)
            put_volume  = ∑ volume of all PE tokens (ATM ±10)
        """
        start = time.time()

        # Snapshot the current mapping (thread-safe read)
        with self._lock:
            symbol_tokens = dict(self._symbol_tokens)

        if not symbol_tokens:
            return

        # Collect all tokens we care about
        all_tokens = list(self._token_map.keys())

        # Fetch latest ticks in bulk
        ticks = self._tick_agg.get_ticks(all_tokens)

        # Compute volumes per symbol
        now = datetime.now()

        with self._lock:
            for symbol, token_groups in symbol_tokens.items():
                sv = self._volumes.get(symbol)
                if sv is None:
                    sv = SymbolVolume(symbol)
                    self._volumes[symbol] = sv

                # Sum CE volumes
                ce_vol = 0
                ce_count = 0
                for token in token_groups.get("CE", []):
                    tick = ticks.get(token)
                    if tick:
                        ce_vol += tick.volume
                        ce_count += 1

                # Sum PE volumes
                pe_vol = 0
                pe_count = 0
                for token in token_groups.get("PE", []):
                    tick = ticks.get(token)
                    if tick:
                        pe_vol += tick.volume
                        pe_count += 1

                sv.call_volume = ce_vol
                sv.put_volume = pe_vol
                sv.call_tokens = ce_count
                sv.put_tokens = pe_count
                sv.updated_at = now
                
                # Custom logging for all symbols' strike breakdowns (periodically)
                if self._update_count % 60 == 0:
                    ce_strikes = []
                    pe_strikes = []
                    for token in token_groups.get("CE", []):
                        tick = ticks.get(token)
                        if tick:
                            strike = self._token_map.get(token, (None, None, 1, 0.0))[3]
                            ce_strikes.append(f"{strike}:{tick.volume}")
                    
                    for token in token_groups.get("PE", []):
                        tick = ticks.get(token)
                        if tick:
                            strike = self._token_map.get(token, (None, None, 1, 0.0))[3]
                            pe_strikes.append(f"{strike}:{tick.volume}")
                            
                    if ce_strikes or pe_strikes:
                        logger.info("[%s] Breakdown (CE): %s", symbol, " | ".join(ce_strikes))
                        logger.info("[%s] Breakdown (PE): %s", symbol, " | ".join(pe_strikes))

        elapsed = time.time() - start
        self._update_count += 1
        self._last_update_time = time.time()
        self._last_update_duration = elapsed

        # Fire callback
        if self._on_update:
            try:
                self._on_update(self.get_volumes())
            except Exception as e:
                logger.error("on_update callback error: %s", e)

        if self._update_count % 60 == 0:  # Log every 60s
            logger.info(
                "Volume update #%d: %d symbols, took %.2fms.",
                self._update_count, len(symbol_tokens), elapsed * 1000,
            )
