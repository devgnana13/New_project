"""
ATM Resolver — Fetches live LTP for underlying stocks/indices via the
Kite quote API and determines ATM strikes with CE/PE instrument tokens.

Design Goals:
    1. Fetch LTP for all symbols using Kite's batch quote endpoint,
       minimizing REST API calls (1 call per ~450 symbols).
    2. Compute ATM strike for each symbol using its strike step size.
    3. Select ATM ±10 strikes → return CE + PE instrument tokens.
    4. Cache ATM results for 30 seconds to reduce CPU usage and API load.
    5. Thread-safe: can be called from multiple threads concurrently.

Architecture:
    ┌──────────────────────────────────────────────────────────────┐
    │                      ATM RESOLVER                            │
    │                                                              │
    │  ┌─────────────┐    ┌──────────────┐    ┌───────────────┐   │
    │  │ Quote Fetch  │───▶│ ATM Calculate │───▶│ Token Resolve │   │
    │  │ (Kite API)   │    │ (per symbol)  │    │ (from InsMgr) │   │
    │  └─────────────┘    └──────────────┘    └───────────────┘   │
    │       │                    │                     │           │
    │       │              30s cache                   │           │
    │       └────────────────────┴─────────────────────┘           │
    │                            │                                │
    │                   ATM Resolution Result:                     │
    │                   {symbol: {atm, tokens[], expiry, ...}}     │
    └──────────────────────────────────────────────────────────────┘

Usage:
    from core.instrument_manager import InstrumentManager
    from core.atm_resolver import ATMResolver

    # 1. Initialize instrument manager (loads instruments once/day)
    ins_mgr = InstrumentManager(api_key="xxx", access_token="yyy")
    ins_mgr.load_instruments()

    # 2. Initialize ATM resolver
    resolver = ATMResolver(ins_mgr)

    # 3. Resolve ATM strikes for all symbols (fetches LTP + computes ATM)
    result = resolver.resolve_all()
    # result["RELIANCE"]["atm_strike"] → 2850.0
    # result["RELIANCE"]["tokens"]     → [list of 42 instrument dicts]

    # 4. Get just the flat token list for WebSocket subscription
    all_tokens = resolver.get_all_tokens()
    # [token1, token2, ..., tokenN]

    # 5. Get worker batches
    batches = resolver.get_worker_batches()
    # {"worker_1": [token1, ...], "worker_2": [token2, ...], ...}
"""

import time
import logging
import threading
from datetime import datetime
from typing import Optional

from kiteconnect import KiteConnect

from core.constants import (
    STOCK_SYMBOLS,
    INDEX_SYMBOLS,
    QUOTE_EXCHANGE_MAP,
    STRIKE_INTERVALS,
    DEFAULT_STRIKE_INTERVAL,
    ATM_UPDATE_INTERVAL_SECONDS,
    STRIKE_RANGE,
    MAX_INSTRUMENTS_PER_WORKER,
    QUOTE_BATCH_SIZE,
)
from core.instrument_manager import InstrumentManager

logger = logging.getLogger(__name__)


class ATMResolution:
    """
    Data class holding the ATM resolution result for a single symbol.
    """
    __slots__ = (
        "symbol", "spot_price", "atm_strike", "strike_interval",
        "expiry", "tokens", "token_list", "strikes",
        "resolved_at", "strike_range",
    )

    def __init__(
        self,
        symbol: str,
        spot_price: float,
        atm_strike: float,
        strike_interval: float,
        expiry: str,
        tokens: list[dict],
        strikes: list[float],
        strike_range: int,
    ):
        self.symbol = symbol
        self.spot_price = spot_price
        self.atm_strike = atm_strike
        self.strike_interval = strike_interval
        self.expiry = expiry
        self.tokens = tokens                    # Full instrument dicts
        self.token_list = [t["instrument_token"] for t in tokens]  # Just ints
        self.strikes = strikes                  # List of strike prices covered
        self.strike_range = strike_range
        self.resolved_at = datetime.now()

    def to_dict(self) -> dict:
        """Serialize to JSON-friendly dict."""
        return {
            "symbol": self.symbol,
            "spot_price": self.spot_price,
            "atm_strike": self.atm_strike,
            "strike_interval": self.strike_interval,
            "expiry": self.expiry,
            "strike_range": self.strike_range,
            "strikes": self.strikes,
            "num_tokens": len(self.tokens),
            "ce_tokens": len([t for t in self.tokens if t["option_type"] == "CE"]),
            "pe_tokens": len([t for t in self.tokens if t["option_type"] == "PE"]),
            "resolved_at": self.resolved_at.isoformat(),
        }

    def __repr__(self) -> str:
        return (
            f"ATMResolution(symbol={self.symbol}, spot={self.spot_price}, "
            f"atm={self.atm_strike}, tokens={len(self.tokens)}, "
            f"expiry={self.expiry})"
        )


class ATMResolver:
    """
    Resolves ATM strikes for all configured stocks and indices by
    fetching LTP from the Kite quote API and mapping to instrument tokens.

    The resolver caches its results for ATM_UPDATE_INTERVAL_SECONDS (30s)
    to avoid excessive API calls and CPU usage. When the cache expires,
    the next call to resolve_all() will re-fetch LTPs and re-compute ATM.

    Thread-safe: multiple threads can call resolve_all() concurrently;
    only one will perform the actual refresh.
    """

    def __init__(
        self,
        instrument_manager: InstrumentManager,
        kite: KiteConnect = None,
        symbols: list[str] = None,
        strike_range: int = None,
        update_interval: int = None,
    ):
        """
        Args:
            instrument_manager: Loaded InstrumentManager instance.
            kite:               KiteConnect instance for quote API.
                                If None, uses the one from instrument_manager.
            symbols:            List of symbols to resolve. Defaults to STOCK_SYMBOLS.
            strike_range:       Number of strikes above/below ATM (default 10).
            update_interval:    Cache TTL in seconds (default 30).
        """
        self._ins_mgr = instrument_manager
        self._kite = kite or instrument_manager.kite
        self._symbols = symbols or STOCK_SYMBOLS
        self._strike_range = strike_range or STRIKE_RANGE
        self._update_interval = update_interval or ATM_UPDATE_INTERVAL_SECONDS

        # ── Cache ──
        self._cache: dict[str, ATMResolution] = {}
        self._cache_timestamp: Optional[float] = None
        self._lock = threading.Lock()

        # ── Stats ──
        self._refresh_count = 0
        self._last_ltp_fetch_time: Optional[float] = None
        self._errors: list[dict] = []

    # ──────────────────────────────────────────────────────────
    #  PUBLIC API
    # ──────────────────────────────────────────────────────────

    def resolve_all(self, force: bool = False) -> dict[str, ATMResolution]:
        """
        Resolve ATM strikes for all symbols.

        If the cache is still fresh (within update_interval), returns
        the cached result immediately. Otherwise, performs a full
        refresh: fetch LTPs → compute ATMs → resolve tokens.

        Args:
            force: If True, ignore cache and re-fetch.

        Returns:
            Dict of {symbol: ATMResolution} for each successfully
            resolved symbol.
        """
        if not force and self._is_cache_fresh():
            return self._cache

        with self._lock:
            # Double-check after acquiring lock (another thread may have refreshed)
            if not force and self._is_cache_fresh():
                return self._cache

            return self._refresh()

    def resolve_symbol(self, symbol: str, force: bool = False) -> Optional[ATMResolution]:
        """
        Resolve ATM for a single symbol.

        Uses the cached result if fresh; otherwise triggers a full refresh.

        Returns:
            ATMResolution for the symbol, or None if resolution failed.
        """
        result = self.resolve_all(force=force)
        return result.get(symbol.upper())

    def get_all_tokens(self, force: bool = False) -> list[int]:
        """
        Return a flat list of all instrument tokens across all symbols.

        Returns:
            List of instrument token integers.
        """
        resolutions = self.resolve_all(force=force)
        tokens = []
        for res in resolutions.values():
            tokens.extend(res.token_list)
        return tokens

    def get_all_tokens_with_details(self, force: bool = False) -> list[dict]:
        """
        Return a flat list of all instrument dicts across all symbols.

        Each dict includes: instrument_token, tradingsymbol, strike,
        option_type, expiry, symbol, lot_size.
        """
        resolutions = self.resolve_all(force=force)
        tokens = []
        for res in resolutions.values():
            tokens.extend(res.tokens)
        return tokens

    def get_worker_batches(self, force: bool = False) -> dict[str, list[int]]:
        """
        Partition all resolved tokens into batches for WebSocket workers.

        Each batch contains at most MAX_INSTRUMENTS_PER_WORKER tokens.
        Symbols are kept together within batches.

        Returns:
            Dict of {"worker_1": [tokens], "worker_2": [tokens], ...}
        """
        resolutions = self.resolve_all(force=force)
        return self._ins_mgr.partition_into_batches(
            {sym: res.tokens for sym, res in resolutions.items()},
            max_per_batch=MAX_INSTRUMENTS_PER_WORKER,
        )

    def get_spot_prices(self) -> dict[str, float]:
        """
        Return the last fetched spot prices for all symbols.

        Returns:
            Dict of {symbol: spot_price}. Empty if no fetch has been done.
        """
        return {
            sym: res.spot_price
            for sym, res in self._cache.items()
        }

    def get_stats(self) -> dict:
        """Return summary statistics about the resolver state."""
        return {
            "symbols_configured": len(self._symbols),
            "symbols_resolved": len(self._cache),
            "total_tokens": sum(len(r.token_list) for r in self._cache.values()),
            "cache_age_seconds": (
                round(time.time() - self._cache_timestamp, 1)
                if self._cache_timestamp else None
            ),
            "update_interval": self._update_interval,
            "strike_range": self._strike_range,
            "refresh_count": self._refresh_count,
            "last_ltp_fetch_ms": (
                round(self._last_ltp_fetch_time * 1000, 1)
                if self._last_ltp_fetch_time is not None else None
            ),
            "recent_errors": self._errors[-5:],
        }

    def get_summary(self) -> list[dict]:
        """
        Return a summary of all resolved symbols for dashboard display.

        Returns:
            List of dicts with symbol, spot, ATM, token count, expiry.
        """
        return [res.to_dict() for res in self._cache.values()]

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — Core refresh logic
    # ──────────────────────────────────────────────────────────

    def _refresh(self) -> dict[str, ATMResolution]:
        """
        Full refresh cycle:
          1. Fetch LTP for all symbols from Kite quote API
          2. Compute ATM strike for each symbol
          3. Resolve CE + PE tokens from InstrumentManager
          4. Update cache
        """
        start = time.time()
        logger.info(
            "Refreshing ATM resolution for %d symbols...",
            len(self._symbols),
        )

        # Step 1: Fetch LTPs
        spot_prices = self._fetch_all_ltps()
        if not spot_prices:
            logger.error("Failed to fetch any LTPs. Keeping stale cache.")
            return self._cache

        # Step 2 + 3: Compute ATM and resolve tokens
        new_cache: dict[str, ATMResolution] = {}

        for symbol in self._symbols:
            symbol = symbol.upper()
            if symbol not in spot_prices:
                self._log_error(symbol, "No LTP available — skipped")
                continue

            try:
                resolution = self._resolve_single(symbol, spot_prices[symbol])
                if resolution:
                    new_cache[symbol] = resolution
            except Exception as e:
                self._log_error(symbol, str(e))
                logger.error("[%s] Resolution failed: %s", symbol, e)

        # Step 4: Update cache
        self._cache = new_cache
        self._cache_timestamp = time.time()
        self._refresh_count += 1

        elapsed = time.time() - start
        total_tokens = sum(len(r.token_list) for r in new_cache.values())

        logger.info(
            "ATM refresh complete: %d/%d symbols resolved, "
            "%d total tokens, took %.2fs.",
            len(new_cache), len(self._symbols), total_tokens, elapsed,
        )
        return self._cache

    def _resolve_single(self, symbol: str, spot_price: float) -> Optional[ATMResolution]:
        """
        Resolve ATM for a single symbol given its spot price.
        """
        # Get strike interval
        interval = self._ins_mgr.get_strike_interval(symbol)

        # Compute ATM
        atm_strike = self._ins_mgr.get_atm_strike(symbol, spot_price)

        # Get nearest expiry
        expiry = self._ins_mgr.get_nearest_expiry(symbol)
        if not expiry:
            self._log_error(symbol, f"No expiry found")
            return None

        # Resolve ATM ± strike_range tokens
        try:
            tokens = self._ins_mgr.get_strike_tokens(
                symbol=symbol,
                spot_price=spot_price,
                strike_range=self._strike_range,
                expiry=expiry,
            )
        except ValueError as e:
            self._log_error(symbol, str(e))
            return None

        if not tokens:
            self._log_error(symbol, "No tokens resolved")
            return None

        # Compute covered strikes
        strikes = sorted(set(t["strike"] for t in tokens))

        resolution = ATMResolution(
            symbol=symbol,
            spot_price=spot_price,
            atm_strike=atm_strike,
            strike_interval=interval,
            expiry=expiry,
            tokens=tokens,
            strikes=strikes,
            strike_range=self._strike_range,
        )

        logger.debug(
            "[%s] ATM=%.1f (spot=%.1f, step=%.0f, expiry=%s, tokens=%d)",
            symbol, atm_strike, spot_price, interval, expiry, len(tokens),
        )
        return resolution

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — LTP Fetching
    # ──────────────────────────────────────────────────────────

    def _fetch_all_ltps(self) -> dict[str, float]:
        """
        Fetch LTP for all symbols using Kite's batch quote API.

        Kite allows ~500 instruments per quote() call, so we batch
        the symbols accordingly. Indices need special exchange-prefixed
        keys (e.g., "NSE:NIFTY 50"), while stocks use "NSE:SYMBOL".

        Returns:
            Dict of {symbol: ltp} for all successfully fetched symbols.
        """
        start = time.time()
        spot_prices: dict[str, float] = {}

        # Build quote keys with proper exchange prefix
        quote_keys = []
        key_to_symbol = {}  # Map quote key back to our symbol name

        for symbol in self._symbols:
            symbol = symbol.upper()
            if symbol in QUOTE_EXCHANGE_MAP:
                # Index — use special key
                key = QUOTE_EXCHANGE_MAP[symbol]
            else:
                # Stock — use NSE:SYMBOL
                key = f"NSE:{symbol}"

            quote_keys.append(key)
            key_to_symbol[key] = symbol

        # Batch quote requests (max QUOTE_BATCH_SIZE per call)
        for batch_start in range(0, len(quote_keys), QUOTE_BATCH_SIZE):
            batch = quote_keys[batch_start:batch_start + QUOTE_BATCH_SIZE]

            try:
                quotes = self._kite.quote(batch)

                for key, data in quotes.items():
                    symbol = key_to_symbol.get(key)
                    if symbol and "last_price" in data:
                        ltp = data["last_price"]
                        if ltp > 0:
                            spot_prices[symbol] = ltp
                        else:
                            logger.warning("[%s] LTP is 0, skipping.", symbol)

            except Exception as e:
                logger.error(
                    "Quote API error for batch %d-%d: %s",
                    batch_start, batch_start + len(batch), e,
                )

        elapsed = time.time() - start
        self._last_ltp_fetch_time = elapsed

        logger.info(
            "Fetched LTP for %d/%d symbols in %.2fs (%d API calls).",
            len(spot_prices),
            len(self._symbols),
            elapsed,
            -(-len(quote_keys) // QUOTE_BATCH_SIZE),  # Ceiling division
        )
        return spot_prices

    # ──────────────────────────────────────────────────────────
    #  PRIVATE — Cache management
    # ──────────────────────────────────────────────────────────

    def _is_cache_fresh(self) -> bool:
        """Check if the cached ATM data is still within the update interval."""
        if self._cache_timestamp is None:
            return False
        return (time.time() - self._cache_timestamp) < self._update_interval

    def _log_error(self, symbol: str, message: str) -> None:
        """Record an error for stats/debugging."""
        self._errors.append({
            "symbol": symbol,
            "error": message,
            "time": datetime.now().isoformat(),
        })
        # Keep only the last 50 errors
        if len(self._errors) > 50:
            self._errors = self._errors[-50:]
