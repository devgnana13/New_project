"""
Instrument Manager — Downloads, caches, and queries the Kite Connect
instruments dump for NFO option contracts.

Design Goals:
    1. Single REST API call per day — download the full NFO dump once,
       cache it locally as a JSON file, and serve all lookups from memory.
    2. Efficient lookup — instruments are indexed by (symbol, expiry,
       strike, option_type) for O(1) access.
    3. ATM ± N strikes — given a spot price, return the 2N+1 nearest
       strikes with their CE and PE instrument tokens.
    4. Worker batching — partition all resolved tokens into batches of
       ≤3,000 for multi-worker WebSocket subscription.

Usage:
    from core.instrument_manager import InstrumentManager

    mgr = InstrumentManager(api_key="xxx", access_token="yyy")
    mgr.load_instruments()

    # Get ATM ± 10 strikes for RELIANCE (nearest monthly expiry)
    tokens = mgr.get_strike_tokens("RELIANCE", spot_price=2850.0, strike_range=10)

    # Bulk resolve for all 150 stocks
    all_tokens = mgr.resolve_all_stocks(spot_prices={"RELIANCE": 2850, ...})

    # Get worker batches
    batches = mgr.partition_into_batches(all_tokens, max_per_batch=3000)
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional

from kiteconnect import KiteConnect
from config import now_ist, today_ist

from core.constants import (
    STOCK_SYMBOLS,
    STRIKE_INTERVALS,
    DEFAULT_STRIKE_INTERVAL,
    INSTRUMENTS_CACHE_DIR,
    INSTRUMENTS_CACHE_FILENAME,
    CACHE_EXPIRY_HOURS,
    MAX_INSTRUMENTS_PER_WORKER,
    DEFAULT_DERIVATIVE_EXCHANGE,
    DERIVATIVE_EXCHANGE_MAP,
)

logger = logging.getLogger(__name__)


class InstrumentManager:
    """
    Downloads the full Kite Connect NFO instruments dump, caches it
    locally, and provides efficient lookup methods for option contracts.

    The cache file is refreshed once per day (or when it expires after
    CACHE_EXPIRY_HOURS). All subsequent lookups are served from the
    in-memory index — zero additional REST calls.
    """

    def __init__(self, api_key: str, access_token: str, cache_dir: str = None):
        """
        Args:
            api_key:      Kite Connect API key.
            access_token: Kite Connect access token (refreshed daily).
            cache_dir:    Directory to store the cached instruments file.
                          Defaults to core.constants.INSTRUMENTS_CACHE_DIR.
        """
        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token)

        self.cache_dir = cache_dir or INSTRUMENTS_CACHE_DIR
        self.cache_path = os.path.join(self.cache_dir, INSTRUMENTS_CACHE_FILENAME)

        # ── In-memory data structures ──
        # Raw list of all NFO option instruments
        self._instruments: list[dict] = []

        # Lookup index: (symbol, expiry, strike, opt_type) → instrument dict
        self._index: dict[tuple, dict] = {}

        # Grouped: symbol → list of instruments
        self._by_symbol: dict[str, list[dict]] = defaultdict(list)

        # Available strikes per (symbol, expiry): sorted list of strike prices
        self._strikes: dict[tuple[str, str], list[float]] = {}

        # Available expiries per symbol: sorted list of expiry dates
        self._expiries: dict[str, list[str]] = {}

        # Auto-detected strike intervals: symbol → interval float
        self._detected_intervals: dict[str, float] = {}

        # Metadata
        self._loaded = False
        self._load_timestamp: Optional[datetime] = None
        self._instrument_count = 0

    # ──────────────────────────────────────────────────────────
    #  PUBLIC API
    # ──────────────────────────────────────────────────────────

    def load_instruments(self, force_refresh: bool = False) -> int:
        """
        Load instruments from cache or download fresh from Kite API.

        The method first checks for a valid cached file. If the cache is
        missing or expired (older than CACHE_EXPIRY_HOURS), it downloads
        a fresh dump from the Kite REST API (1 API call).

        Args:
            force_refresh: If True, ignore cache and download fresh data.

        Returns:
            Number of NFO option instruments loaded.
        """
        raw_instruments = None

        if not force_refresh and self._is_cache_valid():
            logger.info("Loading instruments from cache: %s", self.cache_path)
            raw_instruments = self._load_from_cache()

        if raw_instruments is None:
            logger.info("Downloading fresh instruments dump from Kite API...")
            raw_instruments = self._download_from_api()
            self._save_to_cache(raw_instruments)
            logger.info("Instruments cached to: %s", self.cache_path)

        # Filter to NFO options only and build indexes
        self._instruments = self._filter_nfo_options(raw_instruments)
        self._build_indexes()

        self._loaded = True
        self._load_timestamp = now_ist()
        self._instrument_count = len(self._instruments)

        logger.info(
            "Loaded %d NFO option instruments (cache timestamp: %s)",
            self._instrument_count,
            self._load_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        )
        return self._instrument_count

    def get_instrument(
        self, symbol: str, expiry: str, strike: float, option_type: str
    ) -> Optional[dict]:
        """
        Look up a single instrument by its exact attributes.

        Args:
            symbol:      Underlying symbol (e.g., "RELIANCE").
            expiry:      Expiry date string as "YYYY-MM-DD".
            strike:      Strike price (float).
            option_type: "CE" or "PE".

        Returns:
            Instrument dict or None if not found.
        """
        self._ensure_loaded()
        key = (symbol.upper(), expiry, float(strike), option_type.upper())
        return self._index.get(key)

    def get_instrument_token(
        self, symbol: str, expiry: str, strike: float, option_type: str
    ) -> Optional[int]:
        """
        Return just the instrument_token for a specific contract.

        Returns:
            Instrument token (int) or None.
        """
        inst = self.get_instrument(symbol, expiry, strike, option_type)
        return inst["instrument_token"] if inst else None

    def get_expiries(self, symbol: str) -> list[str]:
        """
        Return all available expiry dates for a symbol, sorted ascending.

        Args:
            symbol: Underlying symbol (e.g., "RELIANCE").

        Returns:
            List of expiry date strings ["2026-03-27", "2026-04-24", ...].
        """
        self._ensure_loaded()
        return self._expiries.get(symbol.upper(), [])

    def get_nearest_expiry(self, symbol: str) -> Optional[str]:
        """
        Return the nearest (current month) expiry for a symbol.

        Returns:
            Expiry date string or None.
        """
        expiries = self.get_expiries(symbol)
        if not expiries:
            return None

        today = today_ist()
        # Find the first expiry that hasn't passed yet
        for exp in expiries:
            if exp >= today:
                return exp

        # All expiries have passed (shouldn't happen during market hours)
        return expiries[-1] if expiries else None

    def get_strikes(self, symbol: str, expiry: str) -> list[float]:
        """
        Return all available strike prices for a (symbol, expiry) pair,
        sorted ascending.

        Args:
            symbol: Underlying symbol.
            expiry: Expiry date string.

        Returns:
            Sorted list of strikes [2700.0, 2725.0, 2750.0, ...].
        """
        self._ensure_loaded()
        key = (symbol.upper(), expiry)
        return self._strikes.get(key, [])

    def get_strike_interval(self, symbol: str) -> float:
        """
        Get the strike interval for a symbol.

        Priority:
          1. Explicit mapping in constants.STRIKE_INTERVALS
          2. Auto-detected from the instruments dump
          3. DEFAULT_STRIKE_INTERVAL fallback
        """
        symbol = symbol.upper()
        if symbol in STRIKE_INTERVALS:
            return STRIKE_INTERVALS[symbol]
        if symbol in self._detected_intervals:
            return self._detected_intervals[symbol]
        return DEFAULT_STRIKE_INTERVAL

    def get_atm_strike(self, symbol: str, spot_price: float) -> float:
        """
        Calculate the ATM (At-The-Money) strike for a given spot price.

        Uses the stock's strike interval (explicit or auto-detected)
        to round the spot price to the nearest valid strike.

        Args:
            symbol:     Underlying symbol.
            spot_price: Current spot / LTP of the underlying.

        Returns:
            ATM strike price (float).
        """
        interval = self.get_strike_interval(symbol)
        return round(spot_price / interval) * interval

    def get_strike_tokens(
        self,
        symbol: str,
        spot_price: float,
        strike_range: int = 10,
        expiry: str = None,
    ) -> list[dict]:
        """
        Return instrument tokens for ATM ± strike_range strikes (CE + PE).

        This is the primary method used to resolve the 42 instruments
        per stock (21 strikes × 2 contracts).

        Args:
            symbol:       Underlying symbol (e.g., "RELIANCE").
            spot_price:   Current spot / LTP of the underlying.
            strike_range: Number of strikes above and below ATM. Default 10.
            expiry:       Expiry date string. Defaults to nearest expiry.

        Returns:
            List of instrument dicts, each containing:
            {
                "instrument_token": int,
                "tradingsymbol": str,
                "strike": float,
                "option_type": "CE" | "PE",
                "expiry": str,
                "symbol": str,
                "lot_size": int,
            }

        Raises:
            ValueError: If the symbol has no instruments or no valid expiry.
        """
        self._ensure_loaded()
        symbol = symbol.upper()

        if expiry is None:
            expiry = self.get_nearest_expiry(symbol)
            if expiry is None:
                raise ValueError(
                    f"No expiry found for symbol '{symbol}'. "
                    f"Ensure the instruments dump is loaded and the symbol exists in NFO."
                )

        # Calculate ATM and generate target strikes
        atm = self.get_atm_strike(symbol, spot_price)
        interval = self.get_strike_interval(symbol)
        available_strikes = self.get_strikes(symbol, expiry)

        if not available_strikes:
            raise ValueError(
                f"No strikes found for {symbol} expiry {expiry}. "
                f"Check if the symbol is available in NFO options."
            )

        # Generate target strikes: ATM - 10*interval ... ATM ... ATM + 10*interval
        target_strikes = [
            atm + (i * interval)
            for i in range(-strike_range, strike_range + 1)
        ]

        # Collect matching instruments
        result = []
        missing_strikes = []

        for strike in target_strikes:
            for opt_type in ("CE", "PE"):
                inst = self.get_instrument(symbol, expiry, strike, opt_type)
                if inst:
                    result.append({
                        "instrument_token": inst["instrument_token"],
                        "tradingsymbol": inst["tradingsymbol"],
                        "strike": inst["strike"],
                        "option_type": opt_type,
                        "expiry": expiry,
                        "symbol": symbol,
                        "lot_size": inst.get("lot_size", 0),
                    })
                else:
                    missing_strikes.append((strike, opt_type))

        if missing_strikes:
            logger.warning(
                "[%s] %d contracts not found in instruments dump: %s",
                symbol,
                len(missing_strikes),
                missing_strikes[:6],  # Log first 6 for brevity
            )

        logger.debug(
            "[%s] Resolved %d instruments (ATM=%.1f, range=±%d, expiry=%s)",
            symbol, len(result), atm, strike_range, expiry,
        )
        return result

    def resolve_all_stocks(
        self,
        spot_prices: dict[str, float],
        strike_range: int = 10,
        stock_list: list[str] = None,
    ) -> dict[str, list[dict]]:
        """
        Bulk-resolve instruments for all stocks.

        Args:
            spot_prices:  Dict of {symbol: spot_price} for all stocks.
            strike_range: Number of strikes above/below ATM (default 10).
            stock_list:   Optional subset of stocks. Defaults to STOCK_LIST.

        Returns:
            Dict of {symbol: [instrument_dicts]} for each stock.
        """
        self._ensure_loaded()
        stocks = stock_list or STOCK_SYMBOLS
        all_instruments = {}
        total_tokens = 0

        for symbol in stocks:
            if symbol not in spot_prices:
                logger.warning(
                    "No spot price provided for %s — skipping.", symbol
                )
                continue

            try:
                tokens = self.get_strike_tokens(
                    symbol=symbol,
                    spot_price=spot_prices[symbol],
                    strike_range=strike_range,
                )
                all_instruments[symbol] = tokens
                total_tokens += len(tokens)
            except ValueError as e:
                logger.error("Failed to resolve %s: %s", symbol, e)

        logger.info(
            "Resolved %d total instruments across %d stocks.",
            total_tokens, len(all_instruments),
        )
        return all_instruments

    def partition_into_batches(
        self,
        resolved_instruments: dict[str, list[dict]],
        max_per_batch: int = None,
    ) -> dict[str, list[int]]:
        """
        Partition instrument tokens into batches for WebSocket workers.

        Each batch contains at most max_per_batch tokens (default 3000).
        Stocks are kept together — a single stock's 42 tokens will never
        be split across batches.

        Args:
            resolved_instruments: Output from resolve_all_stocks().
            max_per_batch:        Max instruments per worker (default 3000).

        Returns:
            Dict of {"worker_1": [token1, token2, ...], "worker_2": [...], ...}
        """
        max_per_batch = max_per_batch or MAX_INSTRUMENTS_PER_WORKER

        batches = {}
        current_batch = []
        batch_number = 1

        for symbol, instruments in resolved_instruments.items():
            tokens = [inst["instrument_token"] for inst in instruments]

            # If adding this stock would exceed the batch limit,
            # finalize current batch and start a new one
            if len(current_batch) + len(tokens) > max_per_batch and current_batch:
                batch_key = f"worker_{batch_number}"
                batches[batch_key] = current_batch
                logger.info(
                    "Batch '%s': %d instruments", batch_key, len(current_batch)
                )
                current_batch = []
                batch_number += 1

            current_batch.extend(tokens)

        # Don't forget the last batch
        if current_batch:
            batch_key = f"worker_{batch_number}"
            batches[batch_key] = current_batch
            logger.info(
                "Batch '%s': %d instruments", batch_key, len(current_batch)
            )

        total = sum(len(b) for b in batches.values())
        logger.info(
            "Created %d worker batches with %d total instruments.",
            len(batches), total,
        )
        return batches

    def get_all_symbols(self) -> list[str]:
        """Return all unique underlying symbols in the loaded instruments."""
        self._ensure_loaded()
        return sorted(self._expiries.keys())

    def get_stats(self) -> dict:
        """Return summary statistics about the loaded instruments."""
        return {
            "loaded": self._loaded,
            "load_timestamp": (
                self._load_timestamp.isoformat() if self._load_timestamp else None
            ),
            "total_instruments": self._instrument_count,
            "unique_symbols": len(self._expiries),
            "cache_path": self.cache_path,
            "cache_valid": self._is_cache_valid(),
        }

    # ──────────────────────────────────────────────────────────
    #  PRIVATE METHODS — Cache I/O
    # ──────────────────────────────────────────────────────────

    def _is_cache_valid(self) -> bool:
        """
        Check if the cached instruments file exists and is fresh.

        The cache is considered valid if:
          1. The file exists.
          2. It was modified within the last CACHE_EXPIRY_HOURS hours.
        """
        if not os.path.exists(self.cache_path):
            return False

        try:
            file_mtime = os.path.getmtime(self.cache_path)
            file_time = datetime.fromtimestamp(file_mtime)
            age = datetime.now() - file_time

            is_valid = age < timedelta(hours=CACHE_EXPIRY_HOURS)
            if is_valid:
                logger.debug(
                    "Cache is valid (age: %s, max: %dh).",
                    str(age).split(".")[0],
                    CACHE_EXPIRY_HOURS,
                )
            else:
                logger.info(
                    "Cache expired (age: %s, max: %dh). Will re-download.",
                    str(age).split(".")[0],
                    CACHE_EXPIRY_HOURS,
                )
            return is_valid
        except OSError as e:
            logger.warning("Error checking cache file: %s", e)
            return False

    def _load_from_cache(self) -> Optional[list[dict]]:
        """Load instruments from the local JSON cache file."""
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(
                "Loaded %d instruments from cache.", len(data)
            )
            return data
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to load cache: %s. Will re-download.", e)
            return None

    def _save_to_cache(self, instruments: list[dict]) -> None:
        """Save instruments to the local JSON cache file."""
        os.makedirs(self.cache_dir, exist_ok=True)

        # Serialize datetime/date objects for JSON compatibility
        serializable = []
        for inst in instruments:
            record = {}
            for key, value in inst.items():
                if hasattr(value, "isoformat"):
                    record[key] = value.isoformat()
                elif hasattr(value, "strftime"):
                    record[key] = value.strftime("%Y-%m-%d")
                else:
                    record[key] = value
            serializable.append(record)

        try:
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(serializable, f, indent=None, separators=(",", ":"))
            logger.info(
                "Saved %d instruments to cache (%s).",
                len(instruments),
                self.cache_path,
            )
        except OSError as e:
            logger.error("Failed to save cache: %s", e)

    def _download_from_api(self) -> list[dict]:
        """
        Download the full instruments dump from Kite Connect REST API.

        Downloads from both NFO (NSE F&O) and BFO (BSE F&O for SENSEX)
        exchanges. This makes 2 API calls total.

        Returns:
            Combined list of all NFO + BFO instrument dicts.

        Raises:
            Exception: If the API call fails.
        """
        start_time = time.time()
        all_instruments = []

        # Determine which exchanges to download
        exchanges = {DEFAULT_DERIVATIVE_EXCHANGE}  # Always download NFO
        for sym in STOCK_SYMBOLS:
            exchange = DERIVATIVE_EXCHANGE_MAP.get(sym, DEFAULT_DERIVATIVE_EXCHANGE)
            exchanges.add(exchange)

        for exchange in sorted(exchanges):
            try:
                instruments = self.kite.instruments(exchange)
                logger.info(
                    "Downloaded %d instruments from %s.",
                    len(instruments), exchange,
                )
                all_instruments.extend(instruments)
            except Exception as e:
                logger.error(
                    "Failed to download %s instruments: %s", exchange, e
                )
                # Don't raise for BFO failure; NFO is the primary exchange
                if exchange == DEFAULT_DERIVATIVE_EXCHANGE:
                    raise

        elapsed = time.time() - start_time
        logger.info(
            "Total: %d instruments from %d exchanges in %.2fs.",
            len(all_instruments), len(exchanges), elapsed,
        )
        return all_instruments

    # ──────────────────────────────────────────────────────────
    #  PRIVATE METHODS — Filtering & Indexing
    # ──────────────────────────────────────────────────────────

    def _filter_nfo_options(self, instruments: list[dict]) -> list[dict]:
        """
        Filter the raw instruments dump to keep only option contracts.

        Criteria:
          - exchange in ("NFO", "BFO")
          - segment in ("NFO-OPT", "BFO-OPT")
          - instrument_type in ("CE", "PE")
        """
        valid_exchanges = {"NFO", "BFO"}
        valid_segments = {"NFO-OPT", "BFO-OPT"}

        options = [
            inst for inst in instruments
            if (
                inst.get("exchange") in valid_exchanges
                and inst.get("segment") in valid_segments
                and inst.get("instrument_type") in ("CE", "PE")
            )
        ]

        logger.info(
            "Filtered %d option contracts from %d total instruments.",
            len(options),
            len(instruments),
        )
        return options

    def _build_indexes(self) -> None:
        """
        Build in-memory lookup indexes for fast instrument queries.

        Creates:
          - _index:    (symbol, expiry, strike, opt_type) → instrument dict
          - _by_symbol: symbol → [instrument dicts]
          - _strikes:  (symbol, expiry) → sorted [strike prices]
          - _expiries: symbol → sorted [expiry dates]
        """
        self._index.clear()
        self._by_symbol.clear()

        strikes_set: dict[tuple[str, str], set[float]] = defaultdict(set)
        expiries_set: dict[str, set[str]] = defaultdict(set)

        for inst in self._instruments:
            # Normalize the instrument data
            symbol = inst.get("name", "").upper()
            raw_expiry = inst.get("expiry", "")
            strike = float(inst.get("strike", 0))
            opt_type = inst.get("instrument_type", "").upper()
            token = inst.get("instrument_token")

            # Handle expiry — could be a date object or string
            if hasattr(raw_expiry, "strftime"):
                expiry = raw_expiry.strftime("%Y-%m-%d")
            elif isinstance(raw_expiry, str) and raw_expiry:
                expiry = raw_expiry[:10]  # Truncate time portion if present
            else:
                continue  # Skip instruments without expiry

            if not symbol or not opt_type or token is None:
                continue

            # Store normalized fields back
            inst["name"] = symbol
            inst["expiry_str"] = expiry
            inst["strike"] = strike
            inst["instrument_type"] = opt_type

            # Build primary lookup index
            key = (symbol, expiry, strike, opt_type)
            self._index[key] = inst

            # Build symbol grouping
            self._by_symbol[symbol].append(inst)

            # Collect unique strikes and expiries
            strikes_set[(symbol, expiry)].add(strike)
            expiries_set[symbol].add(expiry)

        # Sort and store
        self._strikes = {
            key: sorted(strikes)
            for key, strikes in strikes_set.items()
        }
        self._expiries = {
            symbol: sorted(expiries)
            for symbol, expiries in expiries_set.items()
        }

        # Auto-detect strike intervals from the actual data
        self._detect_strike_intervals()

        logger.info(
            "Built indexes: %d unique keys, %d symbols, %d (symbol,expiry) pairs.",
            len(self._index),
            len(self._expiries),
            len(self._strikes),
        )

    def _detect_strike_intervals(self) -> None:
        """
        Auto-detect strike intervals for each symbol by computing
        the minimum gap between consecutive strikes. This handles
        stocks that are not explicitly listed in STRIKE_INTERVALS.
        """
        self._detected_intervals.clear()

        for (symbol, expiry), strikes in self._strikes.items():
            if symbol in STRIKE_INTERVALS:
                continue  # Already explicitly configured
            if symbol in self._detected_intervals:
                continue  # Already detected from another expiry
            if len(strikes) < 2:
                continue

            # Compute the minimum gap between consecutive sorted strikes
            gaps = [strikes[i+1] - strikes[i] for i in range(len(strikes) - 1)]
            min_gap = min(gaps)

            if min_gap > 0:
                self._detected_intervals[symbol] = min_gap

        if self._detected_intervals:
            logger.info(
                "Auto-detected strike intervals for %d symbols: %s",
                len(self._detected_intervals),
                dict(list(self._detected_intervals.items())[:10]),
            )

    # ──────────────────────────────────────────────────────────
    #  PRIVATE METHODS — Validation
    # ──────────────────────────────────────────────────────────

    def _ensure_loaded(self) -> None:
        """Raise if instruments haven't been loaded yet."""
        if not self._loaded:
            raise RuntimeError(
                "Instruments not loaded. Call load_instruments() first."
            )
