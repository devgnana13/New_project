import time
import logging
import threading
from datetime import datetime
from typing import Optional, Callable

from core.aggregator import TickAggregator
from core.database import DatabaseManager
from config import now_ist

logger = logging.getLogger(__name__)

DEFAULT_UPDATE_INTERVAL = 1.0

class SymbolOI:
    __slots__ = ("symbol", "strikes", "updated_at", "atm_strike")

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.strikes: dict[str, dict] = {}
        self.updated_at: Optional[datetime] = None
        self.atm_strike: float = 0.0

    def to_dict(self) -> dict:
        return {
            "atm_strike": self.atm_strike,
            "strikes": self.strikes,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

class OIAggregator:
    def __init__(
        self,
        tick_aggregator: TickAggregator,
        db: DatabaseManager,
        update_interval: float = DEFAULT_UPDATE_INTERVAL,
    ):
        self._tick_agg = tick_aggregator
        self._db = db
        self._update_interval = update_interval

        # token -> (symbol, opt_type, lot_size, strike)
        self._token_map: dict[int, tuple[str, str, int, float]] = {}
        # symbol -> { "CE": [tokens], "PE": [tokens] }
        self._symbol_tokens: dict[str, dict[str, list[int]]] = {}

        self._oi: dict[str, SymbolOI] = {}
        self._prev_day_cache: dict[str, dict] = {}
        self._lock = threading.Lock()

        self._thread: Optional[threading.Thread] = None
        self._running = False
        
        # We share state with volume aggregator conceptually, but here we enforce alternating 30s logic
        # by checking a shared state or just doing timing.
        self._start_time: float = 0.0

    def start(self) -> None:
        if self._running: return
        self._running = True
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._update_loop, daemon=True, name="OIAggregator-Update")
        self._thread.start()
        logger.info("OIAggregator started.")

    def stop(self) -> None:
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)

    def build_token_map(self, atm_resolutions: dict) -> None:
        new_token_map = {}
        new_symbol_tokens = {}
        symbols_to_fetch_db = set()

        for symbol, resolution in atm_resolutions.items():
            ce_tokens = []
            pe_tokens = []
            symbols_to_fetch_db.add(symbol)
            for token_dict in resolution.tokens:
                token = token_dict.get("instrument_token")
                opt_type = token_dict.get("option_type")
                lot_size = token_dict.get("lot_size", 1) or 1
                strike = float(token_dict.get("strike", 0.0))
                
                new_token_map[token] = (symbol, opt_type, lot_size, strike)

                if opt_type == "CE": ce_tokens.append(token)
                elif opt_type == "PE": pe_tokens.append(token)

            new_symbol_tokens[symbol] = {"CE": ce_tokens, "PE": pe_tokens}

        # Fetch prev day data
        for sym in symbols_to_fetch_db:
            if sym not in self._prev_day_cache:
                prev = self._db.get_yesterday_oi(sym)
                if prev and "strikes" in prev:
                    self._prev_day_cache[sym] = prev["strikes"]
                else:
                    self._prev_day_cache[sym] = {}

        with self._lock:
            self._token_map = new_token_map
            self._symbol_tokens = new_symbol_tokens

            for symbol in new_symbol_tokens:
                if symbol not in self._oi:
                    self._oi[symbol] = SymbolOI(symbol)
                if symbol in atm_resolutions:
                    self._oi[symbol].atm_strike = atm_resolutions[symbol].atm_strike

            for symbol in list(self._oi.keys()):
                if symbol not in new_symbol_tokens:
                    del self._oi[symbol]

    def _update_loop(self) -> None:
        logger.info("OI update loop started.")
        while self._running:
            try:
                self._compute_oi()
            except Exception as e:
                logger.error("OI computation error: %s", e, exc_info=True)
            time.sleep(self._update_interval)

    def _compute_oi(self) -> None:
        with self._lock:
            symbol_tokens = dict(self._symbol_tokens)
            token_map = dict(self._token_map)
            
        if not symbol_tokens: return

        all_tokens = list(token_map.keys())
        ticks = self._tick_agg.get_ticks(all_tokens)
        now = now_ist()

        with self._lock:
            for symbol, token_groups in symbol_tokens.items():
                soi = self._oi.get(symbol)
                if not soi: continue
                
                prev_data = self._prev_day_cache.get(symbol, {})
                soi.strikes.clear()

                for opt_type in ["CE", "PE"]:
                    for token in token_groups.get(opt_type, []):
                        strike = token_map.get(token, (None, None, 1, 0.0))[3]
                        strike_str = str(strike)
                        
                        if strike_str not in soi.strikes:
                            p_ce = prev_data.get(strike_str, {}).get("ce_oi", 0)
                            p_pe = prev_data.get(strike_str, {}).get("pe_oi", 0)
                            soi.strikes[strike_str] = {
                                "ce_oi": 0, "pe_oi": 0, 
                                "ce_prev": p_ce, "pe_prev": p_pe,
                                "ce_chng": 0, "pe_chng": 0
                            }

                        tick = ticks.get(token)
                        if tick:
                            val = tick.oi
                            if opt_type == "CE":
                                soi.strikes[strike_str]["ce_oi"] = val
                                soi.strikes[strike_str]["ce_chng"] = val - soi.strikes[strike_str]["ce_prev"]
                            else:
                                soi.strikes[strike_str]["pe_oi"] = val
                                soi.strikes[strike_str]["pe_chng"] = val - soi.strikes[strike_str]["pe_prev"]

                soi.updated_at = now

    def get_oi_data(self) -> dict:
        with self._lock:
            return {
                sym: self._oi[sym].to_dict()
                for sym in self._oi
            }
