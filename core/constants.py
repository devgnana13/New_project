"""
Constants for the Options Analytics Platform.

Contains the master stock/index list, exchange mappings, strike interval
definitions, and platform-wide configuration constants.
"""

# ──────────────────────────────────────────────────────────────
#  STOCK & INDEX SYMBOLS — All F&O instruments tracked
# ──────────────────────────────────────────────────────────────

STOCK_SYMBOLS = [
    # Major Indices (First 5)
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX",

    # All F&O Stocks in alphabetical order
    "ABB", "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS",
    "ALKEM", "AMBER", "AMBUJACEM", "ANGELONE", "APLAPOLLO", "APOLLOHOSP", "ASIANPAINT", "ASTRAL",
    "AUBANK", "AUROPHARMA", "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE",
    "BDL", "BEL", "BHARATFORG", "BHARTIARTL", "BLUESTARCO", "BPCL", "BRITANNIA",
    "CAMS", "CDSL", "CGPOWER", "CHOLAFIN", "CIPLA", "COALINDIA",
    "COFORGE", "COLPAL", "CONCOR", "CUMMINSIND", "DABUR", "DALBHARAT", "DELHIVERY", "DIVISLAB", "DLF", "DMART", "DRREDDY",
    "EICHERMOT", "EXIDEIND", "FORTIS", "GLENMARK", "GODREJCP", "GODREJPROP", "GRASIM", "HAL", "HAVELLS",
    "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDPETRO", "HINDUNILVR", "HINDZINC", "ICICIBANK", "ICICIGI", "ICICIPRULI",
    "INDHOTEL", "INDIANB", "INDIGO", "INDUSINDBK", "INDUSTOWER", "INFY", "ITC",
    "JINDALSTEL", "JIOFIN", "JSWSTEEL", "JSWENERGY", "JUBLFOOD", "KALYANKJIL", "KAYNES", "KEI", "KFINTECH", "KOTAKBANK", "KPITTECH", "LAURUSLABS", "LICHSGFIN", "LICI", "LODHA", "LT",
    "LTM", "LUPIN", "M&M", "MANKIND", "MARICO", "MAXHEALTH", "MAZDOCK",
    "MFSL", "MPHASIS", "MUTHOOTFIN",
    "NAUKRI", "NESTLEIND", "NTPC", "OBEROIRLTY",
    "OIL", "PATANJALI", "PAYTM", "PERSISTENT", "PFC", "PGEL", "PHOENIXLTD", "PIDILITIND",
    "PIIND", "PNBHOUSING", "POLICYBZR", "POLYCAB", "PRESTIGE", "RECLTD",
    "RELIANCE", "RVNL", "SBICARD", "SBILIFE", "SBIN", "SHRIRAMFIN", "SIEMENS", "SONACOMS",
    "SRF", "SUNPHARMA", "SUPREMEIND", "SYNGENE", "TATACONSUM", "TATAELXSI",
    "TATAPOWER", "TATATECH", "TCS", "TECHM", "TIINDIA", "TITAN", "TORNTPHARM", "TORNTPOWER",
    "TRENT", "TVSMOTOR", "UNITDSPR", "UNOMINDA", "UPL", "VBL", "VEDL", "VOLTAS",
    "ZYDUSLIFE",
]


# ──────────────────────────────────────────────────────────────
#  INDEX vs STOCK CLASSIFICATION
#  Indices use the "NFO" exchange for derivatives but their spot
#  quote requires a different exchange prefix (NSE for NIFTY/BANKNIFTY,
#  BSE for SENSEX). Stocks use "NSE" for spot quotes.
# ──────────────────────────────────────────────────────────────

INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"}

# Exchange for spot/LTP quote lookup
# Indices need special exchange mapping for the quote API
QUOTE_EXCHANGE_MAP = {
    "NIFTY":      "NSE:NIFTY 50",
    "BANKNIFTY":  "NSE:NIFTY BANK",
    "FINNIFTY":   "NSE:NIFTY FIN SERVICE",
    "MIDCPNIFTY": "NSE:NIFTY MID SELECT",
    "SENSEX":     "BSE:SENSEX",
}

# Exchange for derivatives instruments
# SENSEX options trade on BFO; everything else on NFO
DERIVATIVE_EXCHANGE_MAP = {
    "SENSEX": "BFO",
}
DEFAULT_DERIVATIVE_EXCHANGE = "NFO"


# ──────────────────────────────────────────────────────────────
#  STRIKE INTERVAL (STEP SIZE) MAPPING
#
#  Strike intervals vary by symbol and are set by NSE/BSE.
#  This provides known intervals; for any unlisted stock, the
#  system will auto-detect the interval from the instruments dump.
# ──────────────────────────────────────────────────────────────

STRIKE_INTERVALS = {}

# Default strike interval for stocks not explicitly listed above.
# The ATM Resolver will auto-detect from the instruments dump if possible.
DEFAULT_STRIKE_INTERVAL = 25


# ──────────────────────────────────────────────────────────────
#  ATM RESOLVER CONFIGURATION
# ──────────────────────────────────────────────────────────────

ATM_UPDATE_INTERVAL_SECONDS = 30   # Re-calculate ATM every 30 seconds
STRIKE_RANGE = 10                  # ATM ± 10 strikes


# ──────────────────────────────────────────────────────────────
#  WEBSOCKET CONFIGURATION
# ──────────────────────────────────────────────────────────────

MAX_INSTRUMENTS_PER_WORKER = 3000
WORKER_COUNT = 3


# ──────────────────────────────────────────────────────────────
#  CACHE CONFIGURATION
# ──────────────────────────────────────────────────────────────

INSTRUMENTS_CACHE_DIR = "cache"
INSTRUMENTS_CACHE_FILENAME = "instruments_nfo.json"
CACHE_EXPIRY_HOURS = 18  # Re-download if cache is older than 18 hours


# ──────────────────────────────────────────────────────────────
#  KITE QUOTE API BATCH SIZE
#  Kite allows a maximum of ~500 instruments per quote() call.
# ──────────────────────────────────────────────────────────────

QUOTE_BATCH_SIZE = 450


# ──────────────────────────────────────────────────────────────
#  NSE HOLIDAYS (Market Closed Days, excluding weekends)
#  Update this list at the start of each year.
#  Source: https://www.nseindia.com/resources/exchange-communication-holidays
# ──────────────────────────────────────────────────────────────

NSE_HOLIDAYS_2026 = {
    "2026-01-26",   # Republic Day
    "2026-02-17",   # Maha Shivaratri
    "2026-03-10",   # Ramadan (Eid ul-Fitr) - tentative
    "2026-03-17",   # Holi
    "2026-03-30",   # Id-ul-Fitr (Eid)
    "2026-04-02",   # Ram Navami
    "2026-04-03",   # Mahavir Jayanti
    "2026-04-14",   # Dr. Ambedkar Jayanti / Good Friday
    "2026-05-01",   # Maharashtra Day / May Day
    "2026-05-25",   # Buddha Purnima
    "2026-06-07",   # Eid ul-Adha (Bakrid) - tentative
    "2026-07-07",   # Muharram - tentative
    "2026-08-15",   # Independence Day
    "2026-08-27",   # Janmashtami
    "2026-09-05",   # Milad-un-Nabi - tentative
    "2026-10-02",   # Mahatma Gandhi Jayanti
    "2026-10-20",   # Dussehra
    "2026-10-21",   # Dussehra (additional)
    "2026-11-09",   # Diwali (Laxmi Puja)
    "2026-11-10",   # Diwali (Balipratipada)
    "2026-11-20",   # Guru Nanak Jayanti
    "2026-12-25",   # Christmas
}


# ── IST Timezone (Hostinger VPS may be in UTC, so always use IST) ──
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))


def now_ist():
    """Get current datetime in IST. Works on any VPS regardless of system TZ."""
    return datetime.now(IST)


def is_trading_day(dt) -> bool:
    """
    Check if a given date is a trading day (not weekend, not NSE holiday).

    Args:
        dt: datetime or date object.

    Returns:
        True if the market is open on this date.
    """
    if dt.weekday() in (5, 6):  # Saturday=5, Sunday=6
        return False
    date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)
    return date_str not in NSE_HOLIDAYS_2026


def get_previous_trading_day(from_date=None, max_lookback=10):
    """
    Get the previous trading day relative to `from_date`.

    Skips weekends (Sat, Sun) AND NSE holidays.
    For Monday → returns Friday (unless Friday was a holiday, then Thursday, etc.)

    Args:
        from_date: datetime object. Defaults to today in IST.
        max_lookback: Maximum days to look back (default 10).

    Returns:
        datetime object of the previous trading day, or None if not found.
    """
    if from_date is None:
        from_date = now_ist()

    check_date = from_date - timedelta(days=1)
    for _ in range(max_lookback):
        if is_trading_day(check_date):
            return check_date
        check_date -= timedelta(days=1)

    return None
