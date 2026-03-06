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
