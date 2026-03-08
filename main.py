import logging
import multiprocessing as mp
from logging.config import dictConfig
import os
import time
import threading

from core.database import DatabaseManager
from core.token_manager import TokenManager
from api.app import create_app
from config import KITE_CREDENTIALS, FLASK_PORT

# ──────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────
dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'console': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://sys.stdout',
        'formatter': 'default'
    }},
    'root': {
        'level': os.getenv('LOG_LEVEL', 'INFO'),
        'handlers': ['console']
    }
})

logger = logging.getLogger(__name__)


def wait_for_token(token_manager, db, flask_port):
    """
    Start Flask in token-setup-only mode and wait until a valid
    access token is submitted by the user via the web UI.
    """
    logger.info("=" * 60)
    logger.info("🔑 NO VALID ACCESS TOKEN FOUND FOR TODAY")
    logger.info("=" * 60)
    logger.info("Opening token setup server on http://localhost:%s", flask_port)
    logger.info("Please open your browser and follow the setup steps.")
    logger.info("Login URL: %s", token_manager.get_login_url())
    logger.info("=" * 60)

    # Create a minimal Flask app just for token setup
    setup_app = create_app(
        database=db,
        token_manager=token_manager,
    )

    # We run the Flask setup server in a background thread
    # and poll for the token in the main thread
    server_thread = threading.Thread(
        target=lambda: setup_app.run(
            host="0.0.0.0",
            port=flask_port,
            debug=False,
            use_reloader=False,
        ),
        daemon=True,
    )
    server_thread.start()

    # Poll until token is available
    while True:
        token = token_manager.get_today_token()
        if token:
            logger.info("✅ Access token received! Proceeding with full startup...")
            return token
        time.sleep(2)


def start_full_platform(access_token, db, token_manager):
    """
    Start the full analytics platform with a valid access token.
    """
    from core.instrument_manager import InstrumentManager
    from core.atm_resolver import ATMResolver
    from core.volume_aggregator import VolumeAggregator
    from core.alert_engine import AlertEngine
    from workers.supervisor import WorkerSupervisor

    api_key = KITE_CREDENTIALS["api_key"]

    # Build credentials dict with the token from MongoDB
    live_credentials = {
        "api_key": api_key,
        "api_secret": KITE_CREDENTIALS["api_secret"],
        "access_token": access_token,
    }

    # 2. Setup Instruments and Resolve initial ATM layout
    logger.info("Initializing Instrument Manager...")
    ins_mgr = InstrumentManager(
        api_key=api_key,
        access_token=access_token,
    )
    ins_mgr.load_instruments()

    logger.info("Resolving initial tokens via ATM Resolver...")
    resolver = ATMResolver(ins_mgr)
    resolver.resolve_all()
    batches = resolver.get_worker_batches()

    # 3. Start KiteTicker Streaming Worker Processes
    logger.info("Spawning WebSocket Worker Processes...")
    supervisor = WorkerSupervisor(live_credentials, batches)
    supervisor.start()

    # Get the internal queue created by supervisor
    queue = supervisor.tick_queue
    tick_agg = supervisor.aggregator

    vol_agg = VolumeAggregator(tick_agg)
    vol_agg.build_token_map(resolver.resolve_all())
    vol_agg.start()

    alert_engine = AlertEngine(vol_agg, db)
    alert_engine.start()

    # Background thread to snapshot EOD volumes daily at 4:00 PM
    def eod_snapshot_scheduler():
        import datetime
        logger.info("EOD Snapshot daemon started. Awaiting 16:00 (4:00 PM) trigger...")
        while True:
            now = datetime.datetime.now()

            # ── Skip weekends (Saturday=5, Sunday=6) ──
            if now.weekday() in (5, 6):
                # On weekends, just sleep and check again — never store 0s
                time.sleep(60)
                continue

            if now.hour == 16 and now.minute == 0:
                logger.info("Market closed. Storing live aggregated volumes as EOD for tomorrow...")
                today_str = now.strftime("%Y-%m-%d")
                vols = vol_agg.get_volumes()

                # ── Safety check: Only store if we have real (non-zero) data ──
                total_volume = sum(
                    v.get("call_volume", 0) + v.get("put_volume", 0)
                    for v in vols.values()
                ) if vols else 0

                if total_volume > 0 and db.is_connected:
                    db.store_eod_volumes(vols, date=today_str)
                    logger.info(
                        "Successfully took snapshot of %d symbols into DB for %s (total volume: %d)",
                        len(vols), today_str, total_volume,
                    )
                    db.delete_old_eod_volumes(keep_date=today_str)
                    db.delete_old_alerts(keep_date=today_str)
                    logger.info(
                        "Cleaned up previous day EOD volumes and alerts. Only %s data remains.",
                        today_str,
                    )
                else:
                    logger.warning(
                        "⚠️ Skipping EOD snapshot — total volume is 0 (no live data). "
                        "Previous day data preserved."
                    )

                time.sleep(65)  # Skip past the 16:00 minute
            time.sleep(20)

    threading.Thread(target=eod_snapshot_scheduler, daemon=True).start()

    # 4. Build and Start the Flask Server (full mode)
    logger.info("Starting Flask API Server on port %s...", FLASK_PORT)
    app = create_app(
        volume_aggregator=vol_agg,
        alert_engine=alert_engine,
        database=db,
        tick_aggregator=tick_agg,
        supervisor=supervisor,
        token_manager=token_manager,
    )

    try:
        app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    finally:
        supervisor.stop()
        alert_engine.stop()
        vol_agg.stop()
        tick_agg.stop()
        logger.info("Shutdown complete.")


def main():
    logger.info("Starting Options Analytics Local Platform 🚀")

    # 1. Connect MongoDB (required for token storage)
    db = DatabaseManager()
    if not db.connect():
        logger.error(
            "❌ Could not connect to MongoDB! Token storage requires MongoDB. "
            "Check MONGO_URI in .env"
        )
        return

    # 2. Initialize Token Manager
    api_key = KITE_CREDENTIALS.get("api_key", "")
    api_secret = KITE_CREDENTIALS.get("api_secret", "")

    if not api_key or not api_secret:
        logger.error(
            "❌ KITE_API_KEY or KITE_API_SECRET is missing in your .env file!"
        )
        return

    token_manager = TokenManager(db, api_key, api_secret)
    token_manager.cleanup_old_tokens()

    # 3. Check for today's valid token in MongoDB
    access_token = token_manager.get_today_token()

    if not access_token:
        # No valid token — start in setup mode and wait
        access_token = wait_for_token(token_manager, db, FLASK_PORT)

    # 4. We have a valid token — start the full platform
    logger.info("🚀 Access token ready. Launching full platform...")
    start_full_platform(access_token, db, token_manager)


if __name__ == "__main__":
    # Required for Windows multiprocessing support
    mp.freeze_support()
    main()

