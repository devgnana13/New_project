"""
Flask API Server — Serves live option analytics data to the dashboard.

Endpoints:
    GET /api/live-volumes      Live CE/PE volume totals for all stocks
    GET /api/live-volumes/<sym> Live volumes for a single stock
    GET /api/alerts             Today's triggered alerts
    GET /api/alerts/history     Recent alerts across all days
    GET /api/health             System health & component status

Architecture:
    ┌──────────────────────────────────────────────────────────┐
    │                    FLASK API SERVER                       │
    │                                                          │
    │  Dashboard ──(1/s poll)──▶ /api/live-volumes             │
    │                                ▲                         │
    │                                │                         │
    │                    ┌───────────┴───────────┐             │
    │                    │   VolumeAggregator     │             │
    │                    │   (in-memory, 1s TTL)  │             │
    │                    └───────────────────────┘             │
    │                                                          │
    │  Dashboard ──(poll)──▶ /api/alerts                       │
    │                             ▲                            │
    │                             │                            │
    │                    ┌────────┴────────────┐               │
    │                    │   AlertEngine        │               │
    │                    │   DatabaseManager    │               │
    │                    └─────────────────────┘               │
    └──────────────────────────────────────────────────────────┘

Performance:
    - /api/live-volumes reads directly from VolumeAggregator's
      in-memory dict — sub-millisecond response time.
    - CORS enabled for dashboard (separate origin).
    - JSON responses with proper Content-Type headers.
    - No database queries on the hot path (live-volumes).

Usage:
    from api.app import create_app

    app = create_app(
        volume_aggregator=vol_agg,
        alert_engine=alert_engine,
        database=db,
    )
    app.run(host="0.0.0.0", port=5000)
"""

import os
import time
import logging
from datetime import datetime
from functools import wraps

from flask import Flask, jsonify, request, send_from_directory, redirect
from flask_cors import CORS

logger = logging.getLogger(__name__)


def create_app(
    volume_aggregator=None,
    alert_engine=None,
    database=None,
    tick_aggregator=None,
    supervisor=None,
    token_manager=None,
):
    """
    Application factory — creates and configures the Flask app.

    Args:
        volume_aggregator:  VolumeAggregator instance (live volumes).
        alert_engine:       AlertEngine instance (alert data).
        database:           DatabaseManager instance (historical data).
        tick_aggregator:    TickAggregator instance (raw tick stats).
        supervisor:         WorkerSupervisor instance (worker stats).

    Returns:
        Configured Flask application.
    """
    app = Flask(__name__)
    CORS(app)

    # ── Store references for handlers ──
    app.config["volume_aggregator"] = volume_aggregator
    app.config["alert_engine"] = alert_engine
    app.config["database"] = database
    app.config["tick_aggregator"] = tick_aggregator
    app.config["supervisor"] = supervisor
    app.config["token_manager"] = token_manager

    # ──────────────────────────────────────────────────────────
    #  MIDDLEWARE — Response timing
    # ──────────────────────────────────────────────────────────

    @app.before_request
    def start_timer():
        request._start_time = time.time()

    @app.after_request
    def add_timing_header(response):
        if hasattr(request, "_start_time"):
            elapsed = (time.time() - request._start_time) * 1000
            response.headers["X-Response-Time"] = f"{elapsed:.1f}ms"
        response.headers["X-Timestamp"] = datetime.now().isoformat()
        return response

    # ──────────────────────────────────────────────────────────
    #  ENDPOINT: / (Serve Dashboard)
    # ──────────────────────────────────────────────────────────

    @app.route("/")
    def serve_dashboard():
        """Serve the frontend dashboard. Redirects to token setup if no valid token."""
        tm = app.config.get("token_manager")
        if tm:
            # Invalidate cache to force a fresh DB check for today's date
            tm.invalidate_token()
            if not tm.get_today_token():
                return redirect("/setup")
        dashboard_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard")
        return send_from_directory(dashboard_dir, "index.html")

    @app.route("/oi")
    def serve_oi_dashboard():
        """Serve the OI Analysis dashboard."""
        tm = app.config.get("token_manager")
        if tm:
            tm.invalidate_token()
            if not tm.get_today_token():
                return redirect("/setup")
        dashboard_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard")
        return send_from_directory(dashboard_dir, "oi_analysis.html")

    @app.route("/setup")
    def serve_token_setup():
        """Serve the token setup page."""
        dashboard_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard")
        return send_from_directory(dashboard_dir, "token_setup.html")

    # ──────────────────────────────────────────────────────────
    #  ENDPOINT: /api/token/* (Token Management)
    # ──────────────────────────────────────────────────────────

    @app.route("/api/token/status", methods=["GET"])
    def get_token_status():
        """
        Check if a valid access token exists for today.

        Response:
        {
            "has_valid_token": true/false,
            "date": "2026-03-06",
            "login_url": "https://kite.zerodha.com/connect/login?..."
        }
        """
        tm = app.config.get("token_manager")
        if tm is None:
            return jsonify({
                "has_valid_token": False,
                "error": "Token manager not initialized",
            }), 503

        today_token = tm.get_today_token()
        last_info = tm.get_last_token_info()
        return jsonify({
            "has_valid_token": today_token is not None,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "login_url": tm.get_login_url(),
            "last_token_date": last_info.get("date") if last_info else None,
            "last_token_created_at": last_info.get("created_at") if last_info else None,
        })

    @app.route("/api/token/generate", methods=["POST"])
    def generate_token():
        """
        Convert a request_token to an access_token and store in MongoDB.

        Request Body:
        {
            "request_token": "<token from Kite login redirect>"
        }

        Response (success):
        {
            "success": true,
            "access_token": "..."
        }

        Response (failure):
        {
            "success": false,
            "error": "..."
        }
        """
        tm = app.config.get("token_manager")
        if tm is None:
            return jsonify({
                "success": False,
                "error": "Token manager not initialized",
            }), 503

        data = request.get_json()
        if not data or not data.get("request_token"):
            return jsonify({
                "success": False,
                "error": "Missing 'request_token' in request body",
            }), 400

        result = tm.generate_and_store_token(data["request_token"])
        status_code = 200 if result["success"] else 400
        return jsonify(result), status_code

    # ──────────────────────────────────────────────────────────
    #  ENDPOINT: /api/live-volumes
    # ──────────────────────────────────────────────────────────

    @app.route("/api/live-volumes", methods=["GET"])
    def get_live_volumes():
        """
        Return live CE/PE volume totals for all stocks.
        On non-trading days (weekends/holidays), falls back to
        the last trading day's EOD data.

        Response:
        {
            "status": "ok",
            "timestamp": "2026-03-05T10:30:00",
            "market_closed": false,
            "data_source": "live",
            "count": 170,
            "data": {
                "RELIANCE": {
                    "call_volume": 150000,
                    "put_volume": 120000
                },
                ...
            }
        }
        """
        vol_agg = app.config.get("volume_aggregator")
        if vol_agg is None:
            return jsonify({
                "status": "error",
                "message": "Volume aggregator not initialized",
            }), 503

        volumes = vol_agg.get_volumes()

        # Check if we have any real live data
        total_volume = sum(
            v.get("call_volume", 0) + v.get("put_volume", 0)
            for v in volumes.values()
        ) if volumes else 0

        market_closed = total_volume == 0
        data_source = "live"
        data_date = None

        # If no live data, fall back to last trading day's EOD
        if market_closed and volumes:
            db = app.config.get("database")
            if db:
                from datetime import timedelta
                # Search backwards for the last day with actual data
                for days_back in range(1, 8):
                    check_date = datetime.now() - timedelta(days=days_back)
                    if check_date.weekday() in (5, 6):
                        continue
                    date_str = check_date.strftime("%Y-%m-%d")
                    eod_data = db.get_all_eod_volumes(date=date_str)
                    if eod_data:
                        # Convert EOD format to live volume format
                        for item in eod_data:
                            symbol = item.get("symbol", "")
                            if symbol in volumes:
                                volumes[symbol] = {
                                    "call_volume": item.get("call_volume_total", 0),
                                    "put_volume": item.get("put_volume_total", 0),
                                }
                        data_source = f"eod_{date_str}"
                        data_date = date_str
                        break

        return jsonify({
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "market_closed": market_closed,
            "data_source": data_source,
            "data_date": data_date,
            "count": len(volumes),
            "data": volumes,
        })

    @app.route("/api/live-volumes/<symbol>", methods=["GET"])
    def get_live_volume_symbol(symbol):
        """
        Return live volume for a single stock.

        Response:
        {
            "status": "ok",
            "symbol": "RELIANCE",
            "data": {
                "call_volume": 150000,
                "put_volume": 120000
            }
        }
        """
        vol_agg = app.config.get("volume_aggregator")
        if vol_agg is None:
            return jsonify({
                "status": "error",
                "message": "Volume aggregator not initialized",
            }), 503

        vol = vol_agg.get_symbol_volume(symbol)
        if vol is None:
            return jsonify({
                "status": "error",
                "message": f"Symbol '{symbol.upper()}' not found",
            }), 404

        return jsonify({
            "status": "ok",
            "symbol": symbol.upper(),
            "timestamp": datetime.now().isoformat(),
            "data": vol,
        })

    # ──────────────────────────────────────────────────────────
    #  ENDPOINT: /api/live-volumes/detailed
    # ──────────────────────────────────────────────────────────

    @app.route("/api/live-volumes/detailed", methods=["GET"])
    def get_live_volumes_detailed():
        """
        Return detailed volume data including token counts and PCR.

        Response:
        {
            "status": "ok",
            "count": 170,
            "data": {
                "RELIANCE": {
                    "call_volume": 150000,
                    "put_volume": 120000,
                    "call_tokens": 21,
                    "put_tokens": 21,
                    "pcr": 0.8,
                    "updated_at": "..."
                }
            }
        }
        """
        vol_agg = app.config.get("volume_aggregator")
        if vol_agg is None:
            return jsonify({
                "status": "error",
                "message": "Volume aggregator not initialized",
            }), 503

        detailed = vol_agg.get_detailed_volumes()
        pcrs = vol_agg.get_all_pcr()

        # Merge PCR into detailed data
        for symbol in detailed:
            detailed[symbol]["pcr"] = pcrs.get(symbol)

        return jsonify({
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "count": len(detailed),
            "data": detailed,
        })

    # ──────────────────────────────────────────────────────────
    #  ENDPOINT: /api/oi-data
    # ──────────────────────────────────────────────────────────

    @app.route("/api/oi-data", methods=["GET"])
    def get_oi_data():
        """
        Return detailed OI data including strike-level changes and alerts.
        """
        vol_agg = app.config.get("volume_aggregator")
        db = app.config.get("database")
        if vol_agg is None or db is None:
            return jsonify({
                "status": "error",
                "message": "System not fully initialized",
            }), 503

        detailed = vol_agg.get_detailed_volumes()

        # Build OI response
        results = {}
        for symbol, data in detailed.items():
            live_strikes = data.get("strike_details", {})
            yesterday_data = db.get_yesterday_volume(symbol)
            yesterday_strikes = yesterday_data.get("strike_details", {}) if yesterday_data else {}

            strike_results = {}
            alerts = []
            total_ce_oi_chg = 0
            total_pe_oi_chg = 0
            
            for strike, live in live_strikes.items():
                yest = yesterday_strikes.get(str(strike), yesterday_strikes.get(strike, {}))
                
                ce_oi_live = live.get("ce_oi", 0)
                pe_oi_live = live.get("pe_oi", 0)
                ce_oi_yest = yest.get("ce_oi", 0) if isinstance(yest, dict) else 0
                pe_oi_yest = yest.get("pe_oi", 0) if isinstance(yest, dict) else 0

                ce_oi_chg = ce_oi_live - ce_oi_yest if ce_oi_yest else 0
                pe_oi_chg = pe_oi_live - pe_oi_yest if pe_oi_yest else 0
                
                total_ce_oi_chg += ce_oi_chg
                total_pe_oi_chg += pe_oi_chg

                strike_results[strike] = {
                    "ce_oi": ce_oi_live,
                    "pe_oi": pe_oi_live,
                    "ce_oi_chg": ce_oi_chg,
                    "pe_oi_chg": pe_oi_chg
                }

                if ce_oi_chg <= -100:
                    alerts.append({"type": "CE_OI_DROP", "strike": strike, "change": ce_oi_chg})
                elif ce_oi_chg >= 100:
                    alerts.append({"type": "CE_OI_SPIKE", "strike": strike, "change": ce_oi_chg})

                if pe_oi_chg <= -100:
                    alerts.append({"type": "PE_OI_DROP", "strike": strike, "change": pe_oi_chg})
                elif pe_oi_chg >= 100:
                    alerts.append({"type": "PE_OI_SPIKE", "strike": strike, "change": pe_oi_chg})

            # Get overall
            call_oi = data.get("call_oi", 0)
            put_oi = data.get("put_oi", 0)

            results[symbol] = {
                "symbol": symbol,
                "call_oi": call_oi,
                "put_oi": put_oi,
                "call_oi_chg": total_ce_oi_chg,
                "put_oi_chg": total_pe_oi_chg,
                "strikes": strike_results,
                "alerts": alerts,
                "updated_at": data.get("updated_at")
            }

        return jsonify({
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "count": len(results),
            "data": results,
        })

    # ──────────────────────────────────────────────────────────
    #  ENDPOINT: /api/alerts
    # ──────────────────────────────────────────────────────────

    @app.route("/api/alerts", methods=["GET"])
    def get_alerts():
        """
        Return today's alerts.

        Query params:
            symbol:  Filter by symbol (optional)
            type:    Filter by alert_type (optional)

        Response:
        {
            "status": "ok",
            "count": 5,
            "data": [
                {
                    "symbol": "RELIANCE",
                    "alert_type": "CALL_VOLUME_SPIKE",
                    "message": "...",
                    "data": {...},
                    "date": "2026-03-05"
                }
            ]
        }
        """
        db = app.config.get("database")
        alert_eng = app.config.get("alert_engine")

        # Try database first for persistent alerts
        if db is not None:
            symbol = request.args.get("symbol")
            alert_type = request.args.get("type")
            today = datetime.now().strftime("%Y-%m-%d")

            alerts = db.get_alerts(
                symbol=symbol,
                date=today,
                alert_type=alert_type,
            )

            return jsonify({
                "status": "ok",
                "date": today,
                "count": len(alerts),
                "data": alerts,
            })

        # Fallback to in-memory fired alerts
        if alert_eng is not None:
            fired = alert_eng.get_fired_alerts()
            return jsonify({
                "status": "ok",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "count": len(fired),
                "data": fired,
            })

        return jsonify({
            "status": "error",
            "message": "Alert system not initialized",
        }), 503

    @app.route("/api/alerts/history", methods=["GET"])
    def get_alerts_history():
        """
        Return recent alerts across all dates.

        Query params:
            limit: Max number of alerts (default: 50)

        Response:
        {
            "status": "ok",
            "count": 50,
            "data": [...]
        }
        """
        db = app.config.get("database")
        if db is None:
            return jsonify({
                "status": "error",
                "message": "Database not initialized",
            }), 503

        limit = request.args.get("limit", 50, type=int)
        limit = min(limit, 200)  # Cap at 200

        alerts = db.get_recent_alerts(limit=limit)

        return jsonify({
            "status": "ok",
            "count": len(alerts),
            "data": alerts,
        })

    # ──────────────────────────────────────────────────────────
    #  ENDPOINT: /api/eod-volumes
    # ──────────────────────────────────────────────────────────

    @app.route("/api/eod-volumes", methods=["GET"])
    def get_eod_volumes():
        """
        Return EOD volumes for a given date.

        Query params:
            date:   Date string YYYY-MM-DD (default: last trading day)
            symbol: Filter by symbol (optional)

        Response:
        {
            "status": "ok",
            "data": [
                {"symbol": "RELIANCE", "date": "2026-03-05",
                 "call_volume_total": 150000, "put_volume_total": 120000}
            ]
        }
        """
        db = app.config.get("database")
        if db is None:
            return jsonify({
                "status": "error",
                "message": "Database not initialized",
            }), 503

        date = request.args.get("date")
        symbol = request.args.get("symbol")

        # Default to last trading day (skip weekends)
        if not date:
            from datetime import timedelta
            check_date = datetime.now() - timedelta(days=1)
            # Skip backwards over weekends
            while check_date.weekday() in (5, 6):  # Sat=5, Sun=6
                check_date -= timedelta(days=1)
            date = check_date.strftime("%Y-%m-%d")

        if symbol:
            vol = db.get_eod_volume(symbol, date)
            data = [vol] if vol else []
        else:
            data = db.get_all_eod_volumes(date=date)

        # Fallback: if no data found, search back up to 7 days
        if not data and not request.args.get("date"):
            from datetime import timedelta
            for days_back in range(2, 8):
                fallback_date = datetime.now() - timedelta(days=days_back)
                if fallback_date.weekday() in (5, 6):
                    continue
                fallback_str = fallback_date.strftime("%Y-%m-%d")
                data = db.get_all_eod_volumes(date=fallback_str)
                if data:
                    date = fallback_str
                    break

        return jsonify({
            "status": "ok",
            "date": date,
            "count": len(data),
            "data": data,
        })

    # ──────────────────────────────────────────────────────────
    #  ENDPOINT: /api/health
    # ──────────────────────────────────────────────────────────

    @app.route("/api/health", methods=["GET"])
    def get_health():
        """
        Return system health and component status.

        Response:
        {
            "status": "ok",
            "timestamp": "...",
            "components": {
                "volume_aggregator": {...},
                "alert_engine": {...},
                "tick_aggregator": {...},
                "supervisor": {...},
                "database": {...}
            }
        }
        """
        components = {}

        vol_agg = app.config.get("volume_aggregator")
        if vol_agg:
            components["volume_aggregator"] = vol_agg.get_stats()

        alert_eng = app.config.get("alert_engine")
        if alert_eng:
            components["alert_engine"] = alert_eng.get_stats()

        tick_agg = app.config.get("tick_aggregator")
        if tick_agg:
            components["tick_aggregator"] = tick_agg.get_stats()

        sup = app.config.get("supervisor")
        if sup:
            components["supervisor"] = sup.get_stats()

        db = app.config.get("database")
        if db:
            components["database"] = db.get_stats()

        return jsonify({
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "components": components,
        })

    # ──────────────────────────────────────────────────────────
    #  ENDPOINT: /api/debug/ticks (Diagnostic)
    # ──────────────────────────────────────────────────────────

    @app.route("/api/debug/ticks", methods=["GET"])
    def get_debug_ticks():
        """
        Return raw tick data for a specific symbol's tokens.
        Useful for diagnosing volume issues.

        Query params:
            symbol: Stock symbol (default: NIFTY)
            limit:  Max ticks to return (default: 5)
        """
        vol_agg = app.config.get("volume_aggregator")
        tick_agg = app.config.get("tick_aggregator")

        if not vol_agg or not tick_agg:
            return jsonify({"status": "error", "message": "Aggregators not initialized"}), 503

        symbol = request.args.get("symbol", "NIFTY").upper()
        limit = request.args.get("limit", 5, type=int)

        # Get token mapping for this symbol
        with vol_agg._lock:
            symbol_tokens = vol_agg._symbol_tokens.get(symbol, {})
            token_map = dict(vol_agg._token_map)

        ce_tokens = symbol_tokens.get("CE", [])[:limit]
        pe_tokens = symbol_tokens.get("PE", [])[:limit]

        result = {
            "symbol": symbol,
            "ce_token_count": len(symbol_tokens.get("CE", [])),
            "pe_token_count": len(symbol_tokens.get("PE", [])),
            "ce_ticks": [],
            "pe_ticks": [],
            "total_instruments_in_aggregator": len(tick_agg._ticks),
        }

        for token in ce_tokens:
            tick = tick_agg.get_tick(token)
            mapping = token_map.get(token, ("?", "?", 1, 0.0))
            result["ce_ticks"].append({
                "token": token,
                "strike": mapping[3] if len(mapping) > 3 else "?",
                "has_tick": tick is not None,
                "last_price": tick.last_price if tick else None,
                "volume": tick.volume if tick else None,
                "timestamp": str(tick.timestamp) if tick and tick.timestamp else None,
                "received_at": tick.received_at if tick else None,
            })

        for token in pe_tokens:
            tick = tick_agg.get_tick(token)
            mapping = token_map.get(token, ("?", "?", 1, 0.0))
            result["pe_ticks"].append({
                "token": token,
                "strike": mapping[3] if len(mapping) > 3 else "?",
                "has_tick": tick is not None,
                "last_price": tick.last_price if tick else None,
                "volume": tick.volume if tick else None,
                "timestamp": str(tick.timestamp) if tick and tick.timestamp else None,
                "received_at": tick.received_at if tick else None,
            })

        return jsonify({"status": "ok", "data": result})

    # ──────────────────────────────────────────────────────────
    #  ERROR HANDLERS
    # ──────────────────────────────────────────────────────────

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({
            "status": "error",
            "message": "Endpoint not found",
        }), 404

    @app.errorhandler(500)
    def internal_error(e):
        logger.error("Internal server error: %s", e)
        return jsonify({
            "status": "error",
            "message": "Internal server error",
        }), 500

    return app
