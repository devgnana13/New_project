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
        if tm and not tm.get_today_token():
            return redirect("/setup")
        dashboard_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard")
        return send_from_directory(dashboard_dir, "index.html")

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
        return jsonify({
            "has_valid_token": today_token is not None,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "login_url": tm.get_login_url(),
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

        Response:
        {
            "status": "ok",
            "timestamp": "2026-03-05T10:30:00",
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

        return jsonify({
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
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
            date:   Date string YYYY-MM-DD (default: today)
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

        # Default to yesterday if not supplied
        if not date:
            from datetime import timedelta
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        if symbol:
            vol = db.get_eod_volume(symbol, date)
            data = [vol] if vol else []
        else:
            data = db.get_all_eod_volumes(date=date)

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
