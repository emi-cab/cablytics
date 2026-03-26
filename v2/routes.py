"""
CABlytics V2 — Flask routes.

Endpoints:
  POST /v2/client/create                  Register a new client
  POST /v2/client/<slug>/reviews          Update stored reviews for a client
  GET  /v2/client/<slug>                  Get client config (admin use)
  GET  /v2/clients                        List all clients (admin use)
  POST /v2/report/run                     Trigger a manual report run
  GET  /v2/report/<slug>                  Get latest completed report JSON
  GET  /v2/report/<slug>/status           Get current run status
  GET  /v2/dashboard/<slug>               Serve the client dashboard HTML
  GET  /v2/admin/onboard                  Serve the admin onboarding form
  GET  /v2/admin/clients                  Serve the admin client list page
  GET  /v2/health                         V2-specific health check
"""

import threading
from flask import Blueprint, request, jsonify, render_template, abort

from v2.db import (
    init_db,
    create_client,
    update_client,
    get_client_by_slug,
    list_clients,
    get_latest_report,
    get_active_report,
    list_reports,
    get_run_log,
)
from v2.pipeline import run_pipeline
from v2.scheduler import register_client_job

v2 = Blueprint("v2", __name__, url_prefix="/v2",
               template_folder="templates")

# Initialise DB tables on blueprint load
init_db()


# ── Health ─────────────────────────────────────────────────────────────────────

@v2.route("/health")
def health():
    return jsonify({"status": "healthy", "version": "2"})


# ── Client management ──────────────────────────────────────────────────────────

@v2.route("/client/create", methods=["POST"])
def client_create():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "No JSON payload provided"}), 400

    required = ["client_name", "client_slug", "ga4_property_id"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    # Check slug is not already taken
    if get_client_by_slug(data["client_slug"]):
        return jsonify({"error": f"A client with slug '{data['client_slug']}' already exists"}), 409

    client = create_client(data)

    # Register scheduled job
    try:
        register_client_job(client)
    except Exception as e:
        print(f"[V2][routes] Scheduler registration failed for {client['client_slug']}: {e}", flush=True)

    # Optionally trigger an immediate first run
    if data.get("run_now"):
        thread = threading.Thread(
            target=run_pipeline,
            args=(client["client_slug"], "initial"),
            daemon=True
        )
        thread.start()

    return jsonify({
        "success": True,
        "client": client,
        "dashboard_url": f"/v2/dashboard/{client['client_slug']}",
        "run_triggered": bool(data.get("run_now")),
    }), 201


@v2.route("/client/<slug>/reviews", methods=["POST"])
def client_update_reviews(slug):
    if not get_client_by_slug(slug):
        return jsonify({"error": "Client not found"}), 404

    data = request.get_json(silent=True) or {}
    updatable = ["customer_reviews", "competitor_notes", "current_pdp_copy",
                 "client_context", "target_urls", "monthly_traffic",
                 "dev_hours_per_week", "ga4_property_id"]
    updates = {k: data[k] for k in updatable if k in data}

    if not updates:
        return jsonify({"error": "No updatable fields provided"}), 400

    updated = update_client(slug, updates)
    return jsonify({"success": True, "client": updated})


@v2.route("/client/<slug>")
def client_get(slug):
    client = get_client_by_slug(slug)
    if not client:
        return jsonify({"error": "Client not found"}), 404
    # Strip sensitive review data from API response
    safe = {k: v for k, v in client.items()
            if k not in ("customer_reviews", "competitor_notes")}
    return jsonify(safe)


@v2.route("/clients")
def clients_list():
    clients = list_clients()
    safe = [
        {k: v for k, v in c.items() if k not in ("customer_reviews", "competitor_notes")}
        for c in clients
    ]
    return jsonify(safe)


# ── Report runs ────────────────────────────────────────────────────────────────

@v2.route("/report/run", methods=["POST"])
def report_run():
    data = request.get_json(silent=True) or {}
    slug = data.get("client_slug")
    if not slug:
        return jsonify({"error": "client_slug is required"}), 400

    client = get_client_by_slug(slug)
    if not client:
        return jsonify({"error": "Client not found"}), 404

    # Prevent duplicate concurrent runs
    active = get_active_report(client["id"])
    if active:
        return jsonify({
            "error": "A report run is already in progress for this client",
            "report_id": active["id"],
            "status": active["status"],
        }), 409

    # Run pipeline in background thread
    triggered_by = data.get("triggered_by", "manual")
    thread = threading.Thread(
        target=run_pipeline,
        args=(slug, triggered_by),
        daemon=True
    )
    thread.start()

    return jsonify({
        "success": True,
        "message": f"Pipeline started for {client['client_name']}",
        "client_slug": slug,
    }), 202


@v2.route("/report/<slug>")
def report_get(slug):
    client = get_client_by_slug(slug)
    if not client:
        return jsonify({"error": "Client not found"}), 404

    report = get_latest_report(slug)
    if not report:
        # Check if one is currently running
        active = get_active_report(client["id"])
        if active:
            return jsonify({
                "status": active["status"],
                "message": "Report is currently running",
                "report_id": active["id"],
            }), 202
        return jsonify({"error": "No completed report found for this client"}), 404

    import json
    full = json.loads(report["full_report_json"]) if report.get("full_report_json") else {}
    return jsonify({
        "client": client["client_name"],
        "client_slug": slug,
        "report_id": report["id"],
        "last_run": report["completed_at"],
        "triggered_by": report["run_triggered_by"],
        "status": report["status"],
        "agents": full.get("agents", {}),
    })


@v2.route("/report/<slug>/status")
def report_status(slug):
    client = get_client_by_slug(slug)
    if not client:
        return jsonify({"error": "Client not found"}), 404

    active = get_active_report(client["id"])
    if active:
        log = get_run_log(client["id"], report_id=active["id"])
        return jsonify({
            "status": active["status"],
            "report_id": active["id"],
            "started_at": active["started_at"],
            "log": [{"event": e["event"], "agent": e["agent_number"],
                     "message": e["message"], "timestamp": e["timestamp"]}
                    for e in log],
        })

    latest = get_latest_report(slug)
    if latest:
        return jsonify({
            "status": "complete",
            "report_id": latest["id"],
            "completed_at": latest["completed_at"],
        })

    return jsonify({"status": "no_reports"})


# ── Dashboard ──────────────────────────────────────────────────────────────────

@v2.route("/dashboard/<slug>")
def dashboard(slug):
    client = get_client_by_slug(slug)
    if not client:
        abort(404)

    import json

    report      = get_latest_report(slug)
    active      = get_active_report(client["id"])
    report_data = {}
    agents      = {}

    if report and report.get("full_report_json"):
        try:
            full        = json.loads(report["full_report_json"])
            agents      = full.get("agents", {})
            report_data = report
        except (json.JSONDecodeError, TypeError):
            pass

    recent_runs = list_reports(slug, limit=5)

    return render_template(
        "dashboard.html",
        client=client,
        report=report_data,
        agents=agents,
        active_run=active,
        recent_runs=recent_runs,
    )


# ── Admin pages ────────────────────────────────────────────────────────────────

@v2.route("/admin/onboard")
def admin_onboard():
    return render_template("admin_onboard.html")


@v2.route("/admin/clients")
def admin_clients():
    clients = list_clients()
    reports_by_slug = {}
    for c in clients:
        latest = get_latest_report(c["client_slug"])
        active = get_active_report(c["id"])
        reports_by_slug[c["client_slug"]] = {
            "latest": latest,
            "active": active,
        }
    return render_template("admin_clients.html",
                           clients=clients,
                           reports_by_slug=reports_by_slug)

@v2.route("/admin/edit/<slug>")
def admin_edit(slug):
    client = get_client_by_slug(slug)
    if not client:
        abort(404)
    return render_template("admin_edit_client.html", client=client)
