"""
CABlytics V2 — Flask routes.

Endpoints:
  POST /v2/client/create                       Register a new client
  POST /v2/client/<slug>/reviews               Update client config
  GET  /v2/client/<slug>                       Get client config (admin use)
  GET  /v2/clients                             List all clients (admin use)
  POST /v2/report/run                          Trigger a manual report run
  GET  /v2/report/<slug>                       Get latest completed report JSON
  GET  /v2/report/<slug>/status                Get current run status
  GET  /v2/dashboard/<slug>                    Serve the client dashboard HTML
  GET  /v2/admin/onboard                       Serve the admin onboarding form
  GET  /v2/admin/clients                       Serve the admin client list page
  GET  /v2/admin/edit/<slug>                   Serve the admin edit-client page
  GET  /v2/health                              V2-specific health check

  Phase 2 — page assets CRUD:
  GET    /v2/client/<slug>/page-assets         List page assets for a client (JSON)
  POST   /v2/client/<slug>/page-assets         Create a new page asset
  PATCH  /v2/page-assets/<asset_id>            Update an existing page asset
  DELETE /v2/page-assets/<asset_id>            Delete a page asset
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
    list_page_assets,
    create_page_asset,
    update_page_asset,
    get_page_asset,
    delete_page_asset,
    get_client_by_id,
    VALID_PAGE_TYPES,
)
from v2.pipeline import run_pipeline
from v2.scheduler import register_client_job

v2 = Blueprint("v2", __name__, url_prefix="/v2",
               template_folder="templates")

# Initialise DB tables on blueprint load
init_db()


SENSITIVE_FIELDS = {
    "voc_volunteered",
    "voc_solicited",
    "competitor_notes",
    "clarity_api_token",
}


def _strip_sensitive(client_dict: dict) -> dict:
    return {k: v for k, v in client_dict.items() if k not in SENSITIVE_FIELDS}


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

    if get_client_by_slug(data["client_slug"]):
        return jsonify({"error": f"A client with slug '{data['client_slug']}' already exists"}), 409

    client = create_client(data)

    try:
        register_client_job(client)
    except Exception as e:
        print(f"[V2][routes] Scheduler registration failed for {client['client_slug']}: {e}", flush=True)

    if data.get("run_now"):
        thread = threading.Thread(
            target=run_pipeline,
            args=(client["client_slug"], "initial"),
            daemon=True
        )
        thread.start()

    return jsonify({
        "success": True,
        "client": _strip_sensitive(client),
        "dashboard_url": f"/v2/dashboard/{client['client_slug']}",
        "run_triggered": bool(data.get("run_now")),
    }), 201


@v2.route("/client/<slug>/reviews", methods=["POST"])
def client_update_reviews(slug):
    if not get_client_by_slug(slug):
        return jsonify({"error": "Client not found"}), 404

    data = request.get_json(silent=True) or {}

    updatable = [
        "client_name",
        "ga4_property_id",
        "client_context",
        "voc_volunteered",
        "voc_solicited",
        "competitor_notes",
        "session_insights",
        "clarity_api_token",
        "gsc_site_url",
        "monthly_traffic",
        "dev_hours_per_week",
        "report_frequency",
        "schedule_day",
    ]
    updates = {k: data[k] for k in updatable if k in data}

    if not updates:
        return jsonify({"error": "No updatable fields provided"}), 400

    updated = update_client(slug, updates)

    if "report_frequency" in updates or "schedule_day" in updates:
        try:
            register_client_job(updated)
        except Exception as e:
            print(f"[V2][routes] Scheduler re-registration failed for {slug}: {e}", flush=True)

    return jsonify({"success": True, "client": _strip_sensitive(updated)})


@v2.route("/client/<slug>")
def client_get(slug):
    client = get_client_by_slug(slug)
    if not client:
        return jsonify({"error": "Client not found"}), 404
    return jsonify(_strip_sensitive(client))


@v2.route("/clients")
def clients_list():
    clients = list_clients()
    return jsonify([_strip_sensitive(c) for c in clients])


# ── Page assets (Phase 2) ──────────────────────────────────────────────────────

@v2.route("/client/<slug>/page-assets", methods=["GET"])
def page_assets_list(slug):
    """List all page assets for a client."""
    client = get_client_by_slug(slug)
    if not client:
        return jsonify({"error": "Client not found"}), 404
    assets = list_page_assets(client["id"])
    return jsonify({
        "client_slug": slug,
        "page_assets": assets,
        "valid_page_types": sorted(VALID_PAGE_TYPES),
    })


@v2.route("/client/<slug>/page-assets", methods=["POST"])
def page_assets_create(slug):
    """Create a new page asset for a client."""
    client = get_client_by_slug(slug)
    if not client:
        return jsonify({"error": "Client not found"}), 404

    data = request.get_json(silent=True) or {}

    required = ["page_type", "page_label", "url"]
    missing = [f for f in required if not (data.get(f) or "").strip()]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    if data["page_type"].lower() not in VALID_PAGE_TYPES:
        return jsonify({
            "error": f"Invalid page_type. Must be one of: {', '.join(sorted(VALID_PAGE_TYPES))}"
        }), 400

    asset = create_page_asset(client["id"], data)
    return jsonify({"success": True, "asset": asset}), 201


@v2.route("/page-assets/<int:asset_id>", methods=["PATCH"])
def page_assets_update(asset_id):
    """Update an existing page asset."""
    existing = get_page_asset(asset_id)
    if not existing:
        return jsonify({"error": "Page asset not found"}), 404

    data = request.get_json(silent=True) or {}

    if "page_type" in data and data["page_type"].lower() not in VALID_PAGE_TYPES:
        return jsonify({
            "error": f"Invalid page_type. Must be one of: {', '.join(sorted(VALID_PAGE_TYPES))}"
        }), 400

    asset = update_page_asset(asset_id, data)
    return jsonify({"success": True, "asset": asset})


@v2.route("/page-assets/<int:asset_id>", methods=["DELETE"])
def page_assets_delete(asset_id):
    """
    Delete a page asset. Note: in Phase 3, the screenshot file in Supabase
    Storage is intentionally orphaned (not deleted) to keep the code simple.
    Storage cleanup can be done manually in Supabase if needed.
    """
    existing = get_page_asset(asset_id)
    if not existing:
        return jsonify({"error": "Page asset not found"}), 404

    deleted = delete_page_asset(asset_id)
    return jsonify({"success": deleted, "deleted_id": asset_id})


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

    active = get_active_report(client["id"])
    if active:
        return jsonify({
            "error": "A report run is already in progress for this client",
            "report_id": active["id"],
            "status": active["status"],
        }), 409

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
    page_assets = list_page_assets(client["id"])
    return render_template("admin_edit_client.html",
                           client=client,
                           page_assets=page_assets,
                           valid_page_types=sorted(VALID_PAGE_TYPES))
