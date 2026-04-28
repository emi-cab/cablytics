"""
CABlytics V2 — Flask routes.

Endpoints (Phase 3):
  POST   /v2/client/create
  POST   /v2/client/<slug>/reviews
  GET    /v2/client/<slug>
  GET    /v2/clients

  Page assets:
  GET    /v2/client/<slug>/page-assets
  POST   /v2/client/<slug>/page-assets
  PATCH  /v2/page-assets/<asset_id>
  DELETE /v2/page-assets/<asset_id>
  POST   /v2/page-assets/<asset_id>/screenshot   ← new in Phase 3

  Reports:
  POST   /v2/report/run
  GET    /v2/report/<slug>
  GET    /v2/report/<slug>/status

  Pages (HTML):
  GET    /v2/dashboard/<slug>
  GET    /v2/admin/onboard
  GET    /v2/admin/clients
  GET    /v2/admin/edit/<slug>

  Health:
  GET    /v2/health
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
    list_ad_creatives,
    create_ad_creative,
    update_ad_creative,
    get_ad_creative,
    delete_ad_creative,
    VALID_AD_PLATFORMS,
    VALID_AD_FORMATS,
)
from v2.pipeline import run_pipeline
from v2.scheduler import register_client_job
from v2.storage import (
    upload_screenshot,
    upload_ad_creative,
    public_url_for_path,
    is_configured as storage_is_configured,
    ALLOWED_MIME_TYPES,
    MAX_UPLOAD_BYTES,
    PAGE_SCREENSHOTS_BUCKET,
    AD_CREATIVES_BUCKET,
)

v2 = Blueprint("v2", __name__, url_prefix="/v2",
               template_folder="templates")

init_db()


SENSITIVE_FIELDS = {
    "voc_volunteered",
    "voc_solicited",
    "competitor_notes",
    "clarity_api_token",
}


def _strip_sensitive(client_dict: dict) -> dict:
    return {k: v for k, v in client_dict.items() if k not in SENSITIVE_FIELDS}


def _enrich_asset(asset: dict) -> dict:
    """Add the public screenshot URL alongside the storage path so the UI can render it."""
    if not asset:
        return asset
    out = dict(asset)
    if asset.get("screenshot_path"):
        out["screenshot_url"] = public_url_for_path(asset["screenshot_path"])
    else:
        out["screenshot_url"] = None
    return out


# ── Health ─────────────────────────────────────────────────────────────────────

@v2.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "version": "2",
        "storage_configured": storage_is_configured(),
    })


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


# ── Page assets ────────────────────────────────────────────────────────────────

@v2.route("/client/<slug>/page-assets", methods=["GET"])
def page_assets_list(slug):
    client = get_client_by_slug(slug)
    if not client:
        return jsonify({"error": "Client not found"}), 404
    assets = list_page_assets(client["id"])
    return jsonify({
        "client_slug": slug,
        "page_assets": [_enrich_asset(a) for a in assets],
        "valid_page_types": sorted(VALID_PAGE_TYPES),
    })


@v2.route("/client/<slug>/page-assets", methods=["POST"])
def page_assets_create(slug):
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
    return jsonify({"success": True, "asset": _enrich_asset(asset)}), 201


@v2.route("/page-assets/<int:asset_id>", methods=["PATCH"])
def page_assets_update(asset_id):
    existing = get_page_asset(asset_id)
    if not existing:
        return jsonify({"error": "Page asset not found"}), 404

    data = request.get_json(silent=True) or {}

    if "page_type" in data and data["page_type"].lower() not in VALID_PAGE_TYPES:
        return jsonify({
            "error": f"Invalid page_type. Must be one of: {', '.join(sorted(VALID_PAGE_TYPES))}"
        }), 400

    asset = update_page_asset(asset_id, data)
    return jsonify({"success": True, "asset": _enrich_asset(asset)})


@v2.route("/page-assets/<int:asset_id>", methods=["DELETE"])
def page_assets_delete(asset_id):
    """
    Delete a page asset. The screenshot file in Supabase Storage is intentionally
    orphaned (per project decision) — it stays in the bucket but no longer
    referenced. Storage cleanup can be done manually if needed.
    """
    existing = get_page_asset(asset_id)
    if not existing:
        return jsonify({"error": "Page asset not found"}), 404

    deleted = delete_page_asset(asset_id)
    return jsonify({"success": deleted, "deleted_id": asset_id})


@v2.route("/page-assets/<int:asset_id>/screenshot", methods=["POST"])
def page_assets_upload_screenshot(asset_id):
    """
    Upload a screenshot for a page asset. Multipart form-data:
      • file (required) — the image file

    Returns the updated asset including its new screenshot_url.
    """
    if not storage_is_configured():
        return jsonify({
            "error": "Storage is not configured. SUPABASE_URL and SUPABASE_SERVICE_KEY env vars must be set."
        }), 500

    asset = get_page_asset(asset_id)
    if not asset:
        return jsonify({"error": "Page asset not found"}), 404

    client = get_client_by_id(asset["client_id"])
    if not client:
        return jsonify({"error": "Owning client not found"}), 404

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded under field name 'file'"}), 400

    upload = request.files["file"]
    if not upload or not upload.filename:
        return jsonify({"error": "Empty upload"}), 400

    file_bytes = upload.read()
    content_type = (upload.mimetype or "").lower()
    if content_type == "image/jpg":
        content_type = "image/jpeg"

    if content_type not in ALLOWED_MIME_TYPES:
        return jsonify({
            "error": f"Unsupported file type: {content_type or 'unknown'}. "
                     f"Allowed: {', '.join(sorted(ALLOWED_MIME_TYPES))}"
        }), 400

    if len(file_bytes) > MAX_UPLOAD_BYTES:
        return jsonify({
            "error": f"File too large ({len(file_bytes) // 1024} KB). "
                     f"Maximum is {MAX_UPLOAD_BYTES // 1024 // 1024}MB."
        }), 400

    try:
        storage_path = upload_screenshot(
            client_slug=client["client_slug"],
            asset_id=asset_id,
            file_bytes=file_bytes,
            content_type=content_type,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"error": f"Upload to storage failed: {str(e)}"}), 502

    updated = update_page_asset(asset_id, {"screenshot_path": storage_path})
    return jsonify({"success": True, "asset": _enrich_asset(updated)}), 200


# ── Ad creatives (Phase 6) ─────────────────────────────────────────────────────

def _enrich_ad(ad: dict) -> dict:
    """Add the public ad-creative screenshot URL alongside the storage path."""
    if not ad:
        return ad
    out = dict(ad)
    if ad.get("screenshot_path"):
        out["screenshot_url"] = public_url_for_path(ad["screenshot_path"], bucket=AD_CREATIVES_BUCKET)
    else:
        out["screenshot_url"] = None
    return out


@v2.route("/client/<slug>/ad-creatives", methods=["GET"])
def ad_creatives_list(slug):
    client = get_client_by_slug(slug)
    if not client:
        return jsonify({"error": "Client not found"}), 404

    ads = [_enrich_ad(a) for a in list_ad_creatives(client["id"])]
    page_assets = list_page_assets(client["id"])

    return jsonify({
        "client_slug": slug,
        "ad_creatives": ads,
        "page_assets": [
            {"id": p["id"], "page_type": p["page_type"],
             "page_label": p["page_label"], "url": p["url"]}
            for p in page_assets
        ],
        "valid_platforms": sorted(VALID_AD_PLATFORMS),
        "valid_formats":   sorted(VALID_AD_FORMATS),
    })


@v2.route("/client/<slug>/ad-creatives", methods=["POST"])
def ad_creatives_create(slug):
    client = get_client_by_slug(slug)
    if not client:
        return jsonify({"error": "Client not found"}), 404

    data = request.get_json(silent=True) or {}

    if not (data.get("ad_label") or "").strip():
        return jsonify({"error": "ad_label is required"}), 400
    if not (data.get("platform") or "").strip():
        return jsonify({"error": "platform is required"}), 400

    if data["platform"].lower() not in VALID_AD_PLATFORMS:
        return jsonify({
            "error": f"Invalid platform. Must be one of: {', '.join(sorted(VALID_AD_PLATFORMS))}"
        }), 400

    # If landing_page_asset_id is provided, validate it belongs to this client
    lp_id = data.get("landing_page_asset_id")
    if lp_id:
        try:
            lp_id = int(lp_id)
        except (TypeError, ValueError):
            return jsonify({"error": "landing_page_asset_id must be an integer"}), 400

        page = get_page_asset(lp_id)
        if not page or page["client_id"] != client["id"]:
            return jsonify({"error": "landing_page_asset_id does not belong to this client"}), 400
        data["landing_page_asset_id"] = lp_id

    ad = create_ad_creative(client["id"], data)
    return jsonify({"success": True, "ad": _enrich_ad(ad)}), 201


@v2.route("/ad-creatives/<int:ad_id>", methods=["PATCH"])
def ad_creatives_update(ad_id):
    existing = get_ad_creative(ad_id)
    if not existing:
        return jsonify({"error": "Ad creative not found"}), 404

    data = request.get_json(silent=True) or {}

    if "platform" in data and data["platform"].lower() not in VALID_AD_PLATFORMS:
        return jsonify({
            "error": f"Invalid platform. Must be one of: {', '.join(sorted(VALID_AD_PLATFORMS))}"
        }), 400

    # Validate landing page belongs to same client
    if "landing_page_asset_id" in data:
        lp_id = data.get("landing_page_asset_id")
        if lp_id is None or lp_id == "":
            data["landing_page_asset_id"] = None
        else:
            try:
                lp_id = int(lp_id)
            except (TypeError, ValueError):
                return jsonify({"error": "landing_page_asset_id must be an integer or null"}), 400
            page = get_page_asset(lp_id)
            if not page or page["client_id"] != existing["client_id"]:
                return jsonify({"error": "landing_page_asset_id does not belong to this client"}), 400
            data["landing_page_asset_id"] = lp_id

    ad = update_ad_creative(ad_id, data)
    return jsonify({"success": True, "ad": _enrich_ad(ad)})


@v2.route("/ad-creatives/<int:ad_id>", methods=["DELETE"])
def ad_creatives_delete(ad_id):
    """
    Delete an ad creative. The screenshot file in Supabase Storage is intentionally
    orphaned (per project decision), same pattern as page assets.
    """
    existing = get_ad_creative(ad_id)
    if not existing:
        return jsonify({"error": "Ad creative not found"}), 404

    deleted = delete_ad_creative(ad_id)
    return jsonify({"success": deleted, "deleted_id": ad_id})


@v2.route("/ad-creatives/<int:ad_id>/screenshot", methods=["POST"])
def ad_creatives_upload_screenshot(ad_id):
    """Upload an ad creative screenshot. Multipart form-data with 'file' field."""
    if not storage_is_configured():
        return jsonify({
            "error": "Storage is not configured. SUPABASE_URL and SUPABASE_SERVICE_KEY env vars must be set."
        }), 500

    ad = get_ad_creative(ad_id)
    if not ad:
        return jsonify({"error": "Ad creative not found"}), 404

    client = get_client_by_id(ad["client_id"])
    if not client:
        return jsonify({"error": "Owning client not found"}), 404

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded under field name 'file'"}), 400

    upload = request.files["file"]
    if not upload or not upload.filename:
        return jsonify({"error": "Empty upload"}), 400

    file_bytes = upload.read()
    content_type = (upload.mimetype or "").lower()
    if content_type == "image/jpg":
        content_type = "image/jpeg"

    if content_type not in ALLOWED_MIME_TYPES:
        return jsonify({
            "error": f"Unsupported file type: {content_type or 'unknown'}. "
                     f"Allowed: {', '.join(sorted(ALLOWED_MIME_TYPES))}"
        }), 400

    if len(file_bytes) > MAX_UPLOAD_BYTES:
        return jsonify({
            "error": f"File too large ({len(file_bytes) // 1024} KB). "
                     f"Maximum is {MAX_UPLOAD_BYTES // 1024 // 1024}MB."
        }), 400

    try:
        storage_path = upload_ad_creative(
            client_slug=client["client_slug"],
            ad_id=ad_id,
            file_bytes=file_bytes,
            content_type=content_type,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"error": f"Upload to storage failed: {str(e)}"}), 502

    updated = update_ad_creative(ad_id, {"screenshot_path": storage_path})
    return jsonify({"success": True, "ad": _enrich_ad(updated)}), 200


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
    page_assets   = [_enrich_asset(a) for a in list_page_assets(client["id"])]
    ad_creatives  = [_enrich_ad(a)    for a in list_ad_creatives(client["id"])]
    return render_template("admin_edit_client.html",
                           client=client,
                           page_assets=page_assets,
                           ad_creatives=ad_creatives,
                           valid_page_types=sorted(VALID_PAGE_TYPES),
                           valid_ad_platforms=sorted(VALID_AD_PLATFORMS),
                           valid_ad_formats=sorted(VALID_AD_FORMATS))
