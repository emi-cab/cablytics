"""
CABlytics V2 — Google Search Console data collection.

Provides Agent 3 with search query data from GSC:
  - Top queries that drove traffic to the site (last 28 days)
  - For each query: clicks, impressions, CTR, average position
  - Top landing pages per query
  - Device breakdown

This is purely additive enrichment for Agent 3. If GSC fails for any reason
(auth, missing property, API quota, no gsc_site_url configured), the function
returns an empty result and the pipeline continues with VoC data only.

Auth uses the same service account JSON as GA4, with the readonly webmasters
scope. The service account email must be added as a "Restricted" user in
the client's Search Console property.
"""

import os
import json
import traceback

GSC_SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


def _get_gsc_client():
    """
    Build an authenticated Search Console API client using the same
    GOOGLE_CREDENTIALS_JSON env var as GA4.
    """
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON environment variable is not set.")

    info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(
        info, scopes=GSC_SCOPES
    )

    # cache_discovery=False avoids a noisy warning about google-auth-httplib2
    return build("searchconsole", "v1", credentials=credentials, cache_discovery=False)


def collect_gsc_data(gsc_site_url: str, days: int = 28) -> dict:
    """
    Fetch the last `days` of search analytics for the given GSC property.

    Returns a structured dict:
        {
          "configured":      bool,
          "site_url":        str,
          "period_days":     int,
          "total_clicks":    int,
          "total_impressions": int,
          "avg_ctr":         float,
          "avg_position":    float,
          "top_queries":     [...],
          "top_landing_pages": [...],
          "device_breakdown": [...],
          "error":           str | None,
        }

    On any failure, returns a result with configured=False or error=<message>
    so callers can render a fallback message into the prompt without breaking.
    """
    base = {
        "configured":        bool(gsc_site_url),
        "site_url":          gsc_site_url or "",
        "period_days":       days,
        "total_clicks":      0,
        "total_impressions": 0,
        "avg_ctr":           0.0,
        "avg_position":      0.0,
        "top_queries":       [],
        "top_landing_pages": [],
        "device_breakdown":  [],
        "error":             None,
    }

    if not gsc_site_url:
        return base

    try:
        from datetime import datetime, timedelta, timezone
        from googleapiclient.errors import HttpError

        end_date   = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days)

        service = _get_gsc_client()

        # Query 1: aggregate totals
        totals_req = {
            "startDate":  start_date.isoformat(),
            "endDate":    end_date.isoformat(),
            "rowLimit":   1,
            "dataState":  "all",
        }
        totals = service.searchanalytics().query(
            siteUrl=gsc_site_url, body=totals_req
        ).execute()

        if totals.get("rows"):
            r = totals["rows"][0]
            base["total_clicks"]      = int(r.get("clicks", 0))
            base["total_impressions"] = int(r.get("impressions", 0))
            base["avg_ctr"]           = float(r.get("ctr", 0.0))
            base["avg_position"]      = float(r.get("position", 0.0))

        # Query 2: top queries (most useful for Agent 3)
        queries_req = {
            "startDate":  start_date.isoformat(),
            "endDate":    end_date.isoformat(),
            "dimensions": ["query"],
            "rowLimit":   25,
            "dataState":  "all",
        }
        q_resp = service.searchanalytics().query(
            siteUrl=gsc_site_url, body=queries_req
        ).execute()

        for r in q_resp.get("rows", []):
            base["top_queries"].append({
                "query":       (r.get("keys") or [""])[0],
                "clicks":      int(r.get("clicks", 0)),
                "impressions": int(r.get("impressions", 0)),
                "ctr":         float(r.get("ctr", 0.0)),
                "position":    float(r.get("position", 0.0)),
            })

        # Query 3: top landing pages
        pages_req = {
            "startDate":  start_date.isoformat(),
            "endDate":    end_date.isoformat(),
            "dimensions": ["page"],
            "rowLimit":   15,
            "dataState":  "all",
        }
        p_resp = service.searchanalytics().query(
            siteUrl=gsc_site_url, body=pages_req
        ).execute()

        for r in p_resp.get("rows", []):
            base["top_landing_pages"].append({
                "page":        (r.get("keys") or [""])[0],
                "clicks":      int(r.get("clicks", 0)),
                "impressions": int(r.get("impressions", 0)),
                "ctr":         float(r.get("ctr", 0.0)),
                "position":    float(r.get("position", 0.0)),
            })

        # Query 4: device breakdown
        device_req = {
            "startDate":  start_date.isoformat(),
            "endDate":    end_date.isoformat(),
            "dimensions": ["device"],
            "rowLimit":   10,
            "dataState":  "all",
        }
        d_resp = service.searchanalytics().query(
            siteUrl=gsc_site_url, body=device_req
        ).execute()

        for r in d_resp.get("rows", []):
            base["device_breakdown"].append({
                "device":      (r.get("keys") or [""])[0],
                "clicks":      int(r.get("clicks", 0)),
                "impressions": int(r.get("impressions", 0)),
                "ctr":         float(r.get("ctr", 0.0)),
                "position":    float(r.get("position", 0.0)),
            })

        print(f"[V2][GSC] OK | site={gsc_site_url} | "
              f"queries={len(base['top_queries'])} | clicks={base['total_clicks']}",
              flush=True)

    except HttpError as e:
        msg = f"GSC HTTP {e.resp.status}: {e._get_reason() or 'unknown'}"
        if e.resp.status == 403:
            msg += " (the service account is probably not added as a Restricted user in Search Console for this property)"
        elif e.resp.status == 400:
            msg += " (check that gsc_site_url matches exactly what Search Console shows — domain properties need 'sc-domain:' prefix)"
        base["error"] = msg
        print(f"[V2][GSC] {msg}", flush=True)
    except Exception as e:
        base["error"] = f"GSC error: {e}"
        print(f"[V2][GSC] Failed | site={gsc_site_url} | {e}\n{traceback.format_exc()}", flush=True)

    return base


def build_gsc_summary(gsc_data: dict) -> str:
    """
    Format the GSC data dict as a readable text block for injection into
    Agent 3's prompt. Returns a string suitable for a multi-line section.
    """
    if not gsc_data.get("configured"):
        return "Search Console: not configured for this client."

    if gsc_data.get("error"):
        return f"Search Console: data unavailable — {gsc_data['error']}"

    site = gsc_data.get("site_url", "")
    days = gsc_data.get("period_days", 28)
    lines = [
        f"Search Console — {site} (last {days} days)",
        f"  Totals: {gsc_data['total_clicks']:,} clicks, {gsc_data['total_impressions']:,} impressions, "
        f"avg CTR {gsc_data['avg_ctr']*100:.1f}%, avg position {gsc_data['avg_position']:.1f}",
    ]

    if gsc_data["top_queries"]:
        lines.append("\nTop queries (the actual language people search to find this site):")
        for q in gsc_data["top_queries"][:25]:
            lines.append(
                f"  • \"{q['query']}\"  →  {q['clicks']} clicks, {q['impressions']} impressions, "
                f"CTR {q['ctr']*100:.1f}%, pos {q['position']:.1f}"
            )

    if gsc_data["top_landing_pages"]:
        lines.append("\nTop landing pages from search:")
        for p in gsc_data["top_landing_pages"][:10]:
            lines.append(
                f"  • {p['page']}  →  {p['clicks']} clicks, "
                f"CTR {p['ctr']*100:.1f}%, pos {p['position']:.1f}"
            )

    if gsc_data["device_breakdown"]:
        lines.append("\nDevice breakdown:")
        for d in gsc_data["device_breakdown"]:
            lines.append(
                f"  • {d['device']}: {d['clicks']} clicks, "
                f"CTR {d['ctr']*100:.1f}%, pos {d['position']:.1f}"
            )

    return "\n".join(lines)
