"""
GA4 data layer for CABlytics V2.

All core query functions (run_ga4_report, build_url_filter, get_page_performance,
get_device_breakdown, get_event_data, get_landing_pages, get_geographic_data,
get_time_of_day, get_user_acquisition, get_site_totals, aggregate_pages_to_totals,
sanitise_url_list, get_url_paths) are imported directly from ga4_api.py — no duplication.

This file adds:
  - build_ga4_client()  : creates a GA4 client from the shared service account env var
  - collect_funnel_data(): tailored data collection for Agent 1 (funnel analysis)
  - build_funnel_summary(): formats the collected data as a structured string for the prompt
"""

import os
import json
import re
from datetime import datetime, timedelta

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2 import service_account

# Import all existing query functions from V1 — no rewriting
from ga4_api import (
    run_ga4_report,
    build_url_filter,
    sanitise_url_list,
    get_url_paths,
    get_page_performance,
    get_device_breakdown,
    get_event_data,
    get_landing_pages,
    get_geographic_data,
    get_time_of_day,
    get_user_acquisition,
    get_site_totals,
    aggregate_pages_to_totals,
    collect_all_data,
)


# ── GA4 client factory ─────────────────────────────────────────────────────────

def build_ga4_client() -> BetaAnalyticsDataClient:
    """
    Build a GA4 client from the shared service account stored in the
    GOOGLE_CREDENTIALS_JSON environment variable (same as V1).
    Raises RuntimeError if the env var is missing or invalid.
    """
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not raw:
        raise RuntimeError(
            "GOOGLE_CREDENTIALS_JSON environment variable is not set. "
            "Add it in Render → Environment."
        )
    try:
        credentials_dict = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON is not valid JSON: {e}")

    credentials = service_account.Credentials.from_service_account_info(
        credentials_dict,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)


# ── Funnel data collection for Agent 1 ────────────────────────────────────────

def collect_funnel_data(property_id: str, urls: list[str]) -> dict:
    """
    Collects all GA4 data needed for Agent 1 (Funnel Analyst).
    Reuses collect_all_data() from V1 — same multi-period structure.

    Returns:
        {
            "all_data": { ... },   # full V1 data structure
            "periods": { ... },    # period date ranges
            "urls": [ ... ],       # sanitised URL list
            "collected_at": "...", # ISO timestamp
        }
    """
    client = build_ga4_client()
    clean_urls = sanitise_url_list(urls) if urls else []

    print(f"[V2][Agent1] collect_funnel_data | property={property_id} | urls={clean_urls}", flush=True)

    all_data, periods = collect_all_data(client, property_id, clean_urls)

    return {
        "all_data": all_data,
        "periods": periods,
        "urls": clean_urls,
        "collected_at": datetime.utcnow().isoformat(),
    }


# ── Data summary formatter for Agent 1 prompt ─────────────────────────────────

def build_funnel_summary(ga4_result: dict) -> str:
    """
    Converts the GA4 data dict into a structured plain-text summary
    suitable for injection into the Agent 1 Claude prompt.

    Focuses on:
    - Per-page metrics segmented by device (mobile vs desktop conversion gap)
    - Period-over-period changes (28d vs prior, YoY)
    - Drop-off points across the funnel steps
    - Acquisition channel performance
    - Time-of-day and geographic context
    """
    all_data = ga4_result["all_data"]
    periods  = ga4_result["periods"]
    urls     = ga4_result["urls"]
    lines    = []

    # ── Overview ──────────────────────────────────────────────────────────────
    lines.append("=" * 60)
    lines.append("FUNNEL DATA — CABlytics V2 Agent 1 Input")
    lines.append(f"Collected: {ga4_result['collected_at']}")
    lines.append(f"URLs in scope: {', '.join(urls) if urls else 'all pages'}")
    lines.append("=" * 60)

    # ── Last 28 days: per-page with device breakdown ───────────────────────────
    lines.append("\n## PER-PAGE PERFORMANCE — LAST 28 DAYS (device-segmented)\n")

    current_pages = all_data.get("page_performance", {}).get("current", [])
    previous_pages = all_data.get("page_performance", {}).get("previous", [])
    prev_lookup = {p["pagePath"]: p for p in previous_pages}

    pages_sorted = sorted(current_pages, key=lambda x: x["sessions"], reverse=True)

    for page in pages_sorted:
        path = page["pagePath"]
        p = page

        lines.append(f"Page: {path}")
        lines.append(f"  Sessions: {p['sessions']:,} | Users: {p['totalUsers']:,} (new: {p['newUsers']:,}, returning: {p.get('returningUsers', 0):,})")
        lines.append(f"  Bounce rate: {p['bounceRate']:.1%} | Engagement rate: {p['engagementRate']:.1%}")
        lines.append(f"  Avg session duration: {p['averageSessionDuration']:.1f}s | Avg engagement duration: {p.get('avgEngagementDuration', 0):.1f}s")
        lines.append(f"  Pages per session: {p.get('pagesPerSession', 0):.1f} | Events fired: {p['eventCount']:,}")

        # Device breakdown — key for Agent 1's mobile/desktop gap analysis
        devices = p.get("devices", {})
        if devices:
            total_dev_sessions = sum(devices.values())
            dev_parts = []
            for dev, sess in sorted(devices.items(), key=lambda x: -x[1]):
                pct = (sess / total_dev_sessions * 100) if total_dev_sessions > 0 else 0
                dev_parts.append(f"{dev}: {sess:,} ({pct:.1f}%)")
            lines.append(f"  Device split: {' | '.join(dev_parts)}")

        # Top acquisition channels
        channels = p.get("channels", {})
        if channels:
            ch_str = ", ".join(
                f"{k}: {v}" for k, v in
                sorted(channels.items(), key=lambda x: -x[1])[:4]
            )
            lines.append(f"  Top channels: {ch_str}")

        # Period-over-period comparison
        prev = prev_lookup.get(path)
        if prev:
            s_diff = p["sessions"] - prev["sessions"]
            s_pct  = (s_diff / prev["sessions"] * 100) if prev["sessions"] > 0 else 0
            b_diff = p["bounceRate"] - prev["bounceRate"]
            e_diff = p["engagementRate"] - prev["engagementRate"]
            lines.append(
                f"  vs prior 28 days: sessions {s_diff:+,} ({s_pct:+.1f}%) | "
                f"bounce {b_diff:+.1%} | engagement {e_diff:+.1%}"
            )
        else:
            lines.append("  vs prior 28 days: no prior data for this page")

        lines.append("")

    # ── Year-over-year totals ──────────────────────────────────────────────────
    lines.append("\n## YEAR-ON-YEAR TOTALS — LAST 28 DAYS vs SAME PERIOD LAST YEAR\n")

    yoy = all_data.get("year_over_year", {})
    yoy_range = yoy.get("date_range", {})
    lines.append(f"Current:  {yoy_range.get('current', '')}")
    lines.append(f"Previous: {yoy_range.get('previous', '')}")

    for metric, vals in yoy.get("totals", {}).items():
        curr = vals["current"]
        prev = vals["previous"]
        pct  = vals["change_pct"]
        pct_str = f"{pct:+.1f}%" if pct is not None else "N/A"
        if metric in ("bounceRate", "engagementRate"):
            lines.append(f"  {metric}: {curr:.1%} (was {prev:.1%}, Δ {pct_str})")
        elif metric == "averageSessionDuration":
            lines.append(f"  {metric}: {curr:.1f}s (was {prev:.1f}s, Δ {pct_str})")
        else:
            lines.append(f"  {metric}: {curr:,} (was {prev:,}, Δ {pct_str})")

    # ── YoY per-page ──────────────────────────────────────────────────────────
    yoy_pages = all_data.get("page_performance_yoy", [])
    if yoy_pages:
        lines.append("\n  YoY per-page (last year values):")
        for page in sorted(yoy_pages, key=lambda x: x["sessions"], reverse=True):
            path = page["pagePath"]
            lines.append(
                f"    {path}: {page['sessions']:,} sessions | "
                f"bounce {page['bounceRate']:.1%} | engagement {page['engagementRate']:.1%}"
            )

    # ── Acquisition channels ───────────────────────────────────────────────────
    lines.append("\n## ACQUISITION CHANNELS — LAST 28 DAYS\n")

    acq = all_data.get("acquisition", [])
    ch_agg: dict = {}
    for a in acq:
        ch = a["sessionDefaultChannelGroup"]
        if ch not in ch_agg:
            ch_agg[ch] = {"sessions": 0, "newUsers": 0, "_b": 0, "_e": 0}
        s = a["sessions"]
        ch_agg[ch]["sessions"] += s
        ch_agg[ch]["newUsers"] += a["newUsers"]
        ch_agg[ch]["_b"] += a["bounceRate"] * s
        ch_agg[ch]["_e"] += a["engagementRate"] * s

    for ch, info in sorted(ch_agg.items(), key=lambda x: -x[1]["sessions"]):
        s  = info["sessions"]
        br = info["_b"] / s if s > 0 else 0
        er = info["_e"] / s if s > 0 else 0
        lines.append(
            f"  {ch}: {s:,} sessions | {info['newUsers']:,} new users | "
            f"bounce {br:.1%} | engagement {er:.1%}"
        )

    # ── Landing pages ──────────────────────────────────────────────────────────
    lines.append("\n## LANDING PAGES — LAST 28 DAYS (top 10)\n")

    landings = all_data.get("landing_pages", [])
    lp_agg: dict = {}
    for lp in landings:
        path = lp["landingPage"]
        if path not in lp_agg:
            lp_agg[path] = {"sessions": 0, "channels": {}}
        lp_agg[path]["sessions"] += lp["sessions"]
        ch = lp["sessionDefaultChannelGroup"]
        lp_agg[path]["channels"][ch] = lp_agg[path]["channels"].get(ch, 0) + lp["sessions"]

    for path, info in sorted(lp_agg.items(), key=lambda x: -x[1]["sessions"])[:10]:
        ch_str = ", ".join(
            f"{k}: {v}" for k, v in
            sorted(info["channels"].items(), key=lambda x: -x[1])[:3]
        )
        lines.append(f"  {path}: {info['sessions']:,} sessions (channels: {ch_str})")

    # ── Events ────────────────────────────────────────────────────────────────
    lines.append("\n## KEY EVENTS — LAST 28 DAYS (top 15, GA4 defaults excluded)\n")

    events = all_data.get("events", [])
    for ev in sorted(events, key=lambda x: x.get("eventCount", 0), reverse=True)[:15]:
        lines.append(f"  {ev['eventName']}: {ev['eventCount']:,} events by {ev['totalUsers']:,} users")

    # ── Geography ─────────────────────────────────────────────────────────────
    lines.append("\n## GEOGRAPHY — LAST 28 DAYS (top countries + cities)\n")

    geo = all_data.get("geographic", [])
    country_agg: dict = {}
    for g in geo:
        c = g["country"]
        country_agg[c] = country_agg.get(c, 0) + g["sessions"]
    for country, sess in sorted(country_agg.items(), key=lambda x: -x[1])[:8]:
        lines.append(f"  {country}: {sess:,} sessions")
    lines.append("  Top cities:")
    for g in sorted(geo, key=lambda x: x["sessions"], reverse=True)[:8]:
        lines.append(
            f"    {g['city']}, {g['country']}: {g['sessions']:,} sessions "
            f"(engagement: {g['engagementRate']:.1%})"
        )

    # ── Time of day ───────────────────────────────────────────────────────────
    lines.append("\n## TRAFFIC PATTERNS — LAST 28 DAYS\n")

    tod = all_data.get("time_of_day", [])
    hour_agg: dict = {}
    for t in tod:
        h = int(t["hour"])
        hour_agg[h] = hour_agg.get(h, 0) + t["sessions"]

    if hour_agg:
        peak_h  = max(hour_agg, key=hour_agg.get)
        quiet_h = min(hour_agg, key=hour_agg.get)
        lines.append(f"  Peak hour:    {peak_h:02d}:00 — {hour_agg[peak_h]:,} sessions")
        lines.append(f"  Quietest hour: {quiet_h:02d}:00 — {hour_agg[quiet_h]:,} sessions")

    day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    day_agg: dict = {}
    for t in tod:
        d = int(t["dayOfWeek"])
        day_agg[d] = day_agg.get(d, 0) + t["sessions"]
    if day_agg:
        peak_d = max(day_agg, key=day_agg.get)
        lines.append(f"  Peak day: {day_names[peak_d]} — {day_agg[peak_d]:,} sessions")

    return "\n".join(lines)
