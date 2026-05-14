"""
CABlytics V2 — Manual CSV data summaries.

Parallel to the API path's build_funnel_summary (ga4_queries_v2.py) and
build_gsc_summary (gsc_queries.py), but reads from a manual_data_uploads
row instead of live API responses.

Design contract:
  • Output is text — Agents 1 and 3 consume formatted strings, not raw dicts.
  • Output is *honest about being sparse*: CSV exports lack device splits,
    period-over-period comparisons, time-of-day, and acquisition channels
    that the API path provides. We flag what's missing so the agents don't
    hallucinate insights the data can't support.
  • Same headers / section structure as the API summaries so prompt context
    feels consistent.
"""

from typing import Optional


# ── Funnel summary (Agent 1) ────────────────────────────────────────────────

def build_funnel_summary_from_csv(upload: dict) -> str:
    """
    Build the funnel-data text block for Agent 1 from a manual_data_uploads row.

    `upload` is the hydrated row from get_manual_upload() — it has:
        ga4_data: { row_count, total_sessions, total_users, rows: [...], ... }
        gsc_pages_data: { ... }  (used as a supplementary acquisition signal)
        date_range_start, date_range_end: ISO date strings (user-confirmed)
    """
    ga4 = upload.get("ga4_data") or {}
    gsc_pages = upload.get("gsc_pages_data") or {}
    date_start = upload.get("date_range_start") or "unknown"
    date_end = upload.get("date_range_end") or "unknown"

    lines = []
    lines.append("=" * 60)
    lines.append("FUNNEL DATA — CABlytics V2 Agent 1 Input (MANUAL CSV)")
    lines.append(f"Date range: {date_start} → {date_end}")
    lines.append(f"Data source: manual CSV upload (upload_id={upload.get('id')})")
    lines.append("=" * 60)

    # ── Important context for the agent ──
    lines.append("\n## DATA SOURCE NOTE\n")
    lines.append(
        "This report is built from a CSV export, not the live GA4 API. "
        "CSV exports are necessarily sparser than the API:"
    )
    lines.append("  • No device segmentation (mobile vs desktop) per page")
    lines.append("  • No period-over-period or year-over-year comparison")
    lines.append("  • No acquisition channel breakdown per page")
    lines.append("  • No time-of-day or geographic breakdown")
    lines.append(
        "Base conclusions only on what the data below actually shows. "
        "Do not speculate about device gaps, channel mix, or trends — these "
        "are not in scope for this report."
    )

    # ── GA4 page-level data ──
    if ga4 and ga4.get("rows"):
        rows = ga4["rows"]
        lines.append(f"\n## PER-PAGE PERFORMANCE — {date_start} to {date_end}\n")
        lines.append(f"Pages in scope: {len(rows)}")
        lines.append(f"Totals: {ga4.get('total_sessions', 0):,} sessions, "
                     f"{ga4.get('total_users', 0):,} users\n")

        # Detect what columns the CSV actually provided
        cols_present = set(ga4.get("columns_present", []))
        has_engaged = "engaged_sessions" in cols_present
        has_engagement_time = "avg_engagement_time" in cols_present
        has_events = "event_count" in cols_present
        has_conversions = "conversions" in cols_present

        # Sort by sessions descending
        sorted_rows = sorted(rows, key=lambda r: r.get("sessions", 0), reverse=True)
        for r in sorted_rows:
            path = r.get("page_path", "")
            sessions = r.get("sessions", 0)
            users = r.get("total_users", 0)

            lines.append(f"Page: {path}")
            lines.append(f"  Sessions: {sessions:,} | Users: {users:,}")

            extras = []
            if has_engaged and r.get("engaged_sessions"):
                eng = r["engaged_sessions"]
                eng_rate = (eng / sessions) if sessions else 0
                extras.append(f"engaged sessions: {eng:,} ({eng_rate:.1%})")
            if has_engagement_time and r.get("avg_engagement_time"):
                extras.append(f"avg engagement time: {r['avg_engagement_time']:.1f}s")
            if has_events and r.get("event_count"):
                extras.append(f"events: {r['event_count']:,}")
            if has_conversions and r.get("conversions"):
                conv = r["conversions"]
                cvr = (conv / sessions) if sessions else 0
                extras.append(f"conversions: {conv} ({cvr:.2%} CVR)")
            if extras:
                lines.append("  " + " | ".join(extras))

            lines.append("")

        if ga4.get("web_pixels_rows_excluded"):
            lines.append(
                f"Note: {ga4['web_pixels_rows_excluded']} Shopify web-pixels "
                "sandbox rows were filtered out before analysis."
            )
    else:
        lines.append("\n## PER-PAGE PERFORMANCE\n")
        lines.append("No GA4 data uploaded for this report.")

    # ── GSC as supplementary acquisition signal ──
    if gsc_pages and gsc_pages.get("rows"):
        lines.append("\n## SEARCH ACQUISITION SIGNAL (from GSC Pages export)\n")
        lines.append(
            "Search Console page-level performance for the same period. "
            "Use this as a proxy for organic search acquisition, since the "
            "GA4 CSV doesn't include per-page channel data."
        )
        lines.append(
            f"Totals: {gsc_pages.get('total_clicks', 0):,} clicks, "
            f"{gsc_pages.get('total_impressions', 0):,} impressions\n"
        )
        top_pages = sorted(gsc_pages["rows"], key=lambda r: r.get("clicks", 0),
                           reverse=True)[:10]
        for p in top_pages:
            lines.append(
                f"  {p.get('key', '')}: {p.get('clicks', 0):,} clicks, "
                f"{p.get('impressions', 0):,} impressions, "
                f"CTR {p.get('ctr', 0)*100:.1f}%, pos {p.get('position', 0):.1f}"
            )

    return "\n".join(lines)


# ── GSC summary (Agent 3) ───────────────────────────────────────────────────

def build_gsc_summary_from_csv(upload: dict) -> str:
    """
    Build the GSC text block for Agent 3 from a manual_data_uploads row.

    Prefers Queries data (matches the API path's focus on search language).
    Falls back to Pages data if only Pages was uploaded.
    Returns a "not provided" string if neither is present, so the agent
    runs cleanly on VoC only — matching the existing GSC failure behaviour.
    """
    gsc_queries = upload.get("gsc_queries_data") or {}
    gsc_pages = upload.get("gsc_pages_data") or {}
    date_start = upload.get("date_range_start") or "unknown"
    date_end = upload.get("date_range_end") or "unknown"

    if not gsc_queries.get("rows") and not gsc_pages.get("rows"):
        return "Search Console: not provided in the CSV upload for this report."

    lines = []
    lines.append(f"Search Console — manual CSV upload ({date_start} → {date_end})")

    # Queries are the most useful for Agent 3 (CEPs / customer language)
    if gsc_queries.get("rows"):
        rows = gsc_queries["rows"]
        lines.append(
            f"  Totals: {gsc_queries.get('total_clicks', 0):,} clicks, "
            f"{gsc_queries.get('total_impressions', 0):,} impressions"
        )
        lines.append(
            "\nTop queries (the actual language people search to find this site):"
        )
        top = sorted(rows, key=lambda r: r.get("clicks", 0), reverse=True)[:25]
        for q in top:
            lines.append(
                f'  • "{q.get("key", "")}"  →  {q.get("clicks", 0)} clicks, '
                f'{q.get("impressions", 0)} impressions, '
                f'CTR {q.get("ctr", 0)*100:.1f}%, pos {q.get("position", 0):.1f}'
            )

    if gsc_pages.get("rows"):
        rows = gsc_pages["rows"]
        if not gsc_queries.get("rows"):
            # Only Pages was uploaded — show totals here instead
            lines.append(
                f"  Totals: {gsc_pages.get('total_clicks', 0):,} clicks, "
                f"{gsc_pages.get('total_impressions', 0):,} impressions"
            )
        lines.append("\nTop landing pages from search:")
        top = sorted(rows, key=lambda r: r.get("clicks", 0), reverse=True)[:10]
        for p in top:
            lines.append(
                f"  • {p.get('key', '')}  →  {p.get('clicks', 0)} clicks, "
                f"CTR {p.get('ctr', 0)*100:.1f}%, pos {p.get('position', 0):.1f}"
            )

    if not gsc_queries.get("rows"):
        lines.append(
            "\nNote: Only GSC Pages data was uploaded. For richer customer-language "
            "analysis, upload the Queries CSV next time."
        )

    return "\n".join(lines)