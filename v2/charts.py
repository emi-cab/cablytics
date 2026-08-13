"""
CABlytics V2 — chart generation.

Renders Plotly figures as self-contained inline HTML snippets that drop into
the dashboard tabs. Each public function takes the relevant slice of an agent's
JSON output and returns an HTML string (or None if the data can't support the
chart, so the template can conditionally omit it).

Stakeholder-first design:
  • Plain-word labels (no field names, no event names, no jargon).
  • Each chart carries a computed one-line headline finding — the takeaway a
    stakeholder gets at a glance without decoding bars.
  • Value labels sit on the bars themselves.
  • Charts inherit the dashboard tokens (teal #066060, Geist / Geist Mono).

Rendering: fig.to_html(include_plotlyjs='cdn', full_html=False). Plotly.js loads
once from CDN per page; the first chart on a page passes include_js=True, the
rest False. No PNG/Kaleido here — HTML only, no browser dependency.
"""

import plotly.graph_objects as go


# ── Design tokens (mirror dashboard.html :root) ─────────────────────────────────

INK       = "#272727"
INK_2     = "#494949"
INK_3     = "#C1C1C1"
ACCENT    = "#066060"   # teal
ACCENT_LT = "#66E2E3"
YELLOW    = "#F0FF2A"
YELLOW_DK = "#454601"
GREEN     = "#9DF204"
GREEN_DK  = "#254A02"
RED       = "#FF5252"
BORDER    = "#E5E5E5"

FONT_SANS = "Geist, system-ui, sans-serif"
FONT_MONO = "Geist Mono, monospace"

_CONFIG = {"displayModeBar": False, "responsive": True}


def _base_layout(**overrides) -> dict:
    layout = dict(
        font=dict(family=FONT_SANS, size=13, color=INK_2),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=8, r=8, t=8, b=8),
        showlegend=False,
        hoverlabel=dict(font=dict(family=FONT_MONO, size=12, color="#ffffff"), bgcolor=INK, bordercolor=INK),
        xaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER,
                   tickfont=dict(family=FONT_MONO, size=11, color=INK_3), linecolor=BORDER),
        yaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER,
                   tickfont=dict(family=FONT_MONO, size=11, color=INK_3), linecolor=BORDER),
    )
    layout.update(overrides)
    return layout


def _to_html(fig: go.Figure, include_js: bool) -> str:
    return fig.to_html(
        include_plotlyjs=("cdn" if include_js else False),
        full_html=False, config=_CONFIG, default_height="320px",
    )


def _fmt(n) -> str:
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "—"


def _headline(text: str) -> str:
    return (
        f'<div style="font-family:{FONT_SANS};font-size:1.05rem;font-weight:500;'
        f'color:{INK};line-height:1.4;margin-bottom:16px;">{text}</div>'
    )


# ── Chart 1: Mobile vs Desktop sessions ─────────────────────────────────────────

def mobile_desktop_chart(leak_map: list, device_summary: dict = None, include_js: bool = True) -> str | None:
    rows = []
    for leak in leak_map or []:
        m, d = leak.get("mobile_sessions"), leak.get("desktop_sessions")
        if m is None and d is None:
            continue
        rows.append((leak.get("page", "—"), m or 0, d or 0))
    if not rows:
        return None

    seen, unique = set(), []
    for r in rows:
        if r not in seen:
            seen.add(r); unique.append(r)
    rows = unique

    pages   = [r[0] for r in rows]
    mobile  = [r[1] for r in rows]
    desktop = [r[2] for r in rows]

    # Use Agent 1's own overall_mobile_share (share of ALL devices, incl. tablet)
    # so this headline matches the rest of the report. Fall back to a
    # mobile/(mobile+desktop) computation only if device_summary is absent.
    ds_share = (device_summary or {}).get("overall_mobile_share")
    if ds_share is not None:
        share = round(100 * ds_share)
    else:
        total = mobile[0] + desktop[0]
        share = round(100 * mobile[0] / total) if total else 0
    headline = _headline(f"{share}% of visitors are on mobile.")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=pages, x=mobile, name="Mobile", orientation="h", marker_color=ACCENT,
        hovertemplate="Mobile: %{x:,} visitors<extra></extra>",
        text=[_fmt(v) for v in mobile], textposition="auto",
        textfont=dict(family=FONT_MONO, size=11, color="#fff"),
    ))
    fig.add_trace(go.Bar(
        y=pages, x=desktop, name="Desktop", orientation="h", marker_color=ACCENT_LT,
        hovertemplate="Desktop: %{x:,} visitors<extra></extra>",
        text=[_fmt(v) for v in desktop], textposition="auto",
        textfont=dict(family=FONT_MONO, size=11, color=INK),
    ))
    fig.update_layout(_base_layout(
        barmode="group", showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
                    font=dict(family=FONT_MONO, size=11, color=INK_3)),
        height=max(160, 80 * len(rows) + 60),
    ))
    fig.update_xaxes(title=None)
    fig.update_yaxes(title=None, automargin=True)
    return headline + _to_html(fig, include_js)


# ── Chart 2: Acquisition channels ───────────────────────────────────────────────

def channels_chart(acquisition_insights: list, include_js: bool = False) -> str | None:
    rows = [(c.get("channel", "—"), c.get("sessions") or 0, c.get("bounce_rate"))
            for c in (acquisition_insights or []) if c.get("sessions")]
    if not rows:
        return None
    rows.sort(key=lambda r: r[1], reverse=True)

    channels = [r[0] for r in rows]
    sessions = [r[1] for r in rows]
    bounces  = [r[2] for r in rows]

    total = sum(sessions)
    top_share = round(100 * sessions[0] / total) if total else 0
    headline = _headline(
        f"{channels[0]} is the biggest source of visitors "
        f"({_fmt(sessions[0])}, {top_share}% of the total)."
    )

    def bounce_colour(b):
        # Firmly teal up to ~15% bounce; only genuinely high bounce (16%+) shifts red.
        if b is None:
            return ACCENT
        if b <= 0.15:
            return ACCENT                      # #066060 teal — visitors stay
        t = max(0.0, min(1.0, (b - 0.15) / 0.08))  # 0.15→teal, 0.23→red
        r = int(6 + t * (255 - 6)); g = int(96 + t * (82 - 96)); bl = int(96 + t * (82 - 96))
        return f"rgba({r},{g},{bl},1)"

    colours = [bounce_colour(b) for b in bounces]
    channels, sessions, colours, bounces = (
        channels[::-1], sessions[::-1], colours[::-1], bounces[::-1])

    fig = go.Figure(go.Bar(
        y=channels, x=sessions, orientation="h", marker_color=colours,
        text=[_fmt(v) for v in sessions], textposition="auto",
        textfont=dict(family=FONT_MONO, size=11, color="#fff"),
        customdata=[f"{b*100:.0f}%" if b is not None else "—" for b in bounces],
        hovertemplate="%{y}: %{x:,} visitors · bounce %{customdata}<extra></extra>",
    ))
    fig.update_layout(_base_layout(height=max(160, 52 * len(rows) + 60)))
    fig.update_xaxes(title=None)
    fig.update_yaxes(title=None, automargin=True)

    legend = (
        f'<div style="font-family:{FONT_MONO};font-size:0.7rem;color:{INK_3};'
        f'margin-top:8px;">Bar colour shows bounce rate &mdash; teal = low (visitors stay), '
        f'red = high (visitors leave)</div>'
    )
    return headline + _to_html(fig, include_js) + legend


# ── Chart 3: Priority-ranked test ideas ─────────────────────────────────────────

_EFFORT_WORD   = {1: "Low effort", 2: "Medium effort", 3: "High effort"}
_EFFORT_COLOUR = {1: GREEN_DK, 2: ACCENT, 3: INK_3}

def priority_chart(ranked_tests: list, include_js: bool = False) -> str | None:
    rows = [t for t in (ranked_tests or []) if t.get("priority_score") is not None]
    if not rows:
        return None
    rows.sort(key=lambda t: t.get("priority_score") or 0)

    def short(t):
        """
        Derive a clean short test NAME from the hypothesis. Agent 2 has no
        dedicated short-name field (a future prompt addition), so we extract
        the proposed CHANGE: take the clause after the page reference, cut at
        the first natural boundary word, and cap length. Full hypothesis is in
        the table below the chart.
        """
        rank = t.get("rank", "")
        h = (t.get("hypothesis") or "").strip()
        ttype = (t.get("test_type") or "").strip()

        clause = h.split(" then ")[0].replace("If ", "").strip()

        # strip page-reference boilerplate so the proposed change leads
        for boiler in [
            "the ablePro [other] page ", "the ablePro page ", "ablePro page ",
            "the page headline and hero copy are ", "the page ", "page ",
        ]:
            if clause.lower().startswith(boiler.lower()):
                clause = clause[len(boiler):].strip()
                break

        # normalise leading verbs to a noun-ish phrase
        for verb in ["adds ", "add ", "displays ", "display ", "is ", "are ",
                     "replaces or supplements ", "audited for "]:
            if clause.lower().startswith(verb):
                clause = clause[len(verb):].strip()
                break

        # cut at the first natural boundary so we do not end mid-phrase
        for boundary in [" that ", " which ", " visible ", " in a ", " reachable ",
                         " for cold ", " with a ", " (", " covering ", " \u2014 "]:
            idx = clause.find(boundary)
            if idx > 0:
                clause = clause[:idx].strip()
                break

        name = clause.rstrip(" ,.;:")
        if len(name) > 40:
            name = name[:38].rstrip() + "\u2026"
        if not name:
            name = ttype.title() or "Test"
        name = name[0].upper() + name[1:]
        return f"{rank}. {name}" if rank else name

    labels  = [short(t) for t in rows]
    scores  = [t.get("priority_score") or 0 for t in rows]
    efforts = [t.get("effort_score") or 2 for t in rows]
    colours = [_EFFORT_COLOUR.get(e, ACCENT) for e in efforts]
    effort_words = [_EFFORT_WORD.get(e, "—") for e in efforts]

    top3 = sorted(rows, key=lambda t: t.get("priority_score") or 0, reverse=True)[:3]
    low_top3 = sum(1 for t in top3 if (t.get("effort_score") or 2) == 1)
    headline = _headline(
        f"{low_top3} of the top 3 priorities are low-effort &mdash; quick wins to do first."
        if low_top3 else "Top priorities ranked by expected impact vs effort."
    )

    fig = go.Figure(go.Bar(
        y=labels, x=scores, orientation="h", marker_color=colours,
        text=[f"{s:g}" for s in scores], textposition="auto",
        textfont=dict(family=FONT_MONO, size=11, color="#fff"),
        customdata=effort_words,
        hovertemplate="%{y}<br>Priority %{x:g} · %{customdata}<extra></extra>",
    ))
    fig.update_layout(_base_layout(height=max(200, 46 * len(rows) + 60)))
    fig.update_xaxes(title=None)
    fig.update_yaxes(title=None, automargin=True)

    legend = (
        f'<div style="font-family:{FONT_MONO};font-size:0.7rem;color:{INK_3};'
        f'margin-top:8px;display:flex;gap:16px;flex-wrap:wrap;">'
        f'<span><span style="color:{GREEN_DK};">&#9632;</span> Low effort</span>'
        f'<span><span style="color:{ACCENT};">&#9632;</span> Medium effort</span>'
        f'<span><span style="color:{INK_3};">&#9632;</span> High effort</span>'
        f'<span>Longer bar = higher priority</span></div>'
    )
    return headline + _to_html(fig, include_js) + legend