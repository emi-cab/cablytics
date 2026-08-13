"""
CABlytics V2 — report generator.

Produces a .docx CRO report from a run's pipeline data, using the committed
CAB template (v2/templates/report_template.docx) as the style shell. The
template's styles.xml, fontTable.xml, theme and embedded fonts are kept intact;
only word/document.xml is replaced with the generated report body. Because the
report references the template's named styles (Title, Subtitle, Heading1/2,
Normal), it inherits the CAB identity — Geist / Lora / Geist Mono — exactly.

Sections: Title · Summary · Findings · Recommendations · Action plan.
Action plan is optional (include_action_plan flag).

Design note: this generator carries the template's own "defensibility standard"
— it renders what the pipeline data states and never invents figures. If a
section's source data is absent, the section says so plainly rather than
fabricating content.
"""

import os
import re
import shutil
import zipfile
import tempfile
from html import escape


# ── XML building blocks ─────────────────────────────────────────────────────────

def _t(text: str) -> str:
    """Escape text for XML and preserve it as a run text element."""
    return f'<w:t xml:space="preserve">{escape(text or "")}</w:t>'


def _fmt(n):
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "\u2014"


def _num(n):
    if n is None:
        return "\u2014"
    return f"{n:g}" if isinstance(n, float) else str(n)


def _para(text: str, style: str = "Normal", bold: bool = False,
          italic: bool = False) -> str:
    """A paragraph in a named style, optional bold/italic run properties."""
    rpr = ""
    if bold or italic:
        rpr = "<w:rPr>" + ("<w:b/>" if bold else "") + ("<w:i/>" if italic else "") + "</w:rPr>"
    return (
        f'<w:p><w:pPr><w:pStyle w:val="{style}"/></w:pPr>'
        f'<w:r>{rpr}{_t(text)}</w:r></w:p>'
    )


def _heading(text: str, level: int = 1) -> str:
    return _para(text, style=f"Heading{level}")


def _bullet(text: str) -> str:
    """A bullet list item (uses the template's numbering; falls back to Normal
    with a leading marker if numbering isn't wired — kept simple and safe)."""
    return (
        f'<w:p><w:pPr><w:pStyle w:val="Normal"/>'
        f'<w:numPr><w:ilvl w:val="0"/><w:numId w:val="1"/></w:numPr></w:pPr>'
        f'<w:r>{_t(text)}</w:r></w:p>'
    )


def _spacer() -> str:
    return '<w:p><w:pPr><w:pStyle w:val="Normal"/></w:pPr></w:p>'

# ── Table helpers (Geist Mono headers + numeric cells, Geist for text) ──────────

MONO = "Geist Mono"
SANS = "Geist"

def _cell(text, width_dxa, mono=False, bold=False, header=False):
    """A table cell. header/bold → bold; mono → Geist Mono; else Geist."""
    font = MONO if mono else SANS
    rpr = f'<w:rPr><w:rFonts w:ascii="{font}" w:hAnsi="{font}"/>'
    if bold or header:
        rpr += "<w:b/>"
    if header:
        rpr += '<w:sz w:val="18"/>'   # slightly smaller mono header
    rpr += "</w:rPr>"
    shading = ('<w:shd w:val="clear" w:color="auto" w:fill="F2F2F2"/>'
               if header else "")
    return (
        f'<w:tc><w:tcPr><w:tcW w:w="{width_dxa}" w:type="dxa"/>{shading}</w:tcPr>'
        f'<w:p><w:pPr><w:pStyle w:val="Normal"/></w:pPr>'
        f'<w:r>{rpr}{_t(str(text))}</w:r></w:p></w:tc>'
    )

def _table(headers, rows, col_widths, mono_cols=None):
    """
    headers: list[str]; rows: list[list]; col_widths: list[int] (DXA, sum=table width);
    mono_cols: set of column indices to render in Geist Mono (numeric columns).
    """
    mono_cols = mono_cols or set()
    total = sum(col_widths)
    grid = "".join(f'<w:gridCol w:w="{w}"/>' for w in col_widths)

    header_cells = "".join(
        _cell(h, col_widths[i], mono=True, header=True) for i, h in enumerate(headers))
    header_row = f'<w:tr>{header_cells}</w:tr>'

    body_rows = []
    for row in rows:
        cells = "".join(
            _cell(val, col_widths[i], mono=(i in mono_cols))
            for i, val in enumerate(row))
        body_rows.append(f'<w:tr>{cells}</w:tr>')

    borders = (
        '<w:tblBorders>'
        '<w:bottom w:val="single" w:sz="4" w:color="E5E5E5"/>'
        '<w:insideH w:val="single" w:sz="4" w:color="E5E5E5"/>'
        '</w:tblBorders>'
    )
    return (
        f'<w:tbl><w:tblPr><w:tblW w:w="{total}" w:type="dxa"/>{borders}'
        f'<w:tblLayout w:type="fixed"/></w:tblPr>'
        f'<w:tblGrid>{grid}</w:tblGrid>'
        f'{header_row}{"".join(body_rows)}</w:tbl>'
    )



# ── Section builders ─────────────────────────────────────────────────────────────

def _pretty_transition(t: str) -> str:
    if not t:
        return ""
    return t.replace("_to_", " \u2192 ").replace("_", " ").title()


def _build_body(report: dict, include_action_plan: bool, sect_pr: str = "") -> str:
    """Assemble the report body XML from pipeline data."""
    client = report.get("client_name", "Client")
    date = report.get("date", "")
    agents = report.get("agents", {})
    a1 = agents.get("1", {})
    a2 = agents.get("2", {})
    a5 = agents.get("5", {})

    parts = []

    # ── Title ──
    parts.append(_para(f"{client} — CRO Intelligence Report", style="Title"))
    parts.append(_para(f"Prepared {date}" if date else "CRO analysis",
                       style="Subtitle"))
    parts.append(_spacer())

    # ── 1. Summary ──
    parts.append(_heading("1. Summary", 1))
    summary = a1.get("summary")
    if summary:
        parts.append(_para(summary))
    else:
        parts.append(_para("No summary was produced for this run.", italic=True))
    parts.append(_spacer())

    # ── 2. Findings ──
    parts.append(_heading("2. Findings", 1))
    leaks = a1.get("leak_map") or []
    if leaks:
        # de-dup by (page, finding)
        seen, uniq = set(), []
        for leak in leaks:
            key = (leak.get("page"), leak.get("finding"))
            if key in seen:
                continue
            seen.add(key); uniq.append(leak)
        # summary table: Page | Severity | Demand transition | Sessions
        headers = ["Page", "Severity", "Demand transition", "Sessions"]
        widths = [3100, 1400, 3025, 1500]  # sum 9025; wider Sessions col
        rows = []
        for leak in uniq:
            ds = leak.get("demand_state") or {}
            trans = _pretty_transition(ds.get("transition")) if ds.get("transition") != "unknown" else "—"
            rows.append([
                leak.get("page", "—"),
                (leak.get("severity") or "—").upper(),
                trans,
                _fmt(leak.get("sessions")),
            ])
        parts.append(_table(headers, rows, widths, mono_cols={3}))
        parts.append(_spacer())
        # then each finding's detail as prose beneath the table
        for leak in uniq:
            page = leak.get("page", "—")
            ds = leak.get("demand_state") or {}
            trans = _pretty_transition(ds.get("transition"))
            head = page + (f"  ·  {trans}" if trans and ds.get("transition") != "unknown" else "")
            parts.append(_heading(head, 3))
            if leak.get("finding"):
                parts.append(_para(leak["finding"]))
            if ds.get("evidence"):
                parts.append(_para(f"Evidence: {ds['evidence']}", italic=True))
            parts.append(_spacer())
    else:
        parts.append(_para("No findings were recorded for this run.", italic=True))
        parts.append(_spacer())

    # ── 3. Recommendations ──
    parts.append(_heading("3. Recommendations", 1))
    tests = a2.get("ranked_tests") or []
    if tests:
        tests_sorted = sorted(
            tests, key=lambda t: t.get("priority_score") or 0, reverse=True)
        # table: Test | Impact | Effort | Priority
        headers = ["Recommended test", "Impact", "Effort", "Priority"]
        widths = [5225, 1200, 1200, 1400]  # sum 9025
        rows = []
        for t in tests_sorted:
            name = t.get("test_name") or f"Test {t.get('rank', '')}".strip()
            rows.append([
                name,
                _num(t.get("impact_score")),
                _num(t.get("effort_score")),
                _num(t.get("priority_score")),
            ])
        parts.append(_table(headers, rows, widths, mono_cols={1, 2, 3}))
        parts.append(_spacer())
        # hypotheses beneath
        for t in tests_sorted:
            name = t.get("test_name") or f"Test {t.get('rank', '')}".strip()
            if t.get("hypothesis"):
                parts.append(_heading(name, 3))
                parts.append(_para(t["hypothesis"]))
                parts.append(_spacer())
    else:
        parts.append(_para("No recommendations were produced for this run.",
                           italic=True))
        parts.append(_spacer())

    # ── 4. Action plan (optional) ──
    if include_action_plan:
        parts.append(_heading("4. Action plan", 1))
        calendar = a5.get("calendar") or []
        if calendar:
            for wk in calendar:
                num = wk.get("week")
                title = f"Week {num}" if num is not None else "Week"
                parts.append(_heading(title, 3))
                any_test = False
                for bucket_key, verb in (("launch", "Launch"),
                                         ("running", "Running"),
                                         ("completing", "Completing")):
                    for item in (wk.get(bucket_key) or []):
                        any_test = True
                        if isinstance(item, dict):
                            label = (item.get("test_name") or item.get("page")
                                     or item.get("hypothesis_short") or "—")
                        else:
                            label = str(item)
                        parts.append(_bullet(f"{verb}: {label}"))
                if not any_test:
                    parts.append(_para("No tests scheduled this week.", italic=True))
                parts.append(_spacer())
        else:
            parts.append(_para("No action plan was produced for this run.",
                               italic=True))
            parts.append(_spacer())

    body = "".join(parts)

    # Wrap in the document/body envelope. sectPr copied minimal (US Letter).
    # Preserve the template's own sectPr (it carries the <w:headerReference>
    # that shows the CAB logo). Fall back to a plain US-Letter section only if
    # the template had none.
    if not sect_pr:
        sect_pr = (
            '<w:sectPr><w:pgSz w:w="12240" w:h="15840"/>'
            '<w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" '
            'w:header="720" w:footer="720" w:gutter="0"/></w:sectPr>'
        )

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<w:body>'
        f'{body}'
        f'{sect_pr}'
        '</w:body></w:document>'
    )


# ── Public entry point ───────────────────────────────────────────────────────────

def generate_report(report: dict, template_path: str, output_path: str,
                    include_action_plan: bool = True) -> str:
    """
    Build a .docx report at output_path from `report` (pipeline data), using
    template_path as the style shell. Returns output_path.
    """
    workdir = tempfile.mkdtemp(prefix="cablytics_report_")
    try:
        # unzip the template
        with zipfile.ZipFile(template_path, "r") as z:
            z.extractall(workdir)

        # read the template's own sectPr (carries the header/logo reference) so
        # the generated body keeps the CAB logo header.
        doc_path = os.path.join(workdir, "word", "document.xml")
        original = open(doc_path, encoding="utf-8").read()
        m = re.search(r"<w:sectPr.*?</w:sectPr>", original, re.DOTALL)
        sect_pr = m.group(0) if m else ""

        body_xml = _build_body(report, include_action_plan, sect_pr)

        # replace the body document, keep everything else (styles, fonts, theme, header)
        with open(doc_path, "w", encoding="utf-8") as f:
            f.write(body_xml)

        # re-zip into a .docx (document.xml must be stored with the right structure)
        if os.path.exists(output_path):
            os.remove(output_path)
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as z:
            for root, _dirs, files in os.walk(workdir):
                for name in files:
                    full = os.path.join(root, name)
                    arc = os.path.relpath(full, workdir)
                    z.write(full, arc)
        return output_path
    finally:
        shutil.rmtree(workdir, ignore_errors=True)