"""
GSC CSV parser for CABlytics V2 manual upload path.

Handles several real-world export shapes:

1. Native GSC Performance export (simple):
     Top pages,Clicks,Impressions,CTR,Position
     Top queries,Clicks,Impressions,CTR,Position

2. Native GSC Performance with device pivot enabled (one column per device):
     Top pages,Desktop Clicks,Mobile Clicks,Desktop Impressions,Mobile Impressions,
              Desktop CTR,Mobile CTR,Desktop Position,Mobile Position
   We collapse to total clicks/impressions and weighted-average CTR/position.

3. GA4 → Explore → Search Console integration export (hybrid):
     # metadata header block (like GA4 exports)
     Organic Google Search query,Device category,Organic Google Search clicks,
        Organic Google Search impressions,
        Organic Google Search click-through-rate,
        Organic Google Search average position
   Each query appears once per device; we collapse to a single row per query.

All shapes are normalised to the same output:
    {'source_type': 'pages' | 'queries', 'rows': [{key, clicks, impressions, ctr, position}], ...}
"""

import csv
import io
import re
from typing import IO, Literal, TypedDict, Optional


class GSCRow(TypedDict):
    key: str          # page URL or query string
    clicks: int
    impressions: int
    ctr: float        # stored as decimal (0.0523), not percentage string
    position: float


class GSCParseResult(TypedDict):
    source_type: Literal['pages', 'queries']
    row_count: int
    total_clicks: int
    total_impressions: int
    rows: list[GSCRow]


class GSCParseError(Exception):
    """Raised when a GSC CSV cannot be parsed."""
    pass


_BOM = '\ufeff'

# First-column header variations (case-insensitive)
_PAGE_HEADERS = {
    'top pages', 'page', 'pages', 'landing page',
    'organic google search landing page', 'page path',
}
_QUERY_HEADERS = {
    'top queries', 'query', 'queries', 'search query',
    'organic google search query',
}

# Suffix tokens we expect in metric columns (the prefix may vary per export)
_METRIC_KEYS = {
    'clicks':      ['clicks'],
    'impressions': ['impressions'],
    'ctr':         ['ctr', 'click-through-rate', 'click through rate'],
    'position':    ['position', 'average position'],
}

_DEVICE_TOKENS = ('desktop', 'mobile', 'tablet')


def _strip_bom(s: str) -> str:
    return s.lstrip(_BOM).strip()


def _normalise_header(h: str) -> str:
    return _strip_bom(h).lower()


def _parse_ctr(value: str) -> float:
    """CTR may be exported as '5.23%', '5.23', or '0.0523'. Normalise to decimal."""
    if not value:
        return 0.0
    cleaned = value.strip().rstrip('%').strip()
    try:
        n = float(cleaned)
    except ValueError:
        return 0.0
    if '%' in value or n > 1:
        return round(n / 100, 6)
    return round(n, 6)


def _parse_int(value: str) -> int:
    if not value:
        return 0
    cleaned = re.sub(r'[,\s]', '', value.strip())
    try:
        return int(float(cleaned))
    except ValueError:
        return 0


def _parse_float(value: str) -> float:
    if not value:
        return 0.0
    cleaned = re.sub(r'[,\s]', '', value.strip())
    try:
        return round(float(cleaned), 4)
    except ValueError:
        return 0.0


def _read_text(file: IO[bytes] | IO[str]) -> str:
    raw = file.read()
    if isinstance(raw, bytes):
        return raw.decode('utf-8-sig')
    return raw.lstrip(_BOM)


def _strip_metadata_prelude(text: str) -> str:
    """
    Some GSC exports (especially from GA4's Search Console integration) include
    '#' comment rows at the top, GA4-style. Strip everything up to the first
    real header row.
    """
    lines = text.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith('#'):
            continue
        # First non-empty, non-comment line — assume header
        return '\n'.join(lines[i:])
    return text


def _match_metric(header_norm: str) -> Optional[tuple[str, Optional[str]]]:
    """
    Given a normalised header, identify (metric_name, device_or_None).
    Examples:
        "clicks"                                      → ("clicks", None)
        "desktop clicks"                              → ("clicks", "desktop")
        "mobile ctr"                                  → ("ctr", "mobile")
        "organic google search clicks"                → ("clicks", None)
        "organic google search click-through-rate"    → ("ctr", None)
    Returns None if the header doesn't match a known metric.
    """
    device: Optional[str] = None
    for d in _DEVICE_TOKENS:
        if header_norm.startswith(d + ' ') or header_norm.endswith(' ' + d):
            device = d
            break

    for metric, suffixes in _METRIC_KEYS.items():
        for s in suffixes:
            if header_norm.endswith(s):
                return metric, device
    return None


def parse_gsc_csv(file: IO[bytes] | IO[str]) -> GSCParseResult:
    """
    Parse a GSC CSV export (any of the supported shapes — see module docstring).

    Auto-detects:
      - whether it's a Pages or Queries export (from the first column header)
      - whether it's device-pivoted (Desktop/Mobile columns) → collapses to totals
      - whether it's a GA4-Console hybrid export (# metadata + 'Organic Google Search ...' headers)
      - whether queries appear once per device → collapses on key

    Raises GSCParseError if the file is empty, malformed, or has an unrecognised header.
    """
    text = _read_text(file)
    if not text.strip():
        raise GSCParseError("File is empty")

    text = _strip_metadata_prelude(text)
    if not text.strip():
        raise GSCParseError("File contains only metadata, no data table")

    reader = csv.reader(io.StringIO(text))
    try:
        headers = next(reader)
    except StopIteration:
        raise GSCParseError("File has no header row")

    if not headers:
        raise GSCParseError("Header row is empty")

    headers_norm = [_normalise_header(h) for h in headers]
    first_col = headers_norm[0]

    # Detect source type from first column
    if first_col in _PAGE_HEADERS:
        source_type: Literal['pages', 'queries'] = 'pages'
    elif first_col in _QUERY_HEADERS:
        source_type = 'queries'
    else:
        raise GSCParseError(
            f"Unrecognised first column '{headers[0]}'. "
            f"Expected one of: {sorted(_PAGE_HEADERS | _QUERY_HEADERS)}"
        )

    # Map each non-first column to (metric, device_or_None). Some columns may
    # be 'Device category' or other non-metric dimensions — those return None
    # and are simply ignored.
    column_metrics: list[Optional[tuple[str, Optional[str]]]] = [None]  # placeholder for column 0
    device_category_idx: Optional[int] = None
    for i, h in enumerate(headers_norm[1:], start=1):
        if h == 'device category' or h == 'device':
            device_category_idx = i
            column_metrics.append(None)
        else:
            column_metrics.append(_match_metric(h))

    # Required metrics: clicks, impressions, ctr, position must all be representable
    metrics_found = {m for col in column_metrics if col for m in [col[0]]}
    missing = {'clicks', 'impressions', 'ctr', 'position'} - metrics_found
    if missing:
        raise GSCParseError(
            f"GSC CSV is missing required metrics: {sorted(missing)}. "
            f"Found headers: {headers}"
        )

    # Aggregate per-key. For device-pivoted columns or device-category rows,
    # we sum clicks/impressions and weighted-average CTR/position.
    aggregates: dict[str, dict] = {}

    for row_num, row in enumerate(reader, start=2):
        if not row or not any(cell.strip() for cell in row):
            continue
        try:
            key = row[0].strip()
        except IndexError:
            continue
        if not key or key.lower() in {'totals', 'total', 'grand total'}:
            continue

        # Pull each metric's value, summing across device-pivot columns
        clicks_total = 0
        impressions_total = 0
        # For CTR and position we keep a sum weighted by impressions for proper averaging
        ctr_weighted_sum = 0.0
        pos_weighted_sum = 0.0
        ctr_weight = 0
        pos_weight = 0

        for col_idx, meta in enumerate(column_metrics):
            if meta is None:
                continue
            metric, _device = meta
            try:
                raw_val = row[col_idx]
            except IndexError:
                continue

            if metric == 'clicks':
                clicks_total += _parse_int(raw_val)
            elif metric == 'impressions':
                impressions_total += _parse_int(raw_val)
            elif metric == 'ctr':
                ctr_weighted_sum += _parse_ctr(raw_val)  # weighting added below
            elif metric == 'position':
                pos_weighted_sum += _parse_float(raw_val)

        # If we have device-pivoted CTR/Position columns, they're already
        # device-specific. Use impressions as the weighting if multiple devices
        # were summed, otherwise just take the raw values.
        n_ctr_cols = sum(1 for m in column_metrics if m and m[0] == 'ctr')
        n_pos_cols = sum(1 for m in column_metrics if m and m[0] == 'position')

        # We need a proper weighted average across devices. Re-walk the row
        # to compute per-device contributions weighted by per-device impressions.
        if n_ctr_cols > 1 or n_pos_cols > 1:
            # Group columns by device
            by_device: dict[str, dict] = {}
            for col_idx, meta in enumerate(column_metrics):
                if meta is None:
                    continue
                metric, device = meta
                d = device or '__all__'
                try:
                    raw_val = row[col_idx]
                except IndexError:
                    continue
                by_device.setdefault(d, {})[metric] = raw_val

            ctr_num = 0.0
            ctr_den = 0
            pos_num = 0.0
            pos_den = 0
            for d, vals in by_device.items():
                imp = _parse_int(vals.get('impressions', '0'))
                if 'ctr' in vals:
                    ctr_num += _parse_ctr(vals['ctr']) * imp
                    ctr_den += imp
                if 'position' in vals:
                    pos_num += _parse_float(vals['position']) * imp
                    pos_den += imp
            ctr_avg = (ctr_num / ctr_den) if ctr_den else 0.0
            pos_avg = (pos_num / pos_den) if pos_den else 0.0
        else:
            # Simple single-column metrics
            ctr_avg = ctr_weighted_sum
            pos_avg = pos_weighted_sum

        # Aggregate per key (handles device-category duplicate rows in
        # GA4-Console hybrid exports where each query appears once per device)
        if key in aggregates:
            agg = aggregates[key]
            old_imp = agg['impressions']
            new_imp = impressions_total
            total_imp = old_imp + new_imp
            if total_imp > 0:
                agg['ctr'] = (agg['ctr'] * old_imp + ctr_avg * new_imp) / total_imp
                agg['position'] = (agg['position'] * old_imp + pos_avg * new_imp) / total_imp
            agg['clicks'] += clicks_total
            agg['impressions'] = total_imp
        else:
            aggregates[key] = {
                'clicks': clicks_total,
                'impressions': impressions_total,
                'ctr': ctr_avg,
                'position': pos_avg,
            }

    if not aggregates:
        raise GSCParseError("No data rows found after the header")

    rows: list[GSCRow] = []
    total_clicks = 0
    total_impressions = 0
    for key, agg in aggregates.items():
        rows.append(GSCRow(
            key=key,
            clicks=agg['clicks'],
            impressions=agg['impressions'],
            ctr=round(agg['ctr'], 6),
            position=round(agg['position'], 2),
        ))
        total_clicks += agg['clicks']
        total_impressions += agg['impressions']

    return GSCParseResult(
        source_type=source_type,
        row_count=len(rows),
        total_clicks=total_clicks,
        total_impressions=total_impressions,
        rows=rows,
    )


def preview_rows(result: GSCParseResult, n: int = 5) -> list[GSCRow]:
    """Top N rows by clicks for the validation screen."""
    return sorted(result['rows'], key=lambda r: r['clicks'], reverse=True)[:n]