"""
GSC CSV parser for CABlytics V2 manual upload path.

Handles Google Search Console Performance report exports:
- "Pages.csv" — columns: Top pages, Clicks, Impressions, CTR, Position
- "Queries.csv" — columns: Top queries, Clicks, Impressions, CTR, Position

Both are UTF-8, comma-separated, no metadata rows.
Date range is NOT in the file — must be supplied by the user.
"""

import csv
import io
import re
from typing import IO, Literal, TypedDict


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


# GSC exports include a BOM on the first column header
_BOM = '\ufeff'

# Accepted header variations (GSC has changed labels over time, and locale matters)
_PAGE_HEADERS = {'top pages', 'page', 'pages', 'landing page'}
_QUERY_HEADERS = {'top queries', 'query', 'queries', 'search query'}


def _strip_bom(s: str) -> str:
    return s.lstrip(_BOM).strip()


def _parse_ctr(value: str) -> float:
    """
    GSC CTR can be exported as '5.23%', '5.23', or '0.0523'.
    Normalise everything to a decimal (0.0523).
    """
    if not value:
        return 0.0
    cleaned = value.strip().rstrip('%').strip()
    try:
        n = float(cleaned)
    except ValueError:
        return 0.0
    # If the source string had a % or the value is >1, treat as percentage
    if '%' in value or n > 1:
        return round(n / 100, 6)
    return round(n, 6)


def _parse_int(value: str) -> int:
    """Handle GSC's occasional thousands separators."""
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
        return round(float(cleaned), 2)
    except ValueError:
        return 0.0


def parse_gsc_csv(file: IO[bytes] | IO[str]) -> GSCParseResult:
    """
    Parse a GSC Performance CSV export.

    Auto-detects whether it's a Pages or Queries export based on the first column header.
    Accepts either a binary or text file-like object (Flask gives you binary from request.files).

    Raises GSCParseError if the file is empty, malformed, or has an unrecognised header.
    """
    # Read as text — handle both bytes and str inputs
    if hasattr(file, 'read'):
        raw = file.read()
        if isinstance(raw, bytes):
            text = raw.decode('utf-8-sig')  # utf-8-sig strips BOM automatically
        else:
            text = raw.lstrip(_BOM)
    else:
        raise GSCParseError("Expected a file-like object")

    if not text.strip():
        raise GSCParseError("File is empty")

    reader = csv.reader(io.StringIO(text))
    try:
        headers = next(reader)
    except StopIteration:
        raise GSCParseError("File has no header row")

    if not headers:
        raise GSCParseError("Header row is empty")

    # Normalise headers
    headers = [_strip_bom(h).lower() for h in headers]
    first_col = headers[0]

    # Detect type
    if first_col in _PAGE_HEADERS:
        source_type: Literal['pages', 'queries'] = 'pages'
    elif first_col in _QUERY_HEADERS:
        source_type = 'queries'
    else:
        raise GSCParseError(
            f"Unrecognised first column '{headers[0]}'. "
            f"Expected one of: {sorted(_PAGE_HEADERS | _QUERY_HEADERS)}"
        )

    # Map remaining columns by position-tolerant lookup
    expected = {'clicks', 'impressions', 'ctr', 'position'}
    header_index = {h: i for i, h in enumerate(headers)}
    missing = expected - set(header_index.keys())
    if missing:
        raise GSCParseError(
            f"GSC CSV is missing required columns: {sorted(missing)}. "
            f"Found: {headers}"
        )

    rows: list[GSCRow] = []
    total_clicks = 0
    total_impressions = 0

    for row_num, row in enumerate(reader, start=2):
        if not row or not any(cell.strip() for cell in row):
            continue  # skip blank lines
        try:
            key = row[0].strip()
            if not key:
                continue
            clicks = _parse_int(row[header_index['clicks']])
            impressions = _parse_int(row[header_index['impressions']])
            ctr = _parse_ctr(row[header_index['ctr']])
            position = _parse_float(row[header_index['position']])
        except IndexError:
            raise GSCParseError(f"Row {row_num} has fewer columns than the header")

        rows.append(GSCRow(
            key=key,
            clicks=clicks,
            impressions=impressions,
            ctr=ctr,
            position=position,
        ))
        total_clicks += clicks
        total_impressions += impressions

    if not rows:
        raise GSCParseError("No data rows found after the header")

    return GSCParseResult(
        source_type=source_type,
        row_count=len(rows),
        total_clicks=total_clicks,
        total_impressions=total_impressions,
        rows=rows,
    )


def preview_rows(result: GSCParseResult, n: int = 5) -> list[GSCRow]:
    """Convenience for the validation screen — top N rows by clicks."""
    return sorted(result['rows'], key=lambda r: r['clicks'], reverse=True)[:n]
