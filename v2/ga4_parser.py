"""
GA4 CSV parser for CABlytics V2 manual upload path.

Handles Google Analytics 4 Explorations CSV exports. These are messier than GSC:
- Metadata rows at the top (report name, date range, filters, dimensions, metrics)
- A blank line separator
- Then the actual data table

Users build their own Explorations so column sets vary. This parser maps
flexibly to the fields Agents 1-5 need, and applies the same web-pixels
filter pattern as the API path (NOT_CONTAINS 'web-pixels' on pagePath).
"""

import csv
import io
import re
from datetime import date, datetime
from typing import IO, Optional, TypedDict


class GA4Row(TypedDict):
    page_path: str
    sessions: int
    total_users: int
    engaged_sessions: int
    avg_engagement_time: float  # seconds
    event_count: int
    conversions: int
    source: str  # optional — empty string if not present in export


class GA4ParseResult(TypedDict):
    row_count: int
    total_sessions: int
    total_users: int
    detected_date_range: Optional[tuple[str, str]]  # ISO strings, or None
    rows: list[GA4Row]
    columns_present: list[str]
    web_pixels_rows_excluded: int


class GA4ParseError(Exception):
    """Raised when a GA4 CSV cannot be parsed."""
    pass


# Canonical field → list of accepted header variations (case-insensitive)
_FIELD_ALIASES = {
    'page_path': ['page path', 'page path and screen class', 'page location', 'landing page'],
    'sessions': ['sessions'],
    'total_users': ['total users', 'users', 'active users'],
    'engaged_sessions': ['engaged sessions'],
    'avg_engagement_time': [
        'average engagement time', 'average engagement time per session',
        'avg engagement time', 'user engagement', 'engagement time'
    ],
    'event_count': ['event count', 'events'],
    'conversions': ['conversions', 'key events', 'conversion'],
    'source': ['source', 'session source', 'first user source'],
}

_DATE_RANGE_PATTERNS = [
    # "20241001-20241031"
    re.compile(r'(\d{8})\s*[-–]\s*(\d{8})'),
    # "Oct 1, 2024 - Oct 31, 2024" or "1 Oct 2024 - 31 Oct 2024"
    re.compile(
        r'([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})'
        r'\s*[-–to]+\s*'
        r'([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})',
        re.IGNORECASE
    ),
    # "2024-10-01 - 2024-10-31"
    re.compile(r'(\d{4}-\d{2}-\d{2})\s*[-–]\s*(\d{4}-\d{2}-\d{2})'),
]


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
        return round(float(cleaned), 2)
    except ValueError:
        return 0.0


def _normalise_header(h: str) -> str:
    return h.strip().lstrip('\ufeff').lower()


def _try_parse_date(s: str) -> Optional[date]:
    s = s.strip()
    formats = [
        '%Y%m%d', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y',
        '%b %d, %Y', '%B %d, %Y', '%d %b %Y', '%d %B %Y',
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _detect_date_range(metadata_lines: list[str]) -> Optional[tuple[str, str]]:
    """Scan metadata rows for a date range string."""
    joined = ' '.join(metadata_lines)
    for pattern in _DATE_RANGE_PATTERNS:
        match = pattern.search(joined)
        if match:
            start = _try_parse_date(match.group(1))
            end = _try_parse_date(match.group(2))
            if start and end:
                return (start.isoformat(), end.isoformat())
    return None


def _split_metadata_and_data(text: str) -> tuple[list[str], str]:
    """
    GA4 exports have metadata rows, then a blank line, then the data table.
    Return (metadata_lines, data_csv_text).

    If there's no blank separator, treat the first row that looks like
    proper CSV headers (containing 'sessions' or 'users' or 'page') as
    the start of the data table.
    """
    lines = text.splitlines()
    blank_idx = None
    for i, line in enumerate(lines):
        if not line.strip():
            blank_idx = i
            break

    if blank_idx is not None and blank_idx < len(lines) - 1:
        metadata = lines[:blank_idx]
        data = '\n'.join(lines[blank_idx + 1:])
        return metadata, data

    # Fallback: find the header row heuristically
    data_keywords = {'sessions', 'users', 'page path', 'page', 'event count'}
    for i, line in enumerate(lines):
        lower = line.lower()
        if any(kw in lower for kw in data_keywords) and ',' in line:
            return lines[:i], '\n'.join(lines[i:])

    # No metadata detected — whole file is the data table
    return [], text


def _build_column_map(headers: list[str]) -> dict[str, int]:
    """Map canonical field names to column indices using the alias table."""
    normalised = [_normalise_header(h) for h in headers]
    column_map: dict[str, int] = {}
    for field, aliases in _FIELD_ALIASES.items():
        for alias in aliases:
            if alias in normalised:
                column_map[field] = normalised.index(alias)
                break
    return column_map


def parse_ga4_csv(
    file: IO[bytes] | IO[str],
    exclude_web_pixels: bool = True,
) -> GA4ParseResult:
    """
    Parse a GA4 Explorations CSV export.

    Args:
        file: file-like object (binary or text)
        exclude_web_pixels: apply NOT_CONTAINS 'web-pixels' filter on page_path
                            to match the API path behaviour for Shopify clients.

    Raises GA4ParseError on malformed input.
    """
    # Read as text
    raw = file.read()
    if isinstance(raw, bytes):
        text = raw.decode('utf-8-sig')
    else:
        text = raw.lstrip('\ufeff')

    if not text.strip():
        raise GA4ParseError("File is empty")

    metadata_lines, data_text = _split_metadata_and_data(text)
    detected_range = _detect_date_range(metadata_lines) if metadata_lines else None

    reader = csv.reader(io.StringIO(data_text))
    try:
        headers = next(reader)
    except StopIteration:
        raise GA4ParseError("Could not find a header row in the data section")

    column_map = _build_column_map(headers)

    # Minimum viable: must have page_path AND at least one of sessions/users
    if 'page_path' not in column_map:
        raise GA4ParseError(
            f"GA4 CSV must include a page path column. "
            f"Accepted headers: {_FIELD_ALIASES['page_path']}. "
            f"Found headers: {headers}"
        )
    if 'sessions' not in column_map and 'total_users' not in column_map:
        raise GA4ParseError(
            "GA4 CSV must include a 'sessions' or 'users' column"
        )

    rows: list[GA4Row] = []
    excluded = 0

    for row_num, raw_row in enumerate(reader, start=2):
        if not raw_row or not any(cell.strip() for cell in raw_row):
            continue

        # Defensive: skip totals/summary rows that GA4 sometimes appends
        first_cell = raw_row[0].strip().lower()
        if first_cell in {'totals', 'total', 'grand total', ''}:
            continue

        try:
            page_path = raw_row[column_map['page_path']].strip()
        except IndexError:
            continue

        if not page_path:
            continue

        # Apply web-pixels filter (matches the API path filter)
        if exclude_web_pixels and 'web-pixels' in page_path.lower():
            excluded += 1
            continue

        def _get_int(field: str) -> int:
            if field not in column_map:
                return 0
            try:
                return _parse_int(raw_row[column_map[field]])
            except IndexError:
                return 0

        def _get_float(field: str) -> float:
            if field not in column_map:
                return 0.0
            try:
                return _parse_float(raw_row[column_map[field]])
            except IndexError:
                return 0.0

        def _get_str(field: str) -> str:
            if field not in column_map:
                return ''
            try:
                return raw_row[column_map[field]].strip()
            except IndexError:
                return ''

        rows.append(GA4Row(
            page_path=page_path,
            sessions=_get_int('sessions'),
            total_users=_get_int('total_users'),
            engaged_sessions=_get_int('engaged_sessions'),
            avg_engagement_time=_get_float('avg_engagement_time'),
            event_count=_get_int('event_count'),
            conversions=_get_int('conversions'),
            source=_get_str('source'),
        ))

    if not rows:
        raise GA4ParseError("No data rows found")

    total_sessions = sum(r['sessions'] for r in rows)
    total_users = sum(r['total_users'] for r in rows)

    return GA4ParseResult(
        row_count=len(rows),
        total_sessions=total_sessions,
        total_users=total_users,
        detected_date_range=detected_range,
        rows=rows,
        columns_present=sorted(column_map.keys()),
        web_pixels_rows_excluded=excluded,
    )


def preview_rows(result: GA4ParseResult, n: int = 5) -> list[GA4Row]:
    """Top N rows by sessions for the validation screen."""
    return sorted(result['rows'], key=lambda r: r['sessions'], reverse=True)[:n]
