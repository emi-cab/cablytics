"""
GA4 CSV parser for CABlytics V2 manual upload path.

Handles Google Analytics 4 Explorations CSV exports. These are messy:
- Metadata rows at the top (report name, date range, filters, dimensions, metrics)
- Lines starting with '#' are comments/separators throughout the file
- Multi-segment exports (e.g. comparison "All Users vs Organic vs Paid") produce
  multiple data tables in one file, separated by '# Segment name' headers.
  This parser uses only the FIRST data table (typically "All Users").
- Users build their own Explorations so column sets vary. The parser uses an
  alias table to map flexible headers to canonical fields.
- Applies the same web-pixels filter pattern as the API path
  (NOT_CONTAINS 'web-pixels' on pagePath) for Shopify clients.
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
    segments_detected: int  # how many segments were found (>1 means comparison view)
    segments_used: str      # which segment we actually parsed (e.g. "All Users")


class GA4ParseError(Exception):
    """Raised when a GA4 CSV cannot be parsed."""
    pass


# Canonical field → list of accepted header variations (case-insensitive)
# Order matters within each list — first match wins on ambiguous files.
_FIELD_ALIASES = {
    'page_path': [
        'page path and screen class', 'page path', 'page location', 'landing page',
        'page', 'page title', 'page title and screen class',
    ],
    'sessions': ['sessions', 'views'],  # "Views" is GA4's page-views metric when sessions absent
    'total_users': ['total users', 'active users', 'users'],
    'engaged_sessions': ['engaged sessions'],
    'avg_engagement_time': [
        'average engagement time per active user',
        'average engagement time per session',
        'average engagement time',
        'avg engagement time', 'user engagement', 'engagement time',
    ],
    'event_count': ['event count', 'events'],
    'conversions': ['key events', 'conversions', 'conversion'],  # GA4 renamed Conversions → Key events
    'source': ['source', 'session source', 'first user source'],
}

_DATE_RANGE_PATTERNS = [
    # "Start date: 20260416  End date: 20260513" (GA4 metadata block)
    re.compile(
        r'start\s*date[:\s]+(\d{8}).*?end\s*date[:\s]+(\d{8})',
        re.IGNORECASE | re.DOTALL,
    ),
    # "20241001-20241031"
    re.compile(r'(\d{8})\s*[-–]\s*(\d{8})'),
    # "Oct 1, 2024 - Oct 31, 2024"
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


def _split_into_sections(text: str) -> tuple[list[str], list[list[str]], list[str]]:
    """
    Walk the file and identify:
        prelude: all '#' comment lines before the first data table
        sections: list of [header_line, data_line, data_line, ...] for each
                  data table found (separated by '#' segment headers and/or
                  blank lines)
        segment_names: the '# Name' line preceding each section (or '' for
                       the first one if it has no segment header)

    A "data table" starts at the first non-empty, non-'#' line whose first
    cell looks like a column header (matches a known page-path alias).
    """
    lines = text.splitlines()
    prelude: list[str] = []
    sections: list[list[str]] = []
    segment_names: list[str] = []

    page_aliases = _FIELD_ALIASES['page_path']

    in_data = False
    current: list[str] = []
    current_segment = ''
    pending_segment = ''  # last '#' line seen before a header

    for line in lines:
        stripped = line.strip()

        # Comment / metadata line
        if stripped.startswith('#'):
            if not in_data:
                # Still in prelude — remember the comment in case it's a date
                prelude.append(stripped.lstrip('#').strip())
                # Track recent segment-looking comment ("# All Users", "# Organic traffic")
                content = stripped.lstrip('#').strip()
                if content and not content.startswith('-') and 'date' not in content.lower():
                    pending_segment = content
            else:
                # Finished a data table; remember segment header for next one
                content = stripped.lstrip('#').strip()
                if current:
                    sections.append(current)
                    segment_names.append(current_segment)
                    current = []
                if content and not content.startswith('-') and 'date' not in content.lower():
                    pending_segment = content
                in_data = False
            continue

        # Blank line
        if not stripped:
            if in_data and current:
                # End of a data table
                sections.append(current)
                segment_names.append(current_segment)
                current = []
                in_data = False
            continue

        # Non-empty, non-comment line
        if not in_data:
            # Check if this is a header row (first cell matches a page-path alias)
            first_cell = stripped.split(',')[0].strip().strip('"').lower()
            if first_cell in page_aliases:
                in_data = True
                current_segment = pending_segment or 'All Users'
                pending_segment = ''
                current.append(line)
            else:
                # Not a recognisable header — treat as prelude metadata
                prelude.append(stripped)
        else:
            current.append(line)

    # Capture trailing section
    if current:
        sections.append(current)
        segment_names.append(current_segment)

    return prelude, sections, segment_names


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

    For multi-segment files (e.g. "All Users vs Organic vs Paid" comparison),
    only the FIRST data table is parsed and the rest are reported via
    `segments_detected`. This keeps semantics simple: one report = one segment.

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

    prelude, sections, segment_names = _split_into_sections(text)

    if not sections:
        raise GA4ParseError(
            "Could not find a recognisable data table in the file. "
            f"Looked for a header row with one of: {_FIELD_ALIASES['page_path']}. "
            "If this is a GA4 export, make sure your Exploration includes a "
            "'Page path and screen class' (or similar) dimension."
        )

    # Always use the first section (typically "All Users")
    section_lines = sections[0]
    segment_used = segment_names[0] or 'All Users'

    reader = csv.reader(io.StringIO('\n'.join(section_lines)))
    headers = next(reader)
    column_map = _build_column_map(headers)

    if 'page_path' not in column_map:
        raise GA4ParseError(
            f"GA4 CSV must include a page path column. "
            f"Found headers: {headers}"
        )
    if 'sessions' not in column_map and 'total_users' not in column_map:
        raise GA4ParseError(
            "GA4 CSV must include a 'sessions' / 'views' / 'users' column"
        )

    detected_range = _detect_date_range(prelude) if prelude else None

    rows: list[GA4Row] = []
    excluded = 0

    for raw_row in reader:
        if not raw_row or not any(cell.strip() for cell in raw_row):
            continue

        # Skip totals/summary rows that GA4 sometimes appends
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
        raise GA4ParseError("No data rows found in the first data section")

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
        segments_detected=len(sections),
        segments_used=segment_used,
    )


def preview_rows(result: GA4ParseResult, n: int = 5) -> list[GA4Row]:
    """Top N rows by sessions for the validation screen."""
    return sorted(result['rows'], key=lambda r: r['sessions'], reverse=True)[:n]