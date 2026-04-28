"""
CABlytics V2 — database layer.

Uses PostgreSQL (Supabase) when DATABASE_URL is set, falls back to SQLite
for local development.

Schema (Phase 1):
  • clients
      - VoC split into voc_volunteered + voc_solicited
      - competitor_notes retained
      - clarity_api_token, gsc_site_url placeholders for Phase 4/5
      - session_insights retained for manual Clarity Copilot paste
      - target_urls and current_pdp_copy REMOVED (replaced by client_page_assets)
  • client_page_assets (new)
      - per-client tagged pages with optional copy + screenshot
  • reports, run_log unchanged
"""

import os
import json
from datetime import datetime, timezone
from contextlib import contextmanager

DATABASE_URL = os.environ.get("DATABASE_URL")

# ── Connection ─────────────────────────────────────────────────────────────────

if DATABASE_URL:
    import psycopg2
    import psycopg2.extras

    @contextmanager
    def get_connection():
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _row(cursor, query, params=()):
        cursor.execute(query, params)
        row = cursor.fetchone()
        return dict(row) if row else None

    def _rows(cursor, query, params=()):
        cursor.execute(query, params)
        return [dict(r) for r in cursor.fetchall()]

    PLACEHOLDER = "%s"

else:
    import sqlite3
    from pathlib import Path

    DB_PATH = str(Path(__file__).parent / "v2.db")

    @contextmanager
    def get_connection():
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _row(conn, query, params=()):
        row = conn.execute(query, params).fetchone()
        return dict(row) if row else None

    def _rows(conn, query, params=()):
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    PLACEHOLDER = "?"


# ── Schema ─────────────────────────────────────────────────────────────────────

def init_db():
    """Create all tables if they don't exist."""
    if DATABASE_URL:
        _init_postgres()
    else:
        _init_sqlite()


def _init_postgres():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    id                   SERIAL PRIMARY KEY,
                    client_name          TEXT NOT NULL,
                    client_slug          TEXT NOT NULL UNIQUE,
                    ga4_property_id      TEXT NOT NULL,
                    client_context       TEXT,
                    voc_volunteered      TEXT,
                    voc_solicited        TEXT,
                    competitor_notes     TEXT,
                    session_insights     TEXT,
                    clarity_api_token    TEXT,
                    gsc_site_url         TEXT,
                    monthly_traffic      INTEGER,
                    dev_hours_per_week   INTEGER,
                    report_frequency     TEXT DEFAULT 'monthly',
                    schedule_day         TEXT,
                    created_at           TEXT NOT NULL,
                    updated_at           TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS client_page_assets (
                    id                SERIAL PRIMARY KEY,
                    client_id         INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                    page_type         TEXT NOT NULL,
                    page_label        TEXT NOT NULL,
                    url               TEXT NOT NULL,
                    extracted_copy    TEXT,
                    screenshot_path   TEXT,
                    display_order     INTEGER DEFAULT 0,
                    created_at        TEXT NOT NULL,
                    updated_at        TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS client_ad_creatives (
                    id                       SERIAL PRIMARY KEY,
                    client_id                INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                    landing_page_asset_id    INTEGER REFERENCES client_page_assets(id) ON DELETE SET NULL,
                    platform                 TEXT NOT NULL,
                    ad_format                TEXT,
                    ad_label                 TEXT NOT NULL,
                    headline                 TEXT,
                    primary_text             TEXT,
                    cta_label                TEXT,
                    screenshot_path          TEXT,
                    clicks                   INTEGER,
                    impressions              INTEGER,
                    superads_score           INTEGER,
                    notes                    TEXT,
                    display_order            INTEGER DEFAULT 0,
                    created_at               TEXT NOT NULL,
                    updated_at               TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS reports (
                    id                  SERIAL PRIMARY KEY,
                    client_id           INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                    run_triggered_by    TEXT NOT NULL DEFAULT 'manual',
                    status              TEXT NOT NULL DEFAULT 'pending',
                    agent1_output       TEXT,
                    agent2_output       TEXT,
                    agent3_output       TEXT,
                    agent4_output       TEXT,
                    agent5_output       TEXT,
                    full_report_json    TEXT,
                    error_message       TEXT,
                    started_at          TEXT,
                    completed_at        TEXT
                );

                CREATE TABLE IF NOT EXISTS run_log (
                    id              SERIAL PRIMARY KEY,
                    client_id       INTEGER NOT NULL,
                    report_id       INTEGER,
                    event           TEXT NOT NULL,
                    agent_number    INTEGER,
                    message         TEXT,
                    timestamp       TEXT NOT NULL
                );
            """)


def _init_sqlite():
    with get_connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS clients (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                client_name          TEXT NOT NULL,
                client_slug          TEXT NOT NULL UNIQUE,
                ga4_property_id      TEXT NOT NULL,
                client_context       TEXT,
                voc_volunteered      TEXT,
                voc_solicited        TEXT,
                competitor_notes     TEXT,
                session_insights     TEXT,
                clarity_api_token    TEXT,
                gsc_site_url         TEXT,
                monthly_traffic      INTEGER,
                dev_hours_per_week   INTEGER,
                report_frequency     TEXT DEFAULT 'monthly',
                schedule_day         TEXT,
                created_at           TEXT NOT NULL,
                updated_at           TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS client_page_assets (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id         INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                page_type         TEXT NOT NULL,
                page_label        TEXT NOT NULL,
                url               TEXT NOT NULL,
                extracted_copy    TEXT,
                screenshot_path   TEXT,
                display_order     INTEGER DEFAULT 0,
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS client_ad_creatives (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id                INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                landing_page_asset_id    INTEGER REFERENCES client_page_assets(id) ON DELETE SET NULL,
                platform                 TEXT NOT NULL,
                ad_format                TEXT,
                ad_label                 TEXT NOT NULL,
                headline                 TEXT,
                primary_text             TEXT,
                cta_label                TEXT,
                screenshot_path          TEXT,
                clicks                   INTEGER,
                impressions              INTEGER,
                superads_score           INTEGER,
                notes                    TEXT,
                display_order            INTEGER DEFAULT 0,
                created_at               TEXT NOT NULL,
                updated_at               TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reports (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id           INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                run_triggered_by    TEXT NOT NULL DEFAULT 'manual',
                status              TEXT NOT NULL DEFAULT 'pending',
                agent1_output       TEXT,
                agent2_output       TEXT,
                agent3_output       TEXT,
                agent4_output       TEXT,
                agent5_output       TEXT,
                full_report_json    TEXT,
                error_message       TEXT,
                started_at          TEXT,
                completed_at        TEXT
            );

            CREATE TABLE IF NOT EXISTS run_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id       INTEGER NOT NULL,
                report_id       INTEGER,
                event           TEXT NOT NULL,
                agent_number    INTEGER,
                message         TEXT,
                timestamp       TEXT NOT NULL
            );
        """)


def now_utc():
    return datetime.now(timezone.utc).isoformat()


# ── Clients ────────────────────────────────────────────────────────────────────

# Whitelist of fields that can be set on create or update.
# Keeping this in one place prevents SQL drift between create/update.
CLIENT_FIELDS = (
    "client_name",
    "client_slug",
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
)


def create_client(data: dict) -> dict:
    ts = now_utc()
    cols = list(CLIENT_FIELDS) + ["created_at", "updated_at"]
    values = [data.get(f) if f != "report_frequency"
              else data.get(f, "monthly")
              for f in CLIENT_FIELDS] + [ts, ts]

    placeholders = ", ".join([PLACEHOLDER] * len(cols))
    col_list = ", ".join(cols)

    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO clients ({col_list}) VALUES ({placeholders}) RETURNING *",
                    values,
                )
                row = cur.fetchone()
                return dict(row) if row else None
        else:
            cur = conn.execute(
                f"INSERT INTO clients ({col_list}) VALUES ({placeholders})",
                values,
            )
            row = conn.execute("SELECT * FROM clients WHERE id = ?", (cur.lastrowid,)).fetchone()
            return dict(row) if row else None


def update_client(client_slug: str, data: dict) -> dict | None:
    # Allow updating any client field except slug (which is the lookup key)
    allowed = set(CLIENT_FIELDS) - {"client_slug"}
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return get_client_by_slug(client_slug)

    updates["updated_at"] = now_utc()
    set_clause = ", ".join(f"{k} = {PLACEHOLDER}" for k in updates)
    values = list(updates.values()) + [client_slug]

    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE clients SET {set_clause} WHERE client_slug = {PLACEHOLDER}",
                    values,
                )
        else:
            conn.execute(
                f"UPDATE clients SET {set_clause} WHERE client_slug = ?",
                values,
            )

    return get_client_by_slug(client_slug)


def get_client_by_slug(client_slug: str) -> dict | None:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                return _row(cur, f"SELECT * FROM clients WHERE client_slug = {PLACEHOLDER}", (client_slug,))
        else:
            return _row(conn, "SELECT * FROM clients WHERE client_slug = ?", (client_slug,))


def get_client_by_id(client_id: int) -> dict | None:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                return _row(cur, f"SELECT * FROM clients WHERE id = {PLACEHOLDER}", (client_id,))
        else:
            return _row(conn, "SELECT * FROM clients WHERE id = ?", (client_id,))


def list_clients() -> list[dict]:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                return _rows(cur, "SELECT * FROM clients ORDER BY client_name ASC")
        else:
            return _rows(conn, "SELECT * FROM clients ORDER BY client_name ASC")


def delete_client(client_slug: str) -> bool:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM clients WHERE client_slug = {PLACEHOLDER}", (client_slug,))
                return cur.rowcount > 0
        else:
            result = conn.execute("DELETE FROM clients WHERE client_slug = ?", (client_slug,))
            return result.rowcount > 0


# ── Client page assets ─────────────────────────────────────────────────────────

PAGE_ASSET_FIELDS = (
    "page_type",
    "page_label",
    "url",
    "extracted_copy",
    "screenshot_path",
    "display_order",
)

VALID_PAGE_TYPES = {
    "homepage", "plp", "pdp", "cart", "checkout", "category", "other"
}


def create_page_asset(client_id: int, data: dict) -> dict:
    ts = now_utc()
    page_type = (data.get("page_type") or "other").lower()
    if page_type not in VALID_PAGE_TYPES:
        page_type = "other"

    values = (
        client_id,
        page_type,
        data.get("page_label") or "Untitled",
        data.get("url") or "",
        data.get("extracted_copy"),
        data.get("screenshot_path"),
        data.get("display_order", 0),
        ts,
        ts,
    )

    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO client_page_assets
                        (client_id, page_type, page_label, url, extracted_copy,
                         screenshot_path, display_order, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING *
                """, values)
                row = cur.fetchone()
                return dict(row) if row else None
        else:
            cur = conn.execute("""
                INSERT INTO client_page_assets
                    (client_id, page_type, page_label, url, extracted_copy,
                     screenshot_path, display_order, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, values)
            row = conn.execute(
                "SELECT * FROM client_page_assets WHERE id = ?", (cur.lastrowid,)
            ).fetchone()
            return dict(row) if row else None


def update_page_asset(asset_id: int, data: dict) -> dict | None:
    updates = {k: v for k, v in data.items() if k in PAGE_ASSET_FIELDS}
    if "page_type" in updates:
        pt = (updates["page_type"] or "other").lower()
        updates["page_type"] = pt if pt in VALID_PAGE_TYPES else "other"

    if not updates:
        return get_page_asset(asset_id)

    updates["updated_at"] = now_utc()
    set_clause = ", ".join(f"{k} = {PLACEHOLDER}" for k in updates)
    values = list(updates.values()) + [asset_id]

    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE client_page_assets SET {set_clause} WHERE id = {PLACEHOLDER}",
                    values,
                )
        else:
            conn.execute(
                f"UPDATE client_page_assets SET {set_clause} WHERE id = ?",
                values,
            )

    return get_page_asset(asset_id)


def get_page_asset(asset_id: int) -> dict | None:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                return _row(cur, f"SELECT * FROM client_page_assets WHERE id = {PLACEHOLDER}", (asset_id,))
        else:
            return _row(conn, "SELECT * FROM client_page_assets WHERE id = ?", (asset_id,))


def list_page_assets(client_id: int) -> list[dict]:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                return _rows(cur, """
                    SELECT * FROM client_page_assets
                    WHERE client_id = %s
                    ORDER BY display_order ASC, id ASC
                """, (client_id,))
        else:
            return _rows(conn, """
                SELECT * FROM client_page_assets
                WHERE client_id = ?
                ORDER BY display_order ASC, id ASC
            """, (client_id,))


def delete_page_asset(asset_id: int) -> bool:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM client_page_assets WHERE id = {PLACEHOLDER}", (asset_id,))
                return cur.rowcount > 0
        else:
            result = conn.execute("DELETE FROM client_page_assets WHERE id = ?", (asset_id,))
            return result.rowcount > 0


# ── Reports ────────────────────────────────────────────────────────────────────

def create_report(client_id: int, triggered_by: str = "manual") -> dict:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO reports (client_id, run_triggered_by, status, started_at)
                    VALUES (%s, %s, 'pending', %s)
                    RETURNING *
                """, (client_id, triggered_by, now_utc()))
                row = cur.fetchone()
                return dict(row) if row else None
        else:
            cur = conn.execute("""
                INSERT INTO reports (client_id, run_triggered_by, status, started_at)
                VALUES (?, ?, 'pending', ?)
            """, (client_id, triggered_by, now_utc()))
            row = conn.execute("SELECT * FROM reports WHERE id = ?", (cur.lastrowid,)).fetchone()
            return dict(row) if row else None


def update_report_agent(report_id: int, agent_number: int, output: dict):
    col = f"agent{agent_number}_output"
    val = json.dumps(output, ensure_ascii=True)
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE reports SET {col} = %s WHERE id = %s", (val, report_id))
        else:
            conn.execute(f"UPDATE reports SET {col} = ? WHERE id = ?", (val, report_id))


def complete_report(report_id: int):
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM reports WHERE id = %s", (report_id,))
                row = cur.fetchone()
                if not row:
                    return
                row = dict(row)
                full = {"completed_at": now_utc(), "agents": {}}
                for n in range(1, 6):
                    raw = row.get(f"agent{n}_output")
                    if raw:
                        try:
                            full["agents"][str(n)] = json.loads(raw)
                        except json.JSONDecodeError:
                            full["agents"][str(n)] = {"raw": raw}
                completed_at = full["completed_at"]
                full_json = json.dumps(full, ensure_ascii=True)
                cur.execute("""
                    UPDATE reports SET status = 'complete', full_report_json = %s, completed_at = %s
                    WHERE id = %s
                """, (full_json, completed_at, report_id))
        else:
            row = conn.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
            if not row:
                return
            row = dict(row)
            full = {"completed_at": now_utc(), "agents": {}}
            for n in range(1, 6):
                raw = row.get(f"agent{n}_output")
                if raw:
                    try:
                        full["agents"][str(n)] = json.loads(raw)
                    except json.JSONDecodeError:
                        full["agents"][str(n)] = {"raw": raw}
            completed_at = full["completed_at"]
            full_json = json.dumps(full, ensure_ascii=True)
            conn.execute("""
                UPDATE reports SET status = 'complete', full_report_json = ?, completed_at = ?
                WHERE id = ?
            """, (full_json, completed_at, report_id))


def fail_report(report_id: int, error_message: str):
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE reports SET status = 'failed', error_message = %s, completed_at = %s
                    WHERE id = %s
                """, (error_message, now_utc(), report_id))
        else:
            conn.execute("""
                UPDATE reports SET status = 'failed', error_message = ?, completed_at = ?
                WHERE id = ?
            """, (error_message, now_utc(), report_id))


def get_report_by_id(report_id: int) -> dict | None:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                return _row(cur, f"SELECT * FROM reports WHERE id = {PLACEHOLDER}", (report_id,))
        else:
            return _row(conn, "SELECT * FROM reports WHERE id = ?", (report_id,))


def get_latest_report(client_slug: str) -> dict | None:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                return _row(cur, """
                    SELECT r.* FROM reports r
                    JOIN clients c ON c.id = r.client_id
                    WHERE c.client_slug = %s AND r.status = 'complete'
                    ORDER BY r.completed_at DESC LIMIT 1
                """, (client_slug,))
        else:
            return _row(conn, """
                SELECT r.* FROM reports r
                JOIN clients c ON c.id = r.client_id
                WHERE c.client_slug = ? AND r.status = 'complete'
                ORDER BY r.completed_at DESC LIMIT 1
            """, (client_slug,))


def get_active_report(client_id: int) -> dict | None:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                return _row(cur, """
                    SELECT * FROM reports WHERE client_id = %s
                    AND status IN ('pending', 'running')
                    ORDER BY started_at DESC LIMIT 1
                """, (client_id,))
        else:
            return _row(conn, """
                SELECT * FROM reports WHERE client_id = ?
                AND status IN ('pending', 'running')
                ORDER BY started_at DESC LIMIT 1
            """, (client_id,))


def list_reports(client_slug: str, limit: int = 10) -> list[dict]:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                return _rows(cur, """
                    SELECT r.* FROM reports r
                    JOIN clients c ON c.id = r.client_id
                    WHERE c.client_slug = %s
                    ORDER BY r.started_at DESC LIMIT %s
                """, (client_slug, limit))
        else:
            return _rows(conn, """
                SELECT r.* FROM reports r
                JOIN clients c ON c.id = r.client_id
                WHERE c.client_slug = ? ORDER BY r.started_at DESC LIMIT ?
            """, (client_slug, limit))


# ── Run log ────────────────────────────────────────────────────────────────────

def log_event(client_id: int, event: str, report_id: int = None,
              agent_number: int = None, message: str = None):
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO run_log (client_id, report_id, event, agent_number, message, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (client_id, report_id, event, agent_number, message, now_utc()))
        else:
            conn.execute("""
                INSERT INTO run_log (client_id, report_id, event, agent_number, message, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (client_id, report_id, event, agent_number, message, now_utc()))


def get_run_log(client_id: int, report_id: int = None, limit: int = 50) -> list[dict]:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                if report_id:
                    return _rows(cur, """
                        SELECT * FROM run_log WHERE client_id = %s AND report_id = %s
                        ORDER BY timestamp ASC LIMIT %s
                    """, (client_id, report_id, limit))
                return _rows(cur, """
                    SELECT * FROM run_log WHERE client_id = %s
                    ORDER BY timestamp DESC LIMIT %s
                """, (client_id, limit))
        else:
            if report_id:
                return _rows(conn, """
                    SELECT * FROM run_log WHERE client_id = ? AND report_id = ?
                    ORDER BY timestamp ASC LIMIT ?
                """, (client_id, report_id, limit))
            return _rows(conn, """
                SELECT * FROM run_log WHERE client_id = ?
                ORDER BY timestamp DESC LIMIT ?
            """, (client_id, limit))


# ── Client ad creatives (Phase 6) ──────────────────────────────────────────────

VALID_AD_PLATFORMS = {
    "google", "meta", "tiktok", "linkedin", "other"
}

VALID_AD_FORMATS = {
    "image", "video", "carousel", "other"
}

AD_CREATIVE_FIELDS = (
    "platform",
    "ad_format",
    "ad_label",
    "headline",
    "primary_text",
    "cta_label",
    "screenshot_path",
    "landing_page_asset_id",
    "clicks",
    "impressions",
    "superads_score",
    "notes",
    "display_order",
)


def _normalise_ad_platform(value: str) -> str:
    v = (value or "other").lower().strip()
    return v if v in VALID_AD_PLATFORMS else "other"


def _normalise_ad_format(value: str) -> str | None:
    if value is None or value == "":
        return None
    v = value.lower().strip()
    return v if v in VALID_AD_FORMATS else "other"


def create_ad_creative(client_id: int, data: dict) -> dict:
    ts = now_utc()
    values = (
        client_id,
        _normalise_ad_platform(data.get("platform")),
        _normalise_ad_format(data.get("ad_format")),
        data.get("ad_label") or "Untitled ad",
        data.get("headline"),
        data.get("primary_text"),
        data.get("cta_label"),
        data.get("screenshot_path"),
        data.get("landing_page_asset_id"),
        data.get("clicks"),
        data.get("impressions"),
        data.get("superads_score"),
        data.get("notes"),
        data.get("display_order", 0),
        ts,
        ts,
    )

    cols = (
        "client_id, platform, ad_format, ad_label, headline, primary_text, "
        "cta_label, screenshot_path, landing_page_asset_id, clicks, impressions, "
        "superads_score, notes, display_order, created_at, updated_at"
    )

    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO client_ad_creatives ({cols})
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING *
                """, values)
                row = cur.fetchone()
                return dict(row) if row else None
        else:
            cur = conn.execute(f"""
                INSERT INTO client_ad_creatives ({cols})
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, values)
            row = conn.execute(
                "SELECT * FROM client_ad_creatives WHERE id = ?", (cur.lastrowid,)
            ).fetchone()
            return dict(row) if row else None


def update_ad_creative(ad_id: int, data: dict) -> dict | None:
    updates = {k: v for k, v in data.items() if k in AD_CREATIVE_FIELDS}
    if "platform" in updates:
        updates["platform"] = _normalise_ad_platform(updates["platform"])
    if "ad_format" in updates:
        updates["ad_format"] = _normalise_ad_format(updates["ad_format"])

    if not updates:
        return get_ad_creative(ad_id)

    updates["updated_at"] = now_utc()
    set_clause = ", ".join(f"{k} = {PLACEHOLDER}" for k in updates)
    values = list(updates.values()) + [ad_id]

    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE client_ad_creatives SET {set_clause} WHERE id = {PLACEHOLDER}",
                    values,
                )
        else:
            conn.execute(
                f"UPDATE client_ad_creatives SET {set_clause} WHERE id = ?",
                values,
            )

    return get_ad_creative(ad_id)


def get_ad_creative(ad_id: int) -> dict | None:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                return _row(cur, f"SELECT * FROM client_ad_creatives WHERE id = {PLACEHOLDER}", (ad_id,))
        else:
            return _row(conn, "SELECT * FROM client_ad_creatives WHERE id = ?", (ad_id,))


def list_ad_creatives(client_id: int) -> list[dict]:
    """
    Return all ad creatives for a client, ordered for display. Each row also
    includes the linked landing page's label, URL, and page_type as
    landing_page_label / landing_page_url / landing_page_type so callers don't
    need a second query.
    """
    sql_pg = """
        SELECT a.*,
               p.page_label AS landing_page_label,
               p.url        AS landing_page_url,
               p.page_type  AS landing_page_type
        FROM client_ad_creatives a
        LEFT JOIN client_page_assets p ON p.id = a.landing_page_asset_id
        WHERE a.client_id = %s
        ORDER BY a.display_order ASC, a.id ASC
    """
    sql_lite = sql_pg.replace("%s", "?")

    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                return _rows(cur, sql_pg, (client_id,))
        else:
            return _rows(conn, sql_lite, (client_id,))


def delete_ad_creative(ad_id: int) -> bool:
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM client_ad_creatives WHERE id = {PLACEHOLDER}", (ad_id,))
                return cur.rowcount > 0
        else:
            result = conn.execute("DELETE FROM client_ad_creatives WHERE id = ?", (ad_id,))
            return result.rowcount > 0
