"""
CABlytics V2 — database layer.

Uses PostgreSQL (Supabase) when DATABASE_URL is set, falls back to SQLite
for local development.

All public functions have identical signatures to the original SQLite version
so no other files need changing.
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


def _p(n=1):
    """Return n placeholders as a tuple-friendly string."""
    if PLACEHOLDER == "%s":
        return ", ".join(["%s"] * n)
    return ", ".join(["?"] * n)


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
                    id                  SERIAL PRIMARY KEY,
                    client_name         TEXT NOT NULL,
                    client_slug         TEXT NOT NULL UNIQUE,
                    ga4_property_id     TEXT NOT NULL,
                    client_context      TEXT,
                    target_urls         TEXT,
                    customer_reviews    TEXT,
                    competitor_notes    TEXT,
                    current_pdp_copy    TEXT,
                    monthly_traffic     INTEGER,
                    dev_hours_per_week  INTEGER,
                    report_frequency    TEXT DEFAULT 'monthly',
                    schedule_day        TEXT,
                    created_at          TEXT NOT NULL,
                    updated_at          TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS reports (
                    id                  SERIAL PRIMARY KEY,
                    client_id           INTEGER NOT NULL REFERENCES clients(id),
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
                    id                  SERIAL PRIMARY KEY,
                    client_id           INTEGER NOT NULL,
                    report_id           INTEGER,
                    event               TEXT NOT NULL,
                    agent_number        INTEGER,
                    message             TEXT,
                    timestamp           TEXT NOT NULL
                );
            """)


def _init_sqlite():
    with get_connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS clients (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                client_name         TEXT NOT NULL,
                client_slug         TEXT NOT NULL UNIQUE,
                ga4_property_id     TEXT NOT NULL,
                client_context      TEXT,
                target_urls         TEXT,
                customer_reviews    TEXT,
                competitor_notes    TEXT,
                current_pdp_copy    TEXT,
                monthly_traffic     INTEGER,
                dev_hours_per_week  INTEGER,
                report_frequency    TEXT DEFAULT 'monthly',
                schedule_day        TEXT,
                created_at          TEXT NOT NULL,
                updated_at          TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reports (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id           INTEGER NOT NULL REFERENCES clients(id),
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
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id           INTEGER NOT NULL,
                report_id           INTEGER,
                event               TEXT NOT NULL,
                agent_number        INTEGER,
                message             TEXT,
                timestamp           TEXT NOT NULL
            );
        """)


def now_utc():
    return datetime.now(timezone.utc).isoformat()


# ── Clients ────────────────────────────────────────────────────────────────────

def create_client(data: dict) -> dict:
    ts = now_utc()
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO clients (
                        client_name, client_slug, ga4_property_id,
                        client_context, target_urls, customer_reviews,
                        competitor_notes, current_pdp_copy,
                        monthly_traffic, dev_hours_per_week,
                        report_frequency, schedule_day,
                        created_at, updated_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING *
                """, (
                    data["client_name"], data["client_slug"], data["ga4_property_id"],
                    data.get("client_context"), data.get("target_urls"),
                    data.get("customer_reviews"), data.get("competitor_notes"),
                    data.get("current_pdp_copy"), data.get("monthly_traffic"),
                    data.get("dev_hours_per_week"), data.get("report_frequency", "monthly"),
                    data.get("schedule_day"), ts, ts,
                ))
                row = cur.fetchone()
                return dict(row) if row else None
        else:
            cur = conn.execute("""
                INSERT INTO clients (
                    client_name, client_slug, ga4_property_id,
                    client_context, target_urls, customer_reviews,
                    competitor_notes, current_pdp_copy,
                    monthly_traffic, dev_hours_per_week,
                    report_frequency, schedule_day,
                    created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                data["client_name"], data["client_slug"], data["ga4_property_id"],
                data.get("client_context"), data.get("target_urls"),
                data.get("customer_reviews"), data.get("competitor_notes"),
                data.get("current_pdp_copy"), data.get("monthly_traffic"),
                data.get("dev_hours_per_week"), data.get("report_frequency", "monthly"),
                data.get("schedule_day"), ts, ts,
            ))
            row = conn.execute("SELECT * FROM clients WHERE id = ?", (cur.lastrowid,)).fetchone()
            return dict(row) if row else None


def update_client(client_slug: str, data: dict) -> dict | None:
    allowed = {
        "client_name", "ga4_property_id", "client_context", "target_urls",
        "customer_reviews", "competitor_notes", "current_pdp_copy",
        "monthly_traffic", "dev_hours_per_week", "report_frequency", "schedule_day"
    }
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return get_client_by_slug(client_slug)

    updates["updated_at"] = now_utc()
    ph = PLACEHOLDER
    set_clause = ", ".join(f"{k} = {ph}" for k in updates)
    values = list(updates.values()) + [client_slug]

    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE clients SET {set_clause} WHERE client_slug = {ph}", values)
        else:
            conn.execute(f"UPDATE clients SET {set_clause} WHERE client_slug = ?", values)

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
