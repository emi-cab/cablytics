import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "v2.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables if they don't exist."""
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
                completed_at        TEXT,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            );

            CREATE TABLE IF NOT EXISTS run_log (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id           INTEGER NOT NULL,
                report_id           INTEGER,
                event               TEXT NOT NULL,
                agent_number        INTEGER,
                message             TEXT,
                timestamp           TEXT NOT NULL,
                FOREIGN KEY (client_id) REFERENCES clients(id),
                FOREIGN KEY (report_id) REFERENCES reports(id)
            );
        """)


def now_utc():
    return datetime.now(timezone.utc).isoformat()


# ── Clients ────────────────────────────────────────────────────────────────────

def create_client(data: dict) -> dict:
    """
    Insert a new client row. Returns the created client as a dict.
    Required keys: client_name, client_slug, ga4_property_id
    """
    ts = now_utc()
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO clients (
                client_name, client_slug, ga4_property_id,
                client_context, target_urls, customer_reviews,
                competitor_notes, current_pdp_copy,
                monthly_traffic, dev_hours_per_week,
                report_frequency, schedule_day,
                created_at, updated_at
            ) VALUES (
                :client_name, :client_slug, :ga4_property_id,
                :client_context, :target_urls, :customer_reviews,
                :competitor_notes, :current_pdp_copy,
                :monthly_traffic, :dev_hours_per_week,
                :report_frequency, :schedule_day,
                :created_at, :updated_at
            )
            """,
            {
                "client_name": data["client_name"],
                "client_slug": data["client_slug"],
                "ga4_property_id": data["ga4_property_id"],
                "client_context": data.get("client_context"),
                "target_urls": data.get("target_urls"),
                "customer_reviews": data.get("customer_reviews"),
                "competitor_notes": data.get("competitor_notes"),
                "current_pdp_copy": data.get("current_pdp_copy"),
                "monthly_traffic": data.get("monthly_traffic"),
                "dev_hours_per_week": data.get("dev_hours_per_week"),
                "report_frequency": data.get("report_frequency", "monthly"),
                "schedule_day": data.get("schedule_day"),
                "created_at": ts,
                "updated_at": ts,
            }
        )
        return get_client_by_id(cursor.lastrowid)


def update_client(client_slug: str, data: dict) -> dict | None:
    """
    Update mutable fields on an existing client. Returns the updated client.
    Only updates keys present in data.
    """
    allowed = {
        "client_name", "ga4_property_id", "client_context", "target_urls",
        "customer_reviews", "competitor_notes", "current_pdp_copy",
        "monthly_traffic", "dev_hours_per_week", "report_frequency", "schedule_day"
    }
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return get_client_by_slug(client_slug)

    updates["updated_at"] = now_utc()
    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["slug"] = client_slug

    with get_connection() as conn:
        conn.execute(
            f"UPDATE clients SET {set_clause} WHERE client_slug = :slug",
            updates
        )
    return get_client_by_slug(client_slug)


def get_client_by_slug(client_slug: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM clients WHERE client_slug = ?", (client_slug,)
        ).fetchone()
        return dict(row) if row else None


def get_client_by_id(client_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM clients WHERE id = ?", (client_id,)
        ).fetchone()
        return dict(row) if row else None


def list_clients() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM clients ORDER BY client_name ASC"
        ).fetchall()
        return [dict(r) for r in rows]


def delete_client(client_slug: str) -> bool:
    with get_connection() as conn:
        result = conn.execute(
            "DELETE FROM clients WHERE client_slug = ?", (client_slug,)
        )
        return result.rowcount > 0


# ── Reports ────────────────────────────────────────────────────────────────────

def create_report(client_id: int, triggered_by: str = "manual") -> dict:
    """Create a new report row with status=pending. Returns the report."""
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO reports (client_id, run_triggered_by, status, started_at)
            VALUES (?, ?, 'pending', ?)
            """,
            (client_id, triggered_by, now_utc())
        )
        return get_report_by_id(cursor.lastrowid)


def update_report_agent(report_id: int, agent_number: int, output: dict):
    """Store a single agent's JSON output and mark it in the log."""
    col = f"agent{agent_number}_output"
    with get_connection() as conn:
        conn.execute(
            f"UPDATE reports SET {col} = ? WHERE id = ?",
            (json.dumps(output, ensure_ascii=True), report_id)
        )


def complete_report(report_id: int):
    """Assemble all agent outputs into full_report_json and mark complete."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM reports WHERE id = ?", (report_id,)
        ).fetchone()
        if not row:
            return

        full = {
            "completed_at": now_utc(),
            "agents": {}
        }
        for n in range(1, 6):
            raw = row[f"agent{n}_output"]
            if raw:
                try:
                    full["agents"][str(n)] = json.loads(raw)
                except json.JSONDecodeError:
                    full["agents"][str(n)] = {"raw": raw}

        conn.execute(
            """
            UPDATE reports
            SET status = 'complete',
                full_report_json = ?,
                completed_at = ?
            WHERE id = ?
            """,
            (json.dumps(full, ensure_ascii=True), full["completed_at"], report_id)
        )


def fail_report(report_id: int, error_message: str):
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE reports
            SET status = 'failed', error_message = ?, completed_at = ?
            WHERE id = ?
            """,
            (error_message, now_utc(), report_id)
        )


def get_report_by_id(report_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM reports WHERE id = ?", (report_id,)
        ).fetchone()
        return dict(row) if row else None


def get_latest_report(client_slug: str) -> dict | None:
    """Return the most recent completed report for a client."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT r.*
            FROM reports r
            JOIN clients c ON c.id = r.client_id
            WHERE c.client_slug = ?
              AND r.status = 'complete'
            ORDER BY r.completed_at DESC
            LIMIT 1
            """,
            (client_slug,)
        ).fetchone()
        return dict(row) if row else None


def get_active_report(client_id: int) -> dict | None:
    """Return any in-progress (pending/running) report for a client."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT * FROM reports
            WHERE client_id = ?
              AND status IN ('pending', 'running')
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (client_id,)
        ).fetchone()
        return dict(row) if row else None


def list_reports(client_slug: str, limit: int = 10) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT r.*
            FROM reports r
            JOIN clients c ON c.id = r.client_id
            WHERE c.client_slug = ?
            ORDER BY r.started_at DESC
            LIMIT ?
            """,
            (client_slug, limit)
        ).fetchall()
        return [dict(r) for r in rows]


# ── Run log ────────────────────────────────────────────────────────────────────

def log_event(client_id: int, event: str, report_id: int = None,
              agent_number: int = None, message: str = None):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO run_log (client_id, report_id, event, agent_number, message, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (client_id, report_id, event, agent_number, message, now_utc())
        )


def get_run_log(client_id: int, report_id: int = None, limit: int = 50) -> list[dict]:
    with get_connection() as conn:
        if report_id:
            rows = conn.execute(
                """
                SELECT * FROM run_log
                WHERE client_id = ? AND report_id = ?
                ORDER BY timestamp ASC
                LIMIT ?
                """,
                (client_id, report_id, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM run_log
                WHERE client_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (client_id, limit)
            ).fetchall()
        return [dict(r) for r in rows]
