"""
CABlytics V2 — APScheduler setup.

Schedules one cron job per client based on their report_frequency and schedule_day.
Jobs persist across Render restarts via SQLAlchemy job store (v2_jobs.db).

Called from:
  - routes.py  → register_client_job() when a new client is created
  - app.py     → start_scheduler() on Flask startup
"""

import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

_scheduler = None


def get_scheduler() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is None:
        jobstore_url = "sqlite:///v2_jobs.db"
        _scheduler = BackgroundScheduler(
            jobstores={"default": SQLAlchemyJobStore(url=jobstore_url)},
            executors={"default": ThreadPoolExecutor(max_workers=3)},
            job_defaults={"coalesce": True, "max_instances": 1},
        )
    return _scheduler


def start_scheduler():
    """Start the scheduler. Called once from app.py on Flask startup."""
    scheduler = get_scheduler()
    if not scheduler.running:
        scheduler.start()
        print("[V2][Scheduler] Started.", flush=True)


def register_client_job(client: dict):
    """
    Register or replace the scheduled report job for a client.
    Safe to call multiple times — replaces the existing job if one exists.

    client dict keys used:
      client_slug, report_frequency ("weekly"|"monthly"), schedule_day
    """
    from v2.pipeline import run_pipeline

    scheduler  = get_scheduler()
    slug       = client["client_slug"]
    frequency  = client.get("report_frequency", "monthly")
    day        = client.get("schedule_day", "1")
    job_id     = f"report_{slug}"

    # Remove existing job for this client if present
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    if frequency == "weekly":
        # schedule_day is a weekday abbreviation: mon, tue, wed, thu, fri
        scheduler.add_job(
            run_pipeline,
            trigger="cron",
            day_of_week=str(day),
            hour=6,
            minute=0,
            args=[slug, "scheduled"],
            id=job_id,
            replace_existing=True,
        )
        print(f"[V2][Scheduler] Registered weekly job for {slug} on {day} at 06:00 UTC", flush=True)
    else:
        # schedule_day is a day-of-month integer: 1, 8, 15, 22
        scheduler.add_job(
            run_pipeline,
            trigger="cron",
            day=int(day),
            hour=6,
            minute=0,
            args=[slug, "scheduled"],
            id=job_id,
            replace_existing=True,
        )
        print(f"[V2][Scheduler] Registered monthly job for {slug} on day {day} at 06:00 UTC", flush=True)


def deregister_client_job(client_slug: str):
    """Remove a client's scheduled job. Call when deleting a client."""
    scheduler = get_scheduler()
    job_id    = f"report_{client_slug}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        print(f"[V2][Scheduler] Removed job for {client_slug}", flush=True)


def list_jobs() -> list[dict]:
    """Return a summary of all scheduled jobs. Useful for admin debugging."""
    scheduler = get_scheduler()
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id":       job.id,
            "next_run": str(job.next_run_time) if job.next_run_time else "paused",
            "trigger":  str(job.trigger),
        })
    return jobs
