"""
CABlytics V2 — Five-agent pipeline orchestrator.

Execution order:
  Agents 1 + 3 run in parallel (independent data sources)
  Agent 2 runs after Agent 1
  Agent 4 runs after Agent 3
  Agent 5 runs after Agent 2

Each agent's output is stored to the database as it completes, so the
dashboard can show partial results while the pipeline is still running.
"""

import os
import json
import re
import threading
import anthropic

from v2.db import (
    create_report,
    update_report_agent,
    complete_report,
    fail_report,
    log_event,
    get_client_by_slug,
)
from v2.ga4_queries_v2 import collect_funnel_data, build_funnel_summary
from v2.prompts_v2 import (
    agent1_prompt,
    agent2_prompt,
    agent3_prompt,
    agent4_prompt,
    agent5_prompt,
)


# ── Claude API helpers ─────────────────────────────────────────────────────────

def _get_claude_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY environment variable is not set.")
    return anthropic.Anthropic(api_key=api_key)


def _call_claude(system: str, user: str, agent_num: int) -> dict:
    """
    Make a single Claude API call and return parsed JSON.
    Applies the same JSON sanitisation as V1 (ensure_ascii + regex).
    Raises ValueError if the response cannot be parsed as JSON.
    """
    client = _get_claude_client()

    print(f"[V2][Agent{agent_num}] Calling Claude API...", flush=True)

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        system=system,
        messages=[{"role": "user", "content": user}]
    )

    raw = message.content[0].text

    # Sanitise — same approach as V1
    raw = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', raw)
    raw = raw.strip()

    # Strip markdown code fences if Claude added them despite instructions
    if raw.startswith("```"):
        raw = re.sub(r'^```[a-z]*\n?', '', raw)
        raw = re.sub(r'\n?```$', '', raw)
        raw = raw.strip()

    try:
        parsed = json.loads(raw)
        print(f"[V2][Agent{agent_num}] Claude response parsed OK.", flush=True)
        return parsed
    except json.JSONDecodeError as e:
        print(f"[V2][Agent{agent_num}] JSON parse failed: {e}\nRaw: {raw[:300]}", flush=True)
        raise ValueError(f"Agent {agent_num} returned non-JSON response: {e}")


# ── Individual agent runners ───────────────────────────────────────────────────

def run_agent1(client_data: dict, report_id: int) -> dict:
    """Funnel Analyst — pulls GA4 data and identifies revenue leaks."""
    client_id   = client_data["id"]
    property_id = client_data["ga4_property_id"]
    urls_raw    = client_data.get("target_urls", "")
    context     = client_data.get("client_context", "")

    urls = [u.strip() for u in urls_raw.split("\n") if u.strip()] if urls_raw else []

    log_event(client_id, "agent_started", report_id=report_id, agent_number=1, message="Collecting GA4 data")

    ga4_result     = collect_funnel_data(property_id, urls)
    funnel_summary = build_funnel_summary(ga4_result)

    log_event(client_id, "agent_started", report_id=report_id, agent_number=1, message="Calling Claude API")

    system, user = agent1_prompt(funnel_summary, context)
    output = _call_claude(system, user, agent_num=1)

    update_report_agent(report_id, 1, output)
    log_event(client_id, "agent_complete", report_id=report_id, agent_number=1,
              message=f"Leak map: {len(output.get('leak_map', []))} leaks identified")

    return output


def run_agent2(agent1_output: dict, client_data: dict, report_id: int) -> dict:
    """Hypothesis Engineer — scores and ranks test ideas."""
    client_id = client_data["id"]
    context   = client_data.get("client_context", "")
    traffic   = client_data.get("monthly_traffic")
    dev_hours = client_data.get("dev_hours_per_week")

    log_event(client_id, "agent_started", report_id=report_id, agent_number=2, message="Scoring hypotheses")

    system, user = agent2_prompt(agent1_output, context, traffic, dev_hours)
    output = _call_claude(system, user, agent_num=2)

    update_report_agent(report_id, 2, output)
    log_event(client_id, "agent_complete", report_id=report_id, agent_number=2,
              message=f"Ranked {len(output.get('ranked_tests', []))} tests")

    return output


def run_agent3(client_data: dict, report_id: int) -> dict:
    """Consumer Researcher — mines reviews and surfaces CEPs."""
    client_id = client_data["id"]
    reviews   = client_data.get("customer_reviews", "")
    competitor = client_data.get("competitor_notes", "")
    pdp_copy  = client_data.get("current_pdp_copy", "")
    context   = client_data.get("client_context", "")

    log_event(client_id, "agent_started", report_id=report_id, agent_number=3, message="Analysing reviews")

    system, user = agent3_prompt(reviews, competitor, pdp_copy, context)
    output = _call_claude(system, user, agent_num=3)

    update_report_agent(report_id, 3, output)
    log_event(client_id, "agent_complete", report_id=report_id, agent_number=3,
              message=f"CEPs identified: {len(output.get('ceps', []))}")

    return output


def run_agent4(agent3_output: dict, client_data: dict, report_id: int) -> dict:
    """Copy Optimiser — rewrites headlines and PDP copy based on CEPs."""
    client_id = client_data["id"]
    pdp_copy  = client_data.get("current_pdp_copy", "")
    context   = client_data.get("client_context", "")

    log_event(client_id, "agent_started", report_id=report_id, agent_number=4, message="Generating copy variants")

    system, user = agent4_prompt(agent3_output, pdp_copy, context)
    output = _call_claude(system, user, agent_num=4)

    update_report_agent(report_id, 4, output)
    log_event(client_id, "agent_complete", report_id=report_id, agent_number=4,
              message=f"Headline variants: {len(output.get('headline_variants', []))}")

    return output


def run_agent5(agent2_output: dict, client_data: dict, report_id: int) -> dict:
    """Test Prioritiser — builds the 30-day parallel test calendar."""
    client_id = client_data["id"]
    context   = client_data.get("client_context", "")
    traffic   = client_data.get("monthly_traffic")
    dev_hours = client_data.get("dev_hours_per_week")

    log_event(client_id, "agent_started", report_id=report_id, agent_number=5, message="Building test calendar")

    system, user = agent5_prompt(agent2_output, traffic, dev_hours, context)
    output = _call_claude(system, user, agent_num=5)

    update_report_agent(report_id, 5, output)
    log_event(client_id, "agent_complete", report_id=report_id, agent_number=5,
              message=f"Calendar built: {len(output.get('calendar', []))} weeks")

    return output


# ── Main pipeline orchestrator ─────────────────────────────────────────────────

def run_pipeline(client_slug: str, triggered_by: str = "manual"):
    """
    Entry point for a full V2 pipeline run.

    Execution order:
      1. Create report row (status = running)
      2. Agents 1 + 3 in parallel threads
      3. Agent 2 (after Agent 1 completes)
      4. Agent 4 (after Agent 3 completes)
      5. Agent 5 (after Agent 2 completes)
      6. Mark report complete

    Called by:
      - routes.py POST /v2/report/run  (manual trigger)
      - scheduler.py  (scheduled trigger)

    Errors in any agent are caught, logged, and the report is marked failed.
    """
    client_data = get_client_by_slug(client_slug)
    if not client_data:
        print(f"[V2][Pipeline] Client not found: {client_slug}", flush=True)
        return

    client_id = client_data["id"]
    report    = create_report(client_id, triggered_by=triggered_by)
    report_id = report["id"]

    log_event(client_id, "pipeline_started", report_id=report_id,
              message=f"Triggered by: {triggered_by}")

# Update report status to running
    from v2.db import get_connection, DATABASE_URL
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute("UPDATE reports SET status = 'running' WHERE id = %s", (report_id,))
        else:
            conn.execute("UPDATE reports SET status = 'running' WHERE id = ?", (report_id,))

    print(f"[V2][Pipeline] Starting | client={client_slug} | report_id={report_id}", flush=True)

    try:
        # ── Agents 1 and 3 in parallel ─────────────────────────────────────
        agent1_result = {}
        agent3_result = {}
        agent1_error  = []
        agent3_error  = []

        def _run1():
            try:
                agent1_result.update(run_agent1(client_data, report_id))
            except Exception as e:
                agent1_error.append(str(e))
                print(f"[V2][Agent1] ERROR: {e}", flush=True)

        def _run3():
            try:
                agent3_result.update(run_agent3(client_data, report_id))
            except Exception as e:
                agent3_error.append(str(e))
                print(f"[V2][Agent3] ERROR: {e}", flush=True)

        t1 = threading.Thread(target=_run1, daemon=True)
        t3 = threading.Thread(target=_run3, daemon=True)
        t1.start()
        t3.start()
        t1.join()
        t3.join()

        if agent1_error:
            raise RuntimeError(f"Agent 1 failed: {agent1_error[0]}")
        if agent3_error:
            raise RuntimeError(f"Agent 3 failed: {agent3_error[0]}")

        # ── Agents 2 and 4 — sequential after their dependencies ───────────
        agent2_result = run_agent2(agent1_result, client_data, report_id)
        agent4_result = run_agent4(agent3_result, client_data, report_id)

        # ── Agent 5 — after Agent 2 ─────────────────────────────────────────
        run_agent5(agent2_result, client_data, report_id)

        # ── Mark complete ───────────────────────────────────────────────────
        complete_report(report_id)
        log_event(client_id, "pipeline_complete", report_id=report_id,
                  message="All 5 agents completed successfully")

        print(f"[V2][Pipeline] Complete | client={client_slug} | report_id={report_id}", flush=True)

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[V2][Pipeline] FAILED | client={client_slug} | {e}\n{tb}", flush=True)
        fail_report(report_id, error_message=str(e))
        log_event(client_id, "pipeline_failed", report_id=report_id, message=str(e))
