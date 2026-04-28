"""
CABlytics V2 — Five-agent pipeline orchestrator.

Execution order:
  Agents 1 + 3 run in parallel (independent data sources)
  Agent 2 runs after Agent 1
  Agent 4 runs after Agent 3
  Agent 5 runs after Agent 2

Phase 3 changes:
  • _call_claude now accepts either a string user prompt (most agents) or a
    list of content blocks (Agent 4 when screenshots are present).
  • Agent 4 enriches its page assets with public screenshot URLs before
    handing them to the prompt builder, so Claude can vision-analyse the pages.
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
    list_page_assets,
)
from v2.ga4_queries_v2 import collect_funnel_data, build_funnel_summary
from v2.prompts_v2 import (
    agent1_prompt,
    agent2_prompt,
    agent3_prompt,
    agent4_prompt,
    agent5_prompt,
)
from v2.storage import public_url_for_path


# ── Claude API helpers ─────────────────────────────────────────────────────────

def _get_claude_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY environment variable is not set.")
    return anthropic.Anthropic(api_key=api_key)


def _call_claude(system: str, user, agent_num: int) -> dict:
    """
    Make a single Claude API call and return parsed JSON.

    `user` can be either:
      • a string — sent as a plain text user message (most agents), or
      • a list of content blocks — used by Agent 4 when screenshots are
        present, to enable vision analysis.

    Applies JSON sanitisation (ensure_ascii + regex) on the response.
    Raises ValueError if the response cannot be parsed as JSON.
    """
    client = _get_claude_client()

    # Build the message content depending on whether user is text or blocks
    if isinstance(user, list):
        message_content = user
        image_count = sum(1 for b in user if isinstance(b, dict) and b.get("type") == "image")
        print(f"[V2][Agent{agent_num}] Calling Claude API with {image_count} image(s)...", flush=True)
    else:
        message_content = user
        print(f"[V2][Agent{agent_num}] Calling Claude API...", flush=True)

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        system=system,
        messages=[{"role": "user", "content": message_content}]
    )

    raw = message.content[0].text

    raw = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', raw)
    raw = raw.strip()

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


def _enrich_page_assets_with_urls(page_assets: list[dict]) -> list[dict]:
    """Attach public screenshot URL to each asset that has a screenshot_path."""
    enriched = []
    for a in page_assets:
        copy = dict(a)
        if a.get("screenshot_path"):
            copy["screenshot_url"] = public_url_for_path(a["screenshot_path"])
        enriched.append(copy)
    return enriched


# ── Individual agent runners ───────────────────────────────────────────────────

def run_agent1(client_data: dict, report_id: int) -> dict:
    """Funnel Analyst — pulls GA4 data and identifies revenue leaks."""
    client_id   = client_data["id"]
    property_id = client_data["ga4_property_id"]
    context     = client_data.get("client_context", "")
    session_insights = client_data.get("session_insights", "")

    page_assets = list_page_assets(client_id)
    urls = [a["url"] for a in page_assets if a.get("url")]

    log_event(client_id, "agent_started", report_id=report_id, agent_number=1,
              message=f"Collecting GA4 data ({len(urls)} URLs)")

    ga4_result     = collect_funnel_data(property_id, urls)
    funnel_summary = build_funnel_summary(ga4_result)

    log_event(client_id, "agent_started", report_id=report_id, agent_number=1, message="Calling Claude API")

    system, user = agent1_prompt(funnel_summary, context, session_insights, page_assets)
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
    """Consumer Researcher — mines VoC and surfaces CEPs."""
    client_id        = client_data["id"]
    voc_volunteered  = client_data.get("voc_volunteered", "")
    voc_solicited    = client_data.get("voc_solicited", "")
    competitor_notes = client_data.get("competitor_notes", "")
    context          = client_data.get("client_context", "")

    page_assets = list_page_assets(client_id)

    log_event(client_id, "agent_started", report_id=report_id, agent_number=3,
              message=f"Analysing VoC ({len(page_assets)} page assets)")

    system, user = agent3_prompt(voc_volunteered, voc_solicited,
                                 competitor_notes, page_assets, context)
    output = _call_claude(system, user, agent_num=3)

    update_report_agent(report_id, 3, output)
    log_event(client_id, "agent_complete", report_id=report_id, agent_number=3,
              message=f"CEPs identified: {len(output.get('ceps', []))}")

    return output


def run_agent4(agent3_output: dict, client_data: dict, report_id: int) -> dict:
    """Copy Optimiser — rewrites headlines and page copy based on CEPs and screenshots."""
    client_id = client_data["id"]
    context   = client_data.get("client_context", "")

    page_assets = _enrich_page_assets_with_urls(list_page_assets(client_id))
    screenshot_count = sum(1 for a in page_assets if a.get("screenshot_url"))

    log_event(client_id, "agent_started", report_id=report_id, agent_number=4,
              message=f"Generating copy variants ({len(page_assets)} pages, {screenshot_count} screenshots)")

    system, user = agent4_prompt(agent3_output, page_assets, context)
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
    client_data = get_client_by_slug(client_slug)
    if not client_data:
        print(f"[V2][Pipeline] Client not found: {client_slug}", flush=True)
        return

    client_id = client_data["id"]
    report    = create_report(client_id, triggered_by=triggered_by)
    report_id = report["id"]

    log_event(client_id, "pipeline_started", report_id=report_id,
              message=f"Triggered by: {triggered_by}")

    from v2.db import get_connection, DATABASE_URL
    with get_connection() as conn:
        if DATABASE_URL:
            with conn.cursor() as cur:
                cur.execute("UPDATE reports SET status = 'running' WHERE id = %s", (report_id,))
        else:
            conn.execute("UPDATE reports SET status = 'running' WHERE id = ?", (report_id,))

    print(f"[V2][Pipeline] Starting | client={client_slug} | report_id={report_id}", flush=True)

    try:
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

        agent2_result = run_agent2(agent1_result, client_data, report_id)
        agent4_result = run_agent4(agent3_result, client_data, report_id)
        run_agent5(agent2_result, client_data, report_id)

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
