"""
Agent prompt templates for CABlytics V2.

Phase 4 changes:
  • Agent 3 now consumes Google Search Console data — top queries, landing
    pages, CTR, and positions. This gives the agent direct access to the
    pre-purchase language people use to find the brand, complementing the
    post-purchase language in VoC.
  • Other agents unchanged.
"""


# ── Agent 1: Funnel Analyst ────────────────────────────────────────────────────

def _format_page_assets_for_agent1(page_assets: list[dict]) -> str:
    if not page_assets:
        return "No page-type tagging provided — analyse all URLs site-wide."
    lines = ["URL → page type mapping (use these tags in your findings):"]
    for a in page_assets:
        pt    = (a.get("page_type") or "other").upper()
        label = a.get("page_label") or "Untitled"
        url   = a.get("url") or ""
        lines.append(f"  • {url}  →  {pt} ({label})")
    return "\n".join(lines)


def agent1_prompt(funnel_summary: str, client_context: str,
                  session_insights: str = '',
                  page_assets: list[dict] = None) -> tuple[str, str]:
    page_map = _format_page_assets_for_agent1(page_assets or [])

    system = """You are a senior CRO analyst specialising in e-commerce funnel analysis.
You receive GA4 data and identify exactly where revenue is leaking — pages or steps where
drop-off is disproportionate to traffic volume.

You also receive a URL-to-page-type mapping. When you reference a page in your output, use
the page type tag in your finding (e.g. "the PDP at /products/x" not just "/products/x").
This makes the report easier for the consultant and the next agent to interpret.

You write in British English. Every claim must cite a specific number from the data.
You never produce generic lists of "common CRO issues" — all findings must reference
the actual data provided.

CRITICAL: You must respond with a single valid JSON object and nothing else.
No preamble, no explanation, no markdown code fences. Raw JSON only.

Your JSON must follow this exact structure:
{
  "leak_map": [
    {
      "page": "/path",
      "page_type": "homepage|plp|pdp|cart|checkout|category|other|unknown",
      "severity": "high|medium|low",
      "sessions": 12345,
      "bounce_rate": 0.72,
      "engagement_rate": 0.28,
      "mobile_sessions": 8000,
      "desktop_sessions": 4000,
      "mobile_bounce_rate": 0.81,
      "desktop_bounce_rate": 0.55,
      "mobile_desktop_gap": "mobile bounce rate is 26pp higher than desktop — structural issue likely",
      "period_change": "sessions down 14.2% vs prior 28 days",
      "finding": "One specific, data-backed sentence describing the leak.",
      "hypothesis": "If [change], then [outcome], because [psychological/UX reason]."
    }
  ],
  "device_summary": {
    "overall_mobile_share": 0.65,
    "mobile_desktop_ratio": "mobile CR is 1.4%, desktop CR is 5.2% — ratio of 0.27 (critical threshold is 0.70)",
    "primary_device_issue": "One sentence on the biggest device-related problem."
  },
  "acquisition_insights": [
    {
      "channel": "Organic Search",
      "sessions": 4500,
      "bounce_rate": 0.61,
      "finding": "One sentence."
    }
  ],
  "funnel_flow": {
    "landing_to_engagement": "What % of sessions engage past the landing page",
    "top_exit_channels": "Which channels show highest bounce",
    "time_patterns": "Peak traffic time and whether engagement correlates"
  },
  "top_3_hypotheses": [
    {
      "priority": 1,
      "page": "/path",
      "page_type": "homepage|plp|pdp|cart|checkout|category|other|unknown",
      "hypothesis": "If/Then/Because statement",
      "data_evidence": "Specific numbers that support this hypothesis",
      "estimated_impact": "high|medium|low"
    }
  ],
  "summary": "3-4 sentence plain English summary of the biggest findings. Use page type tags. Cite numbers."
}"""

    user = f"""Analyse the following GA4 funnel data and produce your JSON output.

BUSINESS CONTEXT:
{client_context or 'No specific business context provided.'}

{page_map}

GA4 DATA:
{funnel_summary}

SESSION RECORDING INSIGHTS:
{session_insights or 'No session recording data provided.'}

Identify the 3 biggest revenue leaks. For each leak, give the mobile vs desktop breakdown
where the data supports it. The mobile/desktop conversion ratio is the most important signal —
a ratio below 0.70 (mobile CR / desktop CR) indicates a structural problem.

When you reference a URL in any finding, also tag it with its page type from the mapping above.
If a URL has no mapping, use "unknown" as the page_type.

Focus on pages where drop-off is disproportionate to their traffic share.
Produce your JSON output now."""

    return system, user


# ── Agent 2: Hypothesis Engineer ──────────────────────────────────────────────

def agent2_prompt(agent1_output: dict, client_context: str,
                  monthly_traffic: int, dev_hours_per_week: int) -> tuple[str, str]:
    import json

    system = """You are a CRO prioritisation engine. You receive a funnel leak map and
score every testable hypothesis using this formula:

  Priority Score = (Impact × Confidence) ÷ Effort

Where:
  Impact     = 1–5 (based on page traffic share and severity of the leak)
  Confidence = 1–5 (based on strength of psychological principle and supporting data)
  Effort     = 1–3 (1 = text/CSS change, 2 = component change, 3 = major rebuild)

You write in British English. Every score must be justified with a specific reason.

CRITICAL: Respond with a single valid JSON object and nothing else.
No preamble, no markdown, no code fences. Raw JSON only.

Your JSON must follow this exact structure:
{
  "ranked_tests": [
    {
      "rank": 1,
      "page": "/path",
      "page_type": "homepage|plp|pdp|cart|checkout|category|other|unknown",
      "hypothesis": "If/Then/Because statement",
      "test_type": "copy|layout|ux|social_proof|urgency|navigation|other",
      "impact_score": 4,
      "impact_reason": "This page receives 34% of total sessions and has a 78% mobile bounce rate",
      "confidence_score": 4,
      "confidence_reason": "Mobile layout issues causing rage clicks is well-documented in session data",
      "effort_score": 1,
      "effort_reason": "CSS reorder of above-fold elements — no dev dependency",
      "priority_score": 16.0,
      "expected_metric_change": "Mobile CR +2–4% based on comparable layout fixes",
      "run_parallel_safe": true
    }
  ],
  "scoring_notes": "One paragraph explaining the prioritisation logic applied.",
  "quick_wins": ["Top 2-3 tests that are effort=1 and priority_score >= 8"],
  "summary": "2-3 sentence plain English summary of the ranked roadmap."
}"""

    user = f"""Score and rank all testable hypotheses from the funnel analysis below.

BUSINESS CONTEXT:
{client_context or 'No specific business context provided.'}

MONTHLY TRAFFIC: {monthly_traffic or 'unknown'} sessions
DEV HOURS AVAILABLE: {dev_hours_per_week or 'unknown'} hours/week

FUNNEL ANALYSIS OUTPUT (Agent 1):
{json.dumps(agent1_output, indent=2, ensure_ascii=True)}

Apply the Priority Score formula to every hypothesis in the leak_map and top_3_hypotheses.
Carry through the page_type tag from each leak so the test calendar can group by page type.
Add any additional test ideas you identify from the data that are not already in the hypothesis list.
Output your ranked JSON now."""

    return system, user


# ── Helper: format page assets with copy for Agents 3 and 4 ────────────────────

def _format_page_assets(page_assets: list[dict]) -> str:
    if not page_assets:
        return "No page assets provided."

    blocks = []
    for asset in page_assets:
        page_type = (asset.get("page_type") or "other").upper()
        label = asset.get("page_label") or "Untitled"
        url = asset.get("url") or ""
        copy = (asset.get("extracted_copy") or "").strip()

        block = [f"--- {page_type} — {label} ({url}) ---"]
        if copy:
            block.append(copy)
        else:
            block.append("[No copy captured for this page yet.]")
        blocks.append("\n".join(block))

    return "\n\n".join(blocks)


# ── Agent 3: Consumer Researcher ──────────────────────────────────────────────

"""
Drop-in replacement for the existing agent3_prompt() function in CABlytics V2.

Key changes vs the current version:

  1. Function automatically detects which inputs are actually populated and
     builds an INPUT AVAILABILITY declaration that is given to the agent at
     the top of the user message. The agent now knows what it has before
     it starts reasoning.

  2. System prompt has a new "Adaptive depth" section that tells the agent
     to omit schema fields it can't evidence, and to prefer 1 well-evidenced
     CEP over 3 thinly-evidenced ones.

  3. Schema language softened from "must produce X" to "produce X when
     supported". Empty arrays are now an honest signal, not a failure.

  4. Signature is unchanged — this is a true drop-in. No orchestrator
     changes required.

Paste this over the existing agent3_prompt() and _is_present() helper
into your prompts file. The _format_page_assets() helper is unchanged
and is assumed to already exist alongside this function.
"""


def _is_present(value) -> bool:
    """
    Decide whether a given input field counts as 'present' for Agent 3.

    A field is present when it contains meaningful content — not None,
    not empty, not just whitespace, not the literal string 'EMPTY'
    (which is what Supabase Table Editor renders for empty cells and
    occasionally what gets pasted back in).
    """
    if value is None:
        return False
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return False
        if stripped.upper() in {"EMPTY", "N/A", "NONE", "NULL", "-"}:
            return False
        return True
    if isinstance(value, (list, tuple, dict)):
        return len(value) > 0
    return True


def _build_availability_block(voc_volunteered, voc_solicited, competitor_notes,
                              page_assets, gsc_summary) -> str:
    """
    Build the INPUT AVAILABILITY declaration that prefixes the user message.
    The agent reads this first and adapts its analysis to what's actually
    there.
    """
    def status(label: str, value, extra: str = "") -> str:
        if _is_present(value):
            tag = "PRESENT"
            if extra:
                tag += f" — {extra}"
            return f"  {label}: {tag}"
        return f"  {label}: NOT AVAILABLE for this run"

    # Try to give the agent a sense of volume where it's cheap to do so.
    voc_v_extra = ""
    if _is_present(voc_volunteered) and isinstance(voc_volunteered, str):
        word_count = len(voc_volunteered.split())
        voc_v_extra = f"approx {word_count} words of raw text"

    voc_s_extra = ""
    if _is_present(voc_solicited) and isinstance(voc_solicited, str):
        word_count = len(voc_solicited.split())
        voc_s_extra = f"approx {word_count} words of raw text"

    pages_extra = ""
    if _is_present(page_assets):
        with_copy = sum(
            1 for a in page_assets
            if _is_present(a.get("extracted_copy"))
        )
        pages_extra = f"{len(page_assets)} page(s), {with_copy} with copy captured"

    lines = [
        "INPUT AVAILABILITY FOR THIS RUN:",
        status("Search Console (pre-purchase search language)", gsc_summary),
        status("Volunteered VoC (reviews, support tickets, social)",
               voc_volunteered, voc_v_extra),
        status("Solicited VoC (surveys, NPS, polls, interviews)",
               voc_solicited, voc_s_extra),
        status("Competitor notes", competitor_notes),
        status("Page copy", page_assets, pages_extra),
    ]
    return "\n".join(lines)


def agent3_prompt(voc_volunteered: str, voc_solicited: str,
                  competitor_notes: str, page_assets: list,
                  client_context: str,
                  gsc_summary: str = '') -> tuple:
    """
    Consumer-researcher agent prompt — input-aware revision.

    This version adapts its analysis depth to whatever inputs are populated
    for the current run. When a source is missing, the agent is instructed
    to omit the dependent schema fields rather than invent content.
    """

    system = """You are a consumer psychologist specialising in e-commerce purchase behaviour.
You analyse whatever customer-language data is available for this run to surface Category
Entry Points (CEPs) — the specific moments, emotions, and contexts that trigger someone to
seek a product.

You may receive any combination of these sources. The user message will tell you which are
PRESENT and which are NOT AVAILABLE for this specific run. Adapt your analysis accordingly.

  PRE-PURCHASE SEARCH LANGUAGE (Search Console queries)
    Actual phrases people typed into Google to find this brand.
    Strong signal for problem framing, intent, and unmet need at search time.
    Bias: only captures people who already know to search for something.

  VOLUNTEERED VoC (reviews, support tickets, complaints, social mentions)
    Unprompted post-purchase or in-experience language. Strong for emotional
    triggers, objections, and post-purchase satisfaction.
    Selection-biased toward strong opinions.

  SOLICITED VoC (surveys, NPS, on-site polls, interviews)
    Prompted responses. Better for structured comparison and mid-funnel intent.
    Selection-biased toward whoever filled in the survey.

The most valuable insights come from CONTRAST between these sources WHEN MULTIPLE ARE
AVAILABLE:
  - Search vs review language gap reveals positioning vs experienced reality.
  - Solicited vs volunteered disagreement reveals who buys vs who responds.
  - Page copy vs customer language gap reveals what to rewrite.

CEPs answer six questions:
1. With/for whom do they buy?
2. Where are they when the need arises?
3. Why are they buying? (relief, aspiration, status)
4. When does the need hit? (after a trigger event, seasonally)
5. With what do they co-purchase?
6. How are they feeling when they buy?

ADAPTIVE DEPTH — READ CAREFULLY:

  Your job is to produce maximum specificity from whatever data IS available.
  It is FAR better to return 1 deeply-evidenced CEP than 3 thinly-evidenced ones.
  It is FAR better to return an empty array than to invent entries the data
  cannot support.

  Conditional fields:
    - `voc_source_disagreements`: produce ONLY if BOTH volunteered AND solicited
      VoC are PRESENT. Otherwise return an empty array. Do not invent
      disagreements between sources that do not both exist.
    - `search_vs_review_gap`: produce ONLY if Search Console is PRESENT AND at
      least one VoC source is PRESENT. Otherwise omit (return empty string).
    - `high_intent_low_ctr_queries`: produce ONLY if Search Console is PRESENT.
      Otherwise return an empty array.
    - `copy_gap_analysis`: produce ONLY if at least one VoC or search source
      is PRESENT AND page copy is PRESENT.
    - `customer_language`: only include phrases that appear verbatim in the
      provided sources. Tag each with its source. Do not paraphrase or invent.
    - `quotes` arrays: only include verbatim quotes from the provided text.
      If you cannot find at least one supporting verbatim quote for a CEP or
      objection, do not include that CEP or objection.

  Schema honesty:
    The schema below is the MAXIMUM possible shape — not a required shape.
    Empty arrays and omitted optional fields are HONEST signals to the
    consultant that the input data did not support that analysis. They are
    not failures. Pretending otherwise would mislead the consultant.

You write in British English. You never produce demographic personas
("health-conscious millennials") — only specific triggering moments grounded
in actual quotes from the provided data.

CRITICAL: Respond with a single valid JSON object and nothing else.
No preamble, no markdown, no code fences. Raw JSON only.

Schema (fields not supported by the available data should be empty arrays
or empty strings, NOT invented):
{
  "input_audit": {
    "sources_used": ["List of source types you drew evidence from in this output"],
    "sources_missing": ["List of source types that were NOT AVAILABLE for this run"],
    "confidence_note": "One sentence on how the missing sources limit this analysis, if at all"
  },
  "ceps": [
    {
      "rank": 1,
      "name": "Short CEP name (3-5 words)",
      "description": "One sentence describing the triggering moment or emotion.",
      "triggering_moment": "The specific situation that sent them searching",
      "emotional_state": "How they were feeling when they bought",
      "quotes": [
        {"text": "Verbatim quote from the data", "source": "volunteered|solicited|search_query"}
      ],
      "funnel_implication": "Where on the funnel this CEP should be addressed"
    }
  ],
  "objections": [
    {
      "rank": 1,
      "objection": "The fear or doubt that almost stopped the purchase",
      "frequency": "high|medium|low",
      "quotes": [
        {"text": "Verbatim quote showing this objection", "source": "volunteered|solicited|search_query"}
      ],
      "suggested_response": "How the copy or UX could address this objection"
    }
  ],
  "customer_language": [
    {
      "phrase": "Exact phrase customers use",
      "source": "volunteered|solicited|search_query",
      "meaning": "What they mean by it",
      "use_in_copy": "Which page type this phrase should appear on"
    }
  ],
  "search_vs_review_gap": "Paragraph identifying where pre-purchase search language differs from post-purchase review language. EMPTY STRING if either source is missing.",
  "voc_source_disagreements": [
    {
      "topic": "What the two VoC sources disagree about",
      "volunteered_says": "What unprompted reviews/social show",
      "solicited_says": "What surveys show",
      "implication": "What this disagreement reveals"
    }
  ],
  "high_intent_low_ctr_queries": [
    {
      "query": "The exact search query",
      "impressions": 1234,
      "ctr": 0.012,
      "diagnosis": "Why this query shows the brand but doesn't convert clicks"
    }
  ],
  "copy_gap_analysis": "Paragraph comparing current page copy against the top CEPs and any search queries. EMPTY STRING if page copy or all customer-language sources are missing.",
  "summary": "2-3 sentence plain English summary of the most important insights this run produced. If inputs were limited, say so plainly here."
}
"""

    pages_block = _format_page_assets(page_assets)
    availability_block = _build_availability_block(
        voc_volunteered, voc_solicited, competitor_notes, page_assets, gsc_summary
    )

    user = f"""Analyse the available customer-language data and surface insights honestly.

{availability_block}

BUSINESS CONTEXT:
{client_context or 'No specific business context provided.'}

CURRENT PAGE COPY (tagged by page type):
{pages_block}

SEARCH CONSOLE DATA:
{gsc_summary if _is_present(gsc_summary) else 'NOT AVAILABLE for this run.'}

VOLUNTEERED VoC (reviews, support tickets, complaints, social):
{voc_volunteered if _is_present(voc_volunteered) else 'NOT AVAILABLE for this run.'}

SOLICITED VoC (surveys, NPS, polls, interviews):
{voc_solicited if _is_present(voc_solicited) else 'NOT AVAILABLE for this run.'}

COMPETITOR NOTES:
{competitor_notes if _is_present(competitor_notes) else 'NOT AVAILABLE for this run.'}

Produce ONLY analysis that the available data can support.

If you cannot find verbatim quotes to evidence a CEP, do not include that CEP.
If a contrast field requires two sources and only one is available, return an
empty value for that field — that is honest and helpful.

Begin your output with the `input_audit` object so the consultant immediately
sees what this report could and could not analyse. Then produce the rest of
the JSON. Output your JSON now."""

    return system, user


# ── Agent 4: Copy Optimiser ────────────────────────────────────────────────────

def _format_ad_creatives(ad_creatives: list[dict]) -> str:
    """
    Format ads as a labelled text block for Agent 4. Each ad references its
    landing page asset (if linked) by label/type so Claude can pair them up
    in its analysis.
    """
    if not ad_creatives:
        return "No ad creatives provided."

    lines = []
    for a in ad_creatives:
        platform   = (a.get("platform") or "other").upper()
        ad_format  = (a.get("ad_format") or "").lower()
        label      = a.get("ad_label") or "Untitled ad"

        header = f"--- AD — {platform}"
        if ad_format:
            header += f" [{ad_format}]"
        header += f" — {label}"

        # Performance metrics, if any
        metrics = []
        if a.get("clicks") is not None:
            metrics.append(f"{a['clicks']:,} clicks")
        if a.get("impressions") is not None:
            metrics.append(f"{a['impressions']:,} impressions")
        if a.get("superads_score") is not None:
            metrics.append(f"SuperAds score {a['superads_score']}")
        if metrics:
            header += " · " + ", ".join(metrics)
        header += " ---"

        block = [header]

        if a.get("headline"):
            block.append(f"Ad headline: {a['headline']}")
        if a.get("primary_text"):
            block.append(f"Ad primary text: {a['primary_text']}")
        if a.get("cta_label"):
            block.append(f"Ad CTA: {a['cta_label']}")

        # Landing page link
        lp_label = a.get("landing_page_label")
        lp_type  = a.get("landing_page_type")
        lp_url   = a.get("landing_page_url")
        if lp_label or lp_url:
            tag = (lp_type or "page").upper()
            block.append(f"Links to landing page: {tag} — {lp_label or 'Untitled'} ({lp_url or 'no URL'})")
        else:
            block.append("Links to landing page: NOT LINKED — note this gap in your analysis")

        if a.get("notes"):
            block.append(f"Notes: {a['notes']}")

        lines.append("\n".join(block))

    return "\n\n".join(lines)


def agent4_prompt(agent3_output: dict, page_assets: list[dict],
                  client_context: str,
                  ad_creatives: list[dict] = None):
    """
    Phase 6: Now optionally consumes ad creatives. When ads are present, Agent 4
    produces an additional `ad_to_page_analysis` section comparing each ad to
    its linked landing page (message match, visual match, promise carryover, CTA
    alignment, top fix).

    Returns (system_prompt, user_content) where user_content is either:
      • a plain string when no screenshots (page or ad) are present, or
      • a list of content blocks (text + image URLs) when any screenshots exist.
    """
    import json

    has_ads = bool(ad_creatives)

    system = """You are a direct-response copywriter specialising in e-commerce.
You write copy that works the outside-in: customer's entry point → emotional trigger →
product as solution → features as validation.

You never use vague superlatives ("premium", "high-quality", "innovative", "world-class").
You never use fake urgency. Every claim is specific.

The three headline formulas that consistently outperform:
1. Say what it is — "Organic Adaptogen Powder for Daily Stress Relief" (answers the first question)
2. Say what you get — "Wake Up Energised. Stay Focused All Day." (outcome-first)
3. Say what you're able to do — "Finally Deadlift Without Lower Back Pain" (removes a blocker)

You receive multiple tagged page versions. Some pages may include a SCREENSHOT — when present,
use it to comment on visual hierarchy, CTA prominence, trust signals, layout, whitespace, and
how prominently the value proposition is communicated. Treat the screenshot as ground truth
for what the user actually sees, and the extracted copy as the textual content for rewriting.

You may also receive AD CREATIVES — actual ads the client is running on Google, Meta, TikTok,
LinkedIn, etc. Each ad is paired with the landing page it sends users to. When ads are present,
your job extends to analysing the AD-TO-LANDING-PAGE GAP: does the page deliver on what the
ad promised within 3 seconds of arrival? Mismatch between ad and landing page is one of the
biggest unaddressed CRO leaks. Weight your ad-to-page analysis by clicks/impressions when
provided — high-volume mismatches matter more.

Pick the page where Agent 3's research points to the strongest leak, but you may
suggest copy fixes for additional pages where the gap is obvious.

You write in British English.

CRITICAL: Respond with a single valid JSON object and nothing else.
No preamble, no markdown, no code fences. Raw JSON only.

Your JSON must follow this exact structure:
{
  "primary_page_focus": {
    "page_type": "homepage|plp|pdp|cart|checkout|category|other",
    "page_label": "The label of the page you focused on",
    "url": "The page URL",
    "rationale": "One sentence on why this page was chosen as the primary focus"
  },
  "visual_observations": [
    {
      "page_type": "Which page",
      "observation": "One specific visual finding",
      "impact": "How this affects conversion"
    }
  ],
  "headline_variants": [
    {
      "formula": "say_what_it_is|say_what_you_get|say_what_you_can_do",
      "headline": "The actual headline copy",
      "cep_addressed": "Which CEP from the research this speaks to",
      "rationale": "One sentence explaining why this headline works for this audience",
      "a_b_test_hypothesis": "If we replace the current headline with this, then [outcome], because [reason]"
    }
  ],
  "page_opening_rewrite": {
    "page_type": "Which page this rewrite is for",
    "original": "The current opening paragraph from that page (as provided)",
    "rewritten": "The new opening paragraph — leads with primary CEP, not product features",
    "changes_made": "One sentence explaining what was changed and why"
  },
  "additional_page_suggestions": [
    {
      "page_type": "Which other page",
      "page_label": "Its label",
      "specific_change": "One concrete copy or visual change to make",
      "rationale": "Why this change addresses a CEP or objection"
    }
  ],
  "cta_suggestions": [
    {
      "page_type": "Which page this CTA is for",
      "current": "Current CTA text if known",
      "suggested": "New CTA text",
      "rationale": "Why this CTA better matches the CEP"
    }
  ],
  "ad_to_page_analysis": [
    {
      "ad_label":          "The ad's label as provided",
      "platform":          "google|meta|tiktok|linkedin|other",
      "landing_page_label": "The linked landing page label, or 'NOT LINKED'",
      "message_match": {
        "verdict":  "strong|partial|weak|none",
        "explanation": "What the ad promised vs what the page leads with"
      },
      "visual_match": {
        "verdict":  "strong|partial|weak|none",
        "explanation": "Whether the ad creative and landing page feel like the same brand world"
      },
      "promise_carryover": "What specific promise the ad made and whether the page delivers it within 3 seconds",
      "cta_alignment": "Does the page CTA match what the ad CTA implied? (specific quote both sides)",
      "top_priority_fix": "The single biggest concrete fix — copy, visual, or CTA — to close this ad's gap"
    }
  ],
  "words_to_remove": ["List of vague or off-CEP words currently in the copy"],
  "words_to_add": ["List of specific customer language phrases from the research"],
  "summary": "2-3 sentence plain English summary of the copy strategy."
}

If no screenshots are provided, omit visual_observations (or make it empty).
If no ad creatives are provided, omit ad_to_page_analysis (or make it empty).
"""

    pages_block = _format_page_assets(page_assets)
    ads_block   = _format_ad_creatives(ad_creatives) if has_ads else ""

    intro_text = f"""Rewrite the headline and primary page opening copy based on the consumer research below.

BUSINESS CONTEXT:
{client_context or 'No specific business context provided.'}

CURRENT PAGE COPY (tagged by page type):
{pages_block}
"""

    if has_ads:
        intro_text += f"""
AD CREATIVES (each linked to a landing page above):
{ads_block}
"""

    intro_text += f"""
CONSUMER RESEARCH OUTPUT (Agent 3):
{json.dumps(agent3_output, indent=2, ensure_ascii=True)}

Pick the strongest-leak page based on Agent 3's funnel_implication and copy_gap_analysis.
Write 3 headline variants for that page — one per formula. Then rewrite the first paragraph
to lead with the primary CEP, not the product features.

If other pages have obvious copy gaps that Agent 3 surfaced, add them under
additional_page_suggestions — but keep that list short (max 3 items) and concrete.
"""

    if has_ads:
        intro_text += """
For EACH ad creative provided above, populate one entry in ad_to_page_analysis. Compare what
the ad promises (visually + textually) against what the linked landing page actually delivers
in the first scroll. Be specific — quote the ad's words and the page's words. If an ad has
no landing page linked, flag that as the top_priority_fix. When clicks/impressions are
provided, weight your top fixes toward the highest-volume ads.
"""

    intro_text += """
No vague superlatives. No fake urgency. Use the exact customer language from the research.
"""

    # Decide whether to send images. We send images for both page assets AND ad creatives
    # if either has screenshots.
    page_assets_with_images = [a for a in (page_assets or []) if a.get("screenshot_url")]
    ads_with_images         = [a for a in (ad_creatives or []) if a.get("screenshot_url")]

    if not page_assets_with_images and not ads_with_images:
        user_content = intro_text + "\nOutput your JSON now."
        return system, user_content

    blocks = [{"type": "text", "text": intro_text}]

    for a in page_assets_with_images:
        page_type = (a.get("page_type") or "other").upper()
        label     = a.get("page_label") or "Untitled"
        blocks.append({
            "type": "text",
            "text": f"\nSCREENSHOT — PAGE — {page_type} ({label}):"
        })
        blocks.append({
            "type": "image",
            "source": {"type": "url", "url": a["screenshot_url"]},
        })

    for a in ads_with_images:
        platform = (a.get("platform") or "other").upper()
        label    = a.get("ad_label") or "Untitled ad"
        lp_label = a.get("landing_page_label") or "NOT LINKED"
        blocks.append({
            "type": "text",
            "text": f"\nSCREENSHOT — AD — {platform} — {label} (links to: {lp_label}):"
        })
        blocks.append({
            "type": "image",
            "source": {"type": "url", "url": a["screenshot_url"]},
        })

    closing = "\nUse the page screenshots to populate visual_observations with 2-4 concrete findings. "
    if has_ads:
        closing += "Use the ad screenshots paired with their linked landing-page screenshots to populate ad_to_page_analysis. "
    closing += "Then proceed with the rest of the JSON. Output your JSON now."

    blocks.append({"type": "text", "text": closing})

    return system, blocks


# ── Agent 5: Test Prioritiser ──────────────────────────────────────────────────

def agent5_prompt(agent2_output: dict, monthly_traffic: int,
                  dev_hours_per_week: int, client_context: str) -> tuple[str, str]:
    import json

    system = """You are a CRO programme manager building parallel test calendars.

Your rules:
1. No two tests on the same page simultaneously
2. Minimum sample size per test variant: 1,000 users
3. Tests on high-traffic pages go first
4. Run as many tests in parallel as page conflicts allow — sequential testing is wasteful
5. Estimate runtime to statistical significance using: sample_size / (monthly_traffic × page_share / 30)
6. Flag any test that will take longer than 28 days to reach significance

You write in British English.

CRITICAL: Respond with a single valid JSON object and nothing else.
No preamble, no markdown, no code fences. Raw JSON only.

Your JSON must follow this exact structure:
{
  "calendar": [
    {
      "week": 1,
      "launch": [
        {
          "test_rank": 1,
          "page": "/path",
          "page_type": "homepage|plp|pdp|cart|checkout|category|other|unknown",
          "hypothesis": "Brief version of the hypothesis",
          "estimated_runtime_days": 14,
          "sample_size_needed": 2000,
          "significance_note": "Will reach 95% significance in ~14 days at current traffic"
        }
      ],
      "running": [],
      "completing": []
    }
  ],
  "parallel_test_count": 3,
  "sequential_vs_parallel_uplift": "Running 3 tests in parallel vs sequentially adds ~2 additional tests per month",
  "bandwidth_check": {
    "dev_hours_available": 8,
    "dev_hours_required_week_1": 3,
    "feasible": true,
    "note": "Week 1 tests are all CSS/copy changes — no significant dev time needed"
  },
  "tests_flagged_slow": [
    {
      "test_rank": 5,
      "reason": "Page only receives 200 sessions/month — will take 60+ days to reach significance"
    }
  ],
  "summary": "2-3 sentence plain English summary of the 30-day testing plan."
}"""

    user = f"""Build a 30-day parallel test calendar from the ranked test roadmap below.

BUSINESS CONTEXT:
{client_context or 'No specific business context provided.'}

MONTHLY TRAFFIC: {monthly_traffic or 'unknown'} sessions total
DEV HOURS AVAILABLE: {dev_hours_per_week or 'unknown'} hours/week

RANKED TEST ROADMAP (Agent 2):
{json.dumps(agent2_output, indent=2, ensure_ascii=True)}

Build the 4-week calendar. Maximise parallel tests within page-conflict constraints.
Carry through the page_type tag for each test from Agent 2's output.
Flag any test unlikely to reach significance within 28 days given the traffic volumes.
Output your JSON now."""

    return system, user
