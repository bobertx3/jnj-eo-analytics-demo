"""
Root Cause Intelligence API routes.
Includes AI-powered analysis via Foundation Model API.
"""
import os
import json
import logging
import aiohttp
from fastapi import APIRouter, Query
from typing import Optional
from backend.db import execute_query, get_workspace_host, get_oauth_token, CATALOG, SCHEMA

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/root-cause", tags=["root-cause"])

# LLM model for analysis
MODEL_NAME = os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4")


@router.get("/patterns")
async def get_root_cause_patterns():
    """Get all root cause patterns ranked by priority."""
    rows = execute_query(f"""
    SELECT
      failure_pattern_id,
      failure_pattern_name,
      root_service,
      domain,
      occurrence_count,
      avg_mttr_minutes,
      p50_mttr_minutes,
      p95_mttr_minutes,
      avg_blast_radius,
      max_blast_radius,
      total_revenue_impact,
      avg_revenue_impact,
      total_affected_users as total_user_impact,
      avg_affected_users as avg_user_impact,
      p1_count,
      p2_count,
      p3_count,
      sla_breach_count,
      first_occurrence,
      last_occurrence,
      trend_direction,
      recent_weekly_avg,
      previous_weekly_avg,
      priority_score,
      avg_days_between_occurrences,
      root_cause_explanation
    FROM {CATALOG}.{SCHEMA}.gold_root_cause_patterns
    ORDER BY priority_score DESC
    """)
    return rows


@router.get("/top-systemic-issue")
async def get_top_systemic_issue():
    """Get the single most impactful systemic issue (the 'fix one thing' answer)."""
    rows = execute_query(f"""
    SELECT *
    FROM {CATALOG}.{SCHEMA}.gold_root_cause_patterns
    ORDER BY priority_score DESC
    LIMIT 1
    """)
    return rows[0] if rows else {}


@router.get("/pattern/{pattern_id}/timeline")
async def get_pattern_timeline(pattern_id: str):
    """Get occurrence timeline for a specific failure pattern."""
    rows = execute_query(f"""
    SELECT
      incident_id,
      title,
      severity,
      created_at,
      resolved_at,
      mttr_minutes,
      root_service,
      blast_radius,
      revenue_impact_usd,
      affected_user_count,
      sla_breached
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    WHERE failure_pattern_id = '{pattern_id}'
    ORDER BY created_at DESC
    """)
    return rows


@router.get("/pattern/{pattern_id}/correlated-signals")
async def get_pattern_signals(pattern_id: str):
    """Get correlated alerts and changes for a failure pattern."""
    # Get incidents for this pattern
    incidents = execute_query(f"""
    SELECT incident_id, created_at, root_service
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    WHERE failure_pattern_id = '{pattern_id}'
    """)

    if not incidents:
        return {"alerts": [], "changes": []}

    incident_ids = [f"'{i['incident_id']}'" for i in incidents]
    incident_ids_sql = ",".join(incident_ids)

    alerts = execute_query(f"""
    SELECT
      alert_id, incident_id, service, alert_name, severity,
      fired_at, resolved_at, threshold_value, actual_value,
      is_pre_incident_signal, breach_magnitude_pct
    FROM {CATALOG}.{SCHEMA}.silver_alerts
    WHERE incident_id IN ({incident_ids_sql})
    ORDER BY fired_at DESC
    LIMIT 50
    """)

    return {"alerts": alerts, "incidents": incidents}


@router.post("/ai-analysis")
async def generate_ai_analysis(
    pattern_id: Optional[str] = Query(default=None),
):
    """Generate AI-powered root cause analysis using Foundation Model API."""
    # Gather data for analysis
    if pattern_id:
        patterns = execute_query(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.gold_root_cause_patterns
        WHERE failure_pattern_id = '{pattern_id}'
        """)
    else:
        patterns = execute_query(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.gold_root_cause_patterns
        ORDER BY priority_score DESC LIMIT 5
        """)

    service_ranking = execute_query(f"""
    SELECT service_name, risk_score, incident_count_as_root, total_revenue_impact,
           total_affected_users as total_user_impact, avg_mttr_minutes, unique_failure_patterns
    FROM {CATALOG}.{SCHEMA}.gold_service_risk_ranking
    ORDER BY risk_score DESC LIMIT 10
    """)

    # Build analysis prompt
    data_context = json.dumps({
        "top_failure_patterns": patterns,
        "highest_risk_services": service_ranking,
    }, indent=2, default=str)

    messages = [
        {
            "role": "system",
            "content": """You are an expert Site Reliability Engineer (SRE) and root cause analysis specialist
for a large life sciences (HLS) enterprise. You analyze observability data
from OpenTelemetry signals across infrastructure, applications, and network domains.

Your analysis must be:
1. Evidence-based - cite specific data points, patterns, and metrics
2. Actionable - provide specific remediation recommendations
3. Prioritized - rank issues by user impact, revenue impact, and frequency
4. Business-aware - understand supply chain, manufacturing, R&D, and employee productivity implications

Format your response in structured markdown with clear sections."""
        },
        {
            "role": "user",
            "content": f"""Analyze the following root cause patterns and service risk data from our life sciences
enterprise observability platform. Provide:

1. **Executive Summary** - 2-3 sentence overview of systemic health
2. **Top Systemic Issue** - The single most important thing to fix and why
3. **Pattern Analysis** - For each major pattern, explain the root cause chain
4. **Remediation Roadmap** - Prioritized list of fixes with expected impact
5. **Risk Assessment** - What could get worse if not addressed

Data:
{data_context}"""
        }
    ]

    try:
        host = get_workspace_host()
        token = get_oauth_token()
        url = f"{host}/serving-endpoints/{MODEL_NAME}/invocations"

        payload = {
            "messages": messages,
            "max_tokens": 4096,
            "temperature": 0.3,
        }
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"LLM API error ({response.status}): {error_text}")
                    return {
                        "analysis": _generate_fallback_analysis(patterns, service_ranking),
                        "model": "fallback",
                        "note": f"AI model unavailable (HTTP {response.status}), using rule-based analysis"
                    }
                result = await response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                return {
                    "analysis": content,
                    "model": MODEL_NAME,
                    "patterns_analyzed": len(patterns),
                }
    except Exception as e:
        logger.error(f"AI analysis error: {e}")
        return {
            "analysis": _generate_fallback_analysis(patterns, service_ranking),
            "model": "fallback",
            "note": f"AI model unavailable, using rule-based analysis: {str(e)}"
        }


def _generate_fallback_analysis(patterns: list, services: list) -> str:
    """Generate rule-based analysis when LLM is unavailable."""
    if not patterns:
        return "No failure patterns detected in the analysis period."

    top = patterns[0]
    analysis_parts = []

    analysis_parts.append("## Executive Summary\n")
    analysis_parts.append(
        f"Analysis of {len(patterns)} recurring failure patterns reveals systemic reliability issues "
        f"centered on **{top.get('root_service', 'unknown')}** in the **{top.get('domain', 'unknown')}** domain. "
        f"The top pattern has occurred **{top.get('occurrence_count', 0)} times** with a total revenue impact "
        f"of **${float(top.get('total_revenue_impact', 0)):,.0f}**.\n"
    )

    analysis_parts.append("\n## Top Systemic Issue\n")
    analysis_parts.append(
        f"**{top.get('failure_pattern_name', 'Unknown Pattern')}**\n\n"
        f"- Root Service: `{top.get('root_service', 'N/A')}`\n"
        f"- Occurrences: {top.get('occurrence_count', 0)}\n"
        f"- Trend: {top.get('trend_direction', 'unknown')}\n"
        f"- Average MTTR: {top.get('avg_mttr_minutes', 0)} minutes\n"
        f"- P1 incidents: {top.get('p1_count', 0)}\n"
        f"- SLA breaches: {top.get('sla_breach_count', 0)}\n"
        f"- Total user impact: {top.get('total_user_impact', 0)} users affected\n"
        f"- Recurrence interval: every {top.get('avg_days_between_occurrences', 'N/A')} days\n"
    )

    analysis_parts.append("\n## Remediation Roadmap\n")
    for i, p in enumerate(patterns[:5], 1):
        analysis_parts.append(
            f"{i}. **Fix {p.get('failure_pattern_name', 'Unknown')}** "
            f"(Priority Score: {p.get('priority_score', 0)}) - "
            f"Affecting `{p.get('root_service', 'N/A')}`, "
            f"{p.get('occurrence_count', 0)} occurrences, "
            f"${float(p.get('total_revenue_impact', 0)):,.0f} impact\n"
        )

    if services:
        analysis_parts.append("\n## Highest Risk Services\n")
        for s in services[:5]:
            analysis_parts.append(
                f"- **{s.get('service_name', 'N/A')}**: Risk Score {s.get('risk_score', 0)}, "
                f"{s.get('incident_count_as_root', 0)} incidents as root cause, "
                f"${float(s.get('total_revenue_impact', 0)):,.0f} revenue impact\n"
            )

    return "".join(analysis_parts)
