"""
Genie Space proxy routes.
Proxies natural language queries to Databricks Genie Space API
for the Enterprise RCA Intelligence Q&A.
"""
import os
import time
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from backend.db import get_workspace_host, get_oauth_token, execute_query, CATALOG, SCHEMA

import aiohttp

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/genie", tags=["genie"])

# Genie Space ID -- set at startup or via env
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")


class GenieQueryRequest(BaseModel):
    question: str
    conversation_id: Optional[str] = None


class GenieQueryResponse(BaseModel):
    answer: str
    sql: Optional[str] = None
    data: Optional[list] = None
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None


@router.get("/space-id")
async def get_space_id():
    """Return the configured Genie Space ID."""
    return {"space_id": GENIE_SPACE_ID}


@router.post("/query")
async def query_genie(request: GenieQueryRequest):
    """Send a natural-language question to the Genie Space and return results."""
    host = get_workspace_host()
    token = get_oauth_token()

    if not host or not token:
        raise HTTPException(status_code=500, detail="Cannot authenticate to Databricks workspace")

    # If no Genie Space, fall back to direct SQL-based answers
    if not GENIE_SPACE_ID:
        return await _fallback_sql_answer(request.question)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        async with aiohttp.ClientSession() as session:
            # Start or continue a conversation
            if request.conversation_id:
                url = f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{request.conversation_id}/messages"
            else:
                url = f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"

            payload = {"content": request.question}

            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Genie API error ({resp.status}): {error_text}")
                    # Fall back to SQL-based answer
                    return await _fallback_sql_answer(request.question)

                result = await resp.json()

            conversation_id = result.get("conversation_id", request.conversation_id)
            message_id = result.get("message_id") or result.get("id")

            # Poll for completion
            if message_id and conversation_id:
                answer_data = await _poll_genie_result(session, host, headers, conversation_id, message_id)
            else:
                answer_data = {"answer": "No response received from Genie.", "sql": None, "data": None}

            return GenieQueryResponse(
                answer=answer_data.get("answer", ""),
                sql=answer_data.get("sql"),
                data=answer_data.get("data"),
                conversation_id=conversation_id,
                message_id=message_id,
            )

    except aiohttp.ClientError as e:
        logger.error(f"Genie connection error: {e}")
        return await _fallback_sql_answer(request.question)
    except Exception as e:
        logger.error(f"Genie query error: {e}")
        return await _fallback_sql_answer(request.question)


async def _poll_genie_result(session, host, headers, conversation_id, message_id, max_wait=60):
    """Poll for Genie message completion."""
    url = f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}"

    for _ in range(max_wait // 2):
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                status = data.get("status", "")

                if status in ("COMPLETED", "COMPLETE"):
                    return _extract_genie_answer(data)
                elif status in ("FAILED", "CANCELLED"):
                    return {"answer": f"Query failed: {data.get('error', 'Unknown error')}", "sql": None, "data": None}
        await _async_sleep(2)

    return {"answer": "Query timed out waiting for Genie response.", "sql": None, "data": None}


async def _async_sleep(seconds):
    import asyncio
    await asyncio.sleep(seconds)


def _extract_genie_answer(data):
    """Extract the answer text, SQL, and data from a Genie response."""
    attachments = data.get("attachments", [])
    answer_text = ""
    sql_query = None
    result_data = None

    for att in attachments:
        if att.get("type") == "TEXT":
            answer_text = att.get("text", {}).get("content", "")
        elif att.get("type") == "QUERY":
            query_info = att.get("query", {})
            sql_query = query_info.get("query", "")
            # Extract result table if present
            result_info = query_info.get("result", {})
            columns = result_info.get("columns", [])
            rows = result_info.get("data", [])
            if columns and rows:
                col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns)]
                result_data = [dict(zip(col_names, row)) for row in rows]

    if not answer_text:
        answer_text = data.get("content", "No answer text available.")

    return {"answer": answer_text, "sql": sql_query, "data": result_data}


async def _fallback_sql_answer(question: str):
    """When Genie is not available, answer common questions via direct SQL."""
    q = question.lower()

    # Story 1: Supply chain / shipment delays
    if any(kw in q for kw in ["shipment", "supply chain", "delays in shipment", "shipping", "inventory"]):
        rows = execute_query(f"""
        SELECT
          incident_id, title, severity, root_service, business_unit,
          created_at, mttr_minutes, revenue_impact_usd,
          shipments_delayed, affected_user_count,
          servicenow_ticket_count, servicenow_duplicate_tickets,
          downstream_impact_narrative, root_cause_explanation
        FROM {CATALOG}.{SCHEMA}.silver_incidents
        WHERE business_unit = 'supply-chain'
          AND severity = 'P1'
        ORDER BY created_at DESC
        LIMIT 5
        """)
        if rows:
            r = rows[0]
            answer = (
                f"The most recent P1 supply chain incident was **{r.get('title', '')}** "
                f"on {r.get('created_at', '')[:10]}.\n\n"
                f"**Root Cause:** {r.get('root_cause_explanation', 'N/A')}\n\n"
                f"**Business Impact:** {r.get('downstream_impact_narrative', 'N/A')}\n\n"
                f"- Revenue at risk: ${float(r.get('revenue_impact_usd', 0)):,.0f}\n"
                f"- Shipments delayed: {r.get('shipments_delayed', 0)}\n"
                f"- Affected users: {r.get('affected_user_count', 0)}\n"
                f"- ServiceNow tickets: {r.get('servicenow_ticket_count', 0)} "
                f"({r.get('servicenow_duplicate_tickets', 0)} duplicates)\n"
                f"- MTTR: {r.get('mttr_minutes', 0)} minutes"
            )
            return GenieQueryResponse(answer=answer, data=rows)

    # Story 2: Digital surgery / data science productivity
    if any(kw in q for kw in ["data scientist", "digital surgery", "sagemaker", "productivity", "ml engineer"]):
        rows = execute_query(f"""
        SELECT
          incident_id, title, severity, root_service, business_unit,
          created_at, mttr_minutes, revenue_impact_usd,
          productivity_loss_usd, affected_user_count,
          servicenow_ticket_count, servicenow_duplicate_tickets,
          downstream_impact_narrative, root_cause_explanation
        FROM {CATALOG}.{SCHEMA}.silver_incidents
        WHERE business_unit = 'digital-surgery'
          AND severity = 'P1'
        ORDER BY created_at DESC
        LIMIT 5
        """)
        if rows:
            r = rows[0]
            answer = (
                f"The most recent P1 digital surgery incident was **{r.get('title', '')}** "
                f"on {r.get('created_at', '')[:10]}.\n\n"
                f"**Root Cause:** {r.get('root_cause_explanation', 'N/A')}\n\n"
                f"**Business Impact:** {r.get('downstream_impact_narrative', 'N/A')}\n\n"
                f"- Productivity loss: ${float(r.get('productivity_loss_usd', 0)):,.0f}\n"
                f"- Affected users: {r.get('affected_user_count', 0)}\n"
                f"- ServiceNow tickets: {r.get('servicenow_ticket_count', 0)} "
                f"({r.get('servicenow_duplicate_tickets', 0)} duplicates)\n"
                f"- MTTR: {r.get('mttr_minutes', 0)} minutes"
            )
            return GenieQueryResponse(answer=answer, data=rows)

    # Duplicate tickets
    if any(kw in q for kw in ["duplicate", "servicenow", "tickets"]):
        rows = execute_query(f"""
        SELECT
          failure_pattern_name,
          business_unit,
          SUM(servicenow_ticket_count) as total_tickets,
          SUM(servicenow_duplicate_tickets) as total_duplicates,
          ROUND(SUM(servicenow_duplicate_tickets) * 100.0 / NULLIF(SUM(servicenow_ticket_count), 0), 1) as duplicate_pct
        FROM {CATALOG}.{SCHEMA}.silver_servicenow_correlation
        GROUP BY failure_pattern_name, business_unit
        ORDER BY total_duplicates DESC
        LIMIT 10
        """)
        answer = "Here are the failure patterns with the most duplicate ServiceNow tickets:"
        return GenieQueryResponse(answer=answer, data=rows)

    # Revenue / business impact
    if any(kw in q for kw in ["revenue", "business impact", "cost", "financial"]):
        rows = execute_query(f"""
        SELECT
          business_unit,
          total_incidents,
          total_revenue_impact,
          primary_revenue_model,
          total_affected_users,
          total_servicenow_tickets,
          overall_duplicate_pct,
          total_productivity_loss,
          total_shipments_delayed
        FROM {CATALOG}.{SCHEMA}.gold_business_impact_summary
        ORDER BY total_revenue_impact DESC
        """)
        answer = "Here is the business impact summary by business unit:"
        return GenieQueryResponse(answer=answer, data=rows)

    # Blast radius
    if any(kw in q for kw in ["blast radius", "most impacted", "cascading"]):
        rows = execute_query(f"""
        SELECT
          failure_pattern_name,
          root_service,
          business_unit,
          occurrence_count,
          avg_blast_radius,
          total_revenue_impact,
          all_impacted_services,
          root_cause_explanation
        FROM {CATALOG}.{SCHEMA}.gold_root_cause_patterns
        ORDER BY avg_blast_radius DESC
        LIMIT 10
        """)
        answer = "Here are the root cause patterns with the highest blast radius:"
        return GenieQueryResponse(answer=answer, data=rows)

    # Generic: recent P1 incidents
    rows = execute_query(f"""
    SELECT
      incident_id, title, severity, root_service, business_unit,
      created_at, mttr_minutes, revenue_impact_usd,
      affected_user_count, downstream_impact_narrative,
      root_cause_explanation
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    WHERE severity = 'P1'
    ORDER BY created_at DESC
    LIMIT 10
    """)
    answer = "Here are the most recent P1 incidents across all business units:"
    return GenieQueryResponse(answer=answer, data=rows)
