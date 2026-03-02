"""
Incident-related API routes.
"""
from fastapi import APIRouter, Query
from typing import Optional
from backend.db import execute_query, CATALOG, SCHEMA

router = APIRouter(prefix="/api/incidents", tags=["incidents"])


@router.get("/summary")
async def get_incident_summary():
    """Get high-level incident statistics."""
    rows = execute_query(f"""
    SELECT
      COUNT(*) as total_incidents,
      SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
      SUM(CASE WHEN severity = 'P2' THEN 1 ELSE 0 END) as p2_count,
      SUM(CASE WHEN severity = 'P3' THEN 1 ELSE 0 END) as p3_count,
      ROUND(AVG(mttr_minutes), 1) as avg_mttr,
      ROUND(SUM(revenue_impact_usd), 2) as total_revenue_impact,
      SUM(patient_impact_count) as total_patient_impact,
      SUM(CASE WHEN sla_breached THEN 1 ELSE 0 END) as total_sla_breaches,
      ROUND(AVG(blast_radius), 1) as avg_blast_radius
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    """)
    return rows[0] if rows else {}


@router.get("/ticket-noise")
async def get_ticket_noise(
    days: int = Query(default=90, ge=7, le=365),
    limit: int = Query(default=10, ge=1, le=100),
):
    """Get ServiceNow duplicate-ticket noise by root service."""
    rows = execute_query(f"""
    SELECT
      root_service as service_name,
      COUNT(DISTINCT incident_id) as incident_count,
      SUM(servicenow_ticket_count) as total_tickets,
      SUM(servicenow_duplicate_tickets) as total_duplicates,
      ROUND(
        SUM(servicenow_duplicate_tickets) * 100.0 / NULLIF(SUM(servicenow_ticket_count), 0),
        1
      ) as duplicate_pct,
      ROUND(SUM(revenue_impact_usd), 2) as total_revenue_impact
    FROM {CATALOG}.{SCHEMA}.silver_servicenow_correlation
    WHERE created_at >= current_date() - INTERVAL {days} DAYS
    GROUP BY root_service
    HAVING SUM(servicenow_ticket_count) > 0
    ORDER BY total_duplicates DESC, duplicate_pct DESC
    LIMIT {limit}
    """)
    return rows


@router.get("/timeline")
async def get_incident_timeline(
    days: int = Query(default=90, ge=7, le=180),
    severity: Optional[str] = Query(default=None),
    domain: Optional[str] = Query(default=None),
):
    """Get daily incident counts for timeline chart."""
    where_clauses = ["1=1"]
    if severity:
        where_clauses.append(f"severity = '{severity}'")
    if domain:
        where_clauses.append(f"domain = '{domain}'")
    where_sql = " AND ".join(where_clauses)

    rows = execute_query(f"""
    SELECT
      DATE(created_at) as incident_date,
      COUNT(*) as incident_count,
      SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
      SUM(CASE WHEN severity = 'P2' THEN 1 ELSE 0 END) as p2_count,
      SUM(CASE WHEN severity = 'P3' THEN 1 ELSE 0 END) as p3_count,
      ROUND(SUM(revenue_impact_usd), 2) as daily_revenue_impact,
      SUM(patient_impact_count) as daily_patient_impact,
      ROUND(AVG(mttr_minutes), 1) as avg_mttr
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    WHERE created_at >= current_date() - INTERVAL {days} DAYS
      AND {where_sql}
    GROUP BY DATE(created_at)
    ORDER BY incident_date
    """)
    return rows


@router.get("/recent")
async def get_recent_incidents(limit: int = Query(default=20, ge=1, le=100)):
    """Get most recent incidents with enrichments."""
    rows = execute_query(f"""
    SELECT
      incident_id,
      title,
      description,
      severity,
      severity_level,
      status,
      created_at,
      resolved_at,
      mttr_minutes,
      root_service,
      impacted_services,
      blast_radius,
      domain,
      failure_pattern_id,
      failure_pattern_name,
      revenue_impact_usd,
      patient_impact_count,
      sla_breached,
      correlated_alert_count,
      impact_score
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    ORDER BY created_at DESC
    LIMIT {limit}
    """)
    return rows


@router.get("/by-service")
async def get_incidents_by_service():
    """Get incident breakdown by service."""
    rows = execute_query(f"""
    SELECT
      root_service,
      domain,
      COUNT(*) as incident_count,
      SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
      ROUND(AVG(mttr_minutes), 1) as avg_mttr,
      ROUND(AVG(blast_radius), 1) as avg_blast_radius,
      ROUND(SUM(revenue_impact_usd), 2) as total_revenue_impact,
      SUM(patient_impact_count) as total_patient_impact
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    GROUP BY root_service, domain
    ORDER BY incident_count DESC
    """)
    return rows


@router.get("/by-hour")
async def get_incidents_by_hour():
    """Get incident distribution by hour of day."""
    rows = execute_query(f"""
    SELECT
      incident_hour,
      COUNT(*) as incident_count,
      SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
      ROUND(AVG(mttr_minutes), 1) as avg_mttr
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    GROUP BY incident_hour
    ORDER BY incident_hour
    """)
    return rows


@router.get("/mttr-trend")
async def get_mttr_trend(days: int = Query(default=90)):
    """Get weekly MTTR trend."""
    rows = execute_query(f"""
    SELECT
      WEEKOFYEAR(created_at) as week_num,
      MIN(DATE(created_at)) as week_start,
      ROUND(AVG(mttr_minutes), 1) as avg_mttr,
      ROUND(PERCENTILE(mttr_minutes, 0.5), 1) as p50_mttr,
      ROUND(PERCENTILE(mttr_minutes, 0.95), 1) as p95_mttr,
      COUNT(*) as incident_count
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    WHERE created_at >= current_date() - INTERVAL {days} DAYS
    GROUP BY WEEKOFYEAR(created_at)
    ORDER BY week_num
    """)
    return rows


@router.get("/{incident_id}")
async def get_incident_detail(incident_id: str):
    """Get full detail payload for a single incident."""
    rows = execute_query(f"""
    SELECT
      incident_id,
      title,
      description,
      severity,
      severity_level,
      status,
      created_at,
      resolved_at,
      mttr_minutes,
      root_service,
      impacted_services,
      blast_radius,
      domain,
      failure_pattern_id,
      failure_pattern_name,
      environment,
      region,
      revenue_impact_usd,
      patient_impact_count,
      sla_breached,
      business_unit,
      affected_user_count,
      affected_roles,
      productivity_loss_hours,
      productivity_loss_usd,
      shipments_delayed,
      servicenow_ticket_count,
      servicenow_duplicate_tickets,
      downstream_impact_narrative,
      root_cause_explanation,
      revenue_model,
      correlated_alert_count,
      impact_score
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    WHERE incident_id = '{incident_id}'
    LIMIT 1
    """)
    return rows[0] if rows else {}
