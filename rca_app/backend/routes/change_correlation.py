"""
Change-Incident Correlation API routes.
"""
from fastapi import APIRouter, Query
from backend.db import execute_query, CATALOG, SCHEMA

router = APIRouter(prefix="/api/changes", tags=["changes"])


@router.get("/correlation-summary")
async def get_correlation_summary():
    """Get summary of change-incident correlations by change type."""
    rows = execute_query(f"""
    SELECT
      change_type,
      COUNT(DISTINCT change_id) as total_changes,
      COUNT(DISTINCT incident_id) as correlated_incidents,
      ROUND(AVG(correlation_strength), 3) as avg_correlation_strength,
      ROUND(MAX(correlation_strength), 3) as max_correlation_strength,
      ROUND(AVG(minutes_between), 1) as avg_time_to_incident_min,
      ROUND(SUM(revenue_impact_usd), 2) as total_revenue_impact,
      SUM(blast_radius) as total_blast_radius,
      change_type_incident_rate
    FROM {CATALOG}.{SCHEMA}.gold_change_incident_correlation
    GROUP BY change_type, change_type_incident_rate
    ORDER BY avg_correlation_strength DESC
    """)
    return rows


@router.get("/timeline")
async def get_change_timeline(days: int = Query(default=90)):
    """Get changes and incidents on a timeline for correlation visualization."""
    changes = execute_query(f"""
    SELECT
      change_id,
      service,
      change_type,
      description,
      executed_at,
      executed_by,
      risk_level,
      risk_score,
      domain,
      incidents_within_4h,
      incidents_within_24h
    FROM {CATALOG}.{SCHEMA}.silver_changes
    WHERE executed_at >= current_date() - INTERVAL {days} DAYS
    ORDER BY executed_at
    """)

    incidents = execute_query(f"""
    SELECT
      incident_id,
      title,
      severity,
      created_at,
      resolved_at,
      root_service,
      blast_radius,
      domain,
      failure_pattern_name
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    WHERE created_at >= current_date() - INTERVAL {days} DAYS
    ORDER BY created_at
    """)

    return {
        "changes": changes,
        "incidents": incidents,
    }


@router.get("/high-correlation")
async def get_high_correlation_pairs(min_strength: float = Query(default=0.5)):
    """Get change-incident pairs with strong correlation."""
    rows = execute_query(f"""
    SELECT
      change_id,
      change_service,
      change_type,
      risk_level,
      change_time,
      executed_by,
      incident_id,
      incident_severity,
      incident_title,
      incident_root_service,
      incident_time,
      minutes_between,
      correlation_window,
      correlation_strength,
      revenue_impact_usd,
      blast_radius,
      failure_pattern_id
    FROM {CATALOG}.{SCHEMA}.gold_change_incident_correlation
    WHERE correlation_strength >= {min_strength}
    ORDER BY correlation_strength DESC
    LIMIT 50
    """)
    return rows


@router.get("/risky-change-types")
async def get_risky_change_types():
    """Get change types ranked by incident-causing rate."""
    rows = execute_query(f"""
    WITH change_counts AS (
      SELECT
        change_type,
        COUNT(*) as total_changes
      FROM {CATALOG}.{SCHEMA}.silver_changes
      GROUP BY change_type
    ),
    incident_counts AS (
      SELECT
        change_type,
        COUNT(DISTINCT incident_id) as incidents_caused,
        ROUND(SUM(revenue_impact_usd), 2) as total_impact
      FROM {CATALOG}.{SCHEMA}.gold_change_incident_correlation
      GROUP BY change_type
    )
    SELECT
      cc.change_type,
      cc.total_changes,
      COALESCE(ic.incidents_caused, 0) as incidents_caused,
      ROUND(COALESCE(ic.incidents_caused, 0) * 100.0 / cc.total_changes, 2) as incident_rate_pct,
      COALESCE(ic.total_impact, 0) as total_revenue_impact
    FROM change_counts cc
    LEFT JOIN incident_counts ic ON cc.change_type = ic.change_type
    ORDER BY incident_rate_pct DESC
    """)
    return rows


@router.get("/by-executor")
async def get_changes_by_executor():
    """Get change statistics grouped by who executed them."""
    rows = execute_query(f"""
    SELECT
      executed_by,
      COUNT(*) as total_changes,
      SUM(incidents_within_4h) as incidents_caused_4h,
      SUM(incidents_within_24h) as incidents_caused_24h,
      ROUND(AVG(risk_score), 2) as avg_risk_score,
      SUM(CASE WHEN risk_level = 'high' THEN 1 ELSE 0 END) as high_risk_changes
    FROM {CATALOG}.{SCHEMA}.silver_changes
    GROUP BY executed_by
    ORDER BY incidents_caused_4h DESC
    """)
    return rows
