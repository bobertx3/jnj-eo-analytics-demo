"""
Domain-level Impact Summary API routes.
"""
from fastapi import APIRouter, Query
from typing import Optional
from backend.db import execute_query

router = APIRouter(prefix="/api/domains", tags=["domains"])


@router.get("/summary")
async def get_domain_summary():
    """Get aggregated domain-level impact summary."""
    rows = execute_query("""
    SELECT
      domain,
      SUM(incident_count) as total_incidents,
      SUM(p1_count) as total_p1,
      SUM(p2_count) as total_p2,
      SUM(p3_count) as total_p3,
      ROUND(AVG(avg_mttr_minutes)::numeric, 1) as avg_mttr,
      SUM(total_blast_radius) as total_blast_radius,
      ROUND(SUM(total_revenue_impact)::numeric, 2) as total_revenue_impact,
      SUM(total_affected_users) as total_user_impact,
      SUM(sla_breaches) as total_sla_breaches,
      SUM(alert_count) as total_alerts,
      SUM(critical_alert_count) as total_critical_alerts,
      SUM(change_count) as total_changes,
      SUM(high_risk_changes) as total_high_risk_changes,
      SUM(changes_causing_incidents) as total_changes_causing_incidents,
      ROUND(AVG(domain_risk_score)::numeric, 2) as avg_daily_risk_score,
      ROUND(SUM(domain_risk_score)::numeric, 2) as cumulative_risk_score
    FROM gold_domain_impact_summary
    GROUP BY domain
    ORDER BY cumulative_risk_score DESC
    """)
    return rows


@router.get("/heatmap")
async def get_domain_heatmap(days: int = Query(default=90)):
    """Get domain risk heatmap data (daily risk scores per domain)."""
    rows = execute_query(f"""
    SELECT
      domain,
      summary_date,
      incident_count,
      p1_count,
      total_revenue_impact,
      total_affected_users as total_user_impact,
      alert_count,
      critical_alert_count,
      change_count,
      domain_risk_score
    FROM gold_domain_impact_summary
    WHERE summary_date >= CURRENT_DATE - INTERVAL '{days} days'
    ORDER BY summary_date, domain
    """)
    return rows


@router.get("/trend")
async def get_domain_trend(
    domain: Optional[str] = Query(default=None),
    days: int = Query(default=90),
):
    """Get weekly trend for domains."""
    where_clause = f"AND domain = '{domain}'" if domain else ""

    rows = execute_query(f"""
    SELECT
      domain,
      summary_year,
      summary_month,
      EXTRACT(WEEK FROM summary_date)::int as week_num,
      MIN(summary_date) as week_start,
      SUM(incident_count) as weekly_incidents,
      SUM(p1_count) as weekly_p1,
      ROUND(SUM(total_revenue_impact)::numeric, 2) as weekly_revenue_impact,
      SUM(total_affected_users) as weekly_user_impact,
      ROUND(AVG(domain_risk_score)::numeric, 2) as avg_risk_score,
      SUM(change_count) as weekly_changes,
      SUM(alert_count) as weekly_alerts
    FROM gold_domain_impact_summary
    WHERE summary_date >= CURRENT_DATE - INTERVAL '{days} days'
      {where_clause}
    GROUP BY domain, summary_year, summary_month, EXTRACT(WEEK FROM summary_date)::int
    ORDER BY domain, week_start
    """)
    return rows


@router.get("/{domain_name}/services")
async def get_domain_services(domain_name: str):
    """Get services and their risk metrics for a specific domain."""
    rows = execute_query(f"""
    WITH domain_svc AS (
      SELECT DISTINCT root_service as service_name
      FROM silver_incidents
      WHERE domain = '{domain_name}'
        AND root_service IS NOT NULL
    )
    SELECT
      g.service_name,
      g.risk_rank,
      g.risk_score,
      g.incident_count_as_root,
      g.times_impacted_by_others,
      g.p1_count,
      g.avg_mttr_minutes,
      g.total_revenue_impact,
      g.total_affected_users as total_user_impact,
      g.avg_health_score,
      g.avg_error_rate,
      g.risky_changes
    FROM gold_service_risk_ranking g
    JOIN domain_svc ds ON g.service_name = ds.service_name
    ORDER BY g.risk_score DESC
    """)
    return rows


@router.get("/{domain_name}/incidents")
async def get_domain_incidents(
    domain_name: str,
    days: int = Query(default=90),
    limit: int = Query(default=50),
):
    """Get incidents for a specific domain."""
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
      failure_pattern_name,
      revenue_impact_usd,
      affected_user_count,
      sla_breached
    FROM silver_incidents
    WHERE domain = '{domain_name}'
      AND created_at >= CURRENT_DATE - INTERVAL '{days} days'
    ORDER BY created_at DESC
    LIMIT {limit}
    """)
    return rows


@router.get("/{domain_name}/alerts")
async def get_domain_alerts(
    domain_name: str,
    days: int = Query(default=30),
):
    """Get alert summary for a specific domain."""
    rows = execute_query(f"""
    SELECT
      service,
      alert_name,
      COUNT(*) as alert_count,
      SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_count,
      SUM(CASE WHEN is_incident_correlated THEN 1 ELSE 0 END) as incident_correlated,
      SUM(CASE WHEN is_pre_incident_signal THEN 1 ELSE 0 END) as pre_incident_count,
      ROUND(AVG(breach_magnitude_pct)::numeric, 2) as avg_breach_magnitude
    FROM silver_alerts
    WHERE domain = '{domain_name}'
      AND fired_at >= CURRENT_DATE - INTERVAL '{days} days'
    GROUP BY service, alert_name
    ORDER BY alert_count DESC
    """)
    return rows
