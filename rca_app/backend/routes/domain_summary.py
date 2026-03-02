"""
Domain-level Impact Summary API routes.
"""
from fastapi import APIRouter, Query
from typing import Optional
from backend.db import execute_query, CATALOG, SCHEMA

router = APIRouter(prefix="/api/domains", tags=["domains"])


@router.get("/summary")
async def get_domain_summary():
    """Get aggregated domain-level impact summary."""
    rows = execute_query(f"""
    SELECT
      domain,
      SUM(incident_count) as total_incidents,
      SUM(p1_count) as total_p1,
      SUM(p2_count) as total_p2,
      SUM(p3_count) as total_p3,
      ROUND(AVG(avg_mttr_minutes), 1) as avg_mttr,
      SUM(total_blast_radius) as total_blast_radius,
      ROUND(SUM(total_revenue_impact), 2) as total_revenue_impact,
      SUM(total_affected_users) as total_user_impact,
      SUM(sla_breaches) as total_sla_breaches,
      SUM(alert_count) as total_alerts,
      SUM(critical_alert_count) as total_critical_alerts,
      SUM(change_count) as total_changes,
      SUM(high_risk_changes) as total_high_risk_changes,
      SUM(changes_causing_incidents) as total_changes_causing_incidents,
      ROUND(AVG(domain_risk_score), 2) as avg_daily_risk_score,
      ROUND(SUM(domain_risk_score), 2) as cumulative_risk_score
    FROM {CATALOG}.{SCHEMA}.gold_domain_impact_summary
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
    FROM {CATALOG}.{SCHEMA}.gold_domain_impact_summary
    WHERE summary_date >= current_date() - INTERVAL {days} DAYS
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
      WEEKOFYEAR(summary_date) as week_num,
      MIN(summary_date) as week_start,
      SUM(incident_count) as weekly_incidents,
      SUM(p1_count) as weekly_p1,
      ROUND(SUM(total_revenue_impact), 2) as weekly_revenue_impact,
      SUM(total_affected_users) as weekly_user_impact,
      ROUND(AVG(domain_risk_score), 2) as avg_risk_score,
      SUM(change_count) as weekly_changes,
      SUM(alert_count) as weekly_alerts
    FROM {CATALOG}.{SCHEMA}.gold_domain_impact_summary
    WHERE summary_date >= current_date() - INTERVAL {days} DAYS
      {where_clause}
    GROUP BY domain, summary_year, summary_month, WEEKOFYEAR(summary_date)
    ORDER BY domain, week_start
    """)
    return rows


@router.get("/{domain_name}/services")
async def get_domain_services(domain_name: str):
    """Get services and their risk metrics for a specific domain."""
    # Map domain name to service domains
    domain_services = {
        "application": [
            "ehr-api", "patient-portal", "clinical-decision-support", "pharmacy-service",
            "imaging-service", "fhir-api", "auth-service", "notification-service",
            "ml-inference-service", "terminology-service"
        ],
        "infrastructure": [
            "ehr-database", "auth-database", "drug-interaction-db", "message-queue", "pacs-storage"
        ],
        "network": [
            "hl7-gateway", "dicom-gateway", "load-balancer", "dns-resolver", "vpn-gateway"
        ],
    }

    services = domain_services.get(domain_name, [])
    if not services:
        return []

    svc_list = ",".join([f"'{s}'" for s in services])

    rows = execute_query(f"""
    SELECT
      service_name,
      risk_rank,
      risk_score,
      incident_count_as_root,
      times_impacted_by_others,
      p1_count,
      avg_mttr_minutes,
      total_revenue_impact,
      total_affected_users as total_user_impact,
      avg_health_score,
      avg_error_rate,
      risky_changes
    FROM {CATALOG}.{SCHEMA}.gold_service_risk_ranking
    WHERE service_name IN ({svc_list})
    ORDER BY risk_score DESC
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
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    WHERE domain = '{domain_name}'
      AND created_at >= current_date() - INTERVAL {days} DAYS
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
      ROUND(AVG(breach_magnitude_pct), 2) as avg_breach_magnitude
    FROM {CATALOG}.{SCHEMA}.silver_alerts
    WHERE domain = '{domain_name}'
      AND fired_at >= current_date() - INTERVAL {days} DAYS
    GROUP BY service, alert_name
    ORDER BY alert_count DESC
    """)
    return rows
