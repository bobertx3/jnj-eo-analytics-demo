"""
Service Risk Ranking API routes.
"""
from fastapi import APIRouter, Query
from backend.db import execute_query, CATALOG, SCHEMA

router = APIRouter(prefix="/api/services", tags=["services"])


@router.get("/risk-ranking")
async def get_service_risk_ranking():
    """Get all services ranked by risk score."""
    rows = execute_query(f"""
    SELECT
      service_name,
      risk_rank,
      risk_score,
      incident_count_as_root,
      times_impacted_by_others,
      p1_count,
      p2_count,
      p3_count,
      avg_mttr_minutes,
      total_blast_radius,
      avg_blast_radius,
      total_revenue_impact,
      total_affected_users as total_user_impact,
      sla_breaches,
      unique_failure_patterns,
      avg_health_score,
      min_health_score,
      avg_error_rate,
      avg_cpu_utilization,
      total_changes,
      risky_changes
    FROM {CATALOG}.{SCHEMA}.gold_service_risk_ranking
    ORDER BY risk_rank
    """)
    return rows


@router.get("/health-timeline")
async def get_service_health_timeline(
    service: str = Query(...),
    days: int = Query(default=30, ge=7, le=180),
):
    """Get health score timeline for a specific service."""
    rows = execute_query(f"""
    SELECT
      health_date,
      health_score,
      avg_cpu_pct,
      max_cpu_pct,
      avg_memory_pct,
      max_memory_pct,
      avg_latency_ms,
      max_latency_ms,
      incident_count,
      p1_incident_count,
      error_log_count,
      error_rate_pct
    FROM {CATALOG}.{SCHEMA}.silver_service_health
    WHERE service_name = '{service}'
      AND health_date >= current_date() - INTERVAL {days} DAYS
    ORDER BY health_date
    """)
    return rows


@router.get("/topology")
async def get_service_topology():
    """Get service dependency graph with risk annotations."""
    # Service nodes with risk and domain data derived from silver tables.
    services = execute_query(f"""
    WITH service_domain_candidates AS (
      SELECT
        root_service as service_name,
        LOWER(TRIM(domain)) as domain
      FROM {CATALOG}.{SCHEMA}.silver_incidents
      WHERE root_service IS NOT NULL
        AND root_service != ''
        AND domain IS NOT NULL
        AND domain != ''
      UNION ALL
      SELECT
        service as service_name,
        LOWER(TRIM(domain)) as domain
      FROM {CATALOG}.{SCHEMA}.silver_alerts
      WHERE service IS NOT NULL
        AND service != ''
        AND domain IS NOT NULL
        AND domain != ''
      UNION ALL
      SELECT
        service as service_name,
        LOWER(TRIM(domain)) as domain
      FROM {CATALOG}.{SCHEMA}.silver_changes
      WHERE service IS NOT NULL
        AND service != ''
        AND domain IS NOT NULL
        AND domain != ''
    ),
    normalized_domains AS (
      SELECT
        service_name,
        CASE
          WHEN domain IN ('infra', 'infrastructure') THEN 'infrastructure'
          WHEN domain IN ('app', 'application') THEN 'application'
          WHEN domain IN ('net', 'network') THEN 'network'
          ELSE domain
        END as domain
      FROM service_domain_candidates
    ),
    ranked_domains AS (
      SELECT
        service_name,
        domain,
        ROW_NUMBER() OVER (
          PARTITION BY service_name
          ORDER BY COUNT(*) DESC, domain
        ) as rn
      FROM normalized_domains
      GROUP BY service_name, domain
    ),
    service_domains AS (
      SELECT service_name, domain
      FROM ranked_domains
      WHERE rn = 1
    )
    SELECT
      g.service_name,
      g.risk_score,
      g.risk_rank,
      g.incident_count_as_root,
      g.avg_health_score,
      g.total_revenue_impact,
      COALESCE(sd.domain, 'unknown') as domain
    FROM {CATALOG}.{SCHEMA}.gold_service_risk_ranking g
    LEFT JOIN service_domains sd
      ON g.service_name = sd.service_name
    ORDER BY g.risk_rank
    """)

    # Service dependency edges from network flows
    edges = execute_query(f"""
    WITH network_edges AS (
      SELECT
        src_service,
        dst_service,
        COUNT(*) as network_flow_count,
        0 as incident_link_count,
        AVG(latency_us) as avg_latency_us,
        SUM(CASE WHEN connection_reset THEN 1 ELSE 0 END) as reset_count,
        SUM(CASE WHEN timeout THEN 1 ELSE 0 END) as timeout_count,
        SUM(retransmits) as total_retransmits
      FROM {CATALOG}.{SCHEMA}.bronze_network_flows
      WHERE src_service != '' AND dst_service != ''
      GROUP BY src_service, dst_service
    ),
    incident_edges AS (
      SELECT
        root_service as src_service,
        impacted_service as dst_service,
        0 as network_flow_count,
        COUNT(*) as incident_link_count,
        CAST(NULL AS DOUBLE) as avg_latency_us,
        0 as reset_count,
        0 as timeout_count,
        0 as total_retransmits
      FROM (
        SELECT
          root_service,
          explode(impacted_services) as impacted_service
        FROM {CATALOG}.{SCHEMA}.silver_incidents
        WHERE root_service IS NOT NULL
          AND impacted_services IS NOT NULL
          AND size(impacted_services) > 0
      )
      WHERE impacted_service IS NOT NULL
        AND root_service != impacted_service
      GROUP BY root_service, impacted_service
    ),
    combined_edges AS (
      SELECT * FROM network_edges
      UNION ALL
      SELECT * FROM incident_edges
    )
    SELECT
      src_service,
      dst_service,
      SUM(network_flow_count) as network_flow_count,
      SUM(incident_link_count) as incident_link_count,
      -- Keep flow_count for frontend compatibility: network volume + weighted incident links.
      SUM(network_flow_count) + (SUM(incident_link_count) * 100) as flow_count,
      AVG(avg_latency_us) as avg_latency_us,
      SUM(reset_count) as reset_count,
      SUM(timeout_count) as timeout_count,
      SUM(total_retransmits) as total_retransmits
    FROM combined_edges
    GROUP BY src_service, dst_service
    ORDER BY flow_count DESC
    """)

    return {
        "nodes": services,
        "edges": edges,
    }


@router.get("/metrics-window")
async def get_metrics_window(
    service: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
):
    """Get hourly metrics for a service within a time window (for incident correlation)."""
    rows = execute_query(f"""
    SELECT
      DATE_TRUNC('hour', event_timestamp) AS hour_ts,
      DATE_FORMAT(DATE_TRUNC('hour', event_timestamp), 'MM-dd HH:mm') AS hour_label,
      ROUND(MAX(CASE WHEN metric_name='system.cpu.utilization' THEN metric_value END), 1) AS cpu_pct,
      ROUND(MAX(CASE WHEN metric_name='system.memory.utilization' THEN metric_value END), 1) AS mem_pct,
      ROUND(MAX(CASE WHEN metric_name='http.server.active_requests' THEN metric_value END), 0) AS active_requests,
      ROUND(
        SUM(CASE WHEN metric_name='http.server.request.duration' THEN histogram_sum END)
        / NULLIF(SUM(CASE WHEN metric_name='http.server.request.duration' THEN histogram_count END), 0),
      1) AS avg_latency_ms
    FROM {CATALOG}.{SCHEMA}.bronze_metrics
    WHERE service_name = '{service}'
      AND event_timestamp >= '{start}'
      AND event_timestamp <= '{end}'
    GROUP BY DATE_TRUNC('hour', event_timestamp)
    ORDER BY hour_ts
    """)
    return rows


@router.get("/{service_name}/incidents")
async def get_service_incidents(service_name: str, limit: int = Query(default=20)):
    """Get incidents where a service was root cause or impacted."""
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
      CASE WHEN root_service = '{service_name}' THEN 'root_cause' ELSE 'impacted' END as role
    FROM {CATALOG}.{SCHEMA}.silver_incidents
    WHERE root_service = '{service_name}'
       OR array_contains(impacted_services, '{service_name}')
    ORDER BY created_at DESC
    LIMIT {limit}
    """)
    return rows


@router.get("/{service_name}/alerts")
async def get_service_alerts(service_name: str, days: int = Query(default=30)):
    """Get alerts for a specific service."""
    rows = execute_query(f"""
    SELECT
      alert_id,
      alert_name,
      severity,
      fired_at,
      resolved_at,
      threshold_value,
      actual_value,
      duration_minutes,
      breach_magnitude_pct,
      is_incident_correlated,
      is_pre_incident_signal
    FROM {CATALOG}.{SCHEMA}.silver_alerts
    WHERE service = '{service_name}'
      AND fired_at >= current_date() - INTERVAL {days} DAYS
    ORDER BY fired_at DESC
    LIMIT 50
    """)
    return rows
