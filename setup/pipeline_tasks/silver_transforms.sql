-- Silver Layer Transforms for Enterprise RCA Intelligence Pipeline
-- Creates 6 enriched silver tables from bronze data
-- Run against: jnj_eo_demo.eo_analytics_plane
--
-- Updated for JnJ business unit schema with:
--   business_unit, affected_user_count, affected_roles, productivity_loss_*,
--   shipments_delayed, servicenow_ticket_count/duplicate_tickets,
--   downstream_impact_narrative, root_cause_explanation, revenue_model
-- New table: silver_servicenow_correlation

-- ============================================================================
-- silver_incidents: Enriched incidents with business context and impact scoring
-- ============================================================================
CREATE OR REPLACE TABLE jnj_eo_demo.eo_analytics_plane.silver_incidents AS
SELECT
  i.incident_id,
  i.title,
  i.description,
  i.severity,
  i.status,
  i.created_at,
  i.resolved_at,
  i.mttr_minutes,
  i.root_service,
  i.impacted_services,
  i.blast_radius,
  i.domain,
  i.failure_pattern_id,
  i.failure_pattern_name,
  i.environment,
  i.region,
  i.revenue_impact_usd,
  i.sla_breached,
  -- Business context fields
  i.business_unit,
  i.affected_user_count,
  -- Backward-compatible alias used by legacy API/UI queries
  i.affected_user_count as patient_impact_count,
  i.affected_roles,
  i.productivity_loss_hours,
  i.productivity_loss_usd,
  i.shipments_delayed,
  i.servicenow_ticket_count,
  i.servicenow_duplicate_tickets,
  i.downstream_impact_narrative,
  i.root_cause_explanation,
  i.revenue_model,
  -- Enrichments
  CASE
    WHEN i.severity = 'P1' THEN 'critical'
    WHEN i.severity = 'P2' THEN 'high'
    WHEN i.severity = 'P3' THEN 'medium'
    ELSE 'low'
  END as severity_level,
  DATE(i.created_at) as incident_date,
  HOUR(i.created_at) as incident_hour,
  DAYOFWEEK(i.created_at) as incident_day_of_week,
  WEEKOFYEAR(i.created_at) as incident_week,
  (
    SELECT collect_list(DISTINCT a.alert_name)
    FROM jnj_eo_demo.eo_analytics_plane.bronze_alerts a
    WHERE a.incident_id = i.incident_id
  ) as correlated_alert_types,
  (
    SELECT COUNT(*)
    FROM jnj_eo_demo.eo_analytics_plane.bronze_alerts a
    WHERE a.incident_id = i.incident_id
  ) as correlated_alert_count,
  (
    SELECT collect_list(struct(c.change_id, c.change_type, c.service, c.executed_at))
    FROM jnj_eo_demo.eo_analytics_plane.bronze_topology_changes c
    WHERE c.executed_at BETWEEN i.created_at - INTERVAL 2 HOURS AND i.created_at
      AND (c.service = i.root_service OR array_contains(i.impacted_services, c.service))
  ) as preceding_changes,
  ROUND(
    (CASE WHEN i.severity = 'P1' THEN 100 WHEN i.severity = 'P2' THEN 60 WHEN i.severity = 'P3' THEN 30 ELSE 10 END)
    * (1 + LOG2(GREATEST(i.blast_radius, 1)))
    * (1 + LEAST(i.revenue_impact_usd / 100000, 5))
    , 2
  ) as impact_score,
  current_timestamp() as enriched_at
FROM jnj_eo_demo.eo_analytics_plane.bronze_incidents i;

-- ============================================================================
-- silver_alerts: Enriched alerts with breach analysis
-- ============================================================================
CREATE OR REPLACE TABLE jnj_eo_demo.eo_analytics_plane.silver_alerts AS
SELECT
  a.alert_id,
  a.incident_id,
  a.service,
  a.alert_name,
  a.severity,
  a.fired_at,
  a.resolved_at,
  a.threshold_value,
  a.actual_value,
  a.domain,
  a.environment,
  CASE WHEN a.incident_id IS NOT NULL THEN true ELSE false END as is_incident_correlated,
  TIMESTAMPDIFF(MINUTE, a.fired_at, a.resolved_at) as duration_minutes,
  DATE(a.fired_at) as alert_date,
  HOUR(a.fired_at) as alert_hour,
  DAYOFWEEK(a.fired_at) as alert_day_of_week,
  ROUND(
    CASE WHEN a.threshold_value > 0 THEN (a.actual_value - a.threshold_value) / a.threshold_value * 100
    ELSE 0 END
  , 2) as breach_magnitude_pct,
  CASE
    WHEN a.incident_id IS NOT NULL THEN (
      SELECT CASE WHEN a.fired_at < i.created_at THEN true ELSE false END
      FROM jnj_eo_demo.eo_analytics_plane.bronze_incidents i
      WHERE i.incident_id = a.incident_id
      LIMIT 1
    )
    ELSE false
  END as is_pre_incident_signal,
  current_timestamp() as enriched_at
FROM jnj_eo_demo.eo_analytics_plane.bronze_alerts a;

-- ============================================================================
-- silver_changes: Enriched changes with risk scoring
-- ============================================================================
CREATE OR REPLACE TABLE jnj_eo_demo.eo_analytics_plane.silver_changes AS
SELECT
  c.change_id,
  c.service,
  c.change_type,
  c.description,
  c.executed_at,
  c.executed_by,
  c.risk_level,
  c.rollback_available,
  c.domain,
  c.environment,
  c.region,
  DATE(c.executed_at) as change_date,
  HOUR(c.executed_at) as change_hour,
  DAYOFWEEK(c.executed_at) as change_day_of_week,
  CASE
    WHEN c.risk_level = 'high' THEN 3.0
    WHEN c.risk_level = 'medium' THEN 2.0
    ELSE 1.0
  END
  * CASE WHEN c.rollback_available THEN 1.0 ELSE 1.5 END
  * CASE
      WHEN c.change_type IN ('database_migration', 'network_route_change', 'firewall_rule_update', 'vpc_peering_update', 'security_group_change') THEN 2.0
      WHEN c.change_type IN ('deployment', 'dependency_upgrade', 'terraform_apply') THEN 1.5
      ELSE 1.0
    END as risk_score,
  (
    SELECT COUNT(*)
    FROM jnj_eo_demo.eo_analytics_plane.bronze_incidents i
    WHERE i.created_at BETWEEN c.executed_at AND c.executed_at + INTERVAL 4 HOURS
      AND (i.root_service = c.service OR array_contains(i.impacted_services, c.service))
  ) as incidents_within_4h,
  (
    SELECT COUNT(*)
    FROM jnj_eo_demo.eo_analytics_plane.bronze_incidents i
    WHERE i.created_at BETWEEN c.executed_at AND c.executed_at + INTERVAL 24 HOURS
      AND (i.root_service = c.service OR array_contains(i.impacted_services, c.service))
  ) as incidents_within_24h,
  current_timestamp() as enriched_at
FROM jnj_eo_demo.eo_analytics_plane.bronze_topology_changes c;

-- ============================================================================
-- silver_service_health: Daily composite health scores
-- ============================================================================
CREATE OR REPLACE TABLE jnj_eo_demo.eo_analytics_plane.silver_service_health AS
WITH daily_metrics AS (
  SELECT
    service_name,
    DATE(event_timestamp) as metric_date,
    metric_name,
    AVG(metric_value) as avg_value,
    MAX(metric_value) as max_value,
    MIN(metric_value) as min_value
  FROM jnj_eo_demo.eo_analytics_plane.bronze_metrics
  WHERE service_name IS NOT NULL
  GROUP BY service_name, DATE(event_timestamp), metric_name
),
daily_incidents AS (
  SELECT
    root_service as service_name,
    DATE(created_at) as incident_date,
    COUNT(*) as incident_count,
    SUM(CASE WHEN severity = 'P1' THEN 1 ELSE 0 END) as p1_count,
    SUM(blast_radius) as total_blast_radius,
    AVG(mttr_minutes) as avg_mttr
  FROM jnj_eo_demo.eo_analytics_plane.bronze_incidents
  GROUP BY root_service, DATE(created_at)
),
daily_errors AS (
  SELECT
    service_name,
    DATE(event_timestamp) as log_date,
    COUNT(*) as total_logs,
    SUM(CASE WHEN severity_text IN ('ERROR', 'FATAL') THEN 1 ELSE 0 END) as error_count
  FROM jnj_eo_demo.eo_analytics_plane.bronze_logs
  WHERE service_name IS NOT NULL
  GROUP BY service_name, DATE(event_timestamp)
)
SELECT
  COALESCE(m_cpu.service_name, i.service_name, e.service_name) as service_name,
  COALESCE(m_cpu.metric_date, i.incident_date, e.log_date) as health_date,
  m_cpu.avg_value as avg_cpu_pct,
  m_cpu.max_value as max_cpu_pct,
  m_mem.avg_value as avg_memory_pct,
  m_mem.max_value as max_memory_pct,
  m_lat.avg_value as avg_latency_ms,
  m_lat.max_value as max_latency_ms,
  COALESCE(i.incident_count, 0) as incident_count,
  COALESCE(i.p1_count, 0) as p1_incident_count,
  COALESCE(i.total_blast_radius, 0) as total_blast_radius,
  COALESCE(i.avg_mttr, 0) as avg_mttr_minutes,
  COALESCE(e.error_count, 0) as error_log_count,
  COALESCE(e.total_logs, 0) as total_log_count,
  CASE WHEN COALESCE(e.total_logs, 0) > 0
    THEN ROUND(COALESCE(e.error_count, 0) * 100.0 / e.total_logs, 2)
    ELSE 0
  END as error_rate_pct,
  ROUND(
    100
    - LEAST(COALESCE(m_cpu.max_value, 0), 100) * 0.15
    - LEAST(COALESCE(m_mem.max_value, 0), 100) * 0.10
    - COALESCE(i.incident_count, 0) * 15
    - COALESCE(i.p1_count, 0) * 25
    - LEAST(COALESCE(e.error_count, 0) * 0.1, 20)
  , 2) as health_score,
  current_timestamp() as computed_at
FROM daily_metrics m_cpu
LEFT JOIN daily_metrics m_mem
  ON m_cpu.service_name = m_mem.service_name AND m_cpu.metric_date = m_mem.metric_date
  AND m_mem.metric_name = 'system.memory.utilization'
LEFT JOIN daily_metrics m_lat
  ON m_cpu.service_name = m_lat.service_name AND m_cpu.metric_date = m_lat.metric_date
  AND m_lat.metric_name = 'http.server.request.duration'
LEFT JOIN daily_incidents i
  ON m_cpu.service_name = i.service_name AND m_cpu.metric_date = i.incident_date
LEFT JOIN daily_errors e
  ON m_cpu.service_name = e.service_name AND m_cpu.metric_date = e.log_date
WHERE m_cpu.metric_name = 'system.cpu.utilization';

-- ============================================================================
-- silver_business_impact: Business impact classification with revenue model
-- ============================================================================
CREATE OR REPLACE TABLE jnj_eo_demo.eo_analytics_plane.silver_business_impact AS
SELECT
  i.incident_id,
  i.title,
  i.severity,
  i.root_service,
  i.domain,
  i.business_unit,
  i.created_at,
  i.resolved_at,
  i.mttr_minutes,
  i.revenue_impact_usd,
  i.revenue_model,
  i.affected_user_count,
  i.productivity_loss_usd,
  i.shipments_delayed,
  i.sla_breached,
  i.blast_radius,
  i.failure_pattern_id,
  i.failure_pattern_name,
  i.downstream_impact_narrative,
  i.root_cause_explanation,
  CASE
    WHEN i.revenue_impact_usd > 1000000 THEN 'critical'
    WHEN i.revenue_impact_usd > 100000 THEN 'high'
    WHEN i.revenue_impact_usd > 10000 THEN 'moderate'
    ELSE 'low'
  END as revenue_impact_level,
  CASE
    WHEN i.severity = 'P1' THEN i.mttr_minutes * i.blast_radius
    WHEN i.severity = 'P2' THEN i.mttr_minutes * i.blast_radius * 0.5
    ELSE i.mttr_minutes * i.blast_radius * 0.25
  END as weighted_downtime_minutes,
  DATE(i.created_at) as impact_date,
  WEEKOFYEAR(i.created_at) as impact_week,
  MONTH(i.created_at) as impact_month,
  YEAR(i.created_at) as impact_year,
  current_timestamp() as computed_at
FROM jnj_eo_demo.eo_analytics_plane.bronze_incidents i;

-- ============================================================================
-- silver_servicenow_correlation: ServiceNow ticket dedup analysis (NEW)
-- ============================================================================
CREATE OR REPLACE TABLE jnj_eo_demo.eo_analytics_plane.silver_servicenow_correlation AS
SELECT
  i.incident_id,
  i.business_unit,
  i.failure_pattern_name,
  i.servicenow_ticket_count,
  i.servicenow_duplicate_tickets,
  ROUND(i.servicenow_duplicate_tickets * 100.0 / NULLIF(i.servicenow_ticket_count, 0), 1) as duplicate_pct,
  i.affected_user_count,
  i.affected_roles,
  i.root_cause_explanation,
  i.downstream_impact_narrative,
  i.revenue_impact_usd,
  i.productivity_loss_usd,
  i.shipments_delayed,
  i.severity,
  i.root_service,
  i.domain,
  i.mttr_minutes,
  i.created_at
FROM jnj_eo_demo.eo_analytics_plane.bronze_incidents i
WHERE i.servicenow_ticket_count > 0;
