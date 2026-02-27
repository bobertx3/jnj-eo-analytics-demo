-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ingest Events (Incidents, Alerts, Changes)

-- COMMAND ----------

CREATE OR REPLACE TABLE jnj_eo_demo.eo_analytics_plane.bronze_incidents AS
SELECT
  incident_id,
  title,
  description,
  severity,
  status,
  CAST(created_at AS TIMESTAMP) as created_at,
  CAST(resolved_at AS TIMESTAMP) as resolved_at,
  CAST(mttr_minutes AS INT) as mttr_minutes,
  root_service,
  impacted_services,
  CAST(blast_radius AS INT) as blast_radius,
  domain,
  failure_pattern_id,
  failure_pattern_name,
  environment,
  region,
  CAST(revenue_impact_usd AS DOUBLE) as revenue_impact_usd,
  CAST(sla_breached AS BOOLEAN) as sla_breached,
  business_unit,
  CAST(affected_user_count AS INT) as affected_user_count,
  CAST(affected_user_count AS INT) as patient_impact_count,
  affected_roles,
  CAST(productivity_loss_hours AS DOUBLE) as productivity_loss_hours,
  CAST(productivity_loss_usd AS DOUBLE) as productivity_loss_usd,
  CAST(shipments_delayed AS INT) as shipments_delayed,
  CAST(servicenow_ticket_count AS INT) as servicenow_ticket_count,
  CAST(servicenow_duplicate_tickets AS INT) as servicenow_duplicate_tickets,
  downstream_impact_narrative,
  root_cause_explanation,
  revenue_model,
  current_timestamp() as ingested_at
FROM json.`/Volumes/jnj_eo_demo/eo_analytics_plane/raw_landing/events/incidents.jsonl`;

-- COMMAND ----------

CREATE OR REPLACE TABLE jnj_eo_demo.eo_analytics_plane.bronze_alerts AS
SELECT
  alert_id,
  incident_id,
  service,
  alert_name,
  severity,
  CAST(fired_at AS TIMESTAMP) as fired_at,
  CAST(resolved_at AS TIMESTAMP) as resolved_at,
  CAST(threshold_value AS DOUBLE) as threshold_value,
  CAST(actual_value AS DOUBLE) as actual_value,
  domain,
  environment,
  current_timestamp() as ingested_at
FROM json.`/Volumes/jnj_eo_demo/eo_analytics_plane/raw_landing/events/alerts.jsonl`;

-- COMMAND ----------

CREATE OR REPLACE TABLE jnj_eo_demo.eo_analytics_plane.bronze_topology_changes AS
SELECT
  change_id,
  service,
  change_type,
  description,
  CAST(executed_at AS TIMESTAMP) as executed_at,
  executed_by,
  risk_level,
  CAST(rollback_available AS BOOLEAN) as rollback_available,
  domain,
  environment,
  region,
  current_timestamp() as ingested_at
FROM json.`/Volumes/jnj_eo_demo/eo_analytics_plane/raw_landing/events/topology_changes.jsonl`;
