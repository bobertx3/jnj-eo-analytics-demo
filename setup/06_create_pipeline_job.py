"""
06_create_pipeline_job.py
Creates a Databricks Workflow (Jobs API) for the Enterprise RCA Intelligence pipeline.

Pipeline tasks (in sequence):
  1. ingest_metrics      - Read protobuf .pb files -> bronze_metrics (Python)
  2. ingest_logs         - Read JSONL logs -> bronze_logs (SQL)
  3. ingest_traces       - Read JSON traces -> bronze_traces (SQL)
  4. ingest_events       - Read incidents/alerts/changes JSONL -> bronze tables (SQL)
  5. ingest_network_flows - Generate network flow data -> bronze_network_flows (SQL)
  6. build_silver        - Silver transforms (depends on 1-5) (SQL)
  7. build_gold          - Gold transforms (depends on 6) (SQL)

Uses serverless compute for SQL tasks and the existing jnj_eo_demo-warehouse for SQL statements.
"""
import os
import json
import time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service import jobs, compute

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
CATALOG = "jnj_eo_demo"
SCHEMA = "eo_analytics_plane"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_landing"
WAREHOUSE_ID = "08381690ac2b0e1a"
JOB_NAME = "jnj-eo-analytics-demo-pipeline"

# Workspace path where notebooks/scripts will be uploaded
WORKSPACE_NOTEBOOK_PATH = "/Shared/eo_analytics_plane/jnj-eo-analytics-demo/pipeline_tasks"


def read_sql_file(filename):
    """Read a SQL file from pipeline_tasks directory."""
    path = Path(__file__).parent / "pipeline_tasks" / filename
    return path.read_text()


def upload_notebook(w, local_path, workspace_path):
    """Upload a Python file as a Databricks notebook."""
    import base64
    with open(local_path, "rb") as f:
        content = f.read()

    # Check if it uses # COMMAND ---------- separators (notebook format)
    text = content.decode("utf-8")

    try:
        w.workspace.mkdirs(os.path.dirname(workspace_path))
    except Exception:
        pass

    # Upload as a SOURCE format file
    w.workspace.import_(
        path=workspace_path,
        content=base64.b64encode(content).decode("utf-8"),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    print(f"  Uploaded notebook: {workspace_path}")


def upload_sql_as_notebook(w, sql_content, workspace_path, description=""):
    """Upload SQL content as a Databricks SQL notebook."""
    import base64

    # Wrap SQL in notebook format with -- COMMAND ---------- separators
    # Split on statement boundaries (the === comment blocks)
    statements = []
    current_stmt = []
    for line in sql_content.split("\n"):
        if line.startswith("-- ====") and current_stmt:
            stmt_text = "\n".join(current_stmt).strip()
            if stmt_text:
                statements.append(stmt_text)
            current_stmt = [line]
        else:
            current_stmt.append(line)
    if current_stmt:
        stmt_text = "\n".join(current_stmt).strip()
        if stmt_text:
            statements.append(stmt_text)

    # Build notebook content with COMMAND separators
    notebook_content = f"-- Databricks notebook source\n-- MAGIC %md\n-- MAGIC # {description}\n\n"
    for stmt in statements:
        notebook_content += f"-- COMMAND ----------\n\n{stmt}\n\n"

    try:
        w.workspace.mkdirs(os.path.dirname(workspace_path))
    except Exception:
        pass

    w.workspace.import_(
        path=workspace_path,
        content=base64.b64encode(notebook_content.encode("utf-8")).decode("utf-8"),
        format=ImportFormat.SOURCE,
        language=Language.SQL,
        overwrite=True,
    )
    print(f"  Uploaded SQL notebook: {workspace_path}")


def create_pipeline_job(w):
    """Create the Databricks workflow job with all pipeline tasks."""

    # ── Upload pipeline task notebooks ──────────────────────────────

    # 1. Upload ingest_metrics_pb.py notebook
    metrics_nb_path = Path(__file__).parent / "pipeline_tasks" / "ingest_metrics_pb.py"
    upload_notebook(w, str(metrics_nb_path), f"{WORKSPACE_NOTEBOOK_PATH}/ingest_metrics_pb")

    # 2-5. Create SQL notebooks for ingestion tasks
    # Logs ingestion SQL
    logs_sql = f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_logs AS
WITH raw AS (
  SELECT explode(resourceLogs) as rl
  FROM json.`{VOLUME_PATH}/logs/`
),
with_scope AS (
  SELECT rl.resource.attributes as resource_attrs, explode(rl.scopeLogs) as sl FROM raw
),
with_record AS (
  SELECT resource_attrs, sl.scope.name as scope_name, explode(sl.logRecords) as lr FROM with_scope
)
SELECT
  get(filter(resource_attrs, x -> x.key = 'service.name'), 0).value.stringValue as service_name,
  get(filter(resource_attrs, x -> x.key = 'deployment.environment'), 0).value.stringValue as environment,
  get(filter(resource_attrs, x -> x.key = 'cloud.region'), 0).value.stringValue as region,
  get(filter(resource_attrs, x -> x.key = 'host.name'), 0).value.stringValue as host_name,
  scope_name,
  lr.severityNumber as severity_number,
  lr.severityText as severity_text,
  lr.body.stringValue as log_message,
  lr.traceId as trace_id,
  lr.spanId as span_id,
  get(filter(lr.attributes, x -> x.key = 'failure_pattern'), 0).value.stringValue as failure_pattern_id,
  CAST(lr.timeUnixNano AS BIGINT) as timestamp_unix_nano,
  to_timestamp(CAST(lr.timeUnixNano AS BIGINT) / 1000000000) as event_timestamp,
  current_timestamp() as ingested_at
FROM with_record;
"""
    upload_sql_as_notebook(w, logs_sql, f"{WORKSPACE_NOTEBOOK_PATH}/ingest_logs", "Ingest Logs from JSONL")

    # Traces ingestion SQL
    traces_sql = f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_traces AS
WITH raw AS (
  SELECT explode(resourceSpans) as rs FROM json.`{VOLUME_PATH}/traces/`
),
with_scope AS (SELECT explode(rs.scopeSpans) as ss FROM raw),
with_span AS (SELECT explode(ss.spans) as span FROM with_scope)
SELECT
  span.traceId as trace_id, span.spanId as span_id, span.parentSpanId as parent_span_id,
  span.name as operation_name, span.kind as span_kind,
  CAST(span.startTimeUnixNano AS BIGINT) as start_time_unix_nano,
  CAST(span.endTimeUnixNano AS BIGINT) as end_time_unix_nano,
  (CAST(span.endTimeUnixNano AS BIGINT) - CAST(span.startTimeUnixNano AS BIGINT)) / 1000000.0 as duration_ms,
  span.status.code as status_code, span.status.message as status_message,
  get(filter(span.attributes, x -> x.key = 'service.name'), 0).value.stringValue as service_name,
  get(filter(span.attributes, x -> x.key = 'http.status_code'), 0).value.intValue as http_status_code,
  get(filter(span.attributes, x -> x.key = 'peer.service'), 0).value.stringValue as peer_service,
  get(filter(span.resource.attributes, x -> x.key = 'service.name'), 0).value.stringValue as resource_service_name,
  get(filter(span.resource.attributes, x -> x.key = 'deployment.environment'), 0).value.stringValue as environment,
  get(filter(span.resource.attributes, x -> x.key = 'cloud.region'), 0).value.stringValue as region,
  to_timestamp(CAST(span.startTimeUnixNano AS BIGINT) / 1000000000) as event_timestamp,
  current_timestamp() as ingested_at
FROM with_span;
"""
    upload_sql_as_notebook(w, traces_sql, f"{WORKSPACE_NOTEBOOK_PATH}/ingest_traces", "Ingest Traces from JSON")

    # Events ingestion SQL (incidents + alerts + changes)
    events_sql = f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_incidents AS
SELECT incident_id, title, description, severity, status,
  CAST(created_at AS TIMESTAMP) as created_at, CAST(resolved_at AS TIMESTAMP) as resolved_at,
  CAST(mttr_minutes AS INT) as mttr_minutes, root_service, impacted_services,
  CAST(blast_radius AS INT) as blast_radius, domain, failure_pattern_id, failure_pattern_name,
  environment, region, CAST(revenue_impact_usd AS DOUBLE) as revenue_impact_usd,
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
FROM json.`{VOLUME_PATH}/events/incidents.jsonl`;

-- COMMAND ----------

CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_alerts AS
SELECT alert_id, incident_id, service, alert_name, severity,
  CAST(fired_at AS TIMESTAMP) as fired_at, CAST(resolved_at AS TIMESTAMP) as resolved_at,
  CAST(threshold_value AS DOUBLE) as threshold_value, CAST(actual_value AS DOUBLE) as actual_value,
  domain, environment, current_timestamp() as ingested_at
FROM json.`{VOLUME_PATH}/events/alerts.jsonl`;

-- COMMAND ----------

CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_topology_changes AS
SELECT change_id, service, change_type, description,
  CAST(executed_at AS TIMESTAMP) as executed_at, executed_by, risk_level,
  CAST(rollback_available AS BOOLEAN) as rollback_available,
  domain, environment, region, current_timestamp() as ingested_at
FROM json.`{VOLUME_PATH}/events/topology_changes.jsonl`;
"""
    upload_sql_as_notebook(w, events_sql, f"{WORKSPACE_NOTEBOOK_PATH}/ingest_events", "Ingest Events (Incidents, Alerts, Changes)")

    # Network flows SQL
    network_sql = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.bronze_network_flows (
  flow_id STRING, event_timestamp TIMESTAMP, src_ip STRING, src_port INT,
  dst_ip STRING, dst_port INT, protocol STRING, bytes_sent BIGINT, bytes_received BIGINT,
  packets_sent BIGINT, packets_received BIGINT, latency_us BIGINT, retransmits INT,
  src_service STRING, dst_service STRING, src_zone STRING, dst_zone STRING,
  direction STRING, connection_reset BOOLEAN, timeout BOOLEAN, tls_version STRING,
  dns_query STRING, dns_response_code INT, load_balancer_id STRING,
  environment STRING, region STRING, ingested_at TIMESTAMP
);

-- COMMAND ----------

TRUNCATE TABLE {CATALOG}.{SCHEMA}.bronze_network_flows;

-- COMMAND ----------

INSERT INTO {CATALOG}.{SCHEMA}.bronze_network_flows
SELECT uuid() as flow_id,
  TIMESTAMP '2025-08-29' + make_interval(0,0,0,CAST(floor(rand()*180) AS INT),CAST(floor(rand()*24) AS INT),CAST(floor(rand()*60) AS INT),CAST(floor(rand()*60) AS INT)) as event_timestamp,
  '10.1.0.10', CAST(32768+floor(rand()*32767) AS INT), '10.1.1.11', 3000,
  (CASE WHEN rand()<0.5 THEN 'TCP' WHEN rand()<0.8 THEN 'HTTP' ELSE 'gRPC' END),
  CAST(100+floor(rand()*50000) AS BIGINT), CAST(200+floor(rand()*100000) AS BIGINT),
  CAST(5+floor(rand()*500) AS BIGINT), CAST(5+floor(rand()*500) AS BIGINT),
  CAST(500+floor(rand()*50000) AS BIGINT), CAST(floor(rand()*5) AS INT),
  'load-balancer','patient-portal','dmz','internal','ingress',
  CASE WHEN rand()<0.02 THEN true ELSE false END, CASE WHEN rand()<0.01 THEN true ELSE false END,
  (CASE WHEN rand()<0.6 THEN 'TLS1.3' ELSE 'TLS1.2' END), '', 0, 'lb-prod-01', 'prod',
  (CASE WHEN rand()<0.5 THEN 'us-east-1' ELSE 'us-west-2' END), current_timestamp()
FROM range(500)
UNION ALL
SELECT uuid(), TIMESTAMP '2025-08-29' + make_interval(0,0,0,CAST(floor(rand()*180) AS INT),CAST(floor(rand()*24) AS INT),CAST(floor(rand()*60) AS INT),CAST(floor(rand()*60) AS INT)),
  '10.1.1.10', CAST(32768+floor(rand()*32767) AS INT), '10.1.4.10', 5432,
  (CASE WHEN rand()<0.5 THEN 'TCP' WHEN rand()<0.8 THEN 'HTTP' ELSE 'gRPC' END),
  CAST(100+floor(rand()*50000) AS BIGINT), CAST(200+floor(rand()*100000) AS BIGINT),
  CAST(5+floor(rand()*500) AS BIGINT), CAST(5+floor(rand()*500) AS BIGINT),
  CAST(500+floor(rand()*50000) AS BIGINT), CAST(floor(rand()*5) AS INT),
  'ehr-api','ehr-database','internal','data','internal',
  CASE WHEN rand()<0.02 THEN true ELSE false END, CASE WHEN rand()<0.01 THEN true ELSE false END,
  (CASE WHEN rand()<0.6 THEN 'TLS1.3' ELSE 'TLS1.2' END), '', 0, '', 'prod',
  (CASE WHEN rand()<0.5 THEN 'us-east-1' ELSE 'us-west-2' END), current_timestamp()
FROM range(500);
"""
    upload_sql_as_notebook(w, network_sql, f"{WORKSPACE_NOTEBOOK_PATH}/ingest_network_flows", "Ingest Network Flows")

    # 6. Silver transforms notebook
    silver_sql = read_sql_file("silver_transforms.sql")
    upload_sql_as_notebook(w, silver_sql, f"{WORKSPACE_NOTEBOOK_PATH}/silver_transforms", "Silver Layer Transforms")

    # 7. Gold transforms notebook
    gold_sql = read_sql_file("gold_transforms.sql")
    upload_sql_as_notebook(w, gold_sql, f"{WORKSPACE_NOTEBOOK_PATH}/gold_transforms", "Gold Layer Transforms")

    # ── Create the Job ──────────────────────────────────────────────
    print("\n  Creating Databricks workflow job ...")

    job_settings = build_job_settings()

    # Check if job already exists
    existing_jobs = list(w.jobs.list(name=JOB_NAME))
    if existing_jobs:
        job_id = existing_jobs[0].job_id
        print(f"  Job already exists (ID: {job_id}), updating ...")
        w.jobs.reset(job_id, new_settings=job_settings)
        return job_id

    job = w.jobs.create(
        name=job_settings.name,
        tasks=job_settings.tasks,
        tags=job_settings.tags,
        schedule=job_settings.schedule,
        max_concurrent_runs=job_settings.max_concurrent_runs,
    )
    return job.job_id


def build_job_settings():
    """Build the job settings using SDK dataclasses."""
    return jobs.JobSettings(
        name=JOB_NAME,
        tags={
            "project": "jnj-eo-analytics-demo",
            "team": "hls-platform",
            "env": "prod",
        },
        schedule=jobs.CronSchedule(
            quartz_cron_expression="0 0 2 * * ?",
            timezone_id="UTC",
            pause_status=jobs.PauseStatus.PAUSED,
        ),
        max_concurrent_runs=1,
        tasks=[
            jobs.Task(
                task_key="ingest_metrics",
                description="Read OTLP protobuf metrics from volume and write to bronze_metrics",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{WORKSPACE_NOTEBOOK_PATH}/ingest_metrics_pb",
                    source=jobs.Source.WORKSPACE,
                ),
                new_cluster=compute.ClusterSpec(
                    spark_version="15.4.x-scala2.12",
                    num_workers=0,
                    node_type_id="i3.xlarge",
                    data_security_mode=compute.DataSecurityMode.SINGLE_USER,
                ),
            ),
            jobs.Task(
                task_key="ingest_logs",
                description="Ingest JSONL logs into bronze_logs",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{WORKSPACE_NOTEBOOK_PATH}/ingest_logs",
                    source=jobs.Source.WORKSPACE,
                ),
            ),
            jobs.Task(
                task_key="ingest_traces",
                description="Ingest JSON traces into bronze_traces",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{WORKSPACE_NOTEBOOK_PATH}/ingest_traces",
                    source=jobs.Source.WORKSPACE,
                ),
            ),
            jobs.Task(
                task_key="ingest_events",
                description="Ingest incidents, alerts, and topology changes into bronze tables",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{WORKSPACE_NOTEBOOK_PATH}/ingest_events",
                    source=jobs.Source.WORKSPACE,
                ),
            ),
            jobs.Task(
                task_key="ingest_network_flows",
                description="Generate and ingest network flow data into bronze_network_flows",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{WORKSPACE_NOTEBOOK_PATH}/ingest_network_flows",
                    source=jobs.Source.WORKSPACE,
                ),
            ),
            jobs.Task(
                task_key="build_silver",
                description="Build silver enriched tables from bronze data",
                depends_on=[
                    jobs.TaskDependency(task_key="ingest_metrics"),
                    jobs.TaskDependency(task_key="ingest_logs"),
                    jobs.TaskDependency(task_key="ingest_traces"),
                    jobs.TaskDependency(task_key="ingest_events"),
                    jobs.TaskDependency(task_key="ingest_network_flows"),
                ],
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{WORKSPACE_NOTEBOOK_PATH}/silver_transforms",
                    source=jobs.Source.WORKSPACE,
                ),
            ),
            jobs.Task(
                task_key="build_gold",
                description="Build gold analytics tables from silver data",
                depends_on=[
                    jobs.TaskDependency(task_key="build_silver"),
                ],
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{WORKSPACE_NOTEBOOK_PATH}/gold_transforms",
                    source=jobs.Source.WORKSPACE,
                ),
            ),
        ],
    )


def main():
    w = WorkspaceClient(profile=PROFILE)
    print("Creating Enterprise RCA Intelligence pipeline job ...")
    print(f"  Job name: {JOB_NAME}")
    print(f"  Warehouse ID: {WAREHOUSE_ID}")
    print(f"  Notebook path: {WORKSPACE_NOTEBOOK_PATH}")
    print()

    job_id = create_pipeline_job(w)

    host = w.config.host.rstrip("/")
    job_url = f"{host}/#job/{job_id}"
    print(f"\n  Pipeline job created successfully!")
    print(f"  Job ID: {job_id}")
    print(f"  Job URL: {job_url}")
    print(f"  Schedule: Daily at 2:00 AM UTC (PAUSED -- enable when ready)")
    print(f"\n  Tasks:")
    print(f"    1. ingest_metrics      (Python notebook -- protobuf decoder)")
    print(f"    2. ingest_logs         (SQL -- JSONL logs)")
    print(f"    3. ingest_traces       (SQL -- JSON traces)")
    print(f"    4. ingest_events       (SQL -- incidents/alerts/changes)")
    print(f"    5. ingest_network_flows (SQL -- synthetic network flows)")
    print(f"    6. build_silver        (SQL -- 5 silver tables, depends on 1-5)")
    print(f"    7. build_gold          (SQL -- 4 gold tables, depends on 6)")


if __name__ == "__main__":
    main()
