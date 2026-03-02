# Pipeline setup — volume → bronze → silver → gold

This folder contains the **data pipeline**: it reads from the Unity Catalog volume (populated by `data_setup/`) and builds bronze, silver, and gold tables, plus Genie and app permissions.

The pipeline does **not** generate synthetic data. Raw data must already exist in the volume (see **data_setup/**).

## Pipeline flow

1. **Load from volume → bronze** — Ingest metrics, logs, traces, events, network flows from the volume into bronze Delta tables.
2. **Bronze → silver** — Enrich and join bronze into silver tables (incidents, alerts, changes, service health, business impact, etc.).
3. **Silver → gold** — Aggregate silver into gold analytics tables (root cause patterns, service risk ranking, change correlation, domain impact, business impact summary).
4. **Genie space** — Create the Genie Space for natural language Q&A over gold/silver tables.
5. **Permissions** — Grant the app access to UC resources.

## Scripts (run in order)

| Script | Purpose |
|--------|---------|
| **03_create_bronze_tables.py** | Create bronze tables and load from volume (one-time or refresh). |
| **04_create_silver_tables.py** | Create silver tables from bronze. |
| **05_create_gold_tables.py** | Create gold tables from silver. |
| **06_create_pipeline_job.py** | Create the Databricks Job that runs the pipeline (volume → bronze → silver → gold) on a schedule. |
| **07_create_genie_space.py** | Create the Genie Space; add tables via the UI. |
| **08_grant_app_uc_permissions.py** | Grant the app permissions on catalog/schema/warehouse. |

## Pipeline job (pipeline_tasks/)

The job defined in **06** runs the notebooks in **pipeline_tasks/**:

- **ingest_metrics_pb.py** — Volume metrics (.pb) → bronze_metrics
- **ingest_logs**, **ingest_traces**, **ingest_events** (SQL) — Volume → bronze_logs, bronze_traces, bronze_incidents/alerts/topology_changes
- **ingest_network_flows** (SQL) — Volume → bronze_network_flows
- **silver_transforms.sql** — Bronze → silver
- **gold_transforms.sql** — Silver → gold

All table writes are overwrite/replace, so the pipeline is idempotent.

## Commands

```bash
# From repo root
python setup/03_create_bronze_tables.py
python setup/04_create_silver_tables.py
python setup/05_create_gold_tables.py
python setup/06_create_pipeline_job.py   # optional: deploy the scheduled job
python setup/07_create_genie_space.py
python setup/08_grant_app_uc_permissions.py
```

Or deploy the app + job via the bundle: `databricks bundle deploy --target prod`.
