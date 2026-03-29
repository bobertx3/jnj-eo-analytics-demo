# Setup Pipeline

End-to-end data setup: create the schema/volume, generate synthetic data, build the medallion tables, configure Genie, and grant app permissions. Run scripts in order.

## Scripts

| # | Script | Phase | Purpose |
|---|--------|-------|---------|
| 00 | `00_create_schema_and_volume.py` | Schema | Create UC schema + managed volume with subdirs |
| 00b | `00b_load_static_data.py` | Data load | Copy repo `static_data/` into UC volume raw_landing |
| 01 | `01_generate_raw_telemetry.py` | Data gen | Generate OTLP metrics/logs/traces/events (skips if volume non-empty) |
| 02 | `02_generate_protobuf_network_flows.py` | Data gen | Generate network flow .pb files (skips if volume non-empty) |
| 03 | `03_create_bronze_tables.py` | Pipeline | Volume → bronze Delta tables |
| 04 | `04_create_silver_tables.py` | Pipeline | Bronze → silver (enrichment, scoring, correlation) |
| 05 | `05_create_gold_tables.py` | Pipeline | Silver → gold (analytics aggregations) |
| 06 | `06_create_genie_space.py` | Config | Create Genie Space (add tables via UI after) |
| 07 | `07_grant_app_uc_permissions.py` | Config | Grant app service principal SELECT on all tables |

## Usage

```bash
# From repo root, with DATABRICKS_PROFILE set or using the DEFAULT profile
python setup_pipeline/00_create_schema_and_volume.py
python setup_pipeline/00b_load_static_data.py
python setup_pipeline/01_generate_raw_telemetry.py
python setup_pipeline/02_generate_protobuf_network_flows.py
python setup_pipeline/03_create_bronze_tables.py
python setup_pipeline/04_create_silver_tables.py
python setup_pipeline/05_create_gold_tables.py
python setup_pipeline/06_create_genie_space.py
python setup_pipeline/07_grant_app_uc_permissions.py
```

Scripts 01-02 skip if volume already has data. Scripts 03-05 overwrite tables (idempotent).

## Proto schemas

| File | Used by | Purpose |
|------|---------|---------|
| `otlp_metrics.proto` | `01_generate_raw_telemetry.py` | Defines the OTLP MetricsData wire format used to serialize synthetic metrics into `.pb` files. Also read at ingestion time by `data_pipelines/01_ingest_metrics_pb.py` to decode field numbers. |
| `network_flow.proto` | `02_generate_protobuf_network_flows.py` | Documents the custom `NFLOW` binary format used to encode network flow records into `.pb` files. Script 02 also uploads this file to the volume alongside the data. |
