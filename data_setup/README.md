# Data setup — synthetic data generation

This folder is for **creating and landing raw data** into the Unity Catalog volume. It is separate from the pipeline: the pipeline only reads from the volume and builds bronze → silver → gold.

Run these scripts when you need to **populate or refresh** the raw landing volume. Data is written only when the volume is empty (existing data is not overwritten).

## Order

1. **00_create_schema_and_volume.py** — Create catalog schema and volume with subdirs: `metrics`, `logs`, `traces`, `events`, `network_flows`.
2. **01_generate_raw_telemetry.py** — Generate OTLP metrics (.pb), logs (.jsonl), traces (.json), and events (.jsonl) into the volume.
3. **02_generate_protobuf_network_flows.py** — Generate network flow data (.pb) into the volume.

## Commands

```bash
# From repo root, with DATABRICKS_PROFILE set as needed
python data_setup/00_create_schema_and_volume.py
python data_setup/01_generate_raw_telemetry.py
python data_setup/02_generate_protobuf_network_flows.py
```

## Adding more synthetic data

You can add new scripts here (e.g. `03_my_custom_data.py`) that write into the same volume paths or new subdirs. The pipeline will pick up whatever is in the volume when it runs. Keep the same conventions:

- **metrics/** — OTLP protobuf `.pb` files (and `otlp_metrics.proto` schema).
- **logs/** — JSONL log files.
- **traces/** — JSON trace files.
- **events/** — JSONL for incidents, alerts, topology changes.
- **network_flows/** — Protobuf `.pb` files (and `network_flow.proto` schema).

Scripts 01 and 02 skip writing if their target subdirs already contain data, so re-running is safe.
