# Enterprise Root Cause Intelligence

**Correlating signals across domains and time to reveal systemic causes, not just incidents.**

An enterprise-wide observability platform built on Databricks, ingesting OpenTelemetry signals across infrastructure, applications, and network domains for healthcare/HLS environments.

## Architecture

- **Frontend**: React 18 with Recharts/Canvas visualizations
- **Backend**: FastAPI with Databricks SQL Warehouse connectivity
- **Data**: Unity Catalog Delta tables (Bronze/Silver/Gold medallion architecture)
- **AI**: Databricks Foundation Model API for explainable root cause analysis
- **Auth**: Databricks OAuth / Service Principal

## Quick Start

### 1. Setup Data Pipeline

```bash
# Create schema and volume
python setup/00_create_schema_and_volume.py

# Generate 180 days of realistic HLS telemetry
python setup/01_generate_raw_telemetry.py
python setup/02_generate_protobuf_network_flows.py

# Build Bronze tables (raw parsed)
python setup/03_create_bronze_tables.py

# Build Silver tables (enriched)
python setup/04_create_silver_tables.py

# Build Gold tables (analytics-ready)
python setup/05_create_gold_tables.py
```

### 2. Run Locally

```bash
# Install backend
pip install -r requirements.txt

# Install and build frontend
cd frontend && npm install && npm run build && cd ..

# Start server
DATABRICKS_PROFILE=hls python app.py
```

### 3. Deploy to Databricks

```bash
databricks apps create enterprise-rca-intelligence --profile hls
databricks sync . /Workspace/Users/<your-email>/enterprise-rca-intelligence --profile hls
databricks apps deploy enterprise-rca-intelligence \
  --source-code-path /Workspace/Users/<your-email>/enterprise-rca-intelligence \
  --profile hls
```

## Data Model

| Layer | Table | Description |
|-------|-------|-------------|
| Bronze | bronze_metrics | Parsed OTLP metrics |
| Bronze | bronze_logs | Parsed structured logs |
| Bronze | bronze_traces | Parsed distributed trace spans |
| Bronze | bronze_incidents | Raw incident records |
| Bronze | bronze_alerts | Raw alert records |
| Bronze | bronze_topology_changes | Raw change events |
| Bronze | bronze_network_flows | Parsed network flow data |
| Silver | silver_incidents | Enriched with root cause candidates |
| Silver | silver_alerts | Correlated with incident linkage |
| Silver | silver_changes | Risk-scored changes |
| Silver | silver_service_health | Per-service daily health scores |
| Silver | silver_business_impact | Revenue/patient impact events |
| Gold | gold_root_cause_patterns | Recurring failure pattern signatures |
| Gold | gold_service_risk_ranking | Services ranked by composite risk |
| Gold | gold_change_incident_correlation | Change-incident statistical correlation |
| Gold | gold_domain_impact_summary | Domain-level executive summary |

## Key Features

1. **Executive Dashboard** - Top systemic issues, domain risk heatmap, business impact trend
2. **Root Cause Intelligence** - AI-powered pattern detection with explainable insights
3. **Service Risk Ranking** - Ranked by incident frequency, blast radius, business impact
4. **Change Correlation** - Timeline showing changes vs incidents with correlation strength
5. **Domain Deep Dive** - Per-domain incident and alert explorer
6. **Topology Explorer** - Canvas-based service dependency graph with failure propagation
