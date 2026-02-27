# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Enterprise Root Cause Intelligence — a Databricks App that correlates OpenTelemetry signals across infrastructure, applications, and network domains for HLS (healthcare) environments. Built for a JnJ Enterprise Observability demo.

## Architecture

- **Frontend**: React 18 SPA (Vite, JSX) in `frontend/` — Recharts for charts, Canvas for topology, Lucide icons, react-router-dom for routing
- **Backend**: FastAPI in `backend/` — serves the API at `/api/*` and the built React SPA as static files
- **Data**: Databricks SQL Warehouse via `databricks-sdk` — queries Unity Catalog Delta tables in `bx4.eo_analytics_plane` (Bronze/Silver/Gold medallion)
- **AI**: Databricks Foundation Model API for root cause analysis; Genie Space for natural language queries
- **Deployment**: Databricks Apps via `databricks.yml` (DABs bundle), `app.yaml` for runtime config

### Backend Structure

- `app.py` — Uvicorn entry point, imports `backend.main:app`
- `backend/main.py` — FastAPI app setup, CORS, router registration, SPA catch-all
- `backend/db.py` — Databricks SQL connection (auto-detects local dev via profile vs deployed App via service principal). Hardcoded `CATALOG = "bx4"`, `SCHEMA = "eo_analytics_plane"`
- `backend/routes/` — API route modules: `incidents`, `root_cause`, `service_ranking`, `change_correlation`, `domain_summary`, `genie`

### Frontend Structure

- `frontend/src/App.jsx` — Layout with sidebar nav + react-router Routes
- `frontend/src/pages/` — Page components matching routes (ExecutiveDashboard, RootCauseIntelligence, ServiceRiskRanking, ChangeCorrelation, DomainDeepDive, TopologyExplorer, GenieChat)
- `frontend/src/components/` — Shared UI (ChartTooltip, InfoExpander, LoadingState, SeverityBadge) + page-specific components
- `frontend/src/hooks/useApi.js` — Custom fetch hook for API calls
- Vite dev server proxies `/api` to `localhost:8000`

## Development Commands

```bash
# Backend
pip install -r requirements.txt
DATABRICKS_PROFILE=DEFAULT python app.py          # Start backend on :8000

# Frontend (separate terminal)
cd frontend && npm install
cd frontend && npm run dev                     # Vite dev server on :5173 (proxies /api to :8000)
cd frontend && npm run build                   # Build to frontend/dist/ for production

# Production-like local run (backend serves built frontend)
cd frontend && npm run build && cd ..
DATABRICKS_PROFILE=DEFAULT python app.py
```

## Data Pipeline Setup

The `setup/` directory contains numbered scripts (00-07) that must run in order to populate the Databricks tables. `setup/pipeline_tasks/` contains notebook versions of the same transforms for the Databricks Job.

```bash
python setup/00_create_schema_and_volume.py    # Create catalog schema + volume
python setup/01_generate_raw_telemetry.py      # Generate 30 days of synthetic OTLP data
python setup/02_generate_protobuf_network_flows.py
python setup/03_create_bronze_tables.py
python setup/04_create_silver_tables.py
python setup/05_create_gold_tables.py
```

## Deployment

```bash
databricks bundle deploy --target prod         # Deploy app + pipeline job via DABs
databricks apps deploy jnj-eo-analytics-demo --profile DEFAULT
```

## Key Conventions

- `frontend/dist/` is kept in version control (not gitignored) for deployment — always rebuild before deploying
- Local dev uses `DATABRICKS_PROFILE=DEFAULT` env var; deployed App auto-authenticates via service principal
- The `DATABRICKS_WAREHOUSE_ID` env var is set via `app.yaml` resource binding in production
- No test framework is configured for either frontend or backend
