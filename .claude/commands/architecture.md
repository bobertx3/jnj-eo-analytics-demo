Analyze this repository and generate a thorough `ARCHITECTURE.md` file in the root of the repo.

Before writing, explore the codebase to understand it fully:
- Read the root directory structure
- Read any existing `CLAUDE.md`, `README.md`, `pyproject.toml`, `package.json`, `databricks.yml`, or similar config files
- Read key source files (routers, entry points, main components, config/settings files)
- Identify frameworks, services, and external dependencies actually used

Write `ARCHITECTURE.md` using this structure and style (adapt sections to what actually exists in the repo — omit sections that don't apply, add new ones if needed):

---

# Architecture

## System Overview

A 1–3 sentence plain-English description of what the system does and its main components (e.g. data pipeline + web app, CLI tool, API service, etc.).

---

## Request Flow

ASCII diagram showing how a request moves through the system end-to-end. Use `│`, `▼`, `├──`, `└──` box-drawing characters. Show the key steps inline as labels.

---

## Component Details

One `###` subsection per major component (frontend, backend, data pipeline, etc.). For each:
- **Framework/language** used
- **Key files** and what they do
- A table of config fields if the component has a config/settings class
- A short code block showing the main logic flow if helpful

---

## Infrastructure / External Services

List any cloud resources, databases, APIs, or managed services the app depends on. Use nested code blocks for hierarchical layouts (e.g. Unity Catalog, S3 bucket structure). Include endpoint names, model names, URLs, and access requirements where known.

---

## Data Pipeline (if applicable)

ASCII diagram of the pipeline stages with a one-line description of what each stage does.

---

## Deployment

One subsection per deployment target. Show the exact commands needed to build and deploy each component. Note what config file drives each deployment.

---

## Directory Structure

Annotated directory tree using backtick code block. Include every meaningful file/folder with a short comment explaining its purpose. Skip `node_modules`, `.venv`, build artifacts, and generated files.

---

Style rules:
- Use `**bold**` for framework/tool names on first mention
- Keep descriptions concise — prefer a short phrase over a full sentence
- Use tables for config/env vars with columns: Field | Default | Purpose
- Do not include speculative or aspirational content — only document what actually exists
- Match the tone and density of a staff-engineer-written doc: precise, scannable, no fluff
