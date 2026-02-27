# Enterprise RCA Intelligence Demo Talk Track

This version is built for a VP audience: business-first, plain English, and one consistent storyline.

## Core Storyline (Use This Throughout)

We are tracing one issue from symptom to business impact:
- The environment shows chronic stress around the SAP integration path (`erp-sap-connector`).
- The acute business-impact event is an app-layer failure on `check-inventory-api` when SAP calls time out.
- That disruption propagates to order and shipment workflows, increasing delayed fulfillment risk.
- The platform value is that it lets us quickly answer three executive questions:
  1. What is the likely root-cause chain?
  2. What is the blast radius (who else is affected)?
  3. What is the revenue impact and why?

Revenue impact (concise explanation):
- At incident level, the model uses a severity-weighted impact multiplied by blast radius.
- At dashboard level, values are summed across incidents to show cumulative business exposure.

## 1) Executive Dashboard

![Executive Dashboard](images/executive-dashboard.png)

**Overview**
- Enterprise KPI landing page: incident volume, severity, MTTR, revenue impact, patient impact, and SLA performance.

**Storyline narrative (what to say)**
- "Start with business impact: we have meaningful cumulative revenue exposure, and the top systemic issue points at the SAP integration path."
- "This tells leadership where to focus first without reading technical logs."
- "From here, we pivot from KPI signal to root-cause evidence and blast radius."

**Key visual callouts**
- Top KPI cards (incidents, MTTR, revenue impact, SLA breaches).
- Top systemic issue callout card.
- Incident timeline and domain pie chart.
- Domain impact summary + ServiceNow duplicate-ticket table.

## 2) Root Cause Intelligence

![Root Cause Intelligence](images/root-cause-intelligence.png)

**Overview**
- Ranked recurring failure patterns with operational and business impact dimensions.

**Storyline narrative (what to say)**
- "This is where we separate symptom from cause: SAP connector overload appears as a recurring systemic pattern."
- "We also see the high-impact API timeout pattern (`check-inventory-api` to SAP) that drives the acute shipment risk."
- "The key value: one page ties frequency, blast radius, MTTR, and revenue so we can prioritize remediation."

**Key visual callouts**
- Horizontal priority ranking chart.
- Pattern list with trend labels (improving/stable/worsening).
- Pattern detail panel and radar profile.

## 3) Service Risk Ranking

![Service Risk Ranking](images/service-risk-ranking.png)

**Overview**
- Service-level risk leaderboard and service detail trends.

**Storyline narrative (what to say)**
- "Now we convert pattern insight into service ownership: which teams should act first."
- "The SAP connector and inventory API rise to the top, showing both chronic risk and direct business sensitivity."
- "This gives an action list for engineering leaders: stabilize these services to reduce shipment and revenue risk fastest."

**Key visual callouts**
- Risk score bar chart (top services).
- Incidents vs revenue bubble chart.
- Full ranking table for operational prioritization.

## 4) Change Correlation

![Change Correlation](images/change-correlation.png)

**Overview**
- Correlates change events with incidents to identify likely triggers and risky change types.

**Storyline narrative (what to say)**
- "This page answers: did a change likely contribute to incident timing?"
- "For the SAP/inventory thread, this helps confirm whether instability is mostly load/systemic or change-induced."
- "Leadership outcome: better guardrails and release policy around high-risk changes."

**Key visual callouts**
- Changes vs incidents timeline.
- Incident rate by change type.
- Correlation strength chart and high-correlation table.
- Changes by executor summary.

## 5) Domain Deep Dive

![Domain Deep Dive](images/domain-deep-dive.png)

**Overview**
- Domain owner view (Infrastructure, Application, Network) with trends, services, alerts, and incidents.

**Storyline narrative (what to say)**
- "This is where we quantify where risk is concentrated by domain while staying tied to one business narrative."
- "For this storyline, infrastructure and application domains are the key handoff points between SAP bridge stress and API failure impact."
- "The revenue explainer keeps the model transparent: severity-weighted per-incident impact rolled up across the domain."

**Key visual callouts**
- Domain selector tiles with incident + revenue totals.
- Domain KPI strip (incidents, P1, MTTR, revenue impact, SLA breaches).
- Weekly incident and revenue/risk trend charts.
- Service and alert detail tables.

## 6) Topology Explorer

![Topology Explorer](images/topology-explorer.png)

**Overview**
- Dependency graph showing propagation paths and anomalous traffic.

**Storyline narrative (what to say)**
- "This is the blast-radius proof: from SAP integration path to inventory API, then into order and shipment services."
- "It visually explains why one API timeout can become a multi-service business event."
- "For executives, this is the fastest way to understand cascading impact without deep technical detail."

**Key visual callouts**
- Domain zones (network/application/infrastructure).
- Risk-encoded node and edge styling.
- Node detail side panel and legend.

## 7) Ask Genie

![Ask Genie](images/ask-genie.png)

**Overview**
- Natural-language investigation layer for follow-up questions and decision support.

**Storyline narrative (what to say)**
- "After the walkthrough, leadership can self-serve answers on this exact storyline."
- "Example: 'How many incidents were root cause vs impacted for check-inventory-api and erp-sap-connector?'"
- "The SQL and supporting data keep answers auditable for executive readouts."

**Key visual callouts**
- Collapsible sample question panel.
- Chat workflow with question, answer, SQL, and tabular evidence.

## Suggested Demo Flow (7-10 minutes)

1. Executive Dashboard (business exposure and top issue)
2. Root Cause Intelligence (systemic vs acute root-cause patterns)
3. Service Risk Ranking (ownership and prioritization)
4. Topology Explorer (blast radius and propagation path)
5. Domain Deep Dive (domain accountability and trend depth)
6. Change Correlation (change-risk context)
7. Ask Genie (self-serve executive Q&A)
