# Demo Narrative — Enterprise Root Cause Intelligence

This walkthrough follows a single investigation thread: from spotting a business-impact signal on the Executive Dashboard, through root cause analysis and blast radius confirmation, to self-serve Q&A.

---

## 1. Executive Dashboard — Spot the Signal

![Executive Dashboard](images/executive-dashboard.png)

We have **30 incidents** in the last 30 days, 8 of them P1 (critical). Total revenue at risk: **$19.8M**. Average resolution time is **64.9 minutes**, with **7 SLA breaches**.

The top systemic issue jumps out immediately: **ERP SAP Connector Batch Sync Overload** — 9 occurrences, $10.2M impact, 180 users affected. The incident timeline shows these events clustering and accelerating in late February.

The domain breakdown tells us infrastructure accounts for 11 incidents and $10.7M, while application has 18 incidents and $9.0M. The ServiceNow ticket table reveals erp-sap-connector generated 27 tickets (9 duplicates) and check-inventory-api generated 8 tickets (3 duplicates) — teams are filing separate tickets for related problems without realizing it.

**What we know so far:** Something is chronically wrong with the SAP integration path, and it's getting worse.

---

## 2. Root Cause Intelligence — Identify the Patterns

![Root Cause Intelligence](images/root-cause-intelligence.png)

Instead of 30 individual incidents, the platform has grouped them into **recurring failure patterns**. The top two are both in the supply chain:

**Pattern #1 — ERP SAP Connector Batch Sync Overload** (priority score: 1,126)
- Root service: `erp-sap-connector`
- 9 occurrences over 27 days, trend: **worsening**
- Root cause: The nightly batch sync (a scheduled job that moves large volumes of order/inventory data to SAP) is colliding with the morning rush of live orders. The connector's connection pool gets maxed out and everything backs up.
- Each incident delays 70-120 shipments. Combined: **$10.2M revenue at risk**.

**Pattern #2 — check-inventory-api Network Timeout to SAP ERP** (priority score: 819)
- Root service: `check-inventory-api`
- 1 occurrence — but it was a **P1** with an **SLA breach**
- Root cause: On Feb 19, a VPC security group change blocked port 8443 from the inventory API to the SAP connector. The API couldn't reach SAP, its connection pool exhausted, and it started returning 503s to every order request.
- **601 shipments frozen, $7.5M at risk in a single incident.**

These are two independent failure modes that share one critical dependency: the SAP connector. Pattern #1 is chronic stress (the connector itself is overwhelmed). Pattern #2 is an acute break (the network path to the connector was cut). Together they account for **$17.7M** of the enterprise's $19.8M total exposure.

Click **Generate AI Analysis** on the SAP connector pattern to see the AI-generated root cause chain and remediation steps. The AI confirms the batch sync scheduling overlap and recommends staggering the sync window.

---

## 3. Service Risk Ranking — Prioritize by Service

![Service Risk Ranking](images/service-risk-ranking.png)

This view ranks every service by a composite risk score combining incident frequency, blast radius, revenue impact, and health metrics.

- **#1: erp-sap-connector** — risk score 1,198. 9 incidents as root cause, $10.2M impact, health score 75 (worst in the fleet). The health timeline shows repeated dips — each one is an outage.
- **#2: check-inventory-api** — risk score 880. Only 1 incident, but that single P1 carried $7.5M. One bad day cost more than the SAP connector's 9 combined.

The takeaway: these two services sit at the top of the enterprise risk ranking, and they're connected through the same SAP integration path.

---

## 4. Change Correlation — Find the Trigger

![Change Correlation](images/change-correlation.png)

This page answers: did someone change something that broke something else?

The platform tracks every deployment, config update, and network change, then correlates them with incidents that followed. For the Feb 19 P1 on check-inventory-api, it flagged a security group change applied to the inventory API's network shortly before the incident. The system scored the correlation automatically.

The broader view shows incident rates by change type — network and infrastructure changes carry the highest risk. This gives engineering leaders the data to set better guardrails around high-risk change windows.

---

## 5. Domain Deep Dive — Drill into Infrastructure

![Domain Deep Dive](images/domain-deep-dive.png)

Switching to the Infrastructure domain: 11 incidents, $10.7M in revenue impact, 2 SLA breaches. The weekly trend chart shows the problem **accelerating in late February** — incidents nearly every day in the final week.

The services table within infrastructure confirms `erp-sap-connector` dominates with 9 incidents and $10.2M impact, followed by `auth-service` and `sagemaker-inference-endpoint`.

Clicking any incident (e.g., INC-1027) opens a detail drawer that now includes **system metrics charts** — CPU, memory, latency, and active requests around the incident window. For the SAP connector incidents, you can see CPU spiking to 90%+ and active requests surging right before the incident fires — the system was already under stress before the incident was declared.

---

## 6. Topology Explorer — See the Blast Radius

![Topology Explorer](images/topology-explorer.png)

The topology view shows how services are connected — a dependency graph built from actual network flows and incident correlation data.

From here, the SAP integration path is visible: `erp-sap-connector` connects to `check-inventory-api`, which connects to `order-management-service`, `shipment-routing-service`, and `distribution-portal`. When the SAP connector is overloaded or unreachable, the entire chain downstream is affected. This is the blast radius proof — one service's failure propagates to 4-5 others, which is exactly what the incident data shows (blast radius of 4-5 on every SAP-related incident).

---

## 7. Ask Genie — Self-Serve Investigation

![Ask Genie](images/ask-genie.png)

For anyone who wants to skip the dashboards and ask directly: the Genie page accepts plain-English questions backed by the same data.

Try: *"What was a recent P1 event that caused delays in shipment of goods?"* — Genie returns the specific incident (INC-1018), the root cause (VPC security group blocking check-inventory-api), the 601 shipments delayed, and the $7.5M revenue at risk. The SQL query and tabular evidence are shown for auditability.

---

## The Bottom Line

Two failure modes, one shared dependency:

| Pattern | Root Service | Incidents | Revenue at Risk | Fix |
|---------|-------------|-----------|----------------|-----|
| SAP Batch Sync Overload | erp-sap-connector | 9 P2s (chronic) | $10.2M | Stagger the batch sync window to finish before morning order rush |
| Network Timeout to SAP | check-inventory-api | 1 P1 (acute) | $7.5M | Add network validation checks before security group changes |

Combined: **$17.7M in revenue exposure, 1,417 shipments delayed, 35 ServiceNow tickets (12 duplicates)** — from a scheduling overlap and a missing pre-change validation step. Both fixes are low-cost operational changes.
