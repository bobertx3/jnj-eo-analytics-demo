# Demo Narrative — Enterprise Root Cause Intelligence

> **Audience:** Senior leaders (VP+) who understand business outcomes but are not observability experts.
> **Goal:** Show how correlated telemetry turns alert noise into actionable root cause stories with quantified business impact.
> **Tip:** Jargon definitions are included in parentheses throughout. Use them naturally when presenting — they make technical concepts accessible without dumbing things down.

---

## Opening: The Big Picture

> *"This platform pulls together signals from across our entire technology stack — servers, applications, network equipment — and connects the dots automatically. Think of it like a detective board, but instead of string and photos, it's correlating thousands of system events to find the real cause behind outages.*
>
> *In the last 30 days we've had **30 incidents**, 8 of which were P1 — meaning critical, all-hands-on-deck events. The total revenue at risk is **$19.8M**, with **1,417 shipments delayed** and **2,352 employees impacted**. But here's the key insight: two systemic root causes account for the vast majority of that impact. Let me show you both."*

**App screen:** Executive Dashboard — point to the KPI cards at the top, then the incident timeline showing the cluster of events.

---

## Narrative 1: Supply Chain Cascade — "One Overloaded System is Costing Us $18M"

### The story in plain English

We have a system called the **SAP ERP connector** — think of it as the bridge between our ordering systems and our SAP warehouse/shipping system. Every night, it runs a batch job to sync orders, inventory, and shipment data. The problem is that this nightly sync is colliding with the morning rush of live orders, and the bridge gets jammed. When that happens, nothing gets through — inventory can't be checked, orders can't be confirmed, and shipments sit in a queue.

This has been happening **every 1-2 days** for the past month, each time delaying 70-120 shipments. Each incident looks small on its own — a P2 (serious but not critical). But on **February 19**, something worse happened: a network configuration change accidentally blocked the communication path between our inventory-checking system and the SAP connector entirely. That turned a recurring nuisance into a **P1 crisis** — 601 shipments frozen, $7.5M in revenue at risk, in under 2 hours.

The platform connected all of these dots automatically and surfaced it as the **#1 priority issue across the entire enterprise**.

### Walk-through by screen

#### 1. Executive Dashboard

> *"Let's start with the big picture. 30 incidents in the last month. Supply chain is clearly our biggest pain point — **$17.7M in revenue at risk** and **1,417 shipments delayed**. That's not a technology problem — that's a business problem."*

**What to point at:**
- KPI cards: total incidents (30), P1 count (8), revenue at risk ($19.8M)
- Incident timeline: cluster of events in late February getting more frequent
- Domain breakdown: supply-chain dominates revenue impact

#### 2. Root Cause Intelligence

> *"This is where the platform earns its keep. Instead of showing us 30 individual incidents, it's identified **patterns** — recurring issues that share the same root cause. The #1 pattern is called 'ERP SAP Connector Batch Sync Overload.' In simple terms: our SAP data bridge is getting overwhelmed.*
>
> *Here's what that means: every night, the SAP connector runs a big batch job to sync inventory and order data. But the nightly batch is running into the morning when live orders start flooding in. The system can't handle both at once — its connection pool (think of it as the number of phone lines available) gets maxed out, and everything backs up. This has happened **9 times in 27 days**, and the trend is **worsening** — it's getting more frequent, not less.*
>
> *Connected to it is pattern #2 — 'check-inventory-api Network Timeout.' This is the system that checks 'do we have this item in stock?' before confirming an order. It depends on the SAP connector to answer that question. On Feb 19, a network change (a firewall rule update) accidentally blocked the communication channel between these two systems. The inventory checker couldn't reach SAP, its queue of pending requests filled up, and it started rejecting every order. That's the P1 — **601 shipments frozen, $7.5M at risk in a single incident**.*
>
> *These two patterns are connected — fix the SAP connector's overload problem, and you eliminate the conditions that make the inventory API vulnerable."*

**What to point at:**
- Pattern list sorted by priority score (SAP connector at top)
- Radar chart showing high scores on frequency, revenue, and recurrence
- Click "AI Analysis" to show the AI-generated root cause chain and remediation steps
- Trend badge: "worsening" — this problem is getting worse, not better

#### 3. Service Risk Ranking

> *"This view ranks every service in our technology portfolio by risk. Think of it as a 'most wanted' list for reliability problems.*
>
> *The SAP connector — erp-sap-connector — is **ranked #1** across the entire enterprise. 9 incidents where it was the root cause, **$10.2M in revenue impact**, and a health score of 74.7 out of 100 — the worst of any service we run.*
>
> *The inventory checker — check-inventory-api — is **#2**. It only caused 1 incident directly, but that single P1 carried **$7.5M in impact**. One bad day cost more than the SAP connector's 9 incidents combined — because when inventory checking goes down completely, every order in the pipeline stops."*

**What to point at:**
- erp-sap-connector at rank #1 with risk score 1,198
- check-inventory-api at rank #2 with risk score 880
- Health timeline for erp-sap-connector showing repeated dips (each dip = an outage)

#### 4. Change Correlation

> *"This view answers the question: 'Did someone change something that broke something else?'*
>
> *The platform tracks every change made to our systems — deployments, configuration updates, network changes — and looks for incidents that happened shortly after. On February 19, a security group change (basically a firewall rule update) was applied to the inventory API's network. Shortly after, the P1 hit. The system flagged this change-incident pair and scored the correlation.*
>
> *This is the kind of insight that normally takes an SRE team hours of manual investigation to find. The platform found it automatically."*

**What to point at:**
- Timeline overlay showing the Feb 19 change and the P1 incident
- Correlation table with change type, time gap, and correlation strength score

#### 5. Domain Deep Dive

> *"We can also look at this by domain — infrastructure, application, and network. The SAP connector sits in the infrastructure domain. Drilling in, we see 11 incidents, $10.7M in revenue impact, and the weekly trend shows the problem **accelerating in late February** — incidents every day in the last week."*

**What to point at:**
- Infrastructure domain tile
- Weekly trend chart showing escalation
- Service risk bars within the domain (SAP connector at top)

#### 6. Ask Genie

> *"Finally, for anyone who doesn't want to navigate dashboards — you can just ask a question in plain English. 'What was a recent P1 event that caused delays in shipment of goods?' The AI answers with the specific incident, the root cause explanation, and the financial impact. No technical knowledge required."*

**What to point at:**
- Click the "Supply Chain Disruption" starter question
- Genie response with the P1 details, revenue at risk, shipments delayed

### The punchline

> *"Let me bring this together. One system — the SAP data bridge — is getting overloaded because its nightly batch sync overlaps with morning order processing. This has been happening every 1-2 days and has **cascaded into $18M+ in revenue risk, 1,400 delayed shipments, and 148 ServiceNow tickets** (a third of which were duplicates — people filing the same ticket because they didn't know it was already reported).*
>
> *The fix? **Stagger the batch sync window** so it finishes before the morning order rush begins. That's a scheduling change — it costs nothing. And it eliminates our #1 enterprise risk."*

---

## Narrative 2: Digital Surgery Productivity — "A Network Change Took Down 100 Data Scientists"

### The story in plain English

Our Digital Surgery division has data scientists who use AI/ML (machine learning) tools to develop surgical guidance models. They work in a notebook environment (think of it as a sophisticated coding workspace in the cloud) that connects to SageMaker — Amazon's machine learning service that runs their AI models.

On **February 3**, an automated infrastructure script made a routine network routing change to the notebook platform. Think of network routing like a GPS for data — it tells data packets which road to take to reach their destination. This change added a new "road" that conflicted with the existing path to SageMaker. For 5 days, no one noticed because the conflict was intermittent.

On **February 8**, the routing conflict fully kicked in. Data meant for SageMaker started taking a wrong turn — hitting the public internet instead of the private internal path — and getting blocked by security rules. **100 data scientists couldn't run their AI models for nearly 3 hours.** At $1,500/hour loaded cost per person, that's **$450,000 in lost productivity from a single incident.**

To make things worse, because the problem looked different depending on where you sat (network issue? application issue? infrastructure issue?), 15 ServiceNow tickets were filed — **12 of which (80%) were duplicates**. Teams were chasing the same problem from different angles without realizing it.

### Walk-through by screen

#### 1. Executive Dashboard

> *"Look at the digital surgery business unit — **$600K in total productivity loss**, and **59% of their ServiceNow tickets are duplicates**. That duplicate rate is a red flag. It means when something breaks, multiple teams are scrambling independently because they can't see the common root cause. That's exactly the problem this platform solves."*

**What to point at:**
- Business unit breakdown: digital-surgery with 59.3% duplicate ticket rate
- Productivity loss figure for the division

#### 2. Root Cause Intelligence

> *"Two patterns are driving digital surgery's pain.*
>
> *The big one is 'SageMaker VPC Routing Misconfiguration' — a routing conflict (bad directions for data traffic) that caused the AI model service to go offline. It's a P1 with a **177-minute resolution time** (nearly 3 hours) and **$450K in productivity loss**.*
>
> *Then there's a recurring issue: 'ML Training Pipeline GPU Memory Leak.' In simple terms, the system that trains AI models has a memory problem — it slowly uses up all available memory over a 48-hour training run until it crashes. This happens every 1-2 days, each time costing $37,500 when 25 data scientists lose an hour of work waiting for the restart."*

**What to point at:**
- SageMaker pattern: P1 count, SLA breaches, $450K total impact
- ML Training pattern: recurring every 1.7 days, 4 incidents
- Click "AI Analysis" to show the root cause chain linking the network change to the outage

#### 3. Change Correlation

> *"Here's the smoking gun. On **February 3**, an automated script ran a network routing change on the notebook platform — our system scored it a **risk of 9.0 out of 10**, the highest-risk change in our entire log. Five days later, SageMaker went down.*
>
> *But here's what's even more powerful — look at the **pre-incident alerts**. In the 15 minutes before the incident was formally declared, the platform detected 6 warning signs across two services:"*

**Pre-incident alert sequence (INC-1010):**

| Time | What happened (plain English) |
|------|------|
| 10:23 AM | Notebook platform started responding slowly (HighLatency alert) |
| 10:23 AM | SageMaker's storage I/O became saturated (DiskIOSaturation) |
| 10:25 AM | SageMaker's connection queue nearly full (ConnectionPoolNearCapacity) |
| 10:25 AM | Notebook platform latency worsened again |
| 10:27 AM | SageMaker ran out of processing threads (ThreadPoolExhaustion) |
| 10:37 AM | Notebook platform memory usage spiked (HighMemoryUsage) |
| **10:38 AM** | **P1 Incident declared** |

> *"Six alerts in 15 minutes, across two different services. Without this platform, each of those alerts would be investigated separately by different teams. With it, they're all correlated to a single root cause: **the network routing change from 5 days earlier**."*

**What to point at:**
- Change timeline: CHG-3021 on Feb 3 leading to INC-1010 on Feb 8
- Pre-incident signal alerts flagged in the correlation view

#### 4. Service Risk Ranking

> *"The SageMaker inference endpoint sits at risk rank #7, but look at the MTTR — **177 minutes**, the longest resolution time in the entire enterprise. That matters because every minute of downtime is $1,500 x 100 data scientists = $150,000/hour. The ML training pipeline is rank #10 with 4 recurring crashes."*

**What to point at:**
- sagemaker-inference-endpoint: 177 min MTTR (longest in the fleet), 1 SLA breach
- ml-training-pipeline: 4 incidents, $150K total, recurring pattern

#### 5. Topology Explorer

> *"The topology view shows how services are connected — like a wiring diagram for our technology. You can see that the notebook platform connects to SageMaker, which connects to the model registry. When the network route broke, the entire chain went down — every data scientist who needed to run an AI model was blocked."*

**What to point at:**
- Network flow edges between ds-notebook, sagemaker, and model-registry
- Red/amber color coding on affected nodes showing risk level

#### 6. Ask Genie

> *"Ask in plain English: 'What was a recent P1 event impacting digital surgery data scientists?' — and the AI returns the specific incident, the 100 impacted data scientists, the $450K productivity loss, and the root cause explanation. A business leader can get this answer in 10 seconds without reading a dashboard."*

**What to point at:**
- Click the "Digital Surgery Productivity" starter question
- Response with the full incident narrative

### The punchline

> *"A single automated network change — something that happens routinely — created a routing conflict that took 5 days to surface. When it did, it cost **$450,000 in one afternoon** and generated 15 support tickets, 80% of which were duplicates.*
>
> *Two simple fixes protect this division going forward:*
> 1. *Add a **route validation check** to the automation pipeline — before any network change goes live, automatically verify that it doesn't conflict with existing paths. Think of it as a GPS check before repaving a road.*
> 2. *For the recurring GPU memory issue — set up an **automatic restart every 24 hours** for the training workers, before they run out of memory. Like rebooting your computer at night so it runs fresh each morning.*
>
> *Together, those two changes protect **$600K per quarter** in data scientist productivity."*

---

## Closing: Why This Matters

> *"Let me leave you with this. Traditional monitoring is like having smoke detectors in every room — they tell you something is on fire, but not why it started or how much it'll cost to fix. This platform is more like an arson investigator that's always on duty.*
>
> *Across 30 incidents in 30 days:*
>
> - ***$19.8M** total revenue at risk*
> - ***1,417** shipments delayed*
> - ***2,352** employees impacted*
> - ***218** ServiceNow tickets filed, **124 of which (57%) were duplicates** — meaning teams were chasing the same problems without knowing it*
>
> *Two root causes account for the majority of that impact. Both have simple, low-cost fixes:*
>
> 1. ***Stagger the SAP batch sync window** — a scheduling change that eliminates $18M in supply chain risk*
> 2. ***Add route validation to network automation** — a pipeline check that prevents $450K productivity losses*
>
> *That's the shift this platform enables: from reacting to incidents one at a time, to eliminating the systemic causes that generate them."*

---

## Glossary (for reference if asked)

| Term | Simple explanation |
|------|--------------------|
| **P1 / P2 / P3** | Priority levels. P1 = critical (all hands on deck), P2 = serious (dedicated team), P3 = important (scheduled fix) |
| **MTTR** | Mean Time to Resolve — how long it takes to fix a problem, measured in minutes |
| **SLA breach** | We promised a certain level of uptime/performance to the business; a breach means we broke that promise |
| **Blast radius** | How many other services are affected when one service fails — like how many dominos fall |
| **Root cause** | The underlying problem that started the chain reaction, not just the symptom |
| **Connection pool** | Think of it as a limited number of phone lines. When they're all in use, new callers get a busy signal |
| **Batch sync** | A scheduled job that moves large amounts of data between systems at once (vs. real-time one-at-a-time) |
| **VPC / Security group** | Virtual network boundaries and firewall rules that control which systems can talk to each other |
| **Terraform** | An automation tool that manages infrastructure using code — like a recipe that builds and configures servers |
| **SageMaker** | Amazon's cloud service for running AI/machine learning models |
| **ServiceNow tickets** | IT support requests — like help desk tickets but for technology teams |
| **Duplicate tickets** | Multiple teams filing separate support requests for the same underlying problem |

---

## Quick Reference: Key Data Points

| Metric | Value | Source Table |
|--------|-------|-------------|
| Total incidents (30 days) | 30 | silver_incidents |
| P1 incidents | 8 | silver_incidents |
| Total revenue at risk | $19.8M | silver_incidents |
| Total shipments delayed | 1,417 | silver_incidents |
| Total ServiceNow tickets | 218 | silver_incidents |
| Duplicate tickets | 124 (57%) | silver_incidents |
| #1 pattern (SAP overload) | 9 occurrences, ~$10.2M impact, worsening | gold_root_cause_patterns |
| #2 pattern (inventory API timeout) | 1 P1, ~$7.5M impact, 1 SLA breach | gold_root_cause_patterns |
| #1 risk service | erp-sap-connector (score 1,198) | gold_service_risk_ranking |
| Worst MTTR | sagemaker-inference-endpoint (177 min) | silver_incidents |
| Highest duplicate ticket rate | shared-infrastructure (80%) | silver_servicenow_correlation |
| Digital surgery productivity loss | $600K | gold_business_impact_summary |
