# 🌱 GreenOps AI Agent

**A 4-agent AI pipeline that scans Google Cloud projects for wasted resources, calculates carbon footprint, and executes safe optimizations — built with Google ADK and Gemini.**

🔗 **Live Demo:** https://greenops-agent-845589445410.us-central1.run.app

---

## What It Does

GreenOps automatically finds idle cloud resources that are costing money and emitting CO₂ for no reason — and fixes them safely with human approval.

```
Carbon Scout → GreenOps Analyzer → Optimization Executor → Report Generator
```

| Agent | Role |
|---|---|
| 🔍 Carbon Scout | Scans GCP for idle VMs, unattached disks, unused IPs |
| 📊 GreenOps Analyzer | Calculates CO₂ impact and cost waste |
| ⚡ Optimization Executor | Asks human approval, then executes safe actions |
| 📋 Report Generator | Produces a full markdown GreenOps report |

---

## Real-World Impact

- Companies waste **30–35% of cloud budget** on idle resources (Gartner)
- Cloud computing accounts for **~1% of global electricity use**
- GreenOps finds and eliminates waste automatically

**Example findings on a typical GCP project:**
- 3 idle VMs left running → $87/month wasted → 12.4 kg CO₂/month
- 2 unattached disks (700 GB) → $28/month wasted
- 1 unused reserved IP → $7.20/month wasted
- **Total: $122/month saved, 149 kg CO₂/year eliminated**

---

## Tech Stack

- **[Google ADK](https://google.github.io/adk-docs/)** — Agent Development Kit for multi-agent orchestration
- **[Gemini 2.5 Flash (gemini-2.5-pro)](https://deepmind.google/models/gemini/)** — LLM powering all 4 agents
- **[Google Gemini API](https://aistudio.google.com/)** — Standard Gemini API (stable, 60 RPM, no 503/404 errors)
- **[Google Cloud Run](https://cloud.google.com/run)** — Serverless deployment
- **[FastAPI](https://fastapi.tiangolo.com/)** — Web dashboard with SSE streaming
- **Google Cloud SDK** — gcloud CLI for GCP resource scanning
- **Python 3.12+** — core runtime
- **SequentialAgent** — ADK pipeline: each agent passes context to the next

---

## Architecture

```
Browser (SSE) ←── FastAPI Dashboard ←── SequentialAgent Pipeline
                        │
                   Cloud Run (GCP)
                        │
              ┌─────────┴──────────┐
              │  Google Gemini API │
              │  gemini-2.5-pro  │
              └────────────────────┘
```

### Reliability Features
- **Exponential backoff retry** — 5 retries with 20s → 40s → 80s → 120s → 180s delays
- **503 UNAVAILABLE handling** — auto-retries when Gemini API is under high demand
- **429 RESOURCE_EXHAUSTED handling** — extracts retryDelay from API response and waits exact amount
- **Paid Gemini API** — eliminates free-tier quota limits entirely (1000+ RPM)

---

## Project Structure

```
greenops-agent/
├── agents/
│   ├── greenops_pipeline.py       # Real pipeline (connects to live GCP)
│   └── greenops_pipeline_demo.py  # Demo pipeline (simulated resources)
├── tools/
│   ├── gcp_tools.py               # Real GCP tools via gcloud CLI
│   └── gcp_tools_demo.py          # Demo tools with realistic mock data
├── output/                        # Generated GreenOps reports (auto-created, not committed)
├── app.py                         # FastAPI web dashboard with SSE streaming
├── scheduler.py                   # Cloud Scheduler integration (Gmail + Slack alerts)
├── main.py                        # Run real pipeline (CLI)
├── main_demo.py                   # Run demo pipeline (CLI)
├── test_api.py                    # Test Gemini API connectivity
└── .env                           # ⚠️ NOT committed — secrets stay local
```

### 🔒 Secret Protection (.gitignore)

Sensitive files are **never committed to GitHub**:

```
# .gitignore — these are excluded from git
.env                  ← your API key & GCP project ID
*.json                ← service account keys
gcp-sa-key.json       ← GCP credentials
output/               ← generated reports
__pycache__/          ← Python cache
.venv/                ← virtual environment
```

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/raghu-putta/greenops-agent.git
cd greenops-agent
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Mac/Linux
pip install -r requirements.txt
```

### 2. Configure `.env`

```env
GCP_PROJECT_ID=your-gcp-project-id
GCP_REGION=us-central1
GCP_ZONE=us-central1-a
CARBON_FACTOR_KWH=0.000233

# Standard Gemini API (recommended — stable, 60 RPM, no 503/404 errors)
GOOGLE_GENAI_USE_VERTEXAI=0
GOOGLE_API_KEY=your-gemini-api-key
```

### 3. Authenticate with GCP

```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

### 4. Run the demo (no GCP needed)

```bash
python main_demo.py
```

### 5. Run against your real GCP project

```bash
python main.py
```

### 6. Run the web dashboard locally

```bash
uvicorn app:app --reload --port 8000
```
Open: http://localhost:8000

---

## Deploy to Cloud Run

```bash
gcloud run deploy greenops-agent \
  --source . \
  --region us-central1 \
  --project YOUR_PROJECT_ID \
  --set-env-vars GOOGLE_GENAI_USE_VERTEXAI=0,GOOGLE_API_KEY=YOUR_GEMINI_API_KEY,GCP_PROJECT_ID=YOUR_PROJECT_ID
```

---

## Safety Design

The Optimization Executor follows strict rules:

| Risk Level | Action | Agent Behavior |
|---|---|---|
| 🟢 LOW | Stop idle VMs, release unused IPs | **Lists and asks human approval first** |
| 🟡 MEDIUM | Resize active VMs | Lists but does NOT execute — marked for manual review |
| 🔴 HIGH | Delete databases, modify production | **NEVER executes** — escalates with full details |

No action is taken without explicit human confirmation.

---

## Live Demo Output

Below is the **actual output** from running `python main_demo.py` — all 4 agents running in sequence:

---

### 🚀 Pipeline Startup

```
============================================================
  🌱 GreenOps Agentic AI — DEMO MODE
  Simulated GCP project with idle resources
  Time: 2026-05-26 05:44:13
============================================================

  Simulated resources:
  • 3 idle VMs (ml-training, staging-api, data-pipeline)
  • 2 unattached disks (500GB + 200GB)
  • 1 unused reserved IP
  • 1 rightsizing recommendation

------------------------------------------------------------
Running pipeline... (this may take 1-2 minutes)
```

---

### 🔍 Agent 1 — Carbon Scout

```
[CARBON_SCOUT]
----------------------------------------
GreenOps Scan Summary for Project: greenops-demo-project

1. Total Running Idle VMs (3)
   • ml-training-server-01  (us-central1-a, n1-standard-8) — Idle 45 days
   • staging-api-backend     (us-central1-b, n1-standard-4) — Idle 58 days
   • data-pipeline-worker    (us-central1-a, n1-standard-2) — Idle 35 days

2. Total Unattached Disks (2)
   • old-postgres-backup-disk  500 GB — backup migrated to GCS
   • dev-workspace-disk        200 GB — developer left team, disk orphaned

3. Reserved IPs Not In Use (1)
   • prod-load-balancer-ip-old  34.102.140.239
     LB decommissioned March 2026 — IP still reserved

4. Rightsizing Recommendations (1)
   • Change 'analytics-server' from n1-standard-8 → n1-standard-2
     CPU avg 4% over 30 days — severely over-provisioned
```

---

### 📊 Agent 2 — GreenOps Analyzer

```
[GREENOPS_ANALYZER]
----------------------------------------
GreenOps Analysis Report for Project: greenops-demo-project

Environmental Impact
  • Total CO2 per month : 8.35 kg
  • Total CO2 per year  : 0.10 tons

Estimated Monthly Cost Waste
  • Idle VMs          : $30.00  (3 VMs × $10/VM/month)
  • Unattached Disks  : $28.00  (700 GB × $0.04/GB/month)
  • Reserved IPs      : $ 7.20  (1 IP × $7.20/IP/month)
  ─────────────────────────────────────
  • TOTAL WASTE       : $65.20/month

Priority-Ranked Action List
  🟢 LOW RISK
     1. Stop ml-training-server-01  (idle 45 days)
     2. Stop staging-api-backend    (idle 58 days)
     3. Stop data-pipeline-worker   (idle 35 days)
     4. Release prod-load-balancer-ip-old
     5. Delete old-postgres-backup-disk (500 GB)
     6. Delete dev-workspace-disk   (200 GB)

  🟡 MEDIUM RISK (manual review required)
     1. Resize analytics-server: n1-standard-8 → n1-standard-2
        Estimated savings: $87/month

  🔴 HIGH RISK
     None identified.
```

---

### ⚡ Agent 3 — Optimization Executor

```
[OPTIMIZATION_EXECUTOR]
----------------------------------------
I have found the following LOW risk actions.
Do you approve executing these? (yes/no)

  1. Stop ml-training-server-01   (Zone: us-central1-a)
  2. Stop staging-api-backend     (Zone: us-central1-b)
  3. Stop data-pipeline-worker    (Zone: us-central1-a)
  4. Release prod-load-balancer-ip-old (34.102.140.239)
  5. Delete old-postgres-backup-disk   (Zone: us-central1-a)
  6. Delete dev-workspace-disk         (Zone: us-central1-b)

MEDIUM Risk Actions — Pending Manual Review:
  • Resize analytics-server: n1-standard-8 → n1-standard-2
    Estimated savings: $87/month

HIGH Risk Actions: None identified.
```

---

### 📋 Agent 4 — Report Generator

```
[REPORT_GENERATOR]
----------------------------------------
# GreenOps AI Report
Project   : greenops-demo-project
Date      : 2026-05-26
Generated : GreenOps Agentic AI Pipeline (DEMO MODE)

## Executive Summary
The GreenOps analysis identified significant optimization opportunities:
3 idle VMs, 2 unattached disks, 1 unused reserved IP, and 1 rightsizing
recommendation. Low-risk actions were executed, saving $65.20/month
and 8.35 kg CO₂/month.

## Resources Scanned
  Running VMs            : 3 — ml-training-server-01, staging-api-backend, data-pipeline-worker
  Unattached Disks       : 2 — 700 GB total
  Reserved IPs not in use: 1
  Rightsizing recommendations: 1

## Carbon Impact
  Monthly CO2 savings : 8.35 kg
  Annual CO2 savings  : 0.10 tons
  Equivalent to       : 5 trees planted / 250 car miles offset

## Cost Savings
  Monthly savings : $65.20
  Annual savings  : $782.40

## Actions Taken
  ✅ Stopped  ml-training-server-01  (us-central1-a)
  ✅ Stopped  staging-api-backend    (us-central1-b)
  ✅ Stopped  data-pipeline-worker   (us-central1-a)
  ✅ Released prod-load-balancer-ip-old (34.102.140.239)
  ✅ Deleted  old-postgres-backup-disk  (us-central1-a)
  ✅ Deleted  dev-workspace-disk        (us-central1-b)

## Pending Human Review (Medium Risk)
  • Resize analytics-server: n1-standard-8 → n1-standard-2
    Manual validation required — active resource

## Recommendations for Next 30 Days
  1. Approve rightsizing for analytics-server ($87/month savings)
  2. Set automated shutdown schedules for non-production VMs
  3. Schedule monthly scans for orphaned disks and unused IPs
  4. Migrate remaining pipelines to Cloud Run (serverless)
  5. Tag all resources with team/project labels for better tracking

============================================================
  🌱 GreenOps Demo Pipeline Complete
============================================================
✅ Full report saved to: output/greenops_DEMO_report_20260526_054442.md
```

---

## Built With

Built by **Raghu Putta** using Google ADK + Gemini 3.5 Flash.

---

## License

MIT License — free to use, modify, and distribute.
