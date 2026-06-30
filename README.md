# 🌱 GreenOps AI Agent Dashboard

**Production-grade 4-agent agentic AI pipeline for GCP cost optimization and carbon footprint reduction**

[![Live Demo](https://img.shields.io/badge/Live%20Demo-GreenOps%20AI-34d399?style=for-the-badge&logo=google-cloud)](https://greenops-dashboard-845589445410.us-central1.run.app)
[![GitHub](https://img.shields.io/badge/GitHub-raghu--putta-blue?style=for-the-badge&logo=github)](https://github.com/raghu-putta/greenops-agent)
[![Gemini](https://img.shields.io/badge/Powered%20by-Gemini%202.5%20Pro-4285F4?style=for-the-badge&logo=google)](https://ai.google.dev/)
[![Google ADK](https://img.shields.io/badge/Google-ADK-orange?style=for-the-badge&logo=google)](https://google.github.io/adk-docs/)
[![Cloud Run](https://img.shields.io/badge/Cloud%20Run-Deployed-4285F4?style=for-the-badge&logo=google-cloud)](https://cloud.google.com/run)
[![Version](https://img.shields.io/badge/Version-2.0-34d399?style=for-the-badge)](https://github.com/raghu-putta/greenops-agent)

---

## 🤖 What is GreenOps AI?

GreenOps AI is a **production-grade 4-agent agentic AI pipeline** that automatically scans your Google Cloud Platform (GCP) project for wasted cloud spend and carbon emissions, then safely optimizes and generates a downloadable report.

- 💸 **Wasted cloud spend** — idle VMs, unattached disks, unused reserved IPs
- 🌍 **Carbon emissions** — CO2 footprint from idle resources
- ⚡ **Safe optimization** — executes only LOW-risk actions; MEDIUM/HIGH need human approval
- 📊 **Executive reports** — downloadable in PDF, TXT, HTML, CSV, JSON formats

---

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌───────────────────────┐    ┌──────────────────┐
│  Carbon Scout   │───▶│ GreenOps Analyzer│───▶│ Optimization Executor │───▶│ Report Generator │
│  🔍 Scouting    │    │ 📊 Analyzing     │    │ ⚡ Executing          │    │ 📋 Generating    │
│  Scans GCP for  │    │ Calculates CO2 & │    │ Executes LOW risk     │    │ Generates full   │
│  idle resources │    │ cost + risk level│    │ actions safely        │    │ GreenOps report  │
└─────────────────┘    └──────────────────┘    └───────────────────────┘    └──────────────────┘
     Green #34d399          Blue #60a5fa           Orange #f97316              Purple #a78bfa
```

**Tech Stack:**
- 🤖 **Google ADK** — Multi-agent orchestration
- ✨ **Gemini 2.5 Pro** — AI reasoning engine for all 4 agents
- ⚡ **FastAPI** — Backend API with SSE streaming
- 🚀 **Cloud Run** — Serverless deployment (timeout 3600s, min-instances 1)
- 🎨 **Vanilla JS** — Real-time dashboard with SSE event streaming

---

## ✨ Features (v2.0)

| Feature | Description |
|---|---|
| 🤖 **Robot Agent Profiles** | Circular robot images per agent with colored glow borders |
| 🏷️ **Agent Status Labels** | Live status: Scouting / Analyzing / Executing / Generating |
| ⚙️ **Configure GCP Panel** | Alex AI assistant with typewriter effect + privacy-first credential storage |
| 🔐 **Privacy-First Credentials** | SessionStorage → HTTPS → In-memory → Zero retention |
| ⬇️ **Download Report** | PDF, TXT, HTML, CSV, JSON, Copy — all client-side, zero auth needed |
| 🌱 **GreenOps Favicon** | Custom leaf icon in browser tab |
| 📊 **Live Metrics Sidebar** | Monthly savings, CO2 saved/month, Idle VMs, LOW-risk actions |
| 🔄 **SSE Streaming** | Real-time terminal output from all 4 agents |
| 🎯 **Single-Run Lock** | Prevents concurrent pipeline runs (HTTP 409 protection) |
| 👣 **Footer** | Built by Raghu Putta | GitHub | Live Demo | v2.0 |

---

## 🔐 Privacy-First Credential Flow

GreenOps AI uses a **zero-retention credential architecture**:

```
1. User enters credentials in Configure GCP panel
2. Saved to browser sessionStorage ONLY (auto-wiped on tab close)
3. Sent via HTTPS with the Run request (encrypted in transit)
4. Held in backend RAM for ONE pipeline run only
5. Agents use user's own GCP project + Gemini key
6. Run ends → RAM cleared → Zero retention
```

**Single-run lock** ensures one user's credentials never bleed into another user's run.

---

## 🚀 Live Demo

👉 **[https://greenops-dashboard-845589445410.us-central1.run.app](https://greenops-dashboard-845589445410.us-central1.run.app)**

- Click **Run Demo Mode** — simulated GCP scan, no credentials needed
- Click **Configure GCP** → enter your key → **Run Real GCP** — scans your actual project
- After pipeline completes → **Download bar appears** with 6 export formats

---

## ⚙️ Setup & Deployment

### Prerequisites
- Python 3.11+
- Google Cloud SDK
- Gemini API Key (free at [aistudio.google.com/apikey](https://aistudio.google.com/apikey))
- GCP Project with billing enabled

### Local Setup

```bash
git clone https://github.com/raghu-putta/greenops-agent.git
cd greenops-agent
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
uvicorn app:app --reload --port 8000
```

### Deploy to Cloud Run

```bash
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT/cloud-run-source-deploy/greenops-dashboard:latest .

gcloud run deploy greenops-dashboard \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT/cloud-run-source-deploy/greenops-dashboard:latest \
  --region us-central1 \
  --allow-unauthenticated \
  --timeout 3600 \
  --min-instances 1 \
  --set-env-vars GOOGLE_API_KEY=your-key,GCP_PROJECT_ID=your-project,GCP_REGION=us-central1,GCP_ZONE=us-central1-a,GOOGLE_GENAI_USE_VERTEXAI=0
```

> ⚠️ **Important:** `--timeout 3600` and `--min-instances 1` are required. Without them, SSE connections get truncated and the pipeline never outputs to the terminal.

---

## 🔑 Environment Variables

| Variable | Description | Required |
|---|---|---|
| `GOOGLE_API_KEY` | Gemini API Key from aistudio.google.com | ✅ |
| `GCP_PROJECT_ID` | Your GCP Project ID | ✅ |
| `GCP_REGION` | GCP Region (default: us-central1) | ✅ |
| `GCP_ZONE` | GCP Zone (default: us-central1-a) | ✅ |
| `GOOGLE_GENAI_USE_VERTEXAI` | Set to 0 for Gemini API (not Vertex) | ✅ |
| `CARBON_FACTOR_KWH` | Carbon factor kg/kWh (default: 0.000233) | Optional |
| `SCHEDULER_SECRET` | Secret header for scheduled scans | Optional |

---

## 🌿 Agent Details

### Agent 1: Carbon Scout 🔍 (Green)
Scans GCP project for idle VMs, unattached disks, unused reserved IPs, and rightsizing recommendations from GCP Recommender API.

### Agent 2: GreenOps Analyzer 📊 (Blue)
Calculates carbon footprint and cost waste from Carbon Scout findings. Classifies each finding as LOW / MEDIUM / HIGH risk.

### Agent 3: Optimization Executor ⚡ (Orange)
Safely executes **LOW risk** optimizations only. MEDIUM and HIGH risk actions are flagged for human review — the AI never auto-deletes or auto-stops risky resources.

### Agent 4: Report Generator 📋 (Purple)
Generates comprehensive GreenOps report: executive summary, carbon impact, cost savings, 30-day recommendations. Downloadable in 6 formats.

---

## 🐛 Key Issues Solved (v2.0)

| Issue | Root Cause | Fix |
|---|---|---|
| Terminal output blank | Cloud Run default 5min timeout truncated SSE | `--timeout 3600 --min-instances 1` |
| Configure GCP not opening | JS syntax error at line 508 killed all JS | Validated with `node --check` before deploy |
| Slow pipeline start | Gemini API cold start | Warmup ping on app startup |
| Download bar not appearing | Two `<script>` blocks; function in block 1 called from block 0 | Merged into single script block |
| Broken emoji in terminal | SSE missing UTF-8 charset | Added `charset=utf-8` to SSE media_type |

---

## 🗺️ Roadmap

### Phase 2 — RAG Integration (Next Article!)
- Google ADK + LangChain + Vertex AI RAG Engine
- 35–48% accuracy improvement in recommendations
- Real-time GCP pricing + carbon emission factors database
- Est. cost: $165–470/month

### Phase 3 — Commercial Grade
- Google OAuth sign-in (no API key copy-paste)
- Multi-cloud support (AWS, Azure)
- Multi-user concurrent pipeline support

---

## 👨‍💻 Built By

**Raghu Putta** — Cloud Economist | FinOps Engineer | GreenOps Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat&logo=linkedin)](https://linkedin.com/in/raghu-putta)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?style=flat&logo=github)](https://github.com/raghu-putta)

---

## 📄 License

MIT License — feel free to use, modify and distribute.

---

*Built with ❤️ using Google ADK + Gemini 2.5 Pro | v2.0 | June 2026*
