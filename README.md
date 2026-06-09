# 🌱 GreenOps AI Agent

**AI-powered GCP cost optimization and carbon footprint reduction using Google ADK + Gemini 2.5 Pro**

[![Live Demo](https://img.shields.io/badge/Live%20Demo-GreenOps%20AI-34d399?style=for-the-badge&logo=google-cloud)](https://greenops-dashboard-845589445410.us-central1.run.app)
[![GitHub](https://img.shields.io/badge/GitHub-raghu--putta-blue?style=for-the-badge&logo=github)](https://github.com/raghu-putta/greenops-agent)
[![Gemini](https://img.shields.io/badge/Powered%20by-Gemini%202.5%20Pro-4285F4?style=for-the-badge&logo=google)](https://ai.google.dev/)
[![Google ADK](https://img.shields.io/badge/Google-ADK-orange?style=for-the-badge&logo=google)](https://google.github.io/adk-docs/)

---

## 🤖 What is GreenOps AI?

GreenOps AI is a **4-agent agentic AI pipeline** that automatically scans your Google Cloud Platform (GCP) project for:

- 💸 **Wasted cloud spend** — idle VMs, unattached disks, unused reserved IPs
- 🌍 **Carbon emissions** — CO2 footprint from idle resources
- ⚡ **Optimization opportunities** — rightsizing recommendations from GCP Recommender
- 📊 **Executive reports** — automated GreenOps report with action plans

---

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌───────────────────────┐    ┌──────────────────┐
│  Carbon Scout   │───▶│ GreenOps Analyzer│───▶│ Optimization Executor │───▶│ Report Generator │
│  Scans GCP for  │    │ Calculates CO2 & │    │ Executes LOW risk     │    │ Generates full   │
│  idle resources │    │ cost waste       │    │ actions safely        │    │ GreenOps report  │
└─────────────────┘    └──────────────────┘    └───────────────────────┘    └──────────────────┘
```

**Tech Stack:**
- 🤖 **Google ADK** — Multi-agent orchestration
- ✨ **Gemini 2.5 Pro** — AI reasoning for all 4 agents
- ⚡ **FastAPI** — Backend API with SSE streaming
- 🚀 **Cloud Run** — Serverless deployment
- 🎨 **Vanilla JS** — Real-time dashboard UI

---

## 🚀 Live Demo

👉 **[https://greenops-dashboard-845589445410.us-central1.run.app](https://greenops-dashboard-845589445410.us-central1.run.app)**

Click **Run Demo Mode** to see the 4 agents in action with simulated GCP data.

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
gcloud builds submit --tag gcr.io/YOUR_PROJECT/greenops-dashboard .
gcloud run deploy greenops-dashboard \
  --image gcr.io/YOUR_PROJECT/greenops-dashboard \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars GOOGLE_API_KEY=your-key,GCP_PROJECT_ID=your-project
```

---

## 🔑 Environment Variables

| Variable | Description | Required |
|---|---|---|
| `GOOGLE_API_KEY` | Gemini API Key | ✅ |
| `GCP_PROJECT_ID` | Your GCP Project ID | ✅ |
| `GCP_REGION` | GCP Region (default: us-central1) | ✅ |
| `GCP_ZONE` | GCP Zone (default: us-central1-a) | ✅ |
| `GOOGLE_GENAI_USE_VERTEXAI` | Set to 0 for Gemini API | ✅ |
| `CARBON_FACTOR_KWH` | Carbon factor (default: 0.000233) | Optional |

---

## 🌿 Agent Details

### Agent 1: Carbon Scout 🔍
Scans GCP project for idle VMs, unattached disks, unused reserved IPs, and rightsizing recommendations.

### Agent 2: GreenOps Analyzer 📊
Calculates carbon footprint and cost waste from Carbon Scout findings. Classifies risks (LOW/MEDIUM/HIGH).

### Agent 3: Optimization Executor ⚡
Safely executes LOW risk optimizations after human approval. Never touches HIGH risk resources.

### Agent 4: Report Generator 📋
Generates comprehensive GreenOps report with executive summary, recommendations, and carbon impact.

---

## 👨‍💻 Built By

**Raghu Putta** — Cloud Economist | FinOps Engineer | GreenOps Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat&logo=linkedin)](https://linkedin.com)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?style=flat&logo=github)](https://github.com/raghu-putta)

---

## 📄 License

MIT License — feel free to use, modify and distribute.

---

*Built with ❤️ using Google ADK + Gemini 2.5 Pro*
