# 🌱 GreenOps AI Agent

**A 4-agent AI pipeline that scans Google Cloud projects for wasted resources, calculates carbon footprint, and executes safe optimizations — built with Google ADK and Gemini.**

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
- **[Gemini 2.5 Flash](https://deepmind.google/models/gemini/)** — LLM powering all 4 agents
- **Google Cloud SDK** — gcloud CLI for GCP resource scanning
- **Python 3.12+** — core runtime
- **SequentialAgent** — ADK pipeline: each agent passes context to the next

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
├── output/                        # Generated GreenOps reports (markdown)
├── main.py                        # Run real pipeline
├── main_demo.py                   # Run demo pipeline
├── test_api.py                    # Test Gemini API connectivity
└── .env                           # Config (not committed)
```

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/YOUR_USERNAME/greenops-agent.git
cd greenops-agent
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Mac/Linux
pip install google-adk python-dotenv
```

### 2. Configure `.env`

```env
GCP_PROJECT_ID=your-gcp-project-id
GCP_REGION=us-central1
GCP_ZONE=us-central1-a
CARBON_FACTOR_KWH=0.000233

# Get free API key from https://aistudio.google.com/apikey
GOOGLE_API_KEY=your-gemini-api-key
GOOGLE_GENAI_USE_VERTEXAI=0
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

## Sample Output

```
[CARBON_SCOUT]
Found 3 idle VMs, 2 unattached disks, 1 unused IP

[GREENOPS_ANALYZER]
Monthly CO2: 12.4 kg | Annual: 0.149 tons
Monthly cost waste: $122.20
LOW risk actions: 4 | MEDIUM: 1 | HIGH: 0

[OPTIMIZATION_EXECUTOR]
I have found the following LOW risk actions. Do you approve? (yes/no)
→ Stop ml-training-server-01 (idle 45 days, us-central1-a)
→ Stop staging-api-backend (idle 58 days, us-central1-b)

[REPORT_GENERATOR]
# GreenOps AI Report
...full markdown report saved to output/
```

---

## Built With

Built by **Raghu** using Google ADK + Gemini 2.5 Flash.

---

## License

MIT License — free to use, modify, and distribute.
