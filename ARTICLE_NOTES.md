# 📝 GreenOps AI — Article Notes & Key Points
*Reference document for writing the publication article. Built by Raghu Putta.*

---

## 1. Project Overview (the hook)

**GreenOps AI** — a 4-agent agentic AI pipeline that scans a GCP project for wasted cloud spend and carbon emissions, then safely optimizes and reports.

- **Live Demo:** https://greenops-dashboard-845589445410.us-central1.run.app
- **GitHub:** https://github.com/raghu-putta/greenops-agent
- **Stack:** FastAPI + Google ADK + Gemini 2.5 Pro + Cloud Run
- **Built by:** Raghu Putta — Cloud Economist | FinOps Engineer | GreenOps Engineer

---

## 2. The 4-Agent Architecture

```
Carbon Scout → GreenOps Analyzer → Optimization Executor → Report Generator
```

| Agent | Color | Role |
|---|---|---|
| 🔍 Carbon Scout | Green #34d399 | Scans GCP for idle VMs, unattached disks, unused reserved IPs, rightsizing recommendations |
| 📊 GreenOps Analyzer | Blue #60a5fa | Calculates carbon footprint + cost waste, classifies risk (LOW/MEDIUM/HIGH) |
| ⚡ Optimization Executor | Orange #f97316 | Executes only LOW-risk actions; MEDIUM/HIGH wait for human approval |
| 📋 Report Generator | Purple #a78bfa | Produces executive report: savings, CO2 impact, 30-day recommendations |

**Key design principle:** human-in-the-loop safety — the AI never deletes/stops anything risky without approval.

---

## 3. Configure GCP — Privacy-First Credential Flow (article highlight!)

### The 6-step zero-retention flow:

1. **User enters credentials** — Gemini API Key, GCP Project ID, Region, Zone in the Configure GCP panel
2. **Browser-only storage** — saved in `sessionStorage`: belongs only to that browser tab, never written to any database, never logged, auto-wiped when tab closes
3. **Run Real GCP clicked** — browser sends credentials with that ONE request over HTTPS (encrypted)
4. **Backend in-memory only** — credentials held in RAM for this single pipeline run; nothing touches disk
5. **Agents use the USER's credentials** — they scan the user's own project, with the user's own Gemini key
6. **Credentials evaporate** — run ends → RAM cleared; tab closes → sessionStorage cleared. **Zero retention.**

### Safety mechanism:
- **Single-run lock** (`pipeline_status["running"]` → HTTP 409) means only one pipeline at a time — one user's credentials can never bleed into another user's run
- Defaults reset at the start of every run as a second safety layer

### UX details worth mentioning:
- Alex AI assistant with typewriter effect cycling motivational quotes (28ms/char, glowing-eye animation, blinking cursor)
- Password field with show/hide toggle (👁)
- "Test Connection" validation button
- Helpful links: aistudio.google.com/apikey (free key), console.cloud.google.com

---

## 4. Credential Architecture Decision (great "engineering trade-offs" section)

| Option | Approach | Verdict |
|---|---|---|
| **A. Env-override per run** ✅ CHOSEN | Credentials applied in RAM for one run, reset after; protected by single-run lock | Standard for demo/portfolio apps; honest privacy story |
| **B. Parameter refactor** | Pass credentials through every agent constructor | Cleaner for high-traffic, but overkill now |
| **C. Google OAuth sign-in** | Users log in with Google; no key copy-paste | True commercial SaaS grade → **Phase 3 roadmap** |

**Article angle:** "Ship the safe simple version, upgrade when traffic demands it" — how real startups do it.

---

## 5. War Story: The SSE Truncation Bug (best article material!)

**Symptom:** Terminal output completely blank even though backend was sending events.

**Root cause found in Cloud Run logs:** `"Truncated response body. Usually implies that the request timed out"` — Cloud Run's **default 5-minute timeout was killing the SSE (Server-Sent Events) stream** mid-pipeline.

**The fix:**
```bash
gcloud run services update greenops-dashboard --region us-central1 --timeout 3600 --min-instances 1
```
- `--timeout 3600` → connections allowed up to 60 minutes
- `--min-instances 1` → one warm instance always ready (no cold starts)

**Second bug found via F12 DevTools:** one JavaScript syntax error at a single line killed ALL page JS (`openPanel is not defined`, `showReportDl is not defined`). Lesson: **one bad token nukes the whole script block** — validate JS with `node --check` before shipping.

**Other fixes worth a paragraph:**
- Gemini warmup ping on app startup (1-token generate) to cut cold-start latency
- UTF-8 charset on SSE media type to fix broken emoji (ðŸŒ± → 🌱)
- PowerShell `Set-Content` corrupts UTF-8 emoji in source files → always edit via Python scripts

---

## 6. Features Shipped (v2.0 changelog)

- 🤖 Circular robot profile images per agent with agent-colored glow borders
- 🏷️ Agent-specific status labels: Scouting / Analyzing / Executing / Generating
- ⚙️ Configure GCP panel with Alex AI assistant (typewriter + animations)
- ⬇️ Download report bar after pipeline completes: **PDF, TXT, HTML, CSV, JSON, Copy** — all client-side (Blob API), zero auth required
- 🌱 GreenOps leaf favicon
- 📊 Live Metrics sidebar: monthly savings, CO2 saved, idle VMs, LOW-risk actions
- 👣 Footer: Built by Raghu Putta | GitHub | Live Demo | v2.0 | Powered by Google ADK + Gemini 2.5 Pro
- 🔐 Privacy-first user credential wiring (Configure GCP → agents)

---

## 7. Phase 2 Roadmap: RAG (second article topic!)

**"How I added RAG to GreenOps AI — making it 40% more accurate"**

### Research findings (March 2026 data):
- Google ADK + LangChain are fully compatible: ADK orchestrates agents, LangChain powers RAG retrieval tools (ChromaDB / Vertex AI RAG Engine as vector store)
- ADK + Gemini 2.5 Flash benchmarks: 1.2s cold start / 0.4s warm; 3-agent sequential pipeline ≈ 4.2s
- RAG accuracy boost: **35–48% improvement** in recommendation precision
- Cost: vector DB + embeddings ≈ **$165–470/month** extra
- RAG adds ~0.8s retrieval latency per query

### Planned RAG knowledge sources:
1. Real-time GCP pricing data (replace hardcoded estimates)
2. Carbon emission factors database
3. Historical scan reports (agents learn from past scans)
4. Official GCP documentation (agents cite docs in reports)

### Recommended approach: **Vertex AI RAG Engine** — native GCP, no extra infrastructure

---

## 8. Phase 3 Roadmap

- Google OAuth sign-in (Option C) — commercial-grade credential handling
- Multi-cloud support potential (AWS/Azure scanners)
- Scheduled scans (endpoint `/scheduled-scan` already exists with secret header)

---

## 9. Numbers & Facts Cheat Sheet

| Fact | Value |
|---|---|
| Agents | 4 (sequential pipeline) |
| Model | Gemini 2.5 Pro |
| Framework | Google ADK |
| Deployment | Cloud Run, us-central1, min-instances 1, timeout 3600s |
| Download formats | 6 (PDF/TXT/HTML/CSV/JSON/Copy) |
| Credential retention | Zero (sessionStorage + in-memory) |
| RAG accuracy gain (Phase 2) | 35–48% |
| RAG est. cost | $165–470/mo |

---

*Last updated: June 12, 2026*
