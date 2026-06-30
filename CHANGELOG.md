# 📋 GreenOps AI — Changelog

## [2.0.0] — June 2026

### ✨ New Features
- **Robot Agent Profiles** — Circular AI robot images for each of the 4 agents
- **Colored Agent Names** — Carbon Scout (green), GreenOps Analyzer (blue), Optimization Executor (orange), Report Generator (purple)
- **Agent Status Labels** — Live status: Scouting / Analyzing / Executing / Generating / Done
- **Configure GCP Panel** — Full credential management with Alex AI assistant
  - Typewriter effect cycling 7 motivational quotes (28ms/char)
  - Glowing eye animation + blinking cursor
  - Password show/hide toggle
  - Test Connection button
  - 8 region options
- **Privacy-First Credential Architecture** — sessionStorage → HTTPS → in-memory → zero retention
- **Download Report Bar** — Appears automatically after pipeline completes
  - PDF (styled print dialog)
  - TXT (plain text file)
  - HTML (web page)
  - CSV (spreadsheet)
  - JSON (developer format)
  - Copy to Clipboard
- **GreenOps Leaf Favicon** — Custom icon in browser tab
- **Live Metrics Sidebar** — Monthly savings, CO2 saved/month, Idle VMs, LOW-risk actions count
- **Professional Footer** — Built by Raghu Putta | GitHub | Live Demo | v2.0
- **Gemini API Warmup** — Pre-warm on app startup to reduce cold start latency

### 🐛 Bug Fixes
- **CRITICAL** — Cloud Run 5-minute timeout was truncating SSE connections → Fixed with `--timeout 3600 --min-instances 1`
- **CRITICAL** — JS syntax error at line 508 killed all page JavaScript → Fixed by validating with `node --check` before every deploy
- **CRITICAL** — Download bar invisible because it was inside hidden welcome div → Moved to correct position before footer
- **CRITICAL** — Two separate `<script>` blocks prevented `showReportDl()` from being visible → Merged into single block
- Broken emoji in terminal output (ðŸŒ±) → Fixed by adding charset=utf-8 to SSE media_type
- Slow pipeline start → Gemini warmup ping on app startup
- `openPanel is not defined` errors → Resolved by single merged script block

### 🔐 Security
- Credential wiring: user credentials from Configure GCP now flow to agents (previously only server env vars were used)
- Single-run lock prevents concurrent pipeline runs and credential bleed between users
- Default environment reset at start of every run as second safety layer

### 🏗️ Infrastructure
- Cloud Run timeout: 300s → 3600s
- Cloud Run min-instances: 0 → 1 (eliminates cold starts)
- All app.py edits via Python scripts only (prevents PowerShell UTF-8 encoding corruption)
- JavaScript validated with Node.js before every deploy

---

## [1.0.0] — May 2026

### 🎉 Initial Release
- 4-agent pipeline: Carbon Scout → GreenOps Analyzer → Optimization Executor → Report Generator
- FastAPI backend with SSE streaming
- Real-time terminal output dashboard
- Cloud Run deployment
- GCP resource scanning (VMs, disks, IPs, rightsizing)
- Carbon footprint calculation
- Demo mode with simulated GCP data
