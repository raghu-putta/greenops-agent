# GreenOps — Marketplace Listing Content
> Ready-to-paste copy for Google Cloud Marketplace, Product Hunt, and GitHub.
> Written in plain English. No jargon. No fluff.

---

## ═══════════════════════════════════════════
## 1. GOOGLE CLOUD MARKETPLACE
## ═══════════════════════════════════════════

### Product Name
```
GreenOps — AI Cloud Cost & Carbon Intelligence Agent
```

### Tagline (one line, shown under the name)
```
Stop wasting money on idle GCP resources. Get hourly AI-powered alerts in Gmail and Slack — automatically.
```

### Short Description (160 characters — used in search results)
```
AI agent that scans your GCP project every hour, finds wasted resources, calculates CO₂ impact, and alerts you via Gmail and Slack.
```

### Category
```
Primary:   Operations > Monitoring & Management
Secondary: AI & Machine Learning > Intelligent Agents
```

### Long Description (this is the main listing body)

---

**What is GreenOps?**

GreenOps is an AI-powered agent that watches your Google Cloud Platform project around the clock. Every hour, it scans for wasted cloud resources — idle virtual machines, unused storage disks, and reserved IP addresses sitting idle — and tells you exactly what's costing you money and why.

It doesn't just find problems. It uses a 4-agent AI pipeline (built on Google ADK and Gemini) to analyze each issue, estimate the monthly cost waste, calculate the carbon emissions, and give you a prioritized action plan. Then it sends a full report straight to your Gmail inbox and Slack channel — automatically, every hour, without you doing anything.

---

**Who is this for?**

- Startup founders and engineers who want to keep GCP costs under control without hiring a full FinOps team
- Cloud architects responsible for sustainability and carbon reporting
- DevOps and SRE teams who want proactive waste alerts instead of surprise bills
- Any GCP user who wants to know what's wasting money before their next invoice

---

**What does it actually find?**

- **Idle Virtual Machines** — VMs that are running but doing nothing. Each idle VM costs real money every hour.
- **Unattached Persistent Disks** — Storage disks that were created but never deleted after the VM was removed. Silent cost drain.
- **Unused Reserved IPs** — IP addresses you reserved but aren't pointing at anything. Each one costs ~$7.20/month.
- **Rightsizing Recommendations** — VMs that are oversized for their actual workload. Suggested machine type downgrades from GCP Recommender.

---

**How does the AI pipeline work?**

GreenOps runs a 4-agent sequential pipeline every hour:

1. **Carbon Scout** — Calls the GCP Compute API and collects raw data on all your running resources. No changes made — scan only.
2. **GreenOps Analyzer** — Takes the scan results and calculates the real dollar cost and CO₂ footprint of each wasteful resource.
3. **Optimization Executor** — Proposes specific actions (stop this VM, release that IP). LOW risk actions are listed for your approval — nothing is executed automatically without your say-so.
4. **Report Generator** — Formats everything into a clean, readable report and sends it to Gmail and Slack.

---

**What you get out of the box:**

- ✅ Fully automated hourly GCP scans — no manual work
- ✅ Rich HTML email report in your Gmail inbox every hour
- ✅ Slack Block Kit message in your #greenops-alerts channel
- ✅ Carbon footprint calculation (kg CO₂/month and tons/year)
- ✅ Monthly cost savings estimate per wasteful resource
- ✅ Risk-classified action plan (LOW / MEDIUM / HIGH)
- ✅ Human approval gate — AI proposes, you decide
- ✅ Runs on Cloud Run — scales to zero when idle, zero server management
- ✅ Authenticates via Application Default Credentials — no JSON key files in production

---

**Tech stack:**
Google ADK · Gemini Flash · FastAPI · Google Cloud Run · Cloud Scheduler · Python SDK (google-cloud-compute, google-cloud-recommender)

---

**Pricing:**
Free and open source. Runs entirely within Google Cloud free tier limits (Cloud Run: 2M requests/month free, Gemini AI Studio: 1,500 requests/day free).

---

**Support:**
GitHub Issues · Community maintained

---

### Features List (shown as bullet chips on listing page)
```
• Hourly automated GCP resource scanning
• 4-agent AI pipeline (Google ADK + Gemini)
• Gmail + Slack alerts with full HTML reports
• Carbon footprint calculation per resource
• Cost savings estimation (monthly + annual)
• Risk-classified action plan (LOW/MEDIUM/HIGH)
• Human approval gate — no auto-execution
• Cloud Run serverless deployment
• Application Default Credentials auth (no key files)
• GCP Recommender integration for rightsizing
```

### Getting Started (shown in the "Documentation" tab)

```
Step 1: Clone the repository
   git clone https://github.com/YOUR_USERNAME/greenops-agent

Step 2: Set your environment variables
   Copy .env.example to .env and fill in:
   - GCP_PROJECT_ID (your GCP project to scan)
   - GOOGLE_API_KEY (from aistudio.google.com/app/apikey — free)
   - GMAIL_USER and GMAIL_APP_PASSWORD (for email alerts)
   - SLACK_WEBHOOK_URL (from your Slack app settings)
   - SCHEDULER_SECRET (any strong random string)

Step 3: Deploy to Cloud Run
   gcloud run deploy greenops-agent --source . --region us-central1 --allow-unauthenticated

Step 4: Set up hourly automation
   gcloud scheduler jobs create http greenops-hourly-scan \
     --schedule="0 * * * *" \
     --uri="YOUR_SERVICE_URL/scheduled-scan" \
     --http-method=POST \
     --headers="X-Scheduler-Secret=YOUR_SECRET,Content-Type=application/json" \
     --message-body="{}" \
     --location=us-central1

Step 5: Open your dashboard
   Visit your Cloud Run URL and click "Run Real GCP" to see the 4 AI agents run live.
   Within the hour, you'll get your first Gmail + Slack alert.
```

### Screenshots / Demo Links to prepare:
- Screenshot of the live streaming dashboard (4 agents running)
- Screenshot of the Gmail HTML report
- Screenshot of the Slack Block Kit message in #greenops-alerts
- Architecture diagram (from the transcript doc)

---

## ═══════════════════════════════════════════
## 2. PRODUCT HUNT LAUNCH
## ═══════════════════════════════════════════

### Product Name
```
GreenOps
```

### Tagline (60 chars max — the most important line on Product Hunt)
```
AI agent that hunts idle GCP resources and kills your cloud waste
```

### Topics / Tags
```
Artificial Intelligence, Developer Tools, Cloud Computing, Productivity, Open Source, SaaS
```

### First Comment (written by the maker — this shows first and gets the most reads)

---

Hey Product Hunt! 👋

I built GreenOps because I got tired of GCP surprise bills caused by resources I forgot about — idle VMs, orphaned disks, reserved IPs pointing at nothing.

**What it does in one sentence:** GreenOps watches your GCP project 24/7, finds anything wasting money or generating unnecessary CO₂, and texts you about it in Gmail and Slack every single hour.

**Under the hood it's actually pretty cool:**

It runs a 4-agent AI pipeline using Google's Agent Development Kit (ADK) + Gemini:
- Agent 1 scans your GCP project via the Compute API
- Agent 2 calculates real dollar cost and carbon footprint per wasteful resource
- Agent 3 proposes actions (with a human approval gate — nothing auto-deletes)
- Agent 4 formats a full report and fires it off to Gmail + Slack

The whole thing runs on Cloud Run (serverless — scales to zero, you pay nothing when idle) and costs $0 to run on the free tier.

**The part I'm most proud of:** The human approval gate in Agent 3. The AI tells you what to do but never does it without your explicit "yes." I think that's how autonomous cloud agents should work.

Would love to hear what you think — especially if you run into wasteful resources it finds! 🌱

---

### Gallery Images (prepare these 3):
1. `dashboard.png` — the live streaming dashboard with 4 agents running
2. `email-report.png` — the HTML Gmail report
3. `slack-alert.png` — the Slack message in #greenops-alerts

---

## ═══════════════════════════════════════════
## 3. GITHUB — README / MARKETPLACE LISTING
## ═══════════════════════════════════════════

### Repository Description (shown under the repo name, 350 chars max)
```
🌱 AI agent that scans your GCP project every hour for idle VMs, unattached disks, and unused IPs. Calculates CO₂ footprint + cost waste. Sends Gmail + Slack alerts automatically. Built with Google ADK, Gemini, FastAPI, and Cloud Run.
```

### Topics / Tags (add these to the repo)
```
gcp, google-cloud, cloud-cost-optimization, ai-agent, multi-agent, carbon-footprint,
gemini, google-adk, fastapi, cloud-run, finops, greenops, sustainability, devops, python
```

### GitHub README — Hero Section

```markdown
# 🌱 GreenOps

**AI-powered cloud cost & carbon intelligence agent for Google Cloud Platform.**

GreenOps watches your GCP project 24/7. Every hour, it automatically scans for
wasted resources, calculates the CO₂ impact, estimates monthly cost savings,
and sends a full report to your Gmail inbox and Slack channel.

No dashboards to check. No manual work. Just alerts when something needs attention.

[![Deploy to Cloud Run](https://deploy.cloud.run/button.svg)](https://deploy.cloud.run)
```

### README — How It Works Section

```markdown
## How It Works

GreenOps runs a 4-agent AI pipeline on every scheduled scan:

| Agent | Role |
|-------|------|
| 🛰️ Carbon Scout | Calls GCP Compute API — finds idle VMs, orphaned disks, unused IPs |
| 🧠 GreenOps Analyzer | Calculates monthly cost waste + CO₂ footprint per resource |
| ⚙️ Optimization Executor | Proposes actions — LOW/MEDIUM/HIGH risk. Waits for your approval. |
| 📋 Report Generator | Formats and sends full report to Gmail + Slack |

**Human approval gate is non-negotiable.** The agent proposes. You decide.
```

### README — What It Finds Section

```markdown
## What It Finds

- **Idle VMs** — Running but 0% CPU. Waste: ~$10–80/month each depending on machine type
- **Unattached Disks** — Provisioned but not mounted to any VM. Waste: ~$0.04/GB/month
- **Unused Reserved IPs** — Allocated but not pointing anywhere. Waste: $7.20/month each
- **Rightsizing Opportunities** — Oversized VMs flagged by GCP Recommender
```

### README — Quick Start Section

```markdown
## Quick Start

1. **Clone**
   ```bash
   git clone https://github.com/YOUR_USERNAME/greenops-agent
   cd greenops-agent
   ```

2. **Configure** — copy `.env.example` to `.env` and fill in your credentials

3. **Deploy**
   ```bash
   gcloud run deploy greenops-agent --source . --region us-central1 --allow-unauthenticated
   ```

4. **Schedule** — create an hourly Cloud Scheduler job pointing to your service URL

5. **Watch** — open the dashboard and click "Run Real GCP" to see the agents work live
```

### README Badges (add at top of README)

```markdown
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
![Built with Google ADK](https://img.shields.io/badge/Built%20with-Google%20ADK-4285F4?logo=google)
![Powered by Gemini](https://img.shields.io/badge/Powered%20by-Gemini-8E44AD)
![Deployed on Cloud Run](https://img.shields.io/badge/Cloud%20Run-Serverless-blue?logo=googlecloud)
![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue?logo=python)
```

---

## ═══════════════════════════════════════════
## 4. HOW TO ACTUALLY SUBMIT — STEP BY STEP
## ═══════════════════════════════════════════

### Google Cloud Marketplace — Submission Steps

```
1. Go to: https://cloud.google.com/marketplace/sell
2. Click "Become a Partner" → fill in your business details (individual is fine)
3. Choose listing type: "Cloud Run service" or "Solution"
4. Fill in the fields using the copy above
5. Upload 3 screenshots (dashboard, email, slack)
6. Set pricing: FREE (open source)
7. Submit for review — Google reviews in 5–10 business days
8. Once approved, your listing goes live at cloud.google.com/marketplace
```

### Product Hunt — Submission Steps

```
1. Go to: https://www.producthunt.com/posts/new
2. Create a maker account (free)
3. Fill in: Name, Tagline, Topics, URL (your Cloud Run URL or GitHub)
4. Upload 3 gallery images
5. Add the First Comment text above
6. Choose a Tuesday or Wednesday launch — highest traffic days
7. Tell 5–10 people to upvote and leave comments on launch day
8. First 24 hours determine your daily ranking
```

### GitHub — Steps to Polish the Repo

```
1. Update README.md with the content above
2. Add Topics/Tags to the repo (Settings → Topics)
3. Update Description (Settings → About section)
4. Add a LICENSE file (MIT recommended)
5. Create a .env.example file with all variables listed (no real values)
6. Add screenshots to a /docs folder and reference them in README
7. Push everything — the repo is now marketplace-ready
```

---

## ═══════════════════════════════════════════
## 5. PITCH IN ONE PARAGRAPH (for emails, LinkedIn, anywhere)
## ═══════════════════════════════════════════

```
I built GreenOps — an open-source AI agent that scans your Google Cloud Platform
project every hour for wasted resources. It finds idle VMs, orphaned storage disks,
and unused reserved IPs, calculates how much they're costing you per month and how
much CO₂ they're generating, and sends a full report to Gmail and Slack automatically.

Under the hood it's a 4-agent AI pipeline built on Google Agent Development Kit (ADK)
and Gemini, deployed serverless on Cloud Run. The whole system costs $0 to run on
the free tier. Nothing gets deleted or changed without your explicit approval —
the agent proposes, you decide.

It took one weekend to build and now runs forever without any maintenance.
```

---

*Content prepared for Raghu · GreenOps Agent · May 2026*
*GitHub: YOUR_REPO_URL | Cloud Run: https://greenops-dashboard-845589445410.us-central1.run.app*
