"""
GreenOps AI Web Dashboard
Real-time 4-agent pipeline visualization with FastAPI + SSE streaming.

Run:  uvicorn app:app --reload --port 8000
Open: http://localhost:8000
"""
import asyncio
import json
import logging
import os
import re
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse

logger = logging.getLogger(__name__)

load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")
if api_key:
    os.environ["GOOGLE_API_KEY"] = api_key

app = FastAPI(title="GreenOps AI Dashboard")

# ── Global state ──────────────────────────────────────────────────────────────
pipeline_status = {"running": False, "complete": False}
_sse_queues: list = []


async def _broadcast(data: dict):
    """Push event to every connected browser."""
    msg = json.dumps(data)
    for q in list(_sse_queues):
        try:
            await q.put(msg)
        except Exception:
            pass


# ── Rate-limit helpers ────────────────────────────────────────────────────────
MAX_RETRIES = 5

# Exponential backoff delays for 503 errors (seconds): 20, 40, 80, 120, 180
_503_BACKOFF = [20, 40, 80, 120, 180]

def _is_retryable_error(e: Exception) -> bool:
    """Return True for transient Gemini API errors worth retrying:
    - 429 RESOURCE_EXHAUSTED  (quota limit)
    - 503 UNAVAILABLE         (model overloaded / high demand)
    """
    msg = str(e)
    return (
        "429" in msg
        or "RESOURCE_EXHAUSTED" in msg
        or "quota" in msg.lower()
        or "503" in msg
        or "UNAVAILABLE" in msg
        or "high demand" in msg.lower()
        or "overloaded" in msg.lower()
    )

def _retry_delay_seconds(e: Exception, attempt: int) -> int:
    """
    Exponential backoff for retries.
    503 UNAVAILABLE → exponential: 20s, 40s, 80s, 120s, 180s
    429 RESOURCE_EXHAUSTED → extracts retryDelay from payload or defaults to 65s.
    """
    msg = str(e)
    # Always honour explicit retryDelay from API payload
    match = re.search(r"retryDelay['\"]?\s*:\s*['\"](\d+)s", msg)
    if match:
        return int(match.group(1)) + 5
    if "503" in msg or "UNAVAILABLE" in msg or "high demand" in msg.lower() or "overloaded" in msg.lower():
        idx = min(attempt - 1, len(_503_BACKOFF) - 1)
        return _503_BACKOFF[idx]
    return 65  # safe default for 429 quota window


# ── Pipeline runner (with auto-retry on 429) ──────────────────────────────────
async def run_pipeline(mode: str):
    global pipeline_status
    pipeline_status = {"running": True, "complete": False}

    if mode == "demo":
        from agents.greenops_pipeline_demo import greenops_pipeline_demo as pipeline
        project = "greenops-demo-project"
    else:
        from agents.greenops_pipeline import greenops_pipeline as pipeline
        project = os.getenv("GCP_PROJECT_ID", "your-gcp-project")

    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.genai.types import Content, Part

    await _broadcast({
        "type": "start",
        "mode": mode,
        "time": datetime.now().strftime("%H:%M:%S")
    })

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session_service = InMemorySessionService()
            runner = Runner(
                agent=pipeline,
                app_name="greenops_web",
                session_service=session_service
            )
            session = await session_service.create_session(
                app_name="greenops_web",
                user_id="web_user"
            )
            user_msg = Content(
                role="user",
                parts=[Part(text=f"Run full GreenOps analysis for GCP project {project}")]
            )

            async for event in runner.run_async(
                user_id="web_user",
                session_id=session.id,
                new_message=user_msg
            ):
                if hasattr(event, "author") and hasattr(event, "content") and event.content:
                    if event.content.parts:
                        text = event.content.parts[0].text
                        if text and text.strip():
                            await _broadcast({
                                "type": "agent",
                                "agent": event.author.upper(),
                                "text": text,
                                "time": datetime.now().strftime("%H:%M:%S")
                            })

            # ── Success ──────────────────────────────────────────────────────
            await _broadcast({"type": "done", "time": datetime.now().strftime("%H:%M:%S")})
            break  # exit retry loop on success

        except Exception as e:
            if _is_retryable_error(e) and attempt < MAX_RETRIES:
                wait = _retry_delay_seconds(e, attempt)
                msg = str(e)
                if "503" in msg or "UNAVAILABLE" in msg or "high demand" in msg.lower():
                    label = f"⏳ Gemini API model overloaded (attempt {attempt}/{MAX_RETRIES}). Auto-retrying in {wait}s…"
                else:
                    label = (
                        f"⏳ Gemini rate limit hit (attempt {attempt}/{MAX_RETRIES}). "
                        f"Auto-retrying in {wait}s — this is normal on the free plan."
                    )
                await _broadcast({
                    "type": "retry",
                    "attempt": attempt,
                    "max": MAX_RETRIES,
                    "wait": wait,
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "message": label
                })
                logger.warning("Retryable error on attempt %d/%d — waiting %ds: %s", attempt, MAX_RETRIES, wait, e)
                await asyncio.sleep(wait)
                # continue → next attempt
            else:
                # Non-retryable error, or exhausted all retries
                if _is_retryable_error(e):
                    friendly = (
                        "🚫 Pipeline failed after 3 retries.\n\n"
                        "If you saw '503 UNAVAILABLE': Gemini API was overloaded — try again in 30s.\n"
                        "If you saw '429 RESOURCE_EXHAUSTED': quota hit — wait ~1 min or enable billing."
                    )
                    await _broadcast({"type": "error", "message": friendly})
                else:
                    await _broadcast({"type": "error", "message": str(e)})
                break

    pipeline_status = {"running": False, "complete": True}


# ── API endpoints ─────────────────────────────────────────────────────────────
@app.get("/stream")
async def stream():
    """SSE endpoint — browser connects here to receive live events."""
    q: asyncio.Queue = asyncio.Queue()
    _sse_queues.append(q)

    async def generator():
        try:
            while True:
                msg = await q.get()
                yield f"data: {msg}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if q in _sse_queues:
                _sse_queues.remove(q)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.post("/run/{mode}")
async def run(mode: str):
    if pipeline_status["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)
    if mode not in ("demo", "real"):
        return JSONResponse({"error": "mode must be 'demo' or 'real'"}, status_code=400)
    asyncio.create_task(run_pipeline(mode))
    return {"status": "started", "mode": mode}


@app.get("/status")
async def status():
    return pipeline_status


@app.post("/scheduled-scan")
async def scheduled_scan(x_scheduler_secret: str = Header(default="")):
    """
    Cloud Scheduler calls this endpoint every hour.
    Protected by X-Scheduler-Secret header — set SCHEDULER_SECRET env var.
    Runs a full GCP resource scan and sends results to Gmail + Slack.
    """
    expected = os.getenv("SCHEDULER_SECRET", "")
    if not expected:
        raise HTTPException(status_code=500, detail="SCHEDULER_SECRET env var not set")
    if x_scheduler_secret != expected:
        raise HTTPException(status_code=401, detail="Invalid scheduler secret")

    try:
        from scheduler import run_scheduled_scan
        # Run in a thread so it doesn't block the event loop
        import asyncio
        result = await asyncio.get_event_loop().run_in_executor(None, run_scheduled_scan)
        return JSONResponse(result)
    except Exception as e:
        logger.error(f"/scheduled-scan failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


# ── Dashboard HTML ────────────────────────────────────────────────────────────
DASHBOARD = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>🌱 GreenOps AI Dashboard</title>
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  body{font-family:'Segoe UI',system-ui,sans-serif;background:#0d1117;color:#c9d1d9;height:100vh;display:flex;flex-direction:column;overflow:hidden}

  /* ── Header ── */
  .header{background:#161b22;border-bottom:1px solid #30363d;padding:14px 28px;display:flex;align-items:center;justify-content:space-between;flex-shrink:0}
  .logo{font-size:1.25rem;font-weight:700;color:#58a6ff;display:flex;align-items:center;gap:8px}
  .logo-green{color:#3fb950}
  .header-right{display:flex;align-items:center;gap:20px}
  .status-badge{display:flex;align-items:center;gap:7px;font-size:.8125rem;color:#8b949e}
  .dot{width:9px;height:9px;border-radius:50%;background:#30363d;transition:background .3s}
  .dot.running{background:#f0883e;animation:pulse 1.2s ease-in-out infinite}
  .dot.done{background:#3fb950}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}

  /* ── Agent cards ── */
  .agents-strip{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:#21262d;border-bottom:1px solid #21262d;flex-shrink:0}
  .acard{background:#161b22;padding:16px 20px;display:flex;flex-direction:column;gap:5px;border-top:3px solid transparent;transition:all .3s}
  .acard.active{background:#1c2128;border-top-color:#f0883e}
  .acard.done{border-top-color:#3fb950}
  .acard-icon{font-size:1.4rem}
  .acard-num{font-size:.6875rem;font-weight:600;color:#484f58;text-transform:uppercase;letter-spacing:.8px}
  .acard-name{font-size:.9rem;font-weight:600;color:#c9d1d9}
  .acard-status{font-size:.75rem;color:#484f58}
  .acard-status.running{color:#f0883e}
  .acard-status.done{color:#3fb950}

  /* ── Main area ── */
  .main{display:grid;grid-template-columns:1fr 300px;flex:1;overflow:hidden;min-height:0}

  /* ── Terminal ── */
  .terminal{background:#0d1117;padding:18px 22px;overflow-y:auto;font-family:'Consolas','JetBrains Mono','Courier New',monospace;font-size:.8rem;line-height:1.65}
  .terminal::-webkit-scrollbar{width:5px}
  .terminal::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px}
  .welcome{text-align:center;padding:60px 24px;color:#8b949e}
  .welcome h2{color:#58a6ff;font-size:1.1rem;margin-bottom:10px}
  .welcome p{font-size:.8125rem;line-height:1.7}
  .welcome .hint{margin-top:18px;font-size:.75rem;color:#484f58}
  .t-separator{color:#30363d;margin:12px 0}
  .t-timestamp{color:#484f58;font-size:.7rem}
  .t-agent-hdr{margin:18px 0 8px;padding:6px 14px;background:#161b22;border-left:3px solid #3fb950;border-radius:0 4px 4px 0;color:#3fb950;font-weight:700;font-size:.8125rem}
  .t-agent-hdr:first-child{margin-top:0}
  .t-text{color:#c9d1d9;white-space:pre-wrap;word-break:break-word}
  .t-error{color:#f85149}

  /* ── Sidebar ── */
  .sidebar{background:#161b22;border-left:1px solid #21262d;display:flex;flex-direction:column;overflow-y:auto}
  .sidebar::-webkit-scrollbar{width:5px}
  .sidebar::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px}
  .sb-section{padding:18px 18px;border-bottom:1px solid #21262d}
  .sb-title{font-size:.6875rem;font-weight:600;color:#484f58;text-transform:uppercase;letter-spacing:1px;margin-bottom:14px}

  /* Run buttons */
  .btn{width:100%;padding:11px 14px;border:none;border-radius:7px;font-size:.875rem;font-weight:600;cursor:pointer;margin-bottom:8px;transition:all .15s;letter-spacing:.2px}
  .btn-demo{background:#238636;color:#fff}
  .btn-demo:hover:not(:disabled){background:#2ea043;transform:translateY(-1px)}
  .btn-real{background:#1f6feb;color:#fff}
  .btn-real:hover:not(:disabled){background:#388bfd;transform:translateY(-1px)}
  .btn:disabled{opacity:.45;cursor:not-allowed;transform:none!important}
  .model-pill{background:#21262d;border-radius:6px;padding:8px 11px;font-size:.75rem;color:#8b949e;display:flex;align-items:center;gap:6px;margin-top:4px}
  .model-pill b{color:#d2a8ff}

  /* Metrics */
  .metric{background:#21262d;border-radius:8px;padding:12px 14px;margin-bottom:9px;display:flex;align-items:center;gap:12px}
  .metric-icon{font-size:1.35rem;flex-shrink:0}
  .metric-val{font-size:1.1rem;font-weight:700;color:#3fb950;line-height:1.2}
  .metric-lbl{font-size:.7rem;color:#8b949e}

  /* Footer links */
  .sb-links{padding:14px 18px;margin-top:auto}
  .sb-link{display:flex;align-items:center;gap:7px;color:#58a6ff;text-decoration:none;font-size:.8rem;padding:6px 0}
  .sb-link:hover{text-decoration:underline}
        .btn-settings{background:#1a3a2a;color:#34d399;border:1px solid #34d399;padding:10px 20px;border-radius:8px;cursor:pointer;font-size:0.85rem;font-weight:600;margin-top:8px;width:100%;transition:all 0.2s;} .btn-settings:hover{background:#34d399;color:#0a1a0f;} .settings-panel{position:fixed;top:0;left:0;width:100%;height:100%;z-index:1000;display:none;} .settings-overlay{position:absolute;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.75);backdrop-filter:blur(6px);} .settings-drawer{position:absolute;right:0;top:0;width:420px;height:100%;background:#0d1f14;border-left:1px solid #1a3a2a;overflow-y:auto;} .settings-header{display:flex;justify-content:space-between;align-items:center;padding:20px 24px;border-bottom:1px solid #1a3a2a;background:#0a1a0f;} .settings-header h2{color:#34d399;margin:0;font-size:1.1rem;} .settings-close{background:none;border:1px solid #333;color:#999;width:30px;height:30px;border-radius:6px;cursor:pointer;} .settings-body{padding:20px 24px;} .settings-desc{color:#6b7280;font-size:0.82rem;margin-bottom:20px;line-height:1.6;} .settings-group{margin-bottom:18px;} .settings-group label{display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:5px;} .settings-group input,.settings-group select{width:100%;background:#1a2a1f;border:1px solid #2a4a3a;color:#e5e7eb;padding:9px 12px;border-radius:7px;font-size:0.88rem;box-sizing:border-box;} .settings-group input:focus,.settings-group select:focus{outline:none;border-color:#34d399;} .settings-group small{display:block;color:#6b7280;font-size:0.72rem;margin-top:3px;} .settings-group small a{color:#34d399;text-decoration:none;} .settings-actions{display:flex;gap:8px;margin-top:24px;} .btn-save{flex:1;background:#34d399;color:#0a1a0f;border:none;padding:11px;border-radius:7px;font-weight:700;cursor:pointer;font-size:0.88rem;} .btn-test{flex:1;background:transparent;color:#34d399;border:1px solid #34d399;padding:11px;border-radius:7px;font-weight:600;cursor:pointer;font-size:0.88rem;} .conn-status{margin-top:12px;padding:10px;border-radius:7px;font-size:0.82rem;text-align:center;display:none;} .conn-status.ok{background:#1a3a2a;border:1px solid #34d399;color:#34d399;} .conn-status.err{background:#3a1a1a;border:1px solid #ef4444;color:#ef4444;} .settings-footer{margin-top:24px;padding:14px;background:#0a1a0f;border-radius:8px;border:1px solid #1a3a2a;} .settings-footer p{color:#4b5563;font-size:0.72rem;text-align:center;margin:0;line-height:1.6;}
</style>
</head>
<body>

<!-- ── Header ── -->
<div class="header">
  <div class="logo">🌱 GreenOps <span class="logo-green">AI Dashboard</span></div>
  <div class="header-right">
    <div class="status-badge">
      <div class="dot" id="dot"></div>
      <span id="status-txt">Ready</span>
    </div>
  </div>
</div>

<!-- ── Agent cards ── -->
<div class="agents-strip">
  <div class="acard" id="card-carbon_scout">
    <div class="acard-icon">🔍</div>
    <div class="acard-num">Agent 1</div>
    <div class="acard-name">Carbon Scout</div>
    <div class="acard-status" id="cs-status">Waiting</div>
  </div>
  <div class="acard" id="card-greenops_analyzer">
    <div class="acard-icon">📊</div>
    <div class="acard-num">Agent 2</div>
    <div class="acard-name">GreenOps Analyzer</div>
    <div class="acard-status" id="ga-status">Waiting</div>
  </div>
  <div class="acard" id="card-optimization_executor">
    <div class="acard-icon">⚡</div>
    <div class="acard-num">Agent 3</div>
    <div class="acard-name">Optimization Executor</div>
    <div class="acard-status" id="oe-status">Waiting</div>
  </div>
  <div class="acard" id="card-report_generator">
    <div class="acard-icon">📋</div>
    <div class="acard-num">Agent 4</div>
    <div class="acard-name">Report Generator</div>
    <div class="acard-status" id="rg-status">Waiting</div>
  </div>
</div>

<!-- ── Main ── -->
<div class="main">

  <!-- Terminal -->
  <div class="terminal" id="terminal">
    <div class="welcome" style="padding:0;position:relative;overflow:hidden;border-radius:8px">
      <canvas id="bgcanvas" style="width:100%;display:block;border-radius:8px;max-height:340px"></canvas>
      <div style="position:absolute;bottom:0;left:0;right:0;padding:18px;background:linear-gradient(transparent,rgba(2,14,8,0.95));text-align:center">
        <h2 style="color:#34d399;font-size:1rem;margin-bottom:6px">🌱 Welcome to GreenOps AI Dashboard</h2>
        <p style="font-size:0.78rem;color:#6ee7b7">Click <strong style="color:#3fb950">Run Demo</strong> to scan a simulated GCP project or <strong style="color:#58a6ff">Run Real GCP</strong> to scan your actual cloud.</p>
        <div style="font-size:0.7rem;color:#34d399;margin-top:6px;opacity:0.7">Powered by Google ADK + Gemini 2.5 Pro ✨</div>
      </div>
    </div>
  </div>

  <!-- Sidebar -->
  <div class="sidebar">

    <div class="sb-section">
      <div class="sb-title">Run Pipeline</div>
      <button class="btn btn-demo" id="btn-demo" onclick="run('demo')">🧪 Run Demo Mode</button>
      <button class="btn btn-real" id="btn-real" onclick="run('real')">☁️ Run Real GCP</button>
      <button class="btn-settings" id="open-settings-btn" onclick="openSettings()">&#9881; Configure GCP</button>
      <div class="model-pill">✨ Model: <b>gemini-2.5-pro</b></div>
    </div>

    <div class="sb-section">
      <div class="sb-title">Live Metrics</div>
      <div class="metric">
        <div class="metric-icon">💰</div>
        <div>
          <div class="metric-val" id="m-cost">—</div>
          <div class="metric-lbl">Monthly savings</div>
        </div>
      </div>
      <div class="metric">
        <div class="metric-icon">🌿</div>
        <div>
          <div class="metric-val" id="m-co2">—</div>
          <div class="metric-lbl">CO₂ saved / month</div>
        </div>
      </div>
      <div class="metric">
        <div class="metric-icon">🖥️</div>
        <div>
          <div class="metric-val" id="m-vms">—</div>
          <div class="metric-lbl">Idle VMs found</div>
        </div>
      </div>
      <div class="metric">
        <div class="metric-icon">✅</div>
        <div>
          <div class="metric-val" id="m-actions">—</div>
          <div class="metric-lbl">LOW risk actions</div>
        </div>
      </div>
    </div>

    <div class="sb-section">
      <div class="sb-title">About</div>
      <p style="font-size:.775rem;color:#8b949e;line-height:1.7">
        GreenOps AI scans your GCP project for wasted resources, calculates carbon footprint,
        and safely optimizes with human approval. Built with Google ADK + Gemini.
      </p>
    </div>

    <div class="sb-links">
      <a class="sb-link" href="https://github.com/raghu-putta/greenops-agent" target="_blank">⭐ View on GitHub</a>
      <a class="sb-link" href="https://google.github.io/adk-docs/" target="_blank">📖 Google ADK Docs</a>
    </div>

  </div>
</div>

<script>
  const AGENTS = {
    CARBON_SCOUT:         {id:'carbon_scout',       statusId:'cs-status'},
    GREENOPS_ANALYZER:    {id:'greenops_analyzer',   statusId:'ga-status'},
    OPTIMIZATION_EXECUTOR:{id:'optimization_executor',statusId:'oe-status'},
    REPORT_GENERATOR:     {id:'report_generator',    statusId:'rg-status'},
    // orchestrator events are ignored
  };

  let activeAgent = null;
  let es = null;

  // ── SSE connection ──────────────────────────────────────────────────────────
  function connect() {
    es = new EventSource('/stream');
    es.onmessage = onEvent;
    es.onerror   = () => setTimeout(connect, 2000);
  }

  function onEvent(e) {
    const d = JSON.parse(e.data);

    if (d.type === 'start') {
      clearTerminal(); resetCards();
      setBtns(true);
      setStatus('running', `Running ${d.mode === 'demo' ? '🧪 Demo' : '☁️ Real GCP'} pipeline...`);
      print(`<div class="t-timestamp">[${d.time}]  Pipeline started — ${d.mode} mode</div>`);
      print(`<div class="t-separator">─────────────────────────────────────────────────</div>`);
      activeAgent = null;
    }

    else if (d.type === 'agent') {
      const key = d.agent; // e.g. "CARBON_SCOUT"
      const info = AGENTS[key];
      if (!info) return; // skip orchestrator

      if (activeAgent && activeAgent !== key) markDone(activeAgent);

      if (activeAgent !== key) {
        markActive(key);
        activeAgent = key;
        print(`<div class="t-agent-hdr">[ ${d.agent} ]   ${d.time}</div>`);
      }

      print(`<div class="t-text">${esc(d.text)}</div>`);
      extractMetrics(d.text);
    }

    else if (d.type === 'done') {
      if (activeAgent) markDone(activeAgent);
      activeAgent = null;
      setStatus('done', '✅ Pipeline complete');
      setBtns(false);
      print(`<div class="t-separator" style="margin-top:12px">─────────────────────────────────────────────────</div>`);
      print(`<div class="t-timestamp">[${d.time}]  ✅ Done — full report saved to output/</div>`);
    }

    else if (d.type === 'retry') {
      setStatus('running', `⏳ Rate limit — retrying in ${d.wait}s (${d.attempt}/${d.max})…`);
      print(`<div class="t-separator">─────────────────────────────────────────────────</div>`);
      print(`<div class="t-error" style="color:#f0883e">${esc(d.message)}</div>`);
      // Live countdown
      let remaining = d.wait;
      const counterId = 'retry-counter-' + Date.now();
      print(`<div id="${counterId}" class="t-timestamp">  ↻ Retrying in <b>${remaining}s</b>…</div>`);
      const tick = setInterval(() => {
        remaining--;
        const el = document.getElementById(counterId);
        if (el) el.innerHTML = remaining > 0
          ? `  ↻ Retrying in <b>${remaining}s</b>…`
          : `  ↻ Retrying now…`;
        if (remaining <= 0) clearInterval(tick);
      }, 1000);
    }

    else if (d.type === 'error') {
      setStatus('', '❌ Error');
      setBtns(false);
      print(`<div class="t-error" style="white-space:pre-wrap">❌ ${esc(d.message)}</div>`);
    }
  }

  // ── Run ─────────────────────────────────────────────────────────────────────
  function run(mode) {
    fetch(`/run/${mode}`, {method:'POST'})
      .then(r => r.json())
      .then(d => { if (d.error) alert(d.error); })
      .catch(err => alert('Could not start pipeline: ' + err));
  }

  // ── Terminal helpers ────────────────────────────────────────────────────────
  function clearTerminal() {
    document.getElementById('terminal').innerHTML = '';
  }
  function print(html) {
    const t = document.getElementById('terminal');
    const d = document.createElement('div');
    d.innerHTML = html;
    t.appendChild(d);
    t.scrollTop = t.scrollHeight;
  }
  function esc(s) {
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\\n/g,'<br>');
  }

  // ── Card helpers ────────────────────────────────────────────────────────────
  function resetCards() {
    Object.values(AGENTS).forEach(a => {
      const c = document.getElementById('card-' + a.id);
      if (c) c.className = 'acard';
      const s = document.getElementById(a.statusId);
      if (s) { s.className = 'acard-status'; s.textContent = 'Waiting'; }
    });
    ['m-cost','m-co2','m-vms','m-actions'].forEach(id => {
      document.getElementById(id).textContent = '—';
    });
  }
  function markActive(key) {
    const a = AGENTS[key]; if (!a) return;
    const c = document.getElementById('card-' + a.id);
    if (c) c.className = 'acard active';
    const s = document.getElementById(a.statusId);
    if (s) { s.className = 'acard-status running'; s.textContent = '⚙ Running…'; }
  }
  function markDone(key) {
    const a = AGENTS[key]; if (!a) return;
    const c = document.getElementById('card-' + a.id);
    if (c) c.className = 'acard done';
    const s = document.getElementById(a.statusId);
    if (s) { s.className = 'acard-status done'; s.textContent = '✓ Done'; }
  }

  // ── Status bar ──────────────────────────────────────────────────────────────
  function setStatus(state, text) {
    document.getElementById('dot').className = 'dot ' + state;
    document.getElementById('status-txt').textContent = text;
  }

  // ── Buttons ─────────────────────────────────────────────────────────────────
  function setBtns(disabled) {
    document.getElementById('btn-demo').disabled = disabled;
    document.getElementById('btn-real').disabled = disabled;
  }

  // ── Metric extraction ────────────────────────────────────────────────────────
  function extractMetrics(text) {
    // Cost
    const cm = text.match(/TOTAL.*?\\$([\\d,.]+)/i) || text.match(/\\$([\\d,.]+).*?month/i);
    if (cm) document.getElementById('m-cost').textContent = '$' + cm[1] + '/mo';

    // CO2
    const co2 = text.match(/([\\d.]+)\\s*kg.*?CO[₂2]/i) || text.match(/CO[₂2].*?([\\d.]+)\\s*kg/i);
    if (co2) document.getElementById('m-co2').textContent = co2[1] + ' kg';

    // Idle VMs
    const vm = text.match(/(\\d+)\\s+(?:idle\\s+)?VMs?/i) || text.match(/Running VMs.*?(\\d+)/i);
    if (vm) document.getElementById('m-vms').textContent = vm[1];

    // LOW risk actions
    const act = text.match(/LOW\\s+RISK[^\\d]*(\\d+)/i) || text.match(/(\\d+)\\s+LOW\\s+risk/i);
    if (act) document.getElementById('m-actions').textContent = act[1];
  }

  connect();

  // ── Cinematic GreenOps Universe Background ──
  (function(){
    const c=document.getElementById('bgcanvas');
    if(!c)return;
    const ctx=c.getContext('2d');
    let W,H,t=0;
    function resize(){W=c.width=c.offsetWidth*devicePixelRatio||600;H=c.height=Math.min(W*0.55,320*devicePixelRatio);c.style.height=(H/devicePixelRatio)+'px';}
    resize();
    const stars=Array.from({length:200},()=>({x:Math.random(),y:Math.random()*0.5,r:Math.random()*1.2+0.3,a:Math.random(),tw:Math.random()*6}));
    const buildings=[
      {x:0.04,y:0.68,w:0.05,h:0.28,label:'Google',c:'#1a5c3a'},{x:0.11,y:0.64,w:0.046,h:0.32,label:'OpenAI',c:'#1e6b45'},
      {x:0.18,y:0.70,w:0.042,h:0.26,label:'Meta',c:'#154d30'},{x:0.25,y:0.62,w:0.05,h:0.34,label:'Microsoft',c:'#1a6040'},
      {x:0.32,y:0.66,w:0.046,h:0.30,label:'Apple',c:'#17543a'},{x:0.39,y:0.61,w:0.052,h:0.35,label:'AWS',c:'#1c6644'},
      {x:0.46,y:0.67,w:0.048,h:0.29,label:'Tesla',c:'#155030'},{x:0.53,y:0.63,w:0.05,h:0.33,label:'Nvidia',c:'#1b6242'},
      {x:0.60,y:0.69,w:0.046,h:0.27,label:'IBM',c:'#164e2e'},{x:0.67,y:0.64,w:0.048,h:0.32,label:'DeepMind',c:'#1a5e3c'},
      {x:0.74,y:0.66,w:0.052,h:0.30,label:'Anthropic',c:'#1e6848'},{x:0.81,y:0.62,w:0.048,h:0.34,label:'Gemini',c:'#17543a'},
      {x:0.88,y:0.68,w:0.046,h:0.28,label:'GCP',c:'#154e30'},{x:0.94,y:0.64,w:0.05,h:0.32,label:'Oracle',c:'#1b6040'},
    ];
    const streams=Array.from({length:10},(_,i)=>({angle:i/10*Math.PI*2,speed:0.004+Math.random()*0.003,r:0.5+Math.random()*0.5}));
    const particles=Array.from({length:50},()=>({x:Math.random(),y:0.5+Math.random()*0.3,vx:(Math.random()-0.5)*0.0003,vy:-Math.random()*0.0004,a:Math.random(),life:Math.random()}));
    const clouds=[{x:0.08,y:0.06,w:0.16,h:0.08,speed:0},{x:0.68,y:0.04,w:0.2,h:0.09,speed:0},{x:0.35,y:0.1,w:0.14,h:0.07,speed:0.00012},{x:1.1,y:0.16,w:0.18,h:0.07,speed:0.0001}];

    function drawCloud(cx,cy,cw,ch,alpha){
      ctx.save();ctx.globalAlpha=alpha;
      const g=ctx.createRadialGradient(cx+cw/2,cy+ch/2,0,cx+cw/2,cy+ch/2,Math.max(cw,ch)*0.7);
      g.addColorStop(0,'rgba(180,240,200,0.9)');g.addColorStop(1,'rgba(0,0,0,0)');
      ctx.fillStyle=g;ctx.beginPath();ctx.ellipse(cx+cw/2,cy+ch/2,cw/2,ch/2,0,0,Math.PI*2);ctx.fill();
      ctx.strokeStyle='#00ff88';ctx.lineWidth=1.2;ctx.globalAlpha=alpha*0.8;
      const mx=cx+cw/2,my=cy+ch/2;
      ctx.beginPath();ctx.moveTo(mx,my+ch*0.2);ctx.lineTo(mx,my-ch*0.15);ctx.stroke();
      ctx.beginPath();ctx.moveTo(mx,my);ctx.quadraticCurveTo(mx-cw*0.12,my-ch*0.2,mx-cw*0.08,my-ch*0.3);ctx.stroke();
      ctx.beginPath();ctx.moveTo(mx,my-ch*0.05);ctx.quadraticCurveTo(mx+cw*0.12,my-ch*0.25,mx+cw*0.07,my-ch*0.35);ctx.stroke();
      ctx.restore();
    }

    function frame(){
      ctx.clearRect(0,0,W,H);
      // Sky
      const sk=ctx.createLinearGradient(0,0,0,H*0.55);sk.addColorStop(0,'#000005');sk.addColorStop(1,'#041a0f');
      ctx.fillStyle=sk;ctx.fillRect(0,0,W,H*0.55);
      // Stars
      stars.forEach(s=>{ctx.save();ctx.globalAlpha=s.a*(0.5+0.5*Math.sin(t*s.tw+s.x*10));ctx.fillStyle='#fff';ctx.beginPath();ctx.arc(s.x*W,s.y*H,s.r*(W/600),0,Math.PI*2);ctx.fill();ctx.restore();});
      // Clouds
      clouds.forEach((cl,i)=>{if(cl.speed){cl.x-=cl.speed;if(cl.x+cl.w<0)cl.x=1.2;}const pulse=0.08+0.02*Math.sin(t*1.5+i*2);drawCloud(cl.x*W,cl.y*H,cl.w*W,cl.h*H,pulse);});
      // Horizon glow
      const hg=ctx.createLinearGradient(0,H*0.48,0,H*0.58);hg.addColorStop(0,'rgba(0,0,0,0)');hg.addColorStop(0.5,'rgba(16,120,70,0.3)');hg.addColorStop(1,'rgba(0,0,0,0)');
      ctx.fillStyle=hg;ctx.fillRect(0,H*0.48,W,H*0.1);
      // Ground
      const gg=ctx.createLinearGradient(0,H*0.55,0,H);gg.addColorStop(0,'#052e16');gg.addColorStop(1,'#021f0e');
      ctx.fillStyle=gg;ctx.fillRect(0,H*0.55,W,H*0.45);
      // Roads
      ctx.strokeStyle='rgba(52,211,153,0.15)';ctx.lineWidth=1.5;
      [0.75,0.82,0.9].forEach(y=>{ctx.beginPath();ctx.moveTo(0,H*y);ctx.lineTo(W,H*y);ctx.stroke();});
      // Buildings
      buildings.forEach(b=>{
        const bx=b.x*W,by=b.y*H,bw=b.w*W,bh=b.h*H;
        const bg=ctx.createLinearGradient(bx,by,bx+bw,by);bg.addColorStop(0,b.c);bg.addColorStop(0.5,'#1a7a4a');bg.addColorStop(1,b.c);
        ctx.fillStyle=bg;ctx.fillRect(bx,by,bw,bh);
        ctx.fillStyle='rgba(52,211,153,0.2)';
        const rows=Math.floor(bh/10),cols=Math.floor(bw/8);
        for(let r=0;r<rows;r++)for(let cl=0;cl<cols;cl++)if(Math.sin(r*cl+t*0.5+b.x*10)>0.2)ctx.fillRect(bx+cl*8+1,by+r*10+1,5,7);
        const rg=ctx.createRadialGradient(bx+bw/2,by,0,bx+bw/2,by,bw);rg.addColorStop(0,'rgba(52,211,153,0.35)');rg.addColorStop(1,'rgba(0,0,0,0)');
        ctx.fillStyle=rg;ctx.fillRect(bx-bw,by-bw,bw*3,bw*2);
        ctx.save();ctx.fillStyle='#34d399';ctx.font=`bold ${Math.max(6,bw*0.3)}px sans-serif`;ctx.textAlign='center';ctx.shadowColor='#00ff88';ctx.shadowBlur=5;ctx.fillText(b.label,bx+bw/2,by-3);ctx.restore();
        ctx.strokeStyle='rgba(52,211,153,0.6)';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(bx+bw/2,by);ctx.lineTo(bx+bw/2,by-H*0.035);ctx.stroke();
        ctx.fillStyle=`rgba(52,211,153,${0.5+0.5*Math.sin(t*3+b.x*20)})`;ctx.beginPath();ctx.arc(bx+bw/2,by-H*0.035,2,0,Math.PI*2);ctx.fill();
      });
      // Particles
      particles.forEach(p=>{p.x+=p.vx;p.y+=p.vy;p.life-=0.003;if(p.life<=0||p.y<0.05){p.x=Math.random();p.y=0.55+Math.random()*0.2;p.life=0.8+Math.random()*0.2;p.vx=(Math.random()-0.5)*0.0003;p.vy=-Math.random()*0.0004;}ctx.save();ctx.globalAlpha=p.a*p.life*0.5;ctx.fillStyle='#34d399';ctx.beginPath();ctx.arc(p.x*W,p.y*H,1.2,0,Math.PI*2);ctx.fill();ctx.restore();});
      // Globe
      const gx=W*0.5,gy=H*0.42,gr=Math.min(W,H)*0.09;
      const gl=ctx.createRadialGradient(gx,gy,gr*0.5,gx,gy,gr*2);gl.addColorStop(0,'rgba(52,211,153,0.12)');gl.addColorStop(1,'rgba(0,0,0,0)');
      ctx.fillStyle=gl;ctx.beginPath();ctx.arc(gx,gy,gr*2,0,Math.PI*2);ctx.fill();
      const globeG=ctx.createRadialGradient(gx-gr*0.3,gy-gr*0.3,0,gx,gy,gr);globeG.addColorStop(0,'#1a7a4a');globeG.addColorStop(0.5,'#064e3b');globeG.addColorStop(1,'#011a10');
      ctx.save();ctx.beginPath();ctx.arc(gx,gy,gr,0,Math.PI*2);ctx.fillStyle=globeG;ctx.fill();ctx.clip();
      ctx.fillStyle='rgba(52,211,153,0.35)';const rot=t*0.3;
      ctx.save();ctx.translate(gx,gy);ctx.rotate(rot);
      [[-.3,-.1,.22,.28,.3],[.15,-.2,.14,.16,.2],[.35,-.05,.28,.22,.1],[.1,.2,.14,.2,0],[.38,.25,.1,.08,.3]].forEach(([ex,ey,ew,eh,ea])=>{ctx.beginPath();ctx.ellipse(gr*ex,gr*ey,gr*ew,gr*eh,ea,0,Math.PI*2);ctx.fill();});
      ctx.restore();ctx.restore();
      streams.forEach(s=>{s.angle+=s.speed;const sx=gx+Math.cos(s.angle)*gr*(1.3+s.r*0.3),sy=gy+Math.sin(s.angle)*gr*(0.5+s.r*0.2);ctx.save();ctx.globalAlpha=0.6;ctx.fillStyle='#00ff88';ctx.beginPath();ctx.arc(sx,sy,1.5,0,Math.PI*2);ctx.fill();ctx.restore();});
      // Robots
      [0.08,0.92].forEach((rx,ri)=>{
        const rsx=rx*W,rsy=H*0.75,bob=Math.sin(t*1.5+ri*Math.PI)*2,sc=Math.min(W,H)*0.001;
        ctx.save();ctx.translate(rsx,rsy+bob);ctx.scale(ri===0?sc:-sc,sc);
        ctx.fillStyle='#1a3a2a';ctx.fillRect(-18,40,14,50);ctx.fillRect(4,40,14,50);
        const bg2=ctx.createLinearGradient(-25,-20,25,-20);bg2.addColorStop(0,'#1a5c3a');bg2.addColorStop(0.5,'#22c55e');bg2.addColorStop(1,'#1a5c3a');
        ctx.fillStyle=bg2;ctx.fillRect(-25,-20,50,65);ctx.fillStyle='#1a5c3a';ctx.fillRect(-45,-10,20,45);ctx.fillRect(25,-10,20,45);
        const hg2=ctx.createLinearGradient(-20,-85,20,-85);hg2.addColorStop(0,'#1a5c3a');hg2.addColorStop(0.5,'#16a34a');hg2.addColorStop(1,'#1a5c3a');
        ctx.fillStyle=hg2;ctx.fillRect(-20,-85,40,40);
        ctx.fillStyle=`rgba(52,211,153,${0.7+0.3*Math.sin(t*3+ri)})`;ctx.beginPath();ctx.arc(-8,-68,4,0,Math.PI*2);ctx.fill();ctx.beginPath();ctx.arc(8,-68,4,0,Math.PI*2);ctx.fill();
        ctx.strokeStyle='rgba(52,211,153,0.8)';ctx.lineWidth=2;ctx.beginPath();ctx.moveTo(0,-85);ctx.lineTo(0,-105);ctx.stroke();
        ctx.fillStyle=`rgba(52,211,153,${0.5+0.5*Math.sin(t*4+ri)})`;ctx.beginPath();ctx.arc(0,-107,3,0,Math.PI*2);ctx.fill();
        ctx.restore();
      });
      // Vignette
      const vig=ctx.createRadialGradient(W/2,H/2,H*0.25,W/2,H/2,H*0.75);vig.addColorStop(0,'rgba(0,0,0,0)');vig.addColorStop(1,'rgba(0,0,0,0.55)');
      ctx.fillStyle=vig;ctx.fillRect(0,0,W,H);
      t+=0.016;requestAnimationFrame(frame);
    }
    frame();
  })();
        function toggleSettings(){const p=document.getElementById('settings-panel');p.style.display=p.style.display==='block'?'none':'block';if(p.style.display==='block')loadSettings();} function saveSettings(){const s={apiKey:document.getElementById('cfg-api-key').value,projectId:document.getElementById('cfg-project-id').value,region:document.getElementById('cfg-region').value,zone:document.getElementById('cfg-zone').value};sessionStorage.setItem('gops',JSON.stringify(s));const btn=document.querySelector('.btn-save');if(btn){btn.textContent='Saved!';setTimeout(()=>{btn.textContent='Save and Close';},1500);}} function loadSettings(){const s=JSON.parse(sessionStorage.getItem('gops')||'{}');if(s.apiKey)document.getElementById('cfg-api-key').value=s.apiKey;if(s.projectId)document.getElementById('cfg-project-id').value=s.projectId;if(s.region)document.getElementById('cfg-region').value=s.region;if(s.zone)document.getElementById('cfg-zone').value=s.zone;} function getSettings(){return JSON.parse(sessionStorage.getItem('gops')||'{}');} function testConn(){const el=document.getElementById('conn-status');el.style.display='block';el.className='conn-status';el.textContent='Testing...';const s=getSettings();if(!s.projectId||!s.apiKey){el.className='conn-status err';el.textContent='Fill in all fields first';return;}fetch('/test-connection',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(s)}).then(r=>r.json()).then(d=>{el.className=d.success?'conn-status ok':'conn-status err';el.textContent=d.success?'Connected! Ready to scan.':'Error: '+(d.error||'Failed');}).catch(()=>{el.className='conn-status err';el.textContent='Server unreachable';});} window.addEventListener('load',loadSettings);
</script>

  <!-- Settings Panel HTML -->
  <div id="settings-panel" style="position:fixed;top:0;left:0;width:100%;height:100%;z-index:9999;display:none;background:rgba(0,0,0,0.8);" onclick="if(event.target===this)this.style.display='none'">
    <div style="position:absolute;right:0;top:0;width:420px;height:100%;background:#0d1117;border-left:2px solid #34d399;overflow-y:auto;padding:0;">
      <div style="display:flex;justify-content:space-between;align-items:center;padding:20px 24px;border-bottom:1px solid #21262d;background:#010409;">
        <h2 style="color:#34d399;margin:0;font-size:1.1rem;">&#9965; Configure GCP</h2>
        <button onclick="document.getElementById('settings-panel').style.display='none'" style="background:none;border:1px solid #444;color:#999;width:32px;height:32px;border-radius:6px;cursor:pointer;font-size:1rem;">X</button>
      </div>
      <div style="padding:22px 24px;">
        <p style="color:#8b949e;font-size:0.82rem;margin-bottom:22px;line-height:1.6;padding:12px;background:#161b22;border-radius:8px;border:1px solid #21262d;">Enter your Google Cloud details to scan your real GCP project for waste and carbon footprint.</p>
        <div style="margin-bottom:18px;">
          <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">Gemini API Key</label>
          <input type="password" id="cfg-api-key" placeholder="AIzaSy..." autocomplete="off" style="width:100%;background:#161b22;border:1px solid #30363d;color:#e6edf3;padding:10px 12px;border-radius:8px;font-size:0.88rem;box-sizing:border-box;"/>
          <small style="display:block;color:#6e7681;font-size:0.72rem;margin-top:4px;">Free at <a href="https://aistudio.google.com/apikey" target="_blank" style="color:#58a6ff;">aistudio.google.com/apikey</a></small>
        </div>
        <div style="margin-bottom:18px;">
          <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">GCP Project ID</label>
          <input type="text" id="cfg-project-id" placeholder="my-project-123" style="width:100%;background:#161b22;border:1px solid #30363d;color:#e6edf3;padding:10px 12px;border-radius:8px;font-size:0.88rem;box-sizing:border-box;"/>
          <small style="display:block;color:#6e7681;font-size:0.72rem;margin-top:4px;">Find at <a href="https://console.cloud.google.com" target="_blank" style="color:#58a6ff;">console.cloud.google.com</a></small>
        </div>
        <div style="margin-bottom:18px;">
          <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">GCP Region</label>
          <select id="cfg-region" style="width:100%;background:#161b22;border:1px solid #30363d;color:#e6edf3;padding:10px 12px;border-radius:8px;font-size:0.88rem;box-sizing:border-box;">
            <option value="us-central1">us-central1 (Iowa)</option>
            <option value="us-east1">us-east1 (South Carolina)</option>
            <option value="us-west1">us-west1 (Oregon)</option>
            <option value="europe-west1">europe-west1 (Belgium)</option>
            <option value="europe-west2">europe-west2 (London)</option>
            <option value="asia-east1">asia-east1 (Taiwan)</option>
            <option value="asia-south1">asia-south1 (Mumbai)</option>
            <option value="australia-southeast1">australia-southeast1 (Sydney)</option>
          </select>
        </div>
        <div style="margin-bottom:18px;">
          <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">GCP Zone</label>
          <input type="text" id="cfg-zone" placeholder="us-central1-a" style="width:100%;background:#161b22;border:1px solid #30363d;color:#e6edf3;padding:10px 12px;border-radius:8px;font-size:0.88rem;box-sizing:border-box;"/>
          <small style="display:block;color:#6e7681;font-size:0.72rem;margin-top:4px;">Usually your region + "-a"</small>
        </div>
        <div style="display:flex;gap:10px;margin-top:26px;">
          <button onclick="saveSettings();document.getElementById('settings-panel').style.display='none';" style="flex:1;background:#34d399;color:#0a1a0f;border:none;padding:12px;border-radius:8px;font-weight:700;cursor:pointer;font-size:0.88rem;">Save and Close</button>
          <button onclick="testConn()" style="flex:1;background:transparent;color:#34d399;border:1px solid #34d399;padding:12px;border-radius:8px;font-weight:600;cursor:pointer;font-size:0.88rem;">Test Connection</button>
        </div>
        <div id="conn-status" style="margin-top:14px;padding:12px;border-radius:8px;font-size:0.82rem;text-align:center;display:none;"></div>
        <div style="margin-top:26px;padding:14px;background:#161b22;border-radius:8px;border:1px solid #21262d;">
          <p style="color:#484f58;font-size:0.72rem;text-align:center;margin:0;line-height:1.7;">Stored in browser session only. Never sent to our servers.</p>
        </div>
      </div>
    </div>
  </div>

  <div id="gcp-settings-overlay" onclick="if(event.target===this)closeSettings()" style="position:fixed;top:0;left:0;width:100%;height:100%;z-index:99999;display:none;background:rgba(0,0,0,0.85);backdrop-filter:blur(8px);">
    <div id="gcp-settings-drawer" style="position:absolute;right:0;top:0;width:460px;height:100%;background:#0d1117;border-left:2px solid #34d399;overflow-y:auto;font-family:'Segoe UI',system-ui,sans-serif;">

      <div style="background:linear-gradient(135deg,#0a1628,#0d2818);border-bottom:1px solid #1e3a2a;padding:20px 24px 16px;position:sticky;top:0;z-index:10;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
          <div>
            <h2 style="color:#34d399;margin:0;font-size:1.1rem;font-weight:700;">&#9881; Configure GCP</h2>
            <p style="color:#6e7681;font-size:0.72rem;margin:3px 0 0;">Set your cloud credentials</p>
          </div>
          <button onclick="closeSettings()" style="background:#1a1f2e;border:1px solid #30363d;color:#8b949e;width:34px;height:34px;border-radius:8px;cursor:pointer;font-size:1.1rem;">&#10005;</button>
        </div>

        <div style="background:#161b22;border-radius:10px;padding:10px;border:1px solid #21262d;">
          <p style="color:#6e7681;font-size:0.7rem;margin:0 0 8px;text-transform:uppercase;letter-spacing:1px;">Choose Your AI Assistant</p>
          <div style="display:flex;gap:6px;">
            <button onclick="selectRobot('alex')" id="robot-alex" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a3a2a;border:2px solid #34d399;color:#34d399;font-size:0.7rem;font-weight:600;">??<br/>Alex</button>
            <button onclick="selectRobot('aria')" id="robot-aria" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.7rem;font-weight:600;">?????<br/>Aria</button>
            <button onclick="selectRobot('max')"  id="robot-max"  style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.7rem;font-weight:600;">?????<br/>Max</button>
            <button onclick="selectRobot('nova')" id="robot-nova" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.7rem;font-weight:600;">??<br/>Nova</button>
            <button onclick="selectRobot('eco')"  id="robot-eco"  style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.7rem;font-weight:600;">??<br/>Eco</button>
          </div>
        </div>
      </div>

      <div style="padding:16px 24px 0;">
        <div id="robot-bubble" style="background:linear-gradient(135deg,#161b22,#1a2332);border:1px solid #21262d;border-radius:16px 16px 16px 4px;padding:14px 16px;">
          <div style="display:flex;align-items:flex-start;gap:12px;">
            <div id="robot-avatar" style="font-size:2rem;animation:robotBob 2s ease-in-out infinite;flex-shrink:0;">??</div>
            <div>
              <div id="robot-name" style="color:#34d399;font-size:0.72rem;font-weight:700;margin-bottom:4px;text-transform:uppercase;letter-spacing:0.5px;">ALEX</div>
              <div id="robot-message" style="color:#c9d1d9;font-size:0.88rem;line-height:1.5;">Hey Techie! ?? I need your GCP details to get started scanning your cloud!</div>
            </div>
          </div>
        </div>
      </div>

      <div style="padding:20px 24px;">

        <div style="margin-bottom:18px;">
          <label style="display:flex;align-items:center;gap:8px;color:#e6edf3;font-size:0.82rem;font-weight:600;margin-bottom:8px;">
            <span style="font-size:1.1rem;">?</span> Gemini API Key
            <span style="color:#f85149;font-size:0.7rem;">*required</span>
          </label>
          <div style="position:relative;">
            <input type="password" id="cfg-api-key" placeholder="AIzaSy..." autocomplete="off" oninput="onFieldInput()" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 44px 11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
            <span onclick="togglePwd()" style="position:absolute;right:12px;top:50%;transform:translateY(-50%);cursor:pointer;color:#6e7681;font-size:1rem;">&#128065;</span>
          </div>
          <small style="color:#6e7681;font-size:0.72rem;">Free at <a href="https://aistudio.google.com/apikey" target="_blank" style="color:#58a6ff;">aistudio.google.com/apikey</a></small>
        </div>

        <div style="margin-bottom:18px;">
          <label style="display:flex;align-items:center;gap:8px;color:#e6edf3;font-size:0.82rem;font-weight:600;margin-bottom:8px;">
            <span style="font-size:1.1rem;">&#9729;</span> GCP Project ID
            <span style="color:#f85149;font-size:0.7rem;">*required</span>
          </label>
          <input type="text" id="cfg-project-id" placeholder="my-project-123" oninput="onFieldInput()" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
          <small style="color:#6e7681;font-size:0.72rem;">Find at <a href="https://console.cloud.google.com" target="_blank" style="color:#58a6ff;">console.cloud.google.com</a></small>
        </div>

        <div style="margin-bottom:18px;">
          <label style="display:flex;align-items:center;gap:8px;color:#e6edf3;font-size:0.82rem;font-weight:600;margin-bottom:8px;">
            <span style="font-size:1.1rem;">&#127758;</span> GCP Region
          </label>
          <select id="cfg-region" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;cursor:pointer;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'">
            <option value="us-central1">&#127482;&#127480; us-central1 � Iowa, USA</option>
            <option value="us-east1">&#127482;&#127480; us-east1 � South Carolina, USA</option>
            <option value="us-west1">&#127482;&#127480; us-west1 � Oregon, USA</option>
            <option value="europe-west1">&#127466;&#127482; europe-west1 � Belgium</option>
            <option value="europe-west2">&#127468;&#127463; europe-west2 � London, UK</option>
            <option value="asia-east1">&#127481;&#127484; asia-east1 � Taiwan</option>
            <option value="asia-south1">&#127470;&#127475; asia-south1 � Mumbai, India</option>
            <option value="australia-southeast1">&#127462;&#127482; australia-southeast1 � Sydney</option>
          </select>
        </div>

        <div style="margin-bottom:24px;">

cd "C:\Users\raghu\greenops-agent"

$app = Get-Content "app.py" -Raw

$app = $app -replace '<button class="btn-settings"[^>]*>.*?Configure GCP</button>', '<button class="btn-settings" id="open-settings-btn" onclick="openSettings()">&#9881; Configure GCP</button>'

$megaPanel = @'

  <div id="gcp-settings-overlay" onclick="if(event.target===this)closeSettings()" style="position:fixed;top:0;left:0;width:100%;height:100%;z-index:99999;display:none;background:rgba(0,0,0,0.85);backdrop-filter:blur(8px);">
    <div id="gcp-settings-drawer" style="position:absolute;right:0;top:0;width:460px;height:100%;background:#0d1117;border-left:2px solid #34d399;overflow-y:auto;font-family:'Segoe UI',system-ui,sans-serif;">

      <div style="background:linear-gradient(135deg,#0a1628,#0d2818);border-bottom:1px solid #1e3a2a;padding:20px 24px 16px;position:sticky;top:0;z-index:10;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
          <div>
            <h2 style="color:#34d399;margin:0;font-size:1.1rem;font-weight:700;">&#9881; Configure GCP</h2>
            <p style="color:#6e7681;font-size:0.72rem;margin:3px 0 0;">Set your cloud credentials</p>
          </div>
          <button onclick="closeSettings()" style="background:#1a1f2e;border:1px solid #30363d;color:#8b949e;width:34px;height:34px;border-radius:8px;cursor:pointer;font-size:1.1rem;">&#10005;</button>
        </div>

        <div style="background:#161b22;border-radius:10px;padding:10px;border:1px solid #21262d;">
          <p style="color:#6e7681;font-size:0.7rem;margin:0 0 8px;text-transform:uppercase;letter-spacing:1px;">Choose Your AI Assistant</p>
          <div style="display:flex;gap:6px;">
            <button onclick="selectRobot('alex')" id="robot-alex" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a3a2a;border:2px solid #34d399;color:#34d399;font-size:0.7rem;font-weight:600;">??<br/>Alex</button>
            <button onclick="selectRobot('aria')" id="robot-aria" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.7rem;font-weight:600;">?????<br/>Aria</button>
            <button onclick="selectRobot('max')"  id="robot-max"  style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.7rem;font-weight:600;">?????<br/>Max</button>
            <button onclick="selectRobot('nova')" id="robot-nova" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.7rem;font-weight:600;">??<br/>Nova</button>
            <button onclick="selectRobot('eco')"  id="robot-eco"  style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.7rem;font-weight:600;">??<br/>Eco</button>
          </div>
        </div>
      </div>

      <div style="padding:16px 24px 0;">
        <div id="robot-bubble" style="background:linear-gradient(135deg,#161b22,#1a2332);border:1px solid #21262d;border-radius:16px 16px 16px 4px;padding:14px 16px;">
          <div style="display:flex;align-items:flex-start;gap:12px;">
            <div id="robot-avatar" style="font-size:2rem;animation:robotBob 2s ease-in-out infinite;flex-shrink:0;">??</div>
            <div>
              <div id="robot-name" style="color:#34d399;font-size:0.72rem;font-weight:700;margin-bottom:4px;text-transform:uppercase;letter-spacing:0.5px;">ALEX</div>
              <div id="robot-message" style="color:#c9d1d9;font-size:0.88rem;line-height:1.5;">Hey Techie! ?? I need your GCP details to get started scanning your cloud!</div>
            </div>
          </div>
        </div>
      </div>

      <div style="padding:20px 24px;">

        <div style="margin-bottom:18px;">
          <label style="display:flex;align-items:center;gap:8px;color:#e6edf3;font-size:0.82rem;font-weight:600;margin-bottom:8px;">
            <span style="font-size:1.1rem;">?</span> Gemini API Key
            <span style="color:#f85149;font-size:0.7rem;">*required</span>
          </label>
          <div style="position:relative;">
            <input type="password" id="cfg-api-key" placeholder="AIzaSy..." autocomplete="off" oninput="onFieldInput()" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 44px 11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
            <span onclick="togglePwd()" style="position:absolute;right:12px;top:50%;transform:translateY(-50%);cursor:pointer;color:#6e7681;font-size:1rem;">&#128065;</span>
          </div>
          <small style="color:#6e7681;font-size:0.72rem;">Free at <a href="https://aistudio.google.com/apikey" target="_blank" style="color:#58a6ff;">aistudio.google.com/apikey</a></small>
        </div>

        <div style="margin-bottom:18px;">
          <label style="display:flex;align-items:center;gap:8px;color:#e6edf3;font-size:0.82rem;font-weight:600;margin-bottom:8px;">
            <span style="font-size:1.1rem;">&#9729;</span> GCP Project ID
            <span style="color:#f85149;font-size:0.7rem;">*required</span>
          </label>
          <input type="text" id="cfg-project-id" placeholder="my-project-123" oninput="onFieldInput()" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
          <small style="color:#6e7681;font-size:0.72rem;">Find at <a href="https://console.cloud.google.com" target="_blank" style="color:#58a6ff;">console.cloud.google.com</a></small>
        </div>

        <div style="margin-bottom:18px;">
          <label style="display:flex;align-items:center;gap:8px;color:#e6edf3;font-size:0.82rem;font-weight:600;margin-bottom:8px;">
            <span style="font-size:1.1rem;">&#127758;</span> GCP Region
          </label>
          <select id="cfg-region" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;cursor:pointer;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'">
            <option value="us-central1">&#127482;&#127480; us-central1 � Iowa, USA</option>
            <option value="us-east1">&#127482;&#127480; us-east1 � South Carolina, USA</option>
            <option value="us-west1">&#127482;&#127480; us-west1 � Oregon, USA</option>
            <option value="europe-west1">&#127466;&#127482; europe-west1 � Belgium</option>
            <option value="europe-west2">&#127468;&#127463; europe-west2 � London, UK</option>
            <option value="asia-east1">&#127481;&#127484; asia-east1 � Taiwan</option>
            <option value="asia-south1">&#127470;&#127475; asia-south1 � Mumbai, India</option>
            <option value="australia-southeast1">&#127462;&#127482; australia-southeast1 � Sydney</option>
          </select>
        </div>

        <div style="margin-bottom:24px;">
          <label style="display:flex;align-items:center;gap:8px;color:#e6edf3;font-size:0.82rem;font-weight:600;margin-bottom:8px;">
            <span style="font-size:1.1rem;">&#128205;</span> GCP Zone
          </label>
          <input type="text" id="cfg-zone" placeholder="us-central1-a" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
          <small style="color:#6e7681;font-size:0.72rem;">Usually region + "-a" e.g. us-central1-a</small>
        </div>

        <div style="display:flex;gap:10px;margin-bottom:14px;">
          <button onclick="saveCfg()" style="flex:1;background:linear-gradient(135deg,#34d399,#10b981);color:#0a1a0f;border:none;padding:13px;border-radius:10px;font-weight:700;cursor:pointer;font-size:0.9rem;">&#128190; Save &amp; Close</button>
          <button onclick="testCfgConn()" style="flex:1;background:transparent;color:#34d399;border:1.5px solid #34d399;padding:13px;border-radius:10px;font-weight:600;cursor:pointer;font-size:0.9rem;">&#128269; Test Connection</button>
        </div>

        <div style="padding:12px 14px;background:#161b22;border-radius:10px;border:1px solid #21262d;text-align:center;">
          <p style="color:#484f58;font-size:0.72rem;margin:0;line-height:1.7;">&#128274; Stored in browser session only.<br/>Never sent to our servers.</p>
        </div>

      </div>
    </div>
  </div>

  <style>
    @keyframes robotBob{0%,100%{transform:translateY(0)}50%{transform:translateY(-5px)}}
    @keyframes robotShake{0%,100%{transform:rotate(0)}25%{transform:rotate(-10deg)}75%{transform:rotate(10deg)}}
    @keyframes robotSpin{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}
    @keyframes drawerSlide{from{transform:translateX(100%);opacity:0}to{transform:translateX(0);opacity:1}}
    @keyframes bubblePop{from{transform:scale(0.8);opacity:0}to{transform:scale(1);opacity:1}}
  </style>

  <script>
    const robots={
      alex:{avatar:'??',name:'ALEX',color:'#34d399',greet:"Hey Techie! ?? I need your GCP details to get started!",missing:"Oops! ?? Some fields are empty. Fill them all in!",missingKey:"?? Hey! Don't forget your Gemini API Key!",missingProject:"?? Which GCP project should I scan? Add the Project ID!",testing:"? Hold tight! Testing your connection...",success:"?? All systems GO! Ready to scan your cloud!",fail:"?? Something's not right. Double-check your credentials!"},
      aria:{avatar:'?????',name:'ARIA',color:'#f472b6',greet:"Hi there! ? Fill in your details and I'll find cloud waste!",missing:"Hey friend! ?? Some fields are still empty. Complete them!",missingKey:"? Your Gemini API Key is missing � I can't work without it!",missingProject:"?? I need your GCP Project ID to get started!",testing:"?? Checking your connection... fingers crossed! ??",success:"?? Woohoo! Connected! Let's find that cloud waste together!",fail:"?? Connection didn't work. Check your credentials again!"},
      max:{avatar:'?????',name:'MAX',color:'#60a5fa',greet:"Yo! Let's GO! ?? Drop your GCP credentials and let's roll!",missing:"Bro! ? You're missing fields! Fill them ALL in � NOW!",missingKey:"?? API Key is MISSING! Can't run without fuel!",missingProject:"?? Project ID needed! Which GCP are we scanning?",testing:"?? TESTING CONNECTION... come on come on...!",success:"BOOM! ?? Connected and READY TO ROLL! Let's go!",fail:"?? Connection FAILED! Check credentials and retry!"},
      nova:{avatar:'??',name:'NOVA',color:'#a78bfa',greet:"Greetings, Engineer. ?? Awaiting credentials to begin analysis.",missing:"?? Incomplete parameters. All fields are required.",missingKey:"?? Gemini API token not found. Credentials required.",missingProject:"?? Target project identifier missing. Specify Project ID.",testing:"?? Establishing secure connection to Google Cloud...",success:"? Authentication verified. Cloud scanning ready.",fail:"? Connection anomaly detected. Verify credentials."},
      eco:{avatar:'??',name:'ECO',color:'#34d399',greet:"Hey! ?? Let's save the planet! Fill in your GCP details!",missing:"?? Almost there! A few fields need your attention!",missingKey:"?? Your API Key is missing � need it for carbon calculations!",missingProject:"?? Which GCP project should I analyze for carbon waste?",testing:"?? Connecting... checking emissions data...",success:"?? Connected! Ready to calculate carbon footprint!",fail:"?? Connection failed. Let's fix credentials and try again!"}
    };

    let currentRobot='alex';

    function openSettings(){
      document.getElementById('gcp-settings-overlay').style.display='block';
      document.getElementById('gcp-settings-drawer').style.animation='drawerSlide 0.35s cubic-bezier(0.34,1.56,0.64,1)';
      loadSettings();
      setTimeout(()=>robotSay('greet'),400);
    }

    function closeSettings(){
      document.getElementById('gcp-settings-overlay').style.display='none';
    }

    function selectRobot(id){
      currentRobot=id;
      ['alex','aria','max','nova','eco'].forEach(r=>{
        const btn=document.getElementById('robot-'+r);
        btn.style.background='#1a1a2e';btn.style.borderColor='#30363d';btn.style.color='#8b949e';
      });
      const rb=document.getElementById('robot-'+id);
      const r=robots[id];
      rb.style.background='#1a2a1a';rb.style.borderColor=r.color;rb.style.color=r.color;
      document.getElementById('robot-avatar').textContent=r.avatar;
      document.getElementById('robot-name').style.color=r.color;
      document.getElementById('robot-name').textContent=r.name;
      robotSay('greet');
      sessionStorage.setItem('gops-robot',id);
    }

    function robotSay(type,custom){
      const r=robots[currentRobot];
      const msg=custom||r[type]||r.greet;
      const el=document.getElementById('robot-message');
      el.style.opacity='0';
      setTimeout(()=>{el.textContent=msg;el.style.opacity='1';el.style.transition='opacity 0.3s';},400);
      const av=document.getElementById('robot-avatar');
      if(type==='missing'||type==='missingKey'||type==='missingProject'||type==='fail'){
        av.style.animation='robotShake 0.5s ease, robotBob 2s ease-in-out 0.5s infinite';
      } else if(type==='success'){
        av.style.animation='robotSpin 0.6s ease, robotBob 2s ease-in-out 0.6s infinite';
      } else {
        av.style.animation='robotBob 2s ease-in-out infinite';
      }
    }

    function onFieldInput(){
      const key=document.getElementById('cfg-api-key').value;
      const proj=document.getElementById('cfg-project-id').value;
      if(!key&&!proj){robotSay('greet');return;}
      if(!key){robotSay('missingKey');return;}
      if(!proj){robotSay('missingProject');return;}
      robotSay(null,"Looking good! ?? Hit 'Test Connection' or 'Save & Close' when ready!");
    }

    function saveCfg(){
      const key=document.getElementById('cfg-api-key').value.trim();
      const proj=document.getElementById('cfg-project-id').value.trim();
      const region=document.getElementById('cfg-region').value;
      const zone=document.getElementById('cfg-zone').value.trim()||region+'-a';
      if(!key||!proj){
        robotSay('missing');
        if(!key){document.getElementById('cfg-api-key').style.borderColor='#f85149';}
        if(!proj){document.getElementById('cfg-project-id').style.borderColor='#f85149';}
        return;
      }
      sessionStorage.setItem('gops-cfg',JSON.stringify({apiKey:key,projectId:proj,region:region,zone:zone}));
      robotSay('success');
      setTimeout(()=>closeSettings(),1800);
    }

    function testCfgConn(){
      const key=document.getElementById('cfg-api-key').value.trim();
      const proj=document.getElementById('cfg-project-id').value.trim();
      if(!key||!proj){
        robotSay('missing');
        if(!key)document.getElementById('cfg-api-key').style.borderColor='#f85149';
        if(!proj)document.getElementById('cfg-project-id').style.borderColor='#f85149';
        return;
      }
      robotSay('testing');
      fetch('/test-connection',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({apiKey:key,projectId:proj,region:document.getElementById('cfg-region').value,zone:document.getElementById('cfg-zone').value})
      }).then(r=>r.json()).then(d=>{
        robotSay(d.success?'success':'fail');
      }).catch(()=>robotSay('fail'));
    }

    function togglePwd(){
      const i=document.getElementById('cfg-api-key');
      i.type=i.type==='password'?'text':'password';
    }

    function loadSettings(){
      try{
        const s=JSON.parse(sessionStorage.getItem('gops-cfg')||'{}');
        if(s.apiKey)document.getElementById('cfg-api-key').value=s.apiKey;
        if(s.projectId)document.getElementById('cfg-project-id').value=s.projectId;
        if(s.region)document.getElementById('cfg-region').value=s.region;
        if(s.zone)document.getElementById('cfg-zone').value=s.zone;
        selectRobot(sessionStorage.getItem('gops-robot')||'alex');
      }catch(e){}
    }

    function saveSettings(){saveCfg();}
    function getSettings(){try{return JSON.parse(sessionStorage.getItem('gops-cfg')||'{}');}catch(e){return{};}}
    function toggleSettings(){openSettings();}
    function testConn(){testCfgConn();}
    window.addEventListener('load',loadSettings);
  </script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD
