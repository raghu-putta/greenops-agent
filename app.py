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
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD
