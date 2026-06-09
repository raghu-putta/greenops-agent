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
  .acard{background:#161b22;padding:0 10px 0 0;display:flex;flex-direction:row;align-items:stretch;gap:10px;border-top:3px solid transparent;transition:all .3s;overflow:hidden;min-height:64px;}
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

  .acard-icon{width:52px;height:52px;overflow:hidden;border-radius:50%;border:2px solid #34d399;box-shadow:0 0 10px rgba(52,211,153,0.4);flex-shrink:0;}
  .acard-icon img{width:52px;height:52px;object-fit:cover;object-position:center 10%;border-radius:50%;}
  
  .acard:nth-child(1) .acard-num{color:#34d399;}
  .acard:nth-child(1) .acard-name{color:#34d399;font-weight:600;}
  .acard:nth-child(2) .acard-num{color:#60a5fa;}
  .acard:nth-child(2) .acard-name{color:#60a5fa;font-weight:600;}
  .acard:nth-child(3) .acard-num{color:#f97316;}
  .acard:nth-child(3) .acard-name{color:#f97316;font-weight:600;}
  .acard:nth-child(4) .acard-num{color:#a78bfa;}
  .acard:nth-child(4) .acard-name{color:#a78bfa;font-weight:600;}
  .acard:nth-child(1) .acard-icon{border-color:#34d399;box-shadow:0 0 10px rgba(52,211,153,0.5);}
  .acard:nth-child(2) .acard-icon{border-color:#60a5fa;box-shadow:0 0 10px rgba(96,165,250,0.5);}
  .acard:nth-child(3) .acard-icon{border-color:#f97316;box-shadow:0 0 10px rgba(249,115,22,0.5);}
  .acard:nth-child(4) .acard-icon{border-color:#a78bfa;box-shadow:0 0 10px rgba(167,139,250,0.5);}
  @keyframes fadeInUp{from{opacity:0;transform:translateY(20px)}to{opacity:1;transform:translateY(0)}}
  @keyframes pulse-glow{0%,100%{box-shadow:0 0 10px rgba(52,211,153,0.4)}50%{box-shadow:0 0 25px rgba(52,211,153,0.8)}}
  .robot-bubble{animation:fadeInUp 0.4s ease;}
  .robot-avatar-wrap{animation:pulse-glow 2s ease-in-out infinite;}

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
    <div class="acard-icon"><img src="data:image/webp;base64,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" alt="Carbon Scout"/></div>
    <div class="acard-num">Agent 1</div>
    <div class="acard-name">Carbon Scout</div>
    <div class="acard-status" id="cs-status">Waiting</div>
  </div>
  <div class="acard" id="card-greenops_analyzer">
    <div class="acard-icon"><img src="data:image/webp;base64,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" alt="GreenOps Analyzer"/></div>
    <div class="acard-num">Agent 2</div>
    <div class="acard-name">GreenOps Analyzer</div>
    <div class="acard-status" id="ga-status">Waiting</div>
  </div>
  <div class="acard" id="card-optimization_executor">
    <div class="acard-icon"><img src="data:image/jpeg;base64,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" alt="Optimization Executor"/></div>
    <div class="acard-num">Agent 3</div>
    <div class="acard-name">Optimization Executor</div>
    <div class="acard-status" id="oe-status">Waiting</div>
  </div>
  <div class="acard" id="card-report_generator">
    <div class="acard-icon"><img src="data:image/webp;base64,UklGRqY2AABXRUJQVlA4IJo2AACwswCdASrwAPAAPp0+l0iloyIhLth9aLATiUAYR4OlgPUFHHyr5DRzb2DsHOr+O73npz3EH9k6FXT16TtkJcav2Hg75Wfk/8H+7HszY9+0jUj7z85X9J/3/E35Kf7P+V9hH8z/rn/E9QGErp56CPuj9+/4v+H9ZP7zzr/fP9z7Af64f9byyfF5/Lf9H9n/gJ/oX90/5/+O/J36f/83/3/630Z/Vv/t/03+r+Q/+f/3T/yf5D21v/r7p/3d/+fuu/r6nUNa7rmNF4WqJ8D1+y+V+5lRfE68+9mSg0FO9Y4NQJJKNdWyxzhzchMJ++FfoHZqYLqn9EZywhgUQ3m9bq8Cabb/VMEUoy0NonoNz0+SE6BHlpL7jYIw71Kl+wY8h1RuSx0qQzvTCBhrVqJdct7Qgc4Ry2nLMSkVsRe07i65Y+NcTlRKyd/lI/TQ6W1yCACuOX7m4h0+UotS610JnVhKgu5qNvaCdNO7fxspI8wsS7Qs4epyO4/CaXSf2j2Gg0rJNvG+no0loz6sbYc2QYHZ1Jw7ldqmuTF1ACCG7pHWXNQM3OYP26h/Yx8o0PobuFIAPZqSplg7XTqn3gVby+FrUCiOwEjzlY2jWR/E9YOHzVPkvO8uUfNZ66D3wvQIFCaonb5PEXIYMoggUgXYNvNcbhm34w/9hFj4QqrcF2GFWlNgUnsobLyr8VF2NzWn2BYjyhgsQDh0TB2tMKfFuJFKKF8ne8kg7KGQhFv2JE1KDsuGkmcUlPBHjsx0GZyjliAdqYi0kIZP7Ra9917IfaWEM6DMqcTD6vrd18MWigxRv5dhbazoK0q3pcGNjPORpwZ8z7VHknv1vwSp+EU9xEdzs2qECWZQ6xoekpqKE/D57BpdUFqE7l2eGFkceng1BIOO+OegmhdJq9eJs/7BCueWXVzFrvndh9xqKe6JdsHsXpxai+Zi3GKso1a+nE4OAMM4W7w7yos5x6C20g2l1GB1H+U3Tcrp2dmSgxlY302k4hvUh9SGJORCIk40ZGPzWuCM0jvURNjy57Nbn0jj3sx6nCTEpDs9SBRdHjnEi+zjLXV9uuGl61dCApt8D34yvsla9onrvwZYmL5HfhIji3lLOYmaOJeMmf2y7WWERydlYPVeduUhCAzk9spoq85J2Kp1ZCP6bB0uXQC7xd1nZYi+6nyn1FNEy2wJcDfcBkHuX7It7EKODtbzc80Mn1XjROvw+CgQb8sIU5PPTLqh+fwkjQK2LG8dxa8EWH/9hAYwYI6gftt6IV7ysYheWLJPS3SLhEUfOvsGGy56w03t/9o+5I1uVE+SLS22IRt+1b/3rS1us3I5PkFfO0zCbm0G4r10t6eT5xSirwSI1t3LwkYTkau/Ip6XlX4+29D3QJ+6PSQSOm66Zo8HpgJWPV7j0OcP4Sm+5+JbuEbQUHlL6m5cC/h7mEOIfXlOuVO//NcBSU//vRhoHNGbXvR9FqkmsDtAqpKqanZpQSCe1CfmOJNY42w8eU8zFz+oy7QtLDN5tvYD9Hj3D05y32WM+F47XQhbsISu4+bQXaHz+nrhUUZsbXYkrxS6E9TdF3H2AdgfRBEJ7+2asVNN4YhUH2EfKncSyo/+y1t/q+yEpZOR3WYGdUSfkZXmfCwTQvLBIgeUp9qaIr7gdUNE65PUceG26afZx8rlfwjjJvB/2/6PNvSePdrlnuhejdEvlEii94tywKh+ZozLRa0rs0NPGdHdCAIbpsSUJWXC77xmKNnz3hA2W+crfJr8viG76WjPH1x3mqJHvjA+X30JsRW1lr022/ECy+Dq+eiwYf06uXSUeOmGsOqZvgFaTjhS+EqPT6gckX3+mbdGhz+IWXRK2p3TqJqicndExRuE8uvWNl9MDx3sB4W80TzW//9rvgea6cS6KkXhBx08Ubval4UVr/wrtkN1FQMcisAA+WuusISNo7sSUtTaohQo+HLjmO1JW9KHkq4uG4hnGEnib+eK99yYMaWSASb+zxHHzSM+Y3wO99wMB6G5WWz/6XIPBi2ZExCvGQ19IUbQS3qNKb8Qys6VBK52cuxGgMKu97KSZkO/7pni/Wp/rDrzgXg/VzOZuYfF207KAWgyKM2c6Vm/Ak048I9NX7b40JLWc+ysdr9a+gR+D28Fi3OEs12a4cPQ7vTsXZvvkmyzyItXvR5yA/INpWM+aaWQCYK6L68KPYt4t+tExy/RbTOQRElnRmMKygCNHSby65ZNKcRCltfyaOhi5SWIdLGGgh6Cg3Q0DV6n7c0Lza06m1pXLgnjexS9oMmp2oX5e+4OU9KGfiYvgh68y9pDiCwMdvGaGuscR/VCsC6vI5TpJGhkp6u8GxtqJOB+ytmNUb/9HmNn/XuJeiUNRaIBYgAEMFsBMAoAjG1K9DpmT/9JF5i1JhbaQL0zPskwMOUeoKzD+TM9jsDGtNAU0p9b8uMRtbWaOZ+YikQi6pWMDvcR0mnGcW3tT8Yvu7RlTPL4m/CC7CkLjV8nqy+CnxmFaV+ZMmiEGs/W0L6CyxnZEaAJHCqbZkLipIve3f4IR3BBYSyISu/0+b9CJD+dwBXpodKbLCKnC4SwUBdUAy4IDsNSudeGc9Rgv2sB5mDWJ4Q71irZIp4fa6Usy9FxdP8ZMpm3FpCR6vHsSgWKsu9t31FdWCswbuDrYYBTPUf7NXqoyhwTxk1xkTGamoE//5guGXhcgVIzILg7Dj396HzBVZIoDvCcK1JQl4Kxnib0IAK4swpc0s9Yx3fGghk0KnFIcXMGh6+INGX4XzjCdL4IAhQe3eKYWm9n6MBLNvuz7J79kWwLm/3MaNifhY6C73B8ZFuxK6Fa3LHYir41RmshZP63JFAYeWFA5/8rO21rB0kQuA1DacGY2cpD2rFOlK/dlnlcxDaRlgiv5USaWUHZDCQFZrzqr9Gnm3887DOYOLRTpOFp0jd46khg6vds5vKtKcOVlNT8PyTqNBMT4D3b8P6SvUyvfL1ECqhMO+RPVxRwY3xfz8YCN86f5qmejtg/gpHaSzo6yCa4NDMHASz9zKbJRtsBBVk4PhmC4/MfM6ulMVd30UDd/3ldnwwzn2P7lRivaSy+dxHPzs78ziYbj3miUdCPq4QXJxg7fSxW6BCr1j1axQcbVxW6yQ/kv0FjbfhNl9begBCLVwiJ/LNmu7/VtHBDstCb/C0oGYs4hF6D0MHdxj0ExeVBlINV3Egb/6mH2Kr5j1xQn1sTjrPwicZ30522LS7BRE8DcGMEdsLeevuvNz3/H+GUaYWOHU2NRwXH/2AXAjO+tTaK2iKeqbeMpKW3T/NmFyQism1wZwfp2CDniRVxHkTbs0x8lpTkD7vOjkojyfKJqMKGBMeGDSh6LbQJzeoV83ju1m4HuGVpprmF1bOnkbxL3IySg5kN+eBQ1r8mvd2K69dlZThHVXW2fOjRv2Ex/IFQXw9Fsc7VGzH9Nbtcem+S7/XIYiauhRr2aXoiX1fQtHAXstmd6ole4xqzIfluHWPDCN8zNeP+4THOloimQHBCNtopLXGgl4J0Fobia4pU94+xBg4EZSDaSWLiE54B0hZThv/gmOXjRhoKngfLRdcZ6R7WfasxGw2aackigwi0hPwsZuuxU7/D1crHwYIeX2T/mNjzfR7A888bw3TeRuWOO9IBCLhF1QfY+wk0ea4RvIwG+2yt854h+SpRCmeks/aLvkje3xMOtgSj21Y2TabboA/jH89BbDQzTtPYskXZgkzPwHpumuGBv3z5j93RT0sMHmiehwy8iab65hdozLIU43OexbPym4z73GpxC9x9pwbmKXSTXBkzL2cUpusL4n8ZUZk130m7Eps4A1IlYvozLJ605EKWpGoSt9zf6bdedMnr8W2j9qrSOvPuIAFyQX+ECrzEenJXCP6a3ZA5wxokmS1+aKRMI5UtsuqxIU7aqEEdX1cck9SG0pjr3noIzDEQchRkcI5ZNbDd8hoWVr0qDXLaEvc6dZjjG4LgeRFerbT8r9Q+ADzP5Dohxow8G9Rd7LFIbsyFCne49wykMLh9gZVefIqTYETHhNZ2b/vNPjUZ7Fw/rLtpe0hVKOZKOxB3qq4kbYosu83ArnNkAWuYGIkCVyr34vJnK2o5Yak1rZBL6dyqeV5P+ReM28z0Rh2mFejxck9zqTmENanb/cPEdJBQEl++19erG7a0OOFlqA+30ptTV4iI4RGoIU1Zp09f3M547TY7rIzxgGwDCgr1hvKrgShYczH8ojU9fvTxrct8M7gNBkq05mkngJgfw6fAgU6vS5Sqft+pAsaUVmc92ppLQuvTZr5q5MOYQbrc3b6I4CruxYvZlxvJ7mbfDqwNmK4Tqppq3W2alYrfpPWPWGMed4I5JawQUAObiCz968BsbHAbJhy+meXoDTgRhrbff+YpN39Dk2KckAxTyuKH1Obx5Qz/vo95obIxGMfU8P6LMbNGjRpKKTcdw2xODegcUx8R9gmLafIVR2kI2hdOnZzRtxCOEfax5B7UfZzVK7u3IhhPkfaJvMfz/K4mTHxKSz+Ww4vcgeXH8mB5ROhdabxfgTJOODpTKugcecoIO+5tY0n7eYRHFuD+38lw1xjsJKtsM3Iq7fbk1SfEcIhypDkd5iMj/z8a397K7sfND5ja9avHypt0Lm2wnpIdnoYXU1/Fh53NwJ7MPRv+/Xv9ULT3AYlOShE4wtxqNoyEECG0ogk81O1tWR/oBOIa3+WqpQrvxECQMHnbclGNbfH8Ajx4lPS2BN0/Ub5Yl5YZDqVYD67fN9DwXJiIxByh+KtHUxFOxGmiptWvbQzthYKMGwJpHKnZXTAUfVqVcXmTe4Mbj0Mycce6e4ShwOHCKO3+IElds8DYBlR7jzBztuei3uIU5tWvCFvwcR5t9M3Zq5dpc1WFCh8V6sRp2sraX9DsyC03aDal+CNeelCEKhihvaqyz4qfPb5lOaQrehEVtjvSW4mvws+/4GDJxJ0v2qCv5H6ayzD0++zRva2Xq7Sw+IQU07BnUWCbSIZDXl4o+K4eTUlVfZNXT8cGFsYa9I0JXc78v/zQQ7xIZCDgFhuVvUrAg1iemQ20fbVZdFSAzDTVYCB6uXtaFRg6q49gS6THUC8JjEEhZX6TYGbgFz+40oY81d6Oatr5LaO85B0P8rA/57iyFC3I9wNbrq+90J3qgFALJKI8dNSCOL304WBQjdgNw6dtacZxmYFxfN/yIGm5b+gYjeLziLSbW23GbRoND28QayCUcrUfS9C9+Y2+fv/uLD0p+RxrVUKCoCRp7pB++efY/1VC0g5rYypM+tUJWdlt0MZbVxeBCUwveRgstfOSYKEfieuOu7SWBFx2hnQtrAX2diF1wEtUi0v/AngVvqYmooij83BUstdF8UwFiXa1v3fldD/YFL9Bshzvsj17FH1z5+dlVxVSvxLOzbaPsfY9N2ZNKHZ55HP2soE0U6wgFRll2iOkE6dpRJbss+AgUJLm8B994nuiLIdsslm0EzzdQezZ9GIsitDdc/p4K1tIt600bUOSOI9eqkTv1n7gZksSuLb3Zlfs0cspU/vi3xjX9XmeeJYes0sSV3CP75TlOrrXLp+KzktgpvEeQz6X9PU+uhkfVywF27Jey/nYyg86iJ+vzSP0OanHzBklE/1mJHZtLA0Mxn3gfCQFL73ZDDuSAHr8E5YRSIdQEYw9wjZ3ecR9vFU9yff8NCkr3ofYckl4XvRstucATCaPw5qJp6B2rFKSWvlOin0k+1tEsZurDPDysNB+AdqKwthMYUjkHLOaU63PN+c3NyYVu3faLz/hI6XTFnECDmxgCicIzufHbhweJdmOvbwY4FfZiHbXCtp+AMSPEmlidsGEnw3brSwJDyDUxRICdXMbM7ru2RGpec8K6Z6Q3Ztafqlt167CAtr5N/HaKcwocRQ9SCVxKMepf7psgHq000jpZ/gtZGYojBhWDeYOzRxI2lkXzPmvtUOlIS/K9d02Q9cpEsbR5nZ634phOST4n7woyjBLQ0nKEvCyLMv8NHpuD/HCkgO2abxFSsuPpZ7AjUk17sJFVSzHc47qIvQVAS46UxCVVCrJqTpS5TZDWa3Q/xjV1n6fYI/9Ryr22pzj1dyOhakYax9whyoZQNQb35l9t7d25GNzw0do8VWoatJy6OJ27+RV5W5K24q2tNT2ju3y/UwpbTXVQKBNLa8DZa5DxpRlVG1kVhaXl1tNhACuv3QkGEXgbGUBKFbuaCEtdYMSW5w9ULinyNjM1x8Vul2MZhrSWPjljx/oLpLMqkHurEukvLyYsMAhPQL/aR8sEtS4uE9dETGiQgAtR/8PdXHHnLDuK5bY6yP2pHo2zj7G8pf59GAD+4jgHi8AtG0iTBs8AbyFKjxrrhv6O7/5iuqcvX5CAcBvoBBHgRLvl4osbe4fgoy4epUsoYx69H8pGaXgjplCdcnko2K1EJBIcxhyQLlj7CAtT08doexX29WMd54fVG5v/ToC5U9cNx9rlYSpPs33OYj7cUDKFlQSHrvAsMN54PsO3/LdnlHUChP59I5S/VeIym3XO5/WrYhHRkxkWTTz59GhGCV8JTrOM6GTEOqXhLqW//+88P969E1kVR8vSkNrjBNpp/vX/QZ0D9ZBpk72gV203WTjVstepgTxwwd+F73wXwKbcw7xhTrtDVjV3ZNulFykoneEGMOd+uLZV8UodODgxESzUhihVBoQQGuNnO/6BUMNyhoQBBNgUG7HlG0X+gl9uiU9zx3IpETrjN8mZcl9zUm2MPVo4M1PKBNsAjp1yXhyz5NKZHReY7uWVmFxMcMDKi5WSr1EEnbEuILlbw3zmd0d0f/mKTjLgwHFz5+OVt4qORAXtgzG1pKS0AUOzYECHeQ2SxSz1AA4Igmsjqd+QMoiucankoqC42CbogWjHkQt8SSrayIPX0XW+W9vx5ntCfSBmaKUq8sljoDfjAjrBVuA/E+fVFKHCP4T0OdSnsGZSMfL424zio5uZ/G00oDpPeCjEJlxfYiGCoV6r3Zuf/Ng/MP8sZCySVXKU9nmImMsG4CpZ1jkOUWVQvqmquesCi6WuAxkqoNo/cCcoJ+6OQaftVYyhtT4vlZucJfbdFSgKtHFcSroE6zxfCbWj5ykKGw+Bf/0MnszO3Yh2k7WTR4RLzqQWt4pBGf8+O82u/y3CTu1DR1AApyR7gYCV2V2wZ1h3aCGKsSq20fzQOzalkW0vWpwdjAEj3BAPjrN1FpKcwQYKzsGvY9phi2/lTiApfyGgNQ6csmWIayD0DyAYcyLfXH6TpWa1vqwNpi0nC6cEP9AcbJGPAgJKyBgIkdhD+Cwu4j3aiR8+sUNMXDERMEFCxHPBPt80XC43weUmiGXxo9rzayqyufjb6ikSgKeQm+KRUOa8axWIQo2fv8LTWWN1h9dsrH/4jxle/IsFUd1r99fuHT7XA1daZ/29/hJz5qwgqCecklXkGT2cNeumeUByV4eo0O7P6vkl3J8twGgl7NPJcAoxIp0xEobdnFU/8MbCFtN08YiVscqhy66tuVHxwpu75ElfzY9DiIDTwbqaJ9GxDZPGpJcgVKXenZntDmshyqIAHW740ZS9OIU8l/1k17ApHmLnUITcAEYlBdtNDleil2Tyw/c8Zfr3F+g6TfUtLPCFVLWytxX/CRsNyTShvRmD7eQ7hq4oUrBUssOlTq+fel4TH7ZNvQ0SCWrZgW1MJtcH/i949uZeQYcfXE1N0V4zDr5YrgpWIMLwuQnSe9nvFidzgg1ajSz1YF6VlhwqZf4GlVcAddx27PyArQFNeGNcjHt2aNvaZfk9otM6LE3sFe3cnJLu3juq+KNggQkWBsuHdzwqnYtLvAgPe67zqKLwuHL7hELC1S6/qgHrYvU2VEo4u1cxtDx7uCQTIHDgSLW2obnk07PQzOERIvIUcG7DrjkKv9+nKfwfiRDOizOiaq3XjF93gq+wMoIiwieMisR9nAib5yF+NkEV/UvLoaTqS0eIX82SJeEeRdNqH2y7R1lAGgQMNQXmJlb48aNysUDP+g5adOCSJnS3VrV4o/TPD/MHOXZZL0xJ1cSsBZckrYSrCObS4Wc0J7In2Jng+zYGj9IrlVsnF50ukgcc4ji77DnsdMqZ1OzGaHhDz2gJuuOHoYpqqX+L23JTorRmHKq8AOV+w3PtQElfpODBqlq0cTMzdSYgMPKhKVH4bbXwm46VjlNOBwHKfXe0xtgT0N3BSkDFe9RdUGUXfpEo34tlgSoQS9VPYLsNB62g6zTJnmB/uTYigqpwcX9yMJoMOVvTkF2lZrZ2KHD7YaFjdC0M6q+1BD59FvlxsJJgypGwuAksYOxfiUac3rkm88wjIqKDgMegDW6UeAzCG1xzLfftb6ay9HBh8YazBHLM0Z1UULPkvDt6yxREM1knqt1IQHVx79SeXRjXGjCCQ8DBNXmoaeHUGZEktOeRjfjLGDBObYliEvCvdjIfBixqoy4sSegHF6a4FhO3eioJRYKgqYnEvd0TWIItze+8o4kylY1p6h1K7hggFWhYGo/cfKMIRiBW+lpmWD/GUFuLLAjyfrY354WmgfyipGnMZe8dgSl/kiZ+hd346vFqu6Y5bw4wUjH+H39qYmg68YIk0KhLASxeomouheievMyhaUC5QSzrlPRTao0/lVGT4EHK8zbC0OqpEsNQP0Sb3slsSWJnEh1Mw71pmH4KyGwiqmJJb6wvDXh/zTJ+SEadT0o3a8JaBKNvMaX1wdE9tKEk/5Q/OM2VuLXPQnS4gWYgXr1J8urrA34oB/3Nvxxd7+2v+8XZ+bg5WauEbFA1BC4ZVzbvLoXMNMY6DjPG+92cNtJGP6pjrLVwFH12p4JTvtPQ7ZIlv6/XlRBD+zBdw1rEoZhOSjcGF1T+exP82Pim8xmNB//T8Gbi5HFcyeR8RjZ3No4GmGiqIddUNIO34qfLjToLrmLFgY6P19E2k63g6FcEOlclelVnUAZjPVXrkky0msLWNZfjeFxEoELcy7HqLR3zePLNXy71vwejCiuC5Eru5PCnmLmimTNwzW8sMlT818gJubDzw4OzcAUIbZT4GCzM1Z3CnDSReEBOgn2iPZzb2YPgMPJiScLRq1qJ65sI7rEOmGK9kp3VDVYKiKy+MCAndFctRolqRckefacSCQRYgt/uS8fL8GbLOa2nY5BqUtbSHHfsad2cIzAWXVYNJrXty+Dm6TD4QmbputX+9DcbFcH2cQaOn5wu2jbp3Yi+UDn9GBwQRNBG4U60e5mBsMbK4gq11q35isD3cBi7IoVc/lC4X8K4OqQ76YrUHXdz1AjIw1bbhqZ3rze3pCcNF7QqujrxA/GZPWZ8PGnNAJR01uyCiOG82E4HX+MUghBvEHLOxub4ecuY8WeJTnYQDJAK1Xd/fL7v6avmJ9bMyJ2qFq3cMCDqQrFj6FQPwa3XtubFWUJ/4B65IKpE6pX7gb6TimwWXRohXvidz5MjVfpzMsuKPLIQSlvbNsyVcoyN56aOEbAUqlPm1VA2tJGCjAytSe9MmAZflwwLW7lwiZwTQv8DzQrwryHdfePikLfWn1m5WN+mGyrQuuBZvguwb09ztJ/wglqCK9oPiDH31l2qN7Y95nrEDb+Y3zNPgz3P+BcNWSiYipZ2TbdG2jz1mBMdnsjbtLHd1T82aPVcZ1H+J5tBbc1V1qRAFU1Uu0b/CSM8+eDt2cejHDgi9k0vq7VWGJhyf5cOCH83VWkev8yAFzsUum2KnG1GrOs54JkvMFB2DmwEcQzfJLxo8EpYrkibMFr+yO/MWuKWrUjT/4DVhAkg6mrRUpoFv8zqWupRr3F1glzo2cgdft9CYln5mUBcDhYac57Q/uhO/e7OjrnXjKsfs/ORDNASrwtYAK38qnUEu/WMx2TzsW3xj1p0HtxfJ2mdjOJp2DRtBOa1zi/6UMbnh71d5gyt+wlOxQsabZwGRfdwpBiVMC7FFeH+GsI/9cyp2b2bmbUv4ioiLHOvq4EDBdhNaeSRiGpb3LTCHkqYMFjjWis+VUuYq8j9qTsKejIqq2JNswr1KAep6UKZ9Pjs19wYpb1lqry+ewvRFl8+4fn39+0SgHe3ZitPuf5C2AoFPr6kruwf31n5jQphgo2vPDuUbSKX+Fy6jSpWqpJlmtuC2La/5gou12tzlq56oUYHviVn09FN8lTrCrjT400fS37+1PD5E7JgvRlWNlGMgiZQoAVE7jPl4p37DADiA0lEd4uJijXGg3ljyQ1co1kMgao/wfm5EVNXjelhKoEyQn9GTLfCG1LvgY327AlW8nqzMS2XvHBTvvvDFXYPelH2/O/ns6xDXtwuUMH9KBQzhnLoZVugLPXmjdlfU+4VRqDQSbAcHo3fk7mfPM/xi/xcWdiraX243IjZ7W+On/E3srxWzJYe9wiJFd4v37Zv/dxX1FwNxchWYKl7EbqYF/C3fS87RP+vZ8s2iVEgMrZHzrQBkiux0ax4oMxvxtgrUJZIvjo0CXpZ3GnRG+o3mVJsYML2cXejLcXCUKJ40LUCUyaltaDrrZWQWO584H6D8RaNm5OEtueD4aLSA/AqeF/nW2qJrGBQ97JmVXk3IIE+Tvu2lhHxlb71HdJlOyMeIYMU44JQRaKmAeUq/CPamJ11AGKzwXLMaq/d0kapLNrNea8/WHjdzVIc7tvQopp7OFxrtPGzxX/qH8yICHzGW8ZCsfVVkNI5F3TSckXx1yakStbMGqjgIKrg+P5inbWksuw4rtOsTu8eyctbqRJ81U1WMLKYPR3XyG554p4oYDFIVZxzzNp3cHh/KTMOMTp6G3gUEk4/hJxZvwUzt1IujM8xbWUCmHMDddFa/bbUVCgDJwgekgMLissJPMWfll6FgVRdchPjUwuH8lXfdG7yU1znKnbXidjx5cDcvcNV5iSAdc5bOIFtM17WXAdPH4tXOxSL7rUQxrD6JJcv00VDKjUHJsZqLJwm1r4nv9YKdQYnESjBQHfiVFekwfDbCfSAl+uhBD4w0HRUmSc/+kuiZZHopRGSBTHOQL/elfGLRKwhVmiVikBNezvKQAGz6UiWtFj8ibo9z8orTbMDkdCGCJc3LQgMXUzjfGE0lhqshIVDAP8UaGNGWePGTMx5ZPgZu97qfRnXoGRaxrI3kUp6nyJKgna5thm0iJznvMiZfUknD0prk0cp4Zh1Y0Ja7OBorADbyJj+Nz8zoFAYdE3yqehH8VrwvYMli53PcQ0b110fa7C7qNX6J5L/zTOVfnoS4sMm16kFTXiaq3N1gJNbDULn44lM+2F7ZxaAO7nb6jQHcthaLYijyQLaR3HpMXRAnaiQhACwTsfzBUDv8jwahlo9ycRaVAIAQG4bBYRGbulKkEGmt6CxLv+WysG7SC7YCUB6xWZ0CDyiizzxVbVFg/OUZpCqp7aX0byMaC5Pl999shwmIxE2YiUhWIOeO5ClsMb0Nv44VT5gtIoUUswc2O7uHF4oJk5eJ1imvbTs0dcOXv0ZAd/U/gmPAhPRN4YWk72E6SFHweHIBu8B6G1UYvGXgwgbncsSLBrPRREpuTz/MtVaxxXfOoDxTjXun8iPphVegX1o8+HVXnuhZFTB6wLgnBEAdwzyf7hty4nKrAFjJA3Xn+M+k/6dPzVBHE+yz3TnqdPcVlZjN1gZvcopC01K7a4V+k6IUhHTW31YxEy/ZvT+AiBrawXpuYe3kADcBedITHlEfb7R2jXLB27eeISv/+VAFFJDt4DBHACDHJG8JVqKO+9VTi7lcROukGo5n83OA4eBeiBI6lIJKtiiCkdFxO/gvFGYR+XMnav+utZS/8mmJOX2ILXkQri++n9F4couKd03MlrazP9uFxfq9kC/6DW0tMQoQuJg8o94l0ory33+qk0zSkHVU8QNBnsmNOg9rHfckr1+OFHGxM+wd5QVT2rkjUHIqqBQGdr+oOT9JjLe4H61gOWZxWVZZpZc5wRdsgk56xRRfWXwvJfJomuob4NwNTShJu3VYuQV26U3VgPzKwyAY3cvfpdC/4IO9oQIrs3Xa/P8Xt1ZzmpqGSXm2SyvxkltLkqE8SjpUwL+l/bPU5mtZOM/X0qBTjvn6H/BkI2WXXQqqONWSdTTx85VvaJlHA+VixBGn7MK1dsnv594vfqNMPoJ/jhp8mBXkLi5kykMA2zex77xeYzNeu2/U4xVEJ2nFKy6z4RCVTlqzYXK8LKVqwQqeDYgQBeKa4AT3HQsvJ222YvXMtZdbd5kDzYILMJVmTDOMXw2KdU5Xd26IC1kYD/vGM6WcAqMWNVgGXWeS+pHx5M3ltxO9BqRip0MYnLB2TSZj6ZRpbmKzQd52+SlA5Ex9JhRG8eNDfv8sWR7kCzBs8E2RKrVJf5/qiOMfPI+F+8tCSEwGgFgzTcXJz1J3782/YdKFBM+fad/K8o4unGkj2cIkn02icoEX5b1kZGzxxo5M3JzETWV79o/NB3vf2opxNgPxi4Q0vOOmwTrnLf3eozqfwZjrOctzuoR731A3TnAMEciSHO4ABUFCLjgGuEkpFO+e0lVYp7M/0E87nxv+b0JvVh6VlVDr2cjbITallKxgJlsAIZZGgRZxKVCWt/3KvHK82gInaOsbQocc1VK66g8YNXyCLARxlyFBA3w9CF6bsGVb2ms1lVSZgt1kxvPeaFATDQhVc342sZQwdOWl6/ZY4N7LeGypDa8R2YsGK+ooua7cfmca8N/sSORF5r9oBZfqjOT24WUcgQiBMgiVidQ6CeMN2xgHZ2YNEpFBSBD2MvZwN36/dphc4SDOrzIBpUYmg3qxZtRWNViAW1DwJa9xT3eGUJbhPwSucE6yTOmnNMkR5z/lJvmQsELosjX9CKvH9+UVZthiesuh9nayL/B0wOUEDXj4gptq0D0XhxXzaRtcbagN2Xil0Y4kxvAxmmeiwxa5xAytOy8QoEssh9nJih2MoITbbGXyd8sc9NObpy9fpxHKomVfjSrOAp6bqweqtdvZyZSe43G+uFhw3CzVb+ZiK7HXxxRYOfz3fPoCFWVFAfH7AnVq3o9DkQI1YJj1E3AXFl4mVQjyyzxq9P3KSWaawd4EI8EeiaFQmP6DILk1V/pelW/lxB1Ym+m7fEMq2/D8lFL+DsmFuG1OJrD2PRCpCGDbRRG7PTUBVzV1vn1pSqaa3p6VtCOIZJGdynRuY9DwVa7O6lzr7St+aiQxve8s2AfRPwFdD7I1rcRMjWiITr4qipxZzStHMZDglnXMCeAkjY9h6ekyhjFiekq4+4hVXlkUQ/V7dxljvPoe6Ckho+3RZZWh8mwHjDjv1QVD9wegu4v/hekSZ2aZxo/ttT3FF9ivHh95aeKRX2KBb6r3P5AdlEqjVdTCbipw9uN1yw3hSyx/gRHdLBnhAFKb+gVG1IycnNvVQjHMwZINhQ+LQrGoVLOYVnBZPXYVf5qq4ogqreDeQJyTYgxg7SRRdczI6G1hHR3r0h9D3ZfsnpvXZdtwYuwrOhzdd+yQVQTO0qGdu6HTXYekZe4WCVE1Sj6zLZZkGKa47lp9Cbj/WazVwELIBvcNaZjF9J+589JUCevqk5ClknmSSGE2ErQnqE6Lhp0r1jvJLVgDz4mDOnaYvaB+gmO317JMF6r9jH9AXJ8nHu5zJ5kLUJHo5eXOSF5sLEr4h/0o47nRjwHSMKuAh0C5hCpyMA0cCRvKfQ2d1CkN7J6VOwFhOtwfyCW+bOCwCcHgtc/5AuF7xA6AvAX4HFaZWH2QDcXeYSFYkfysnwkz5lg4cI1OUEJl0xAW4ZpXmvlGnoApvrbnNZSfwmJZbtE0omudokG2NPvqJc//2PampXqPfoJA6H0wgEZhbIS0u4n8z2cUtTN1+gIP1MW3SkFDIEb0uT3rv7WaxJ07gxCxiXiSTW9o04x9wSsi9TrhMCotAjgNSlu/NJsuebFz4eyt1YUCJNjf722tM41h5Q4iexgIB2dwuSUxbPZbpnGkyRcteYtjzt9qaV6ve6J2yDOkPXUrIJI59MBP2dUZamZjdVr/OGYWdvRsch0W/PeDhUi+iNgmP9wdXaIilSM6Mo8cT0NaZYXcag8YGKtnsH7soMzFXwAyCkWSHRDGbObgfX5PP+ShUGX9tLtxFtQa1Og2aYoZw8hBflZ2yu3hdlorpsEDpT5NWSNgHPHRWC1xWGqnqGUld/Bh/ugQXa+BX6uIyjs+rgP6RUv0XExoihj0ylfg0cwnVxgmnMmmW1QTsDTh38aV6Pk5wBLfJJZtFFR6Sw7cBpMY1IpZEd0+mpZ0mdmmqWogpgD3G0DXGqAQ7hw2FhYqqZTrP0YnBabgfnJThA+U7Q+TgcL+6Ni8y7AwJ15fKHr+IFFDyD/PNE/HZ427oe24n7vWVKJvczWZ+mZso7268jSe8v/yTR/pjEL4zDWAJLt95bwZK0t9rQJwnxjCUknYW0sxjLehSdG/GkDL85pHR50Wajllii+ZCNuygxSxtp0Vj9oNSPDiwVydNHKVPPk/wt3CxBhzAJpgCPhSc2+6xpxm8nr6cvLl36Yyd41GQSkHcZUsrbkEanryKE4ve9i0Am4OtBJN/ml5sgJ/3UVP7pAROy9Mjpmq6KU2nLLgjjokRfv9blSDbI1qR3rqx1HV0HOhMTfa/HIPy3S5vwbP2joxp0EtVE+9RMdXd+QK8Cp8mNbHHZ8UUI0LJRNbdzDdMX+AeCvS5ckJDahC9UlViBYXyhIXOjOrsx6Uz3iz2U7tiqM8K68WvHBWqaHz/ouKLuqF23bvEmrCfN/DdzSFCjLlI1gfaIBtccmYHGW04Oris1pDQMO1jdasmBNGC0pupgxDLy4LLWmYAqOTz6B6uOJRaPEUFhI4bOD8FPfh2B3AacjheEsZlm+2hgWurCTGyb6Aaw3FAYqjdvIVQLEuFJfaTHS8GsZexcvrUg5PTFTQ85h5jwgWQVMfCqtnd+SgDE3mj4k+pXED6wNqHFPYq4OhMSwcibOgRMLvREmBl33DXmADKMiIHl5HIFpc0p/dRM/91AUnKKeaGJXqXvVRVStQPFXZXmmqE0srEcTr50N56TcydUwxNKgwQAogrnznSsWspO8pKCh7WmDDpqEHZnS4zgBY/uCV5DfukmAb25BvN7ln+m09lewlY/QtoMpsBXnF5/2Iv7GY2swu1hXDgJxPiZ+ZzVegng2kncOo0jfr6PjValqlTWMgZUgDnnPRyYiPuwKOl2CQu7tCKibhUPX9DpeRi9bfChocnF12HQFZgzZQS7bHTXyBnx41t9kKFPbmqjxk0lzdSctoctSGWfo1DF6PxOkyEcCDmsG7Q7GkJJ7jH1z0k8J0iyJKKf9OEcDoM200EUMsRqNAkoIWtYi/Z76L0ActSRrjQ1C5hILumAzCqCSwa//Pcxl0wiMUt5Xt9LKb23pNQ2g+Zq8B9xYGmtaQHDgixYQGqMdE43MFnbUCSX1pPIWhYAGDOf0kCCRCHfuSJz0lFVCrAXIPeJtfh0wjyRs8pqIcurZoMGI5WJ0+kLC2Rc7tlBEDJ5+ys6xctjUH0JC+iChj6jlgSmQDav1vp7kbhLzP3Qp6OJg5Bp6SsAElohItiFE6ZxSEZ8KL8xhGCuwavLv+7yxUFrUXyyX44RvZOdHmxozuVPpUe+0N0S9Pw0SNZ46kzdQUllZPv3tl4T1K66oYMfbSpTFlc9CjjTwDVJuj9oRUJcZ7cvn4yotVArMgdC7qQ58KoisNRai1ENIfzbCoXrOZHoRMoz5PLoZDMUphO34yiZWg7C0Rv5JUIoWoAZv86OjFyJwGEmLoGSlUkD1tsT+M1AQegcC/yeksyMM/AEAS6SL3QzxTsXUIRo0rLzOmuUnWqQnmw4AIVIY3b0c6ijMbMGLn40ZyTSG53WdHykGw1jOrMI0jL9O4zALJv2/1XaGDCcCtkzWZCn84Hsoy90XLEcYIyEbujXmdyTwHsHsfpl27/CXJ9EA2nAfCLmlTB/LLFhLIdYHy6X2YzNYs/ncf746EgyMINfYpcuJPULsFcwsM9mEk9f5i+bg1ZBTl6NqxewbeWxrRY0ZnxYSvb5qfF7CR/ImPU1uk3i8bjxiy9MBlBllP6TBpCKTcHRbYXtRpbYfYqKb4vJgcH87X38j4O7+U+58cITtJKjAEExXPnhL2nk9HxZuB5BxNurwD85nIwymvdcky5gCrb2P4ccDjo4COw3GgjfD2/Kpny2BEja9m6W1ynWGKcIy279fcFeQTFPw2AmpYIgRkem6aY9XJAc5751BS0gMUk5GausxbJIUi2a4xXg7/HDF9eZX/o4l+AMP4jFW4fA+L/ubvUQ2H0m4EDtGupn3g/ZgV1/Ks873Uf/Ce3C7WtFxPws/xtOqxEL8DEyTWRz3eVYlzwIKHgBn9LLP4qDYyB+gT8PrqSCLXu+RvG9Igd7IuEah9DyAxhAybp1ZfIKyCCb0xEKJU1RLhQztKAfjZjIPBMeEzaMPJFic/4rzOKR7oFHs3sSJfd7kwOBZCesFeUCTw9LkeAUNEabk+OlsDCHzERtMtKbr1gF96PIZz6RqJ2FbnZq6mZewdpmUPHfskqGAyTrv4/42STefpzIS4AaDs2cgC1Fzr/aCEJIbZb5WdGqYqOWnpLpSNfFsq72M+MORylscdjFCPNenmzRsMlk0vU4afQ843d/rRxOG3rI5gEZiwHBqGqdxyFgY+CVjJ22IVVir735hLTHy8xV5Xjyks8wMRiv6fEN49GU1pfbEZ/Aagd+fxp51uw9hk8cBn7sEAw1bvd8Eo42Y7v9Wg18uyxNuFI02ik46GWWga8TBEYNktvldqSa5p4c9qGrqPOB8c4vTLwlalk9T1sNL7aYUAFg+8d7W3y5CkpAaDnZl7kY2L7m+kp6et5NAMsKZfxI27vre94NY779Ng+QLmk5qmEiledZ6A4JpXXPN8cuI2hkdRszJXiB4tU/Ch6uNTTqZ1rPwfYbKrmZjuwG5iJoScX+zyF97UXD8ETdDJ5DOY3ppsPqhyVv0WLB0hOMSsVL9Cqvg9rx0OIjpzSJVb+bBz06MRyIcu/Ik+ha1X6APIO6NgJYv+J9oIJcS6ZfylHeTg9JxnzJMZdoV9Q7FnC+BkluiS3Mv/9yskzCKdNaxuf7nojfqnr7xOfF9tOW03pr4own4gtThrQqK0ZqtvpMHWNRDJODMG7ksEmon93jNbzfXV8Gx/MkQ8ewFAqxmilDrbKCN5NZ5bRtktlW6K3pZykcCBVvrNUlE80xOAWChplyaHwFEvs8+PEsO4IMR0C/vy0kZRDS2IS76fga7B+NpAHInV2P0u5TWV5AhOB0KWrZEiW8feZm9RMW2WCbFduzRM/YlpMm6+cW1aYJAWGVYJascSiUN2onr5tXL1VbspQ0yXQI76D1oBhbabphu/TJvXNGm+DeGPIYgkZXJvUbnRfJm1UN5aZIBitmhlzTEYsB6xjBftyXdPeb3vfZs92GeTNJXDl1YHVWtU9n/jqKOVXMxLkPZWLRc01E9c/nzl9ue2ahj9Fw+acOSQ0xBsyOY6IYqhcOf2dBJQ3r8xmz2RFpVD4G1PEjzFNf+jaLfh8vydImuwrufqf89TnrLRtSrV9WvpyBD/kZzh5vO3ehnhvkj404bwEZvMoh3q2dJNv8LPuYsQ/YFrM7vF1e1noGpgCAieI2yW3/EA9IebxIAUwWXRUFJvSoOsE/cr0wXFFafLSOF2A9dX+MO0hvlPvijXQCAA2T7qoA/LLCjePThRSSHaQkJLQI7Z9pGy2R59W8wSrNRl/dalE0Xj9wK13ssxhczbVxMiju6BudVXDHQLPyslKEFBrpmgsU+APP4N2TcvMaM8bA1RvTQqDAyZO/UqIJQWEYz6f+KK8Ijtkqkp7wtn0CxiTS8s8pw1kv4VRCynrLgxit4xHwqhL4hbeBWXV1ciwvtFSlqUrCE2pCzfgAc4UVEbTw7a806ao/g38qVObXZm4YwJID4xyoFG21TA2tYESYzstmJuPI2xbLKjBmD0V5bRZdOUPnE9/9vqgQlS7hv1Xz6Z9opEAJa4tcwrGtJ0pf5sLyUH66QAYAfXkcUt9VEdyGc3QYPRnaKw1nbQdxqQTTvthxN2xC69gzHbVGN+quRFpgaIUGK3/KCv8JYZ/KI91qa4z6F29HbmAJqlzxV7CeFFoairzoh1hX6EruFmh8us4NXG84A8M5TvNL/UnnM/IUSiOxhxLXD3P7srjeKfqHVHTDd6l2CJLZq7GhemrxOUHzI/oco70EKY19zpe1pk6k/t9+IdOd96eeCVeeT3tqvMLkDVM4G4rjvFgcACrV3KZW9kOGgkPiukCpA9IUReLYQzCmdmZ8XIv8FMvG5w2IO1a6VKnXXa5V6AIaYUNF+En6kseXLDTCvDS8cK7xERT8phaZUkWKpoYgLe637FibyMql/RhjD8XEX72Zgp8+Rf5GCNDnGvSHI34faei6vdFsJv9n29b5vG71V9HS5RD4ZMXhyLLtMN+EQD1uRttsFP4EkZ3HFe4TPYdDXoOGpCOock0sAwWcOAPIeeFUKFqw3DmFpRXd+wQXANqsra1IN43PZYRxIUjFj+Hc5WtFpmPOrotcqkLpBro4oetHD0k3PrLek6fqhLtMjLk5TLueEpE3ZRD/kPdNf1Ls3Gzi6YiaRJII4un6bmLMM9f/y+zy71bIn3MgjtyKnpEAAAAA" alt="Report Generator"/></div>
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
      <button class="btn-cfg" onclick="openPanel()" style="background:#1a3a2a;color:#34d399;border:1px solid #34d399;padding:10px 16px;border-radius:8px;cursor:pointer;font-size:0.82rem;font-weight:600;margin-top:8px;width:100%;">&#9881; Configure GCP</button>
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

  <div style="text-align:center;padding:14px;color:#484f58;font-size:0.75rem;border-top:1px solid #21262d;margin-top:8px;">Built by <strong style="color:#34d399;">Raghu Putta</strong> &nbsp;|&nbsp; <a href="https://github.com/raghu-putta/greenops-agent" target="_blank" style="color:#58a6ff;text-decoration:none;">&#9733; GitHub</a> &nbsp;|&nbsp; <a href="https://greenops-dashboard-845589445410.us-central1.run.app" target="_blank" style="color:#58a6ff;text-decoration:none;">&#127760; Live Demo</a> &nbsp;|&nbsp; <span style="color:#34d399;">v2.0</span> &nbsp;|&nbsp; Powered by <span style="color:#34d399;">Google ADK + Gemini 2.5 Pro</span></div>


<div id="gcp-panel" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;z-index:9999;background:rgba(0,0,0,0.85);backdrop-filter:blur(8px);justify-content:center;align-items:center;">
  <div style="background:#0d1117;border:1px solid #34d399;border-radius:16px;padding:0;width:480px;max-width:95%;max-height:90vh;overflow-y:auto;box-shadow:0 0 40px rgba(52,211,153,0.2);">
    <div style="background:linear-gradient(135deg,#0a1628,#0d2818);border-bottom:1px solid #1e3a2a;padding:20px 24px;border-radius:16px 16px 0 0;position:sticky;top:0;">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
        <h2 style="color:#34d399;margin:0;font-size:1.1rem;">&#9881; Configure GCP</h2>
        <button onclick="closePanel()" style="background:#1a1f2e;border:1px solid #30363d;color:#8b949e;width:32px;height:32px;border-radius:8px;cursor:pointer;font-size:1rem;">&#x2715;</button>
      </div>
      <div style="background:#161b22;border-radius:10px;padding:10px;border:1px solid #21262d;text-align:center;">
        <p style="color:#34d399;font-size:0.7rem;margin:0;text-transform:uppercase;letter-spacing:2px;font-weight:600;">&#9733; Your GreenOps AI Assistant &#9733;</p>
      </div>
    </div>
    <div style="padding:16px 24px 0;">
      <div style="background:#161b22;border:1px solid #21262d;border-radius:16px 16px 16px 4px;padding:16px;margin-bottom:4px;">
        <div style="display:flex;align-items:flex-start;gap:14px;">
          <div style="flex-shrink:0;position:relative;width:56px;height:56px;">
            <div id="bot-glow" style="position:absolute;inset:-4px;border-radius:50%;background:radial-gradient(circle,rgba(52,211,153,0.4),transparent);animation:eyeGlow 2s ease-in-out infinite;"></div>
            <div style="width:56px;height:56px;border-radius:50%;border:2px solid #34d399;background:#0d1117;display:flex;align-items:center;justify-content:center;font-size:2rem;position:relative;z-index:1;">&#129302;</div>
          </div>
          <div style="flex:1;min-width:0;">
            <div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;">
              <span style="color:#34d399;font-size:0.72rem;font-weight:700;letter-spacing:1px;">ALEX</span>
              <span style="background:#34d399;color:#0a1a0f;font-size:0.6rem;padding:1px 6px;border-radius:4px;font-weight:700;">AI</span>
            </div>
            <div id="bot-msg" style="color:#c9d1d9;font-size:0.85rem;line-height:1.6;min-height:48px;"></div>
            <span id="bot-cursor" style="display:inline-block;width:2px;height:14px;background:#34d399;margin-left:2px;animation:blink 0.7s step-end infinite;vertical-align:middle;"></span>
          </div>
        </div>
      </div>
    </div>
    <div style="padding:20px 24px;">
      <div style="margin-bottom:14px;">
        <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">&#10024; Gemini API Key *</label>
        <div style="position:relative;">
          <input type="password" id="cfg-api-key" placeholder="AIzaSy..." style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 44px 11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
          <span onclick="var i=document.getElementById('cfg-api-key');i.type=i.type==='password'?'text':'password'" style="position:absolute;right:12px;top:50%;transform:translateY(-50%);cursor:pointer;color:#6e7681;">&#128065;</span>
        </div>
        <small style="color:#6e7681;font-size:0.72rem;">Free at <a href="https://aistudio.google.com/apikey" target="_blank" style="color:#58a6ff;">aistudio.google.com/apikey</a></small>
      </div>
      <div style="margin-bottom:14px;">
        <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">&#9729; GCP Project ID *</label>
        <input type="text" id="cfg-project-id" placeholder="my-project-123" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
        <small style="color:#6e7681;font-size:0.72rem;">Find at <a href="https://console.cloud.google.com" target="_blank" style="color:#58a6ff;">console.cloud.google.com</a></small>
      </div>
      <div style="margin-bottom:14px;">
        <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">&#127758; GCP Region</label>
        <select id="cfg-region" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;cursor:pointer;">
          <option value="us-central1">us-central1 - Iowa, USA</option>
          <option value="us-east1">us-east1 - South Carolina, USA</option>
          <option value="us-west1">us-west1 - Oregon, USA</option>
          <option value="europe-west1">europe-west1 - Belgium</option>
          <option value="europe-west2">europe-west2 - London, UK</option>
          <option value="asia-east1">asia-east1 - Taiwan</option>
          <option value="asia-south1">asia-south1 - Mumbai, India</option>
          <option value="australia-southeast1">australia-southeast1 - Sydney</option>
        </select>
      </div>
      <div style="margin-bottom:20px;">
        <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">&#128205; GCP Zone</label>
        <input type="text" id="cfg-zone" placeholder="us-central1-a" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
        <small style="color:#6e7681;font-size:0.72rem;">Usually region + -a e.g. us-central1-a</small>
      </div>
      <div style="display:flex;gap:10px;margin-bottom:12px;">
        <button onclick="savePanelCfg()" style="flex:1;background:linear-gradient(135deg,#34d399,#10b981);color:#0a1a0f;border:none;padding:13px;border-radius:10px;font-weight:700;cursor:pointer;font-size:0.9rem;">Save and Close</button>
        <button onclick="testPanelConn()" style="flex:1;background:transparent;color:#34d399;border:1.5px solid #34d399;padding:13px;border-radius:10px;font-weight:600;cursor:pointer;font-size:0.9rem;">Test Connection</button>
      </div>
      <p style="color:#484f58;font-size:0.72rem;text-align:center;margin:0;">Stored in browser session only. Never sent to our servers.</p>
    </div>
  </div>
</div>
<script>
var alexQuotes = [
  "Welcome! The cloud awaits your command. Let us scan for waste and save the planet together!",
  "Hi there! I am your cloud optimization partner. Fill in your details and let us make your infrastructure greener!",
  "Yo! Ready to crush some cloud waste? Drop your GCP creds and let us roll!",
  "Precision is my protocol. Enter your credentials and I shall optimize with surgical accuracy.",
  "Every idle VM we stop plants a virtual tree. Let us make your cloud carbon-neutral today!",
  "Your GCP project is waiting to be optimized. Together we can cut costs and carbon emissions!",
  "The best time to optimize your cloud was yesterday. The second best time is right now!"
];
var quoteIdx = 0;
var typeTimer = null;
var cycleTimer = null;
var isTyping = false;

function typeText(text, el, cb) {
  isTyping = true;
  el.textContent = "";
  var i = 0;
  if (typeTimer) clearInterval(typeTimer);
  typeTimer = setInterval(function() {
    if (i < text.length) {
      el.textContent += text[i];
      i++;
    } else {
      clearInterval(typeTimer);
      isTyping = false;
      if (cb) setTimeout(cb, 3000);
    }
  }, 28);
}

function cycleQuote() {
  var el = document.getElementById("bot-msg");
  if (!el) return;
  el.style.opacity = "0";
  el.style.transition = "opacity 0.4s";
  setTimeout(function() {
    quoteIdx = (quoteIdx + 1) % alexQuotes.length;
    el.style.opacity = "1";
    typeText(alexQuotes[quoteIdx], el, cycleQuote);
  }, 400);
}

function startTypewriter() {
  var el = document.getElementById("bot-msg");
  if (!el) return;
  if (typeTimer) clearInterval(typeTimer);
  if (cycleTimer) clearTimeout(cycleTimer);
  quoteIdx = 0;
  typeText(alexQuotes[0], el, cycleQuote);
}

function openPanel() {
  document.getElementById("gcp-panel").style.display = "flex";
  loadPanelSettings();
  setTimeout(startTypewriter, 300);
}

function closePanel() {
  document.getElementById("gcp-panel").style.display = "none";
  if (typeTimer) clearInterval(typeTimer);
}

function savePanelCfg() {
  var key = document.getElementById("cfg-api-key").value.trim();
  var proj = document.getElementById("cfg-project-id").value.trim();
  var region = document.getElementById("cfg-region").value;
  var zone = document.getElementById("cfg-zone").value.trim() || region + "-a";
  if (!key || !proj) {
    if (typeTimer) clearInterval(typeTimer);
    var el = document.getElementById("bot-msg");
    el.style.opacity = "0";
    setTimeout(function() {
      el.style.opacity = "1";
      typeText("Oops! API Key and Project ID are both required. Please fill them in!", el, null);
    }, 300);
    if (!key) document.getElementById("cfg-api-key").style.borderColor = "#f85149";
    if (!proj) document.getElementById("cfg-project-id").style.borderColor = "#f85149";
    return;
  }
  sessionStorage.setItem("gops-cfg", JSON.stringify({apiKey:key, projectId:proj, region:region, zone:zone}));
  if (typeTimer) clearInterval(typeTimer);
  var el = document.getElementById("bot-msg");
  el.style.opacity = "0";
  setTimeout(function() {
    el.style.opacity = "1";
    typeText("All set! Your GCP credentials are saved. Ready to scan the cloud!", el, null);
  }, 300);
  setTimeout(function() { closePanel(); }, 2500);
}

function testPanelConn() {
  if (typeTimer) clearInterval(typeTimer);
  var el = document.getElementById("bot-msg");
  el.style.opacity = "0";
  setTimeout(function() {
    el.style.opacity = "1";
    typeText("Testing your connection... hang tight while I check!", el, null);
  }, 300);
  fetch("/test-connection", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({
      apiKey: document.getElementById("cfg-api-key").value.trim(),
      projectId: document.getElementById("cfg-project-id").value.trim(),
      region: document.getElementById("cfg-region").value
    })
  }).then(function(r) { return r.json(); })
  .then(function(d) {
    if (typeTimer) clearInterval(typeTimer);
    var el = document.getElementById("bot-msg");
    el.style.opacity = "0";
    setTimeout(function() {
      el.style.opacity = "1";
      typeText(d.success ? "Connection successful! All systems GO! Ready to scan!" : "Connection failed. Please check your API key and Project ID.", el, null);
    }, 300);
  }).catch(function() {
    if (typeTimer) clearInterval(typeTimer);
    var el = document.getElementById("bot-msg");
    el.textContent = "Could not reach server. Check your network connection.";
  });
}

function loadPanelSettings() {
  try {
    var s = JSON.parse(sessionStorage.getItem("gops-cfg") || "{}");
    if (s.apiKey) document.getElementById("cfg-api-key").value = s.apiKey;
    if (s.projectId) document.getElementById("cfg-project-id").value = s.projectId;
    if (s.region) document.getElementById("cfg-region").value = s.region;
    if (s.zone) document.getElementById("cfg-zone").value = s.zone;
  } catch(e) {}
}

window.addEventListener("load", function() { loadPanelSettings(); });
</script>

</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD
