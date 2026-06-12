import re
with open("app.py", "r", encoding="utf-8") as fh:
    c = fh.read()

ok = True

# ---- 1. Import Request from FastAPI ----
if "from fastapi import Request" not in c:
    if "app = FastAPI()" in c:
        c = c.replace("app = FastAPI()", "from fastapi import Request\napp = FastAPI()", 1)
        print("1 OK - Request imported")
    else:
        print("1 FAILED - app = FastAPI() not found"); ok = False
else:
    print("1 SKIP - Request already imported")

# ---- 2. /run endpoint accepts user credentials JSON ----
old_run = """@app.post("/run/{mode}")
async def run(mode: str):
    if pipeline_status["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)
    if mode not in ("demo", "real"):
        return JSONResponse({"error": "mode must be 'demo' or 'real'"}, status_code=400)
    asyncio.create_task(run_pipeline(mode))
    return {"status": "started", "mode": mode}"""

new_run = """@app.post("/run/{mode}")
async def run(mode: str, request: Request):
    if pipeline_status["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)
    if mode not in ("demo", "real"):
        return JSONResponse({"error": "mode must be 'demo' or 'real'"}, status_code=400)
    user_cfg = {}
    try:
        body = await request.json()
        if isinstance(body, dict):
            user_cfg = body
    except Exception:
        pass
    asyncio.create_task(run_pipeline(mode, user_cfg))
    return {"status": "started", "mode": mode}"""

if old_run in c:
    c = c.replace(old_run, new_run, 1)
    print("2 OK - /run accepts user credentials")
elif "user_cfg" in c:
    print("2 SKIP - already wired")
else:
    print("2 FAILED - /run pattern not found"); ok = False

# ---- 3. run_pipeline applies user credentials per-run (in-memory only) ----
old_sig = "async def run_pipeline(mode: str):"
new_sig = """async def run_pipeline(mode: str, user_cfg: dict = None):
    global _DEFAULT_ENV
    try:
        _DEFAULT_ENV
    except NameError:
        _DEFAULT_ENV = {e: os.environ.get(e) for e in ("GOOGLE_API_KEY", "GCP_PROJECT_ID", "GCP_REGION", "GCP_ZONE")}
    for _e, _v in _DEFAULT_ENV.items():
        if _v is not None:
            os.environ[_e] = _v
    if user_cfg and mode == "real":
        _cred_map = {"apiKey": "GOOGLE_API_KEY", "projectId": "GCP_PROJECT_ID", "region": "GCP_REGION", "zone": "GCP_ZONE"}
        for _k, _env in _cred_map.items():
            _val = str(user_cfg.get(_k) or "").strip()
            if _val:
                os.environ[_env] = _val"""

if old_sig in c:
    c = c.replace(old_sig, new_sig, 1)
    print("3 OK - run_pipeline uses user credentials in-memory")
elif "user_cfg: dict = None" in c:
    print("3 SKIP - already wired")
else:
    print("3 FAILED - run_pipeline signature not found"); ok = False

# ---- 4. Frontend run() sends saved credentials with the request ----
old_js = """fetch(`/run/${mode}`, {method:'POST'})"""
new_js = """var _cfg={};try{_cfg=JSON.parse(sessionStorage.getItem('gops-cfg')||'{}');}catch(e){}
    fetch(`/run/${mode}`, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(_cfg)})"""

if old_js in c:
    c = c.replace(old_js, new_js, 1)
    print("4 OK - frontend sends credentials on run")
elif "sessionStorage.getItem('gops-cfg')" in c and "body:JSON.stringify(_cfg)" in c:
    print("4 SKIP - already wired")
else:
    print("4 FAILED - run() fetch not found"); ok = False

if ok:
    with open("app.py", "w", encoding="utf-8") as fh:
        fh.write(c)
    print("ALL DONE - saved!")
else:
    print("NOT SAVED - fix the FAILED items first (app.py untouched)")
