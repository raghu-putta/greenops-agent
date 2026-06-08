with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# FIX 1: Add spinner + immediate feedback to run() function
old_run = '  function run(mode) {\n    fetch(`/run/${mode}`, {method:\'POST\'})\n      .then(r => r.json())\n      .then(d => { if (d.error) alert(d.error); })\n      .catch(err => alert(\'Could not start pipeline: \' + err));\n  }'

new_run = '''  function run(mode) {
    var overlay = document.getElementById("spinner-overlay");
    var txt = document.getElementById("spinner-text");
    if (overlay) overlay.classList.add("active");
    if (txt) txt.textContent = mode === "demo" ? "Starting Demo Pipeline..." : "Connecting to GCP...";
    var terminal = document.getElementById("terminal");
    if (terminal) {
      terminal.innerHTML = "";
      var msg = document.createElement("div");
      msg.style.color = "#34d399";
      msg.style.padding = "8px";
      msg.textContent = "[ " + new Date().toLocaleTimeString() + " ]  Initializing " + (mode === "demo" ? "Demo" : "Real GCP") + " pipeline...";
      terminal.appendChild(msg);
    }
    fetch("/run/" + mode, {method:"POST"})
      .then(function(r){ return r.json(); })
      .then(function(d){
        if (overlay) overlay.classList.remove("active");
        if (d.error) alert(d.error);
      })
      .catch(function(err){
        if (overlay) overlay.classList.remove("active");
        alert("Could not start: " + err);
      });
  }'''

if old_run in c:
    c = c.replace(old_run, new_run)
    print("FIX 1 OK - Spinner and immediate feedback added!")
else:
    print("FIX 1 FAILED")
    print(repr(c[c.find("function run"):c.find("function run")+300]))

# FIX 2: Remove 2s delay
old_delay = "    async def delayed_start():\n        await asyncio.sleep(2)\n        await run_pipeline(mode)\n    asyncio.create_task(delayed_start())\n    return {\"status\": \"started\", \"mode\": mode}"
new_nodelay = '    asyncio.create_task(run_pipeline(mode))\n    return {"status": "started", "mode": mode}'
if old_delay in c:
    c = c.replace(old_delay, new_nodelay)
    print("FIX 2 OK - 2s delay removed!")
else:
    print("FIX 2 SKIP - no delay found")

# FIX 3: Add event buffer to _broadcast
old_b = "async def _broadcast(msg: dict):\n    data = json.dumps(msg)\n    for q in list(_sse_queues):\n        await q.put(data)"
new_b = "_event_buffer = []\n\nasync def _broadcast(msg: dict):\n    data = json.dumps(msg)\n    _event_buffer.append(data)\n    if len(_event_buffer) > 200:\n        _event_buffer.pop(0)\n    for q in list(_sse_queues):\n        await q.put(data)"
if old_b in c:
    c = c.replace(old_b, new_b)
    print("FIX 3 OK - Event buffer added!")
else:
    print("FIX 3 FAILED")

# FIX 4: Replay buffer when SSE connects
old_gen = "    async def generator():\n        try:\n            while True:\n                try:\n                    msg = await asyncio.wait_for(q.get(), timeout=20.0)\n                    yield f\"data: {msg}\\n\\n\"\n                except asyncio.TimeoutError:\n                    yield \": heartbeat\\n\\n\"\n        except asyncio.CancelledError:\n            pass\n        finally:\n            if q in _sse_queues:\n                _sse_queues.remove(q)"
new_gen = "    async def generator():\n        for buffered in list(_event_buffer):\n            yield f\"data: {buffered}\\n\\n\"\n        try:\n            while True:\n                try:\n                    msg = await asyncio.wait_for(q.get(), timeout=20.0)\n                    yield f\"data: {msg}\\n\\n\"\n                except asyncio.TimeoutError:\n                    yield \": heartbeat\\n\\n\"\n        except asyncio.CancelledError:\n            pass\n        finally:\n            if q in _sse_queues:\n                _sse_queues.remove(q)"
if old_gen in c:
    c = c.replace(old_gen, new_gen)
    print("FIX 4 OK - Buffer replay added!")
else:
    print("FIX 4 FAILED")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("ALL DONE!")
