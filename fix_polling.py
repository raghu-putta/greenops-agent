with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# Step 1: Add output_log list to store all pipeline output
old_status = 'pipeline_status = {"running": False, "complete": False}'
new_status = 'pipeline_status = {"running": False, "complete": False}\noutput_log = []  # stores all pipeline events for polling'
if old_status in c:
    c = c.replace(old_status, new_status)
    print("FIX 1 OK - output_log added")
else:
    print("FIX 1 FAILED")

# Step 2: Add to _broadcast to also save to output_log
old_b = "async def _broadcast(msg: dict):\n    data = json.dumps(msg)\n    _event_buffer.append(data)\n    if len(_event_buffer) > 200:\n        _event_buffer.pop(0)\n    for q in list(_sse_queues):\n        await q.put(data)"
new_b = "async def _broadcast(msg: dict):\n    data = json.dumps(msg)\n    output_log.append(msg)  # save for polling\n    _event_buffer.append(data)\n    if len(_event_buffer) > 200:\n        _event_buffer.pop(0)\n    for q in list(_sse_queues):\n        await q.put(data)"
if old_b in c:
    c = c.replace(old_b, new_b)
    print("FIX 2 OK - output_log saving added")
else:
    print("FIX 2 FAILED - trying alternate")
    old_b2 = "async def _broadcast(msg: dict):\n    data = json.dumps(msg)\n    for q in list(_sse_queues):\n        await q.put(data)"
    new_b2 = "async def _broadcast(msg: dict):\n    data = json.dumps(msg)\n    output_log.append(msg)  # save for polling\n    for q in list(_sse_queues):\n        await q.put(data)"
    if old_b2 in c:
        c = c.replace(old_b2, new_b2)
        print("FIX 2 OK - alternate pattern worked")

# Step 3: Add /poll endpoint after /status endpoint
old_status_ep = "@app.get(\"/status\")\nasync def status():\n    return pipeline_status"
new_status_ep = "@app.get(\"/status\")\nasync def status():\n    return pipeline_status\n\n@app.get(\"/poll\")\nasync def poll(since: int = 0):\n    \"\"\"Polling endpoint - returns all events since index 'since'\"\"\"\n    return {\n        \"events\": output_log[since:],\n        \"total\": len(output_log),\n        \"running\": pipeline_status[\"running\"],\n        \"complete\": pipeline_status[\"complete\"]\n    }"
if old_status_ep in c:
    c = c.replace(old_status_ep, new_status_ep)
    print("FIX 3 OK - /poll endpoint added")
else:
    print("FIX 3 FAILED")

# Step 4: Clear output_log when new pipeline starts
old_start = '    pipeline_status = {"running": True, "complete": False}'
new_start = '    pipeline_status = {"running": True, "complete": False}\n    output_log.clear()  # clear previous run'
if old_start in c:
    c = c.replace(old_start, new_start)
    print("FIX 4 OK - output_log cleared on new run")
else:
    print("FIX 4 FAILED")

# Step 5: Replace SSE JS with polling JS
old_connect = "  function connect() {\n    es = new EventSource('/stream');\n    es.onmessage = onEvent;\n    es.onerror   = () => setTimeout(connect, 2000);\n  }"
new_connect = """  var pollIndex = 0;
  var pollTimer = null;
  var isPolling = false;

  function startPolling() {
    pollIndex = 0;
    isPolling = true;
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(doPoll, 1500);
    doPoll();
  }

  function stopPolling() {
    isPolling = false;
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  }

  function doPoll() {
    fetch("/poll?since=" + pollIndex)
      .then(function(r){ return r.json(); })
      .then(function(data){
        var events = data.events || [];
        for (var i = 0; i < events.length; i++) {
          onEvent({data: JSON.stringify(events[i])});
        }
        pollIndex = data.total || pollIndex;
        if (!data.running && data.complete) {
          stopPolling();
        }
      })
      .catch(function(err){ console.log("Poll error:", err); });
  }

  function connect() {
    // Use polling instead of SSE - more reliable on Cloud Run
    console.log("Using polling mode");
  }"""

if old_connect in c:
    c = c.replace(old_connect, new_connect)
    print("FIX 5 OK - Polling JS added!")
else:
    print("FIX 5 FAILED - connect() pattern not found")

# Step 6: Start polling when run() is called
old_run_fetch = '    fetch("/run/" + mode, {method:"POST"})'
new_run_fetch = '    startPolling();\n    fetch("/run/" + mode, {method:"POST"})'
if old_run_fetch in c:
    c = c.replace(old_run_fetch, new_run_fetch, 1)
    print("FIX 6 OK - startPolling() called on run!")
else:
    print("FIX 6 FAILED")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("ALL DONE!")
