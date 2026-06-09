with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

old = """    async def generator():
        try:
            while True:
                msg = await q.get()
                yield f\"data: {msg}\\n\\n\"
        except asyncio.CancelledError:
            pass
        finally:
            if q in _sse_queues:
                _sse_queues.remove(q)"""

new = """    async def generator():
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=20.0)
                    yield f\"data: {msg}\\n\\n\"
                except asyncio.TimeoutError:
                    yield \": heartbeat\\n\\n\"
        except asyncio.CancelledError:
            pass
        finally:
            if q in _sse_queues:
                _sse_queues.remove(q)"""

if old in c:
    c = c.replace(old, new)
    print("OK - Heartbeat added!")
else:
    print("FAILED - pattern not found!")
    # Show what's there
    idx = c.find("async def generator")
    if idx >= 0:
        print("Found generator at:", idx)
        print(repr(c[idx:idx+300]))

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("DONE!")