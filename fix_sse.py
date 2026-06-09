with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# Find the generator function inside stream endpoint and add heartbeat
old = '''    async def generator():
        q: asyncio.Queue = asyncio.Queue()
        clients.append(q)
        try:
            while True:
                msg = await q.get()
                yield f"data: {msg}\\n\\n"
        finally:
            clients.remove(q)'''

new = '''    async def generator():
        q: asyncio.Queue = asyncio.Queue()
        clients.append(q)
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {msg}\\n\\n"
                except asyncio.TimeoutError:
                    yield f": heartbeat\\n\\n"
        finally:
            clients.remove(q)'''

if old in c:
    c = c.replace(old, new)
    print("OK - Heartbeat added to generator!")
else:
    print("Pattern not found - searching for generator...")
    # Find it
    lines = c.split('\n')
    for i, line in enumerate(lines, 1):
        if 'async def generator' in line or 'clients.append' in line or 'await q.get' in line:
            print(f"Line {i}: {line.strip()}")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("DONE!")