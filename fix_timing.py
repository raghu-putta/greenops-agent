with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# Add 2 second delay before pipeline starts so browser SSE connects first
old = "    asyncio.create_task(run_pipeline(mode))\n    return {\"status\": \"started\", \"mode\": mode}"
new = """    async def delayed_start():
        await asyncio.sleep(2)
        await run_pipeline(mode)
    asyncio.create_task(delayed_start())
    return {"status": "started", "mode": mode}"""

if old in c:
    c = c.replace(old, new)
    print("OK - Delay added!")
else:
    print("FAILED - pattern not found")
    # Show what's there
    idx = c.find("asyncio.create_task")
    print(repr(c[idx-50:idx+100]))

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("DONE!")