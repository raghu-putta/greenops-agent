with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# Add debug console.log to onEvent function
old = "  function onEvent(e) {\n    const d = JSON.parse(e.data);"
new = "  function onEvent(e) {\n    console.log('SSE EVENT:', e.data);\n    const d = JSON.parse(e.data);"
c = c.replace(old, new)
print("Debug added:", "OK" if "console.log('SSE EVENT'" in c else "FAILED - trying alternate")

# Try alternate
if "FAILED" in "OK":
    old2 = "function onEvent(e) {"
    new2 = "function onEvent(e) {\n    console.log('SSE:', e.data);"
    c = c.replace(old2, new2)

# Also make terminal visible with border for debugging
old3 = '<div class="terminal" id="terminal">'
new3 = '<div class="terminal" id="terminal" style="min-height:200px;border:1px solid #34d399;">'
c = c.replace(old3, new3)
print("Terminal border added")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("DONE!")