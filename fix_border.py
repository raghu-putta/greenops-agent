with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# Remove debug green border from terminal
old = 'id="terminal" style="min-height:200px;border:1px solid #34d399;"'
new = 'id="terminal"'
c = c.replace(old, new)
print("Border removed:", "OK" if old not in c else "FAILED")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("DONE!")