with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# Fix 1 - resetCards uses undefined 'id' variable
old1 = "s.textContent = agentIdleText[id] || 'Idle';"
new1 = "s.textContent = agentIdleText[a.statusId.replace('-status','')] || 'Scouting';"
c = c.replace(old1, new1)
print("Fix 1:", "OK" if old1 not in c else "FAILED")

# Fix 2 - markActive uses undefined 'id' variable  
old2 = "s.textContent = agentRunText[id] || 'Running...';"
new2 = "s.textContent = agentRunText[a.statusId.replace('-status','')] || 'Running...';"
c = c.replace(old2, new2)
print("Fix 2:", "OK" if old2 not in c else "FAILED")

# Fix 3 - markDone uses undefined 'id' variable
old3 = "s.textContent = agentDoneText[id] || 'Done!';"
new3 = "s.textContent = agentDoneText[a.statusId.replace('-status','')] || 'Done!';"
c = c.replace(old3, new3)
print("Fix 3:", "OK" if old3 not in c else "FAILED")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("DONE!")