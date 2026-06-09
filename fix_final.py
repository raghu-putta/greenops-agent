with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# Fix 1 - Remove the skip that blocks all agent output
old1 = "      const info = AGENTS[key];\n      if (!info) return; // skip orchestrator"
new1 = "      const info = AGENTS[key];\n      // show all agent output"
c = c.replace(old1, new1)
print("Fix 1:", "OK" if "show all agent output" in c else "FAILED")

# Fix 2 - Fix markActive to handle unknown keys safely
old2 = "        markActive(key);"
new2 = "        if (info) markActive(key);"
c = c.replace(old2, new2)
print("Fix 2:", "OK" if "if (info) markActive" in c else "FAILED")

# Fix 3 - Fix markDone to handle unknown keys safely
old3 = "      if (activeAgent && activeAgent !== key) markDone(activeAgent);"
new3 = "      if (activeAgent && activeAgent !== key && AGENTS[activeAgent]) markDone(activeAgent);"
c = c.replace(old3, new3)
print("Fix 3:", "OK" if "AGENTS[activeAgent]" in c else "FAILED")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("DONE!")