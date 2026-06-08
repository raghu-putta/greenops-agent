import re

with open("app.py", "r", encoding="utf-8", errors="replace") as f:
    c = f.read()

# Add agent text maps
maps = """
    var agentRunText = {"cs":"Scouting...","ga":"Analyzing...","oe":"Executing...","rg":"Generating..."};
    var agentDoneText = {"cs":"Scouted!","ga":"Analyzed!","oe":"Executed!","rg":"Generated!"};
"""
if "agentRunText" not in c:
    c = c.replace("function setAgentRunning", maps + "\n    function setAgentRunning")
    print("Maps added!")

# Fix running text
c = re.sub(r"s\.className = 'acard-status running'; s\.textContent = '[^']*'",
           "s.className = 'acard-status running'; s.textContent = agentRunText[id] || 'Running...'", c)
print("Running text fixed!")

# Fix done text  
c = re.sub(r"s\.className = 'acard-status done'; s\.textContent = '[^']*'",
           "s.className = 'acard-status done'; s.textContent = agentDoneText[id] || 'Done!'", c)
print("Done text fixed!")

# Fix status in HTML
c = c.replace('id="cs-status">Waiting</div>', 'id="cs-status">Scouting</div>')
c = c.replace('id="ga-status">Waiting</div>', 'id="ga-status">Analyzing</div>')
c = c.replace('id="oe-status">Waiting</div>', 'id="oe-status">Executing</div>')
c = c.replace('id="rg-status">Waiting</div>', 'id="rg-status">Generating</div>')
print("Status labels fixed!")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("DONE!")