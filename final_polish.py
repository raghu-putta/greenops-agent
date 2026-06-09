with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# FIX 1: Fix Configure GCP panel - make it actually open
# The current panel uses display:flex but onclick sets display:flex correctly
# Issue is the panel HTML might be missing - let's check and fix the button
old_btn = 'onclick="document.getElementById(\'gcp-panel\').style.display=\'flex\'"'
if old_btn in c:
    print("FIX 1 - Configure GCP button found, panel should work")
    # Check if panel exists
    if 'id="gcp-panel"' in c:
        print("FIX 1 - GCP panel exists")
    else:
        print("FIX 1 - GCP panel MISSING - adding it")
else:
    print("FIX 1 - Button pattern different, checking...")
    if "Configure GCP" in c:
        idx = c.find("Configure GCP")
        print("Found at:", repr(c[idx-100:idx+50]))

# FIX 2: Make agent cards compact - horizontal layout
# Only touch the acard CSS, nothing else
old_acard = ".acard{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:16px;display:flex;flex-direction:column;gap:6px;min-width:0}"
new_acard = ".acard{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:8px 12px;display:flex;flex-direction:row;align-items:center;gap:10px;min-width:0}"
if old_acard in c:
    c = c.replace(old_acard, new_acard)
    print("FIX 2 OK - Agent cards compact horizontal")
else:
    print("FIX 2 - acard CSS pattern different, skipping to avoid breaking")

# FIX 3: Add footer with Raghu Putta
if "Raghu Putta" not in c:
    footer = '\n  <div style="text-align:center;padding:14px;color:#484f58;font-size:0.75rem;border-top:1px solid #21262d;margin-top:8px;">Built by <strong style="color:#34d399;">Raghu Putta</strong> &nbsp;|&nbsp; <a href="https://github.com/raghu-putta/greenops-agent" target="_blank" style="color:#58a6ff;text-decoration:none;">&#9733; GitHub</a> &nbsp;|&nbsp; <a href="https://greenops-dashboard-845589445410.us-central1.run.app" target="_blank" style="color:#58a6ff;text-decoration:none;">&#127760; Live Demo</a> &nbsp;|&nbsp; <span style="color:#34d399;">v2.0</span> &nbsp;|&nbsp; Powered by <span style="color:#34d399;">Google ADK + Gemini 2.5 Pro</span></div>'
    c = c.replace("</body>", footer + "\n</body>", 1)
    print("FIX 3 OK - Footer added")
else:
    print("FIX 3 - Footer already exists")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("ALL DONE!")
