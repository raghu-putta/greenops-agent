import re

with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# Fix 1 - Remove old download bar and add new one after terminal
c = re.sub(r'\n<div class="report-dl"[^>]*>.*?</div>', '', c, flags=re.DOTALL)

dl_bar = '\n<div id="report-dl" style="display:none;padding:8px 16px;background:#161b22;border-top:1px solid #21262d;gap:8px;flex-wrap:wrap;align-items:center;"><span style="color:#34d399;font-size:0.78rem;font-weight:600;">Download Report:</span><button onclick="rdlPDF()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;border:none;background:linear-gradient(135deg,#ef4444,#dc2626);color:#fff;">PDF</button><button onclick="rdlTXT()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#1a3a2a;color:#34d399;border:1px solid #34d399;">TXT</button><button onclick="rdlHTML()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#1a1f2e;color:#60a5fa;border:1px solid #60a5fa;">HTML</button><button onclick="rdlCSV()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#2a1a2e;color:#a78bfa;border:1px solid #a78bfa;">CSV</button><button onclick="rdlJSON()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#2a2a1a;color:#f97316;border:1px solid #f97316;">JSON</button><button id="rdl-copy" onclick="rdlCopy()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#1a2a1a;color:#34d399;border:1px solid #34d399;">Copy</button></div>'

idx = c.find('class="terminal"')
if idx >= 0:
    end = c.find('</div>', idx) + 6
    c = c[:end] + dl_bar + c[end:]
    print("Download bar added after terminal")

# Fix 2 - Fix showReportDl to use style.display directly
c = c.replace(
    'function showReportDl(){var b=document.getElementById("report-dl");if(b)b.classList.add("show");}',
    'function showReportDl(){var b=document.getElementById("report-dl");if(b)b.style.display="flex";}'
)
print("showReportDl fixed")

# Fix 3 - Check openPanel exists
if "function openPanel" in c:
    print("openPanel EXISTS - OK")
else:
    print("openPanel MISSING - adding")
    extra = 'function openPanel(){var p=document.getElementById("gcp-panel");if(p)p.style.display="flex";loadPanelCfg();setTimeout(function(){var el=document.getElementById("bot-msg");if(el&&typeof typeText==="function")typeText("Welcome! The cloud awaits your command!",el,null);},300);}function closePanel(){var p=document.getElementById("gcp-panel");if(p)p.style.display="none";}function loadPanelCfg(){try{var s=JSON.parse(sessionStorage.getItem("gops-cfg")||"{}");if(s.apiKey)document.getElementById("cfg-api-key").value=s.apiKey;if(s.projectId)document.getElementById("cfg-project-id").value=s.projectId;if(s.region)document.getElementById("cfg-region").value=s.region;if(s.zone)document.getElementById("cfg-zone").value=s.zone;}catch(e){}}'
    c = c.replace("</body>", "<script>" + extra + "</script>\n</body>", 1)

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("DONE!")