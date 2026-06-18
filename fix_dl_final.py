import re

with open("app.py", "r", encoding="utf-8") as fh:
    c = fh.read()

# Remove download bar wherever it currently is
old = '<div id="report-dl"'
if old in c:
    start = c.find(old)
    end = c.find('</div>', start) + 6
    c = c[:start] + c[end:]
    print("Old download bar removed")

# The download bar HTML  
dl_html = """<div id="report-dl" style="display:none;padding:10px 16px;background:#161b22;border-top:2px solid #34d399;gap:8px;flex-wrap:wrap;align-items:center;margin-top:0;">
  <span style="color:#34d399;font-size:0.8rem;font-weight:700;">&#11015; Download Report:</span>
  <button onclick="rdlPDF()" style="padding:7px 14px;border-radius:6px;cursor:pointer;font-size:0.78rem;font-weight:600;border:none;background:linear-gradient(135deg,#ef4444,#dc2626);color:#fff;">PDF</button>
  <button onclick="rdlTXT()" style="padding:7px 14px;border-radius:6px;cursor:pointer;font-size:0.78rem;font-weight:600;background:#1a3a2a;color:#34d399;border:1px solid #34d399;">TXT</button>
  <button onclick="rdlHTML()" style="padding:7px 14px;border-radius:6px;cursor:pointer;font-size:0.78rem;font-weight:600;background:#1a1f2e;color:#60a5fa;border:1px solid #60a5fa;">HTML</button>
  <button onclick="rdlCSV()" style="padding:7px 14px;border-radius:6px;cursor:pointer;font-size:0.78rem;font-weight:600;background:#2a1a2e;color:#a78bfa;border:1px solid #a78bfa;">CSV</button>
  <button onclick="rdlJSON()" style="padding:7px 14px;border-radius:6px;cursor:pointer;font-size:0.78rem;font-weight:600;background:#2a2a1a;color:#f97316;border:1px solid #f97316;">JSON</button>
  <button id="rdl-copy" onclick="rdlCopy()" style="padding:7px 14px;border-radius:6px;cursor:pointer;font-size:0.78rem;font-weight:600;background:#1a2a1a;color:#34d399;border:1px solid #34d399;">Copy</button>
</div>"""

# Place it just before the footer div
footer_marker = '<div style="text-align:center;padding:14px'
if footer_marker in c:
    c = c.replace(footer_marker, dl_html + "\n" + footer_marker, 1)
    print("Download bar placed before footer - CORRECT position")
else:
    # fallback - before </body>
    c = c.replace("</body>", dl_html + "\n</body>", 1)
    print("Download bar placed before </body> - fallback")

print("report-dl in file:", 'id="report-dl"' in c)

with open("app.py", "w", encoding="utf-8") as fh:
    fh.write(c)
print("ALL DONE!")
