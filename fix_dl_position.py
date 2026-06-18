import re
with open("app.py", "r", encoding="utf-8") as fh:
    c = fh.read()

# Remove report-dl from wrong location
old_dl = '\n<div id="report-dl" style="display:none;padding:8px 16px;background:#161b22;border-top:1px solid #21262d;gap:8px;flex-wrap:wrap;align-items:center;"><span style="color:#34d399;font-size:0.78rem;font-weight:600;">Download Report:</span><button onclick="rdlPDF()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;border:none;background:linear-gradient(135deg,#ef4444,#dc2626);color:#fff;">PDF</button><button onclick="rdlTXT()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#1a3a2a;color:#34d399;border:1px solid #34d399;">TXT</button><button onclick="rdlHTML()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#1a1f2e;color:#60a5fa;border:1px solid #60a5fa;">HTML</button><button onclick="rdlCSV()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#2a1a2e;color:#a78bfa;border:1px solid #a78bfa;">CSV</button><button onclick="rdlJSON()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#2a2a1a;color:#f97316;border:1px solid #f97316;">JSON</button><button id="rdl-copy" onclick="rdlCopy()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#1a2a1a;color:#34d399;border:1px solid #34d399;">Copy</button></div>'

new_dl = '<div id="report-dl" style="display:none;padding:8px 16px;background:#161b22;border-top:1px solid #21262d;gap:8px;flex-wrap:wrap;align-items:center;"><span style="color:#34d399;font-size:0.78rem;font-weight:600;">Download Report:</span><button onclick="rdlPDF()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;border:none;background:linear-gradient(135deg,#ef4444,#dc2626);color:#fff;">PDF</button><button onclick="rdlTXT()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#1a3a2a;color:#34d399;border:1px solid #34d399;">TXT</button><button onclick="rdlHTML()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#1a1f2e;color:#60a5fa;border:1px solid #60a5fa;">HTML</button><button onclick="rdlCSV()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#2a1a2e;color:#a78bfa;border:1px solid #a78bfa;">CSV</button><button onclick="rdlJSON()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#2a2a1a;color:#f97316;border:1px solid #f97316;">JSON</button><button id="rdl-copy" onclick="rdlCopy()" style="padding:6px 12px;border-radius:6px;cursor:pointer;font-size:0.75rem;font-weight:600;background:#1a2a1a;color:#34d399;border:1px solid #34d399;">Copy</button></div>'

if old_dl in c:
    c = c.replace(old_dl, '', 1)
    print("Old download bar removed from wrong location")
elif new_dl in c:
    c = c.replace(new_dl, '', 1)
    print("Download bar removed from wrong location (no leading newline)")
else:
    print("WARNING: could not find old dl bar - searching...")
    idx = c.find('id="report-dl"')
    if idx >= 0:
        start = c.rfind('\n', 0, idx)
        end = c.find('</div>', idx) + 6
        c = c[:start] + c[end:]
        print("Removed by position search")

# Add download bar AFTER the terminal div closing tag
terminal_close = '</div>\n      <div id="status-bar"'
if terminal_close in c:
    c = c.replace(terminal_close, '</div>\n' + new_dl + '\n      <div id="status-bar"', 1)
    print("Download bar placed correctly after terminal")
else:
    # fallback - find terminal div and add after it
    idx = c.find('class="terminal"')
    if idx >= 0:
        end = c.find('</div>', idx) + 6
        c = c[:end] + '\n' + new_dl + c[end:]
        print("Download bar placed after terminal (fallback)")
    else:
        print("FAILED - terminal div not found")

print("report-dl in file:", 'id="report-dl"' in c)

with open("app.py", "w", encoding="utf-8") as fh:
    fh.write(c)
print("ALL DONE!")
