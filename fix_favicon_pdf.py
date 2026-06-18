import re
with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

FAVICON = 'data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAxMDAgMTAwIj4KICA8cmVjdCB3aWR0aD0iMTAwIiBoZWlnaHQ9IjEwMCIgcng9IjIwIiBmaWxsPSIjMGQxMTE3Ii8+CiAgPCEtLSBMZWFmIHNoYXBlIC0tPgogIDxwYXRoIGQ9Ik01MCAxNSBDMjUgMTUgMTUgMzUgMTUgNTUgQzE1IDc1IDMwIDg4IDUwIDg4IEM3MCA4OCA4NSA3NSA4NSA1NSBDODUgMzUgNzUgMTUgNTAgMTVaIiBmaWxsPSIjMzRkMzk5IiBvcGFjaXR5PSIwLjIiLz4KICA8IS0tIFJvYm90IGZhY2UgLS0+CiAgPHJlY3QgeD0iMzAiIHk9IjMwIiB3aWR0aD0iNDAiIGhlaWdodD0iMzUiIHJ4PSI4IiBmaWxsPSIjMzRkMzk5Ii8+CiAgPCEtLSBFeWVzIC0tPgogIDxjaXJjbGUgY3g9IjQwIiBjeT0iNDMiIHI9IjYiIGZpbGw9IiMwZDExMTciLz4KICA8Y2lyY2xlIGN4PSI2MCIgY3k9IjQzIiByPSI2IiBmaWxsPSIjMGQxMTE3Ii8+CiAgPGNpcmNsZSBjeD0iNDEiIGN5PSI0MiIgcj0iMi41IiBmaWxsPSIjMzRkMzk5Ii8+CiAgPGNpcmNsZSBjeD0iNjEiIGN5PSI0MiIgcj0iMi41IiBmaWxsPSIjMzRkMzk5Ii8+CiAgPCEtLSBNb3V0aCAtLT4KICA8cmVjdCB4PSIzOCIgeT0iNTQiIHdpZHRoPSIyNCIgaGVpZ2h0PSI0IiByeD0iMiIgZmlsbD0iIzBkMTExNyIvPgogIDwhLS0gQW50ZW5uYSAtLT4KICA8cmVjdCB4PSI0NyIgeT0iMjAiIHdpZHRoPSI2IiBoZWlnaHQ9IjEwIiByeD0iMyIgZmlsbD0iIzM0ZDM5OSIvPgogIDxjaXJjbGUgY3g9IjUwIiBjeT0iMTgiIHI9IjQiIGZpbGw9IiMxMGI5ODEiLz4KICA8IS0tIENpcmN1aXQgbGluZXMgLS0+CiAgPGxpbmUgeDE9IjE1IiB5MT0iNTUiIHgyPSIyNSIgeTI9IjU1IiBzdHJva2U9IiMzNGQzOTkiIHN0cm9rZS13aWR0aD0iMiIvPgogIDxsaW5lIHgxPSI3NSIgeTE9IjU1IiB4Mj0iODUiIHkyPSI1NSIgc3Ryb2tlPSIjMzRkMzk5IiBzdHJva2Utd2lkdGg9IjIiLz4KICA8IS0tIENPMiB0ZXh0IC0tPgogIDx0ZXh0IHg9IjUwIiB5PSI4MiIgdGV4dC1hbmNob3I9Im1pZGRsZSIgZmlsbD0iIzM0ZDM5OSIgZm9udC1zaXplPSIxMCIgZm9udC13ZWlnaHQ9ImJvbGQiIGZvbnQtZmFtaWx5PSJtb25vc3BhY2UiPkdyZWVuT3BzPC90ZXh0Pgo8L3N2Zz4K'

# Replace or add favicon
fav_tag = '<link rel="icon" type="image/svg+xml" href="' + FAVICON + '"/>'
if 'rel="icon"' in c:
    c = re.sub(r'<link rel="icon"[^/]*/>', fav_tag, c)
    print("Favicon replaced!")
else:
    c = c.replace("<head>", "<head>\n  " + fav_tag, 1)
    print("Favicon added!")

pdf_css = '\n  .download-bar{display:none;padding:10px 16px;background:#0d1117;border-top:1px solid #21262d;gap:10px;flex-wrap:wrap;}\n  .download-bar.visible{display:flex;}\n  .dl-btn{padding:8px 16px;border-radius:8px;cursor:pointer;font-size:0.82rem;font-weight:600;border:none;transition:all 0.2s;display:flex;align-items:center;gap:6px;}\n  .dl-btn-pdf{background:linear-gradient(135deg,#34d399,#10b981);color:#0a1a0f;}\n  .dl-btn-txt{background:#1a3a2a;color:#34d399;border:1px solid #34d399;}\n  .dl-btn-copy{background:#1a1f2e;color:#60a5fa;border:1px solid #60a5fa;}\n  .dl-btn:hover{transform:translateY(-1px);box-shadow:0 4px 12px rgba(52,211,153,0.3);}'
if "download-bar" not in c:
    c = c.replace("</style>", pdf_css + "\n  </style>", 1)
    print("Download CSS added")

dl_html = '\n  <div class="download-bar" id="download-bar">\n    <span style="color:#34d399;font-size:0.82rem;font-weight:600;align-self:center;">&#9660; Download Report:</span>\n    <button class="dl-btn dl-btn-pdf" onclick="downloadPDF()">&#128196; PDF</button>\n    <button class="dl-btn dl-btn-txt" onclick="downloadTXT()">&#128196; Text File</button>\n    <button class="dl-btn dl-btn-copy" onclick="copyReport()">&#128203; Copy to Clipboard</button>\n  </div>'
if "download-bar" not in c:
    # Add after terminal
    c = c.replace('<div class="terminal"', dl_html + '\n  <div class="terminal"', 1)
    print("Download bar added")

dl_js = '\n  function getReportText() {\n    var t = document.getElementById("terminal");\n    return t ? (t.innerText || t.textContent) : "";\n  }\n\n  function downloadTXT() {\n    var text = getReportText();\n    var blob = new Blob([text], {type: "text/plain"});\n    var a = document.createElement("a");\n    a.href = URL.createObjectURL(blob);\n    a.download = "GreenOps_Report_" + new Date().toISOString().split("T")[0] + ".txt";\n    a.click();\n  }\n\n  function downloadPDF() {\n    var text = getReportText();\n    var win = window.open("", "_blank");\n    win.document.write("<html><head><title>GreenOps Report</title>");\n    win.document.write("<style>body{font-family:monospace;background:#0d1117;color:#c9d1d9;padding:40px;white-space:pre-wrap;line-height:1.6;}");\n    win.document.write("h1{color:#34d399;} .header{color:#34d399;font-size:1.2rem;margin-bottom:20px;}</style></head><body>");\n    win.document.write("<div class=header>&#127807; GreenOps AI Report - " + new Date().toLocaleDateString() + "</div>");\n    win.document.write("<pre>" + text.replace(/</g,"&lt;").replace(/>/g,"&gt;") + "</pre>");\n    win.document.write("</body></html>");\n    win.document.close();\n    win.print();\n  }\n\n  function copyReport() {\n    var text = getReportText();\n    navigator.clipboard.writeText(text).then(function() {\n      var btn = document.querySelector(".dl-btn-copy");\n      if (btn) { btn.textContent = "Copied!"; setTimeout(function(){ btn.innerHTML = "&#128203; Copy to Clipboard"; }, 2000); }\n    });\n  }\n\n  function showDownloadBar() {\n    var bar = document.getElementById("download-bar");\n    if (bar) bar.classList.add("visible");\n  }\n'
if "downloadPDF" not in c:
    c = c.replace("</body>", "<script>" + dl_js + "</script>\n</body>", 1)
    print("Download JS added")

# Show download bar when pipeline completes
old_done = "setStatus('done'"
if old_done in c and "showDownloadBar" not in c:
    c = c.replace("setStatus('done'", "showDownloadBar(); setStatus('done'", 1)
    print("Download bar hooked to done event")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("ALL DONE!")
