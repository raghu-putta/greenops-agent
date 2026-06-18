import re
with open("app.py", "r", encoding="utf-8") as fh:
    c = fh.read()

NEW_WELCOME = '<div class="welcome" style="padding:16px;background:#0d1117;border-radius:8px;min-height:220px;">\n  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;">\n    <div>\n      <div style="color:#34d399;font-size:1rem;font-weight:700;letter-spacing:0.5px;">🌱 GreenOps AI Dashboard</div>\n      <div style="color:#6e7681;font-size:0.72rem;margin-top:2px;">Real-time GCP Cost &amp; Carbon Optimization</div>\n    </div>\n    <div style="background:#161b22;border:1px solid #21262d;border-radius:8px;padding:6px 12px;font-size:0.72rem;color:#34d399;font-weight:600;">● LIVE</div>\n  </div>\n  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px;">\n    <div style="background:#161b22;border:1px solid #21262d;border-radius:10px;padding:14px;position:relative;overflow:hidden;">\n      <div style="position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,#34d399,#10b981);"></div>\n      <div style="color:#6e7681;font-size:0.7rem;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px;">💰 Monthly Savings</div>\n      <div id="m-savings" style="color:#34d399;font-size:1.6rem;font-weight:700;font-family:monospace;">$0</div>\n      <div style="color:#484f58;font-size:0.68rem;margin-top:4px;">from idle resource removal</div>\n      <div style="margin-top:8px;height:3px;background:#21262d;border-radius:2px;"><div id="bar-savings" style="height:3px;background:#34d399;border-radius:2px;width:0%;transition:width 1.5s ease;"></div></div>\n    </div>\n    <div style="background:#161b22;border:1px solid #21262d;border-radius:10px;padding:14px;position:relative;overflow:hidden;">\n      <div style="position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,#60a5fa,#3b82f6);"></div>\n      <div style="color:#6e7681;font-size:0.7rem;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px;">🌍 CO₂ Saved/Month</div>\n      <div id="m-co2" style="color:#60a5fa;font-size:1.6rem;font-weight:700;font-family:monospace;">0 kg</div>\n      <div style="color:#484f58;font-size:0.68rem;margin-top:4px;">carbon footprint reduced</div>\n      <div style="margin-top:8px;height:3px;background:#21262d;border-radius:2px;"><div id="bar-co2" style="height:3px;background:#60a5fa;border-radius:2px;width:0%;transition:width 1.5s ease;"></div></div>\n    </div>\n    <div style="background:#161b22;border:1px solid #21262d;border-radius:10px;padding:14px;position:relative;overflow:hidden;">\n      <div style="position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,#f97316,#ea580c);"></div>\n      <div style="color:#6e7681;font-size:0.7rem;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px;">🖥️ Idle VMs Found</div>\n      <div id="m-vms" style="color:#f97316;font-size:1.6rem;font-weight:700;font-family:monospace;">0</div>\n      <div style="color:#484f58;font-size:0.68rem;margin-top:4px;">waiting to be optimized</div>\n      <div style="margin-top:8px;height:3px;background:#21262d;border-radius:2px;"><div id="bar-vms" style="height:3px;background:#f97316;border-radius:2px;width:0%;transition:width 1.5s ease;"></div></div>\n    </div>\n    <div style="background:#161b22;border:1px solid #21262d;border-radius:10px;padding:14px;position:relative;overflow:hidden;">\n      <div style="position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,#a78bfa,#7c3aed);"></div>\n      <div style="color:#6e7681;font-size:0.7rem;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px;">⚡ LOW Risk Actions</div>\n      <div id="m-actions" style="color:#a78bfa;font-size:1.6rem;font-weight:700;font-family:monospace;">0</div>\n      <div style="color:#484f58;font-size:0.68rem;margin-top:4px;">safe to auto-execute</div>\n      <div style="margin-top:8px;height:3px;background:#21262d;border-radius:2px;"><div id="bar-actions" style="height:3px;background:#a78bfa;border-radius:2px;width:0%;transition:width 1.5s ease;"></div></div>\n    </div>\n  </div>\n  <div style="text-align:center;color:#484f58;font-size:0.7rem;padding-top:8px;border-top:1px solid #161b22;">\n    Click <strong style="color:#34d399;">Run Demo</strong> to scan a simulated GCP project or <strong style="color:#58a6ff;">Run Real GCP</strong> to scan your actual cloud &nbsp;·&nbsp; Powered by Google ADK + Gemini 2.5 Pro ✨\n  </div>\n</div>'

# Replace welcome div by finding it positionally
idx = c.find('class="welcome"')
if idx < 0:
    print("Welcome div not found!")
else:
    depth = 0
    pos = idx
    while pos < len(c):
        if c[pos:pos+4] == "<div": depth += 1
        if c[pos:pos+6] == "</div>":
            depth -= 1
            if depth == 0:
                end = pos + 6
                break
        pos += 1
    # Find opening < of the welcome div
    start = c.rfind("<div", 0, idx+5)
    c = c[:start] + NEW_WELCOME + c[end:]
    print("Welcome screen replaced!")

# Remove bgcanvas animation JS
pat = re.compile(r"\s*//[\s\S]{0,50}Cinematic[\s\S]*?\}\)\(\);", re.DOTALL)
if pat.search(c):
    c = pat.sub("", c, count=1)
    print("Canvas JS removed")
else:
    pat2 = re.compile(r"\(function\(\)\{\s*const c=document\.getElementById\('bgcanvas'\)[\s\S]*?\}\)\(\);", re.DOTALL)
    if pat2.search(c):
        c = pat2.sub("", c, count=1)
        print("Canvas JS removed alt")
    else:
        print("Canvas JS not found - OK if already removed")

print("metric-card in file:", "m-savings" in c)
print("bgcanvas refs:", c.count("bgcanvas"))

with open("app.py", "w", encoding="utf-8") as fh:
    fh.write(c)
print("ALL DONE!")