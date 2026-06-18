import re, base64

with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# Fix equal margins
c = re.sub(r'\s*\.acard-icon\{[^}]*\}', '', c)
c = re.sub(r'\s*\.acard-icon img\{[^}]*\}', '', c)
c = re.sub(r'\s*\.acard-icon img:hover\{[^}]*\}', '', c)

new_css = """
  .acard-icon{width:56px;height:56px;overflow:hidden;border-radius:50% !important;border:2px solid #34d399;box-shadow:0 0 12px rgba(52,211,153,0.5);flex-shrink:0;margin:4px 0;}
  .acard-icon img{width:56px;height:56px;object-fit:cover;object-position:center 15%;border-radius:50% !important;transition:transform 0.3s ease;}
  .acard-icon img:hover{transform:scale(1.08);}
"""
c = c.replace("</style>", new_css + "</style>", 1)

# Add favicon
try:
    with open("static/greenops-icon-512.png", "rb") as f:
        fav = base64.b64encode(f.read()).decode("utf-8")
    fav_tag = '<link rel="icon" type="image/png" href="data:image/png;base64,' + fav + '"/>'
    if 'rel="icon"' not in c:
        c = c.replace("<head>", "<head>\n  " + fav_tag, 1)
        print("Favicon added!")
    else:
        print("Favicon already exists")
except:
    print("Icon file not found - skipping favicon")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("DONE!")