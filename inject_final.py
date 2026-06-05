import base64, re

files = [
    ("static/carbon_scout_robot.jpg", "jpeg"),
    ("static/robot_analyzer.jpg", "jpeg"),
    ("static/finance_robot.jpg", "jpeg"),
    ("static/report_generator_robot.jpg", "jpeg"),
]

imgs = []
for path, ext in files:
    with open(path, "rb") as f:
        data = base64.b64encode(f.read()).decode("utf-8")
    imgs.append("data:image/" + ext + ";base64," + data)
    print("Loaded: " + path)

with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

new_css = ".acard-icon{width:56px;height:56px;overflow:hidden;border-radius:50%;margin:0 auto 6px;border:2px solid #34d399;box-shadow:0 0 10px rgba(52,211,153,0.4);flex-shrink:0;} .acard-icon img{width:56px;height:56px;object-fit:cover;object-position:center 10%;border-radius:50%;transition:transform 0.3s ease;} .acard-icon img:hover{transform:scale(1.08);}"
old_css1 = ".acard-icon{width:100%;height:120px;overflow:hidden;border-radius:8px 8px 0 0;margin-bottom:8px;} .acard-icon img{width:100%;height:120px;object-fit:cover;object-position:center top;border-radius:8px;transition:transform 0.3s ease;} .acard-icon img:hover{transform:scale(1.05);}"
old_css2 = ".acard-icon{width:100%;height:90px;overflow:hidden;border-radius:10px 10px 0 0;margin-bottom:0;border-bottom:2px solid #34d399;} .acard-icon img{width:100%;height:90px;object-fit:cover;object-position:center 20%;transition:transform 0.4s ease;filter:brightness(0.95) contrast(1.05);} .acard-icon img:hover{transform:scale(1.08);filter:brightness(1.1) contrast(1.1);}"
old_css3 = ".acard-icon{width:64px;height:64px;overflow:hidden;border-radius:50%;margin:0 auto 8px;border:2px solid #34d399;box-shadow:0 0 12px rgba(52,211,153,0.4);} .acard-icon img{width:64px;height:64px;object-fit:cover;object-position:center top;border-radius:50%;transition:transform 0.3s ease,box-shadow 0.3s ease;} .acard-icon img:hover{transform:scale(1.1);box-shadow:0 0 20px rgba(52,211,153,0.7);}"

for old in [old_css1, old_css2, old_css3]:
    if old in content:
        content = content.replace(old, new_css)
        print("CSS replaced!")
        break

pattern1 = r'<div class="acard-icon"><img src="[^"]*" alt="[^"]*"/></div>'
pattern2 = r'<div class="acard-icon">[^<]*</div>'
matches = re.findall(pattern1, content)
if len(matches) < 4:
    matches = re.findall(pattern2, content)
print("Found " + str(len(matches)) + " icons")

alts = ["Carbon Scout", "GreenOps Analyzer", "Optimization Executor", "Report Generator"]
for i in range(min(4, len(matches))):
    new = '<div class="acard-icon"><img src="' + imgs[i] + '" alt="' + alts[i] + '"/></div>'
    content = content.replace(matches[i], new, 1)
    print("Replaced agent " + str(i+1))

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)
print("DONE!")