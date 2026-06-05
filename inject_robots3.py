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

old_css = ".acard-icon{width:100%;height:120px;overflow:hidden;border-radius:8px 8px 0 0;margin-bottom:8px;} .acard-icon img{width:100%;height:120px;object-fit:cover;object-position:center top;border-radius:8px;transition:transform 0.3s ease;} .acard-icon img:hover{transform:scale(1.05);}"
new_css = ".acard-icon{width:64px;height:64px;overflow:hidden;border-radius:50%;margin:0 auto 8px;border:2px solid #34d399;box-shadow:0 0 12px rgba(52,211,153,0.4);} .acard-icon img{width:64px;height:64px;object-fit:cover;object-position:center top;border-radius:50%;transition:transform 0.3s ease,box-shadow 0.3s ease;} .acard-icon img:hover{transform:scale(1.1);box-shadow:0 0 20px rgba(52,211,153,0.7);}"
content = content.replace(old_css, new_css)
print("CSS size fixed!")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)
print("DONE!")