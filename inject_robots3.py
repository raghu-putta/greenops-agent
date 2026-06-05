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

css = ".acard-icon{width:100%;height:120px;overflow:hidden;border-radius:8px 8px 0 0;margin-bottom:8px;} .acard-icon img{width:100%;height:120px;object-fit:cover;object-position:center top;border-radius:8px;transition:transform 0.3s ease;} .acard-icon img:hover{transform:scale(1.05);}"
content = content.replace("</style>", css + "\n  </style>", 1)
print("CSS injected!")

pattern = r'<div class="acard-icon">[^<]*</div>'
matches = re.findall(pattern, content)
print("Found " + str(len(matches)) + " icon divs")

alts = ["Carbon Scout", "GreenOps Analyzer", "Optimization Executor", "Report Generator"]
for i in range(min(4, len(matches))):
    new = '<div class="acard-icon"><img src="' + imgs[i] + '" alt="' + alts[i] + '"/></div>'
    content = content.replace(matches[i], new, 1)
    print("Replaced agent " + str(i+1) + " icon!")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)
print("DONE - app.py saved!")