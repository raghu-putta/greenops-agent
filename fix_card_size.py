with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

# Find and show current acard CSS
import re
matches = re.findall(r'\.acard\{[^}]*\}', content)
for m in matches:
    print("Found:", m[:100])