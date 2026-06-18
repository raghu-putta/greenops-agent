import re
with open("app.py", "r", encoding="utf-8") as fh:
    c = fh.read()
blocks = re.findall(r"<script>(.*?)</script>", c, re.DOTALL)
print("Blocks found:", len(blocks))
c = re.sub(r"<script>.*?</script>", "", c, flags=re.DOTALL)
merged = "\n".join(blocks)
c = c.replace("</body>", "<script>\n" + merged + "\n</script>\n</body>", 1)
print("function run:", "function run(mode)" in c)
print("openPanel:", "function openPanel" in c)
print("showReportDl:", "function showReportDl" in c)
print("hook:", "showReportDl(); setStatus" in c)
print("creds:", "JSON.stringify(_cfg)" in c)
with open("app.py", "w", encoding="utf-8") as fh:
    fh.write(c)
print("ALL DONE!")
