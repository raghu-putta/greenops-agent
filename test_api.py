"""
Quick diagnostic: lists all Gemini models available for your API key
and tests a simple call to confirm which ones work.
"""
import os
import urllib.request
import urllib.error
import json
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")

if not api_key:
    print("ERROR: GOOGLE_API_KEY not found in .env")
    exit(1)

print(f"API key loaded: {api_key[:12]}...")

# ── List models ──────────────────────────────────────────
print("\n=== Models available for generateContent ===")
url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
try:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())
        models = data.get("models", [])
        count = 0
        for m in models:
            if "generateContent" in m.get("supportedGenerationMethods", []):
                print(f"  ✓  {m['name']}")
                count += 1
        print(f"\nTotal models with generateContent support: {count}")
except urllib.error.HTTPError as e:
    body = e.read().decode()
    print(f"HTTP {e.code} error: {body[:400]}")
except Exception as e:
    print(f"Error: {e}")

# ── Quick smoke test — try multiple models ────────────────
def smoke_test(model_name):
    print(f"\n=== Smoke test: {model_name} ===")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
    payload = json.dumps({"contents": [{"parts": [{"text": "Say hello in one word."}]}]}).encode()
    try:
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode())
            text = result["candidates"][0]["content"]["parts"][0]["text"]
            print(f"  ✓  Response: {text.strip()}")
            return True
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  ✗  HTTP {e.code}: {body[:300]}")
        return False
    except Exception as e:
        print(f"  ✗  Error: {e}")
        return False

for model in ["gemini-2.5-flash"]:
    if smoke_test(model):
        print(f"\n✅ Use this model in pipeline: {model}")
        break
