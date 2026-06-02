"""
Test Gemini API connectivity before running the full pipeline.
Requires GOOGLE_API_KEY set in .env file.
"""
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()

print("=== Gemini API Connection Test ===")
print(f"  API Key : {os.getenv('GOOGLE_API_KEY', 'NOT SET')[:12]}...")
print(f"  VertexAI: {os.getenv('GOOGLE_GENAI_USE_VERTEXAI', '0')}")
print()

from google import genai

async def test():
    client = genai.Client()

    print("Testing gemini-2.5-flash via Gemini API...")
    try:
        response = await client.aio.models.generate_content(
            model="gemini-2.5-flash",
            contents="Say hello in one word."
        )
        print(f"✅ SUCCESS — Response: {response.text.strip()}")
    except Exception as e:
        print(f"❌ FAILED — {type(e).__name__}: {e}")

asyncio.run(test())
