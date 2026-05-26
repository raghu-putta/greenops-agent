"""
Test Vertex AI connectivity before running the full pipeline.
Requires Application Default Credentials (gcloud auth application-default login).
"""
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()

print("=== Vertex AI Connection Test ===")
print(f"  Project : {os.getenv('GOOGLE_CLOUD_PROJECT')}")
print(f"  Location: {os.getenv('GOOGLE_CLOUD_LOCATION')}")
print(f"  VertexAI: {os.getenv('GOOGLE_GENAI_USE_VERTEXAI')}")
print()

from google import genai

async def test():
    client = genai.Client()

    print("Calling gemini-2.0-flash via Vertex AI...")
    try:
        response = await client.aio.models.generate_content(
            model="gemini-2.0-flash-001",
            contents="Say hello in one word."
        )
        print(f"✅ SUCCESS — Response: {response.text.strip()}")
    except Exception as e:
        print(f"❌ FAILED — {type(e).__name__}: {e}")

asyncio.run(test())
