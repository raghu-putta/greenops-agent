import asyncio
import os
from datetime import datetime
from dotenv import load_dotenv
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai.types import Content, Part

load_dotenv()
# Set the API key for Google Generative AI (AI Studio free tier)
api_key = os.getenv("GOOGLE_API_KEY")
if api_key:
    os.environ["GOOGLE_API_KEY"] = api_key


async def run_greenops():
    from agents.greenops_pipeline import greenops_pipeline

    PROJECT_ID = os.getenv("GCP_PROJECT_ID", "budget-finance-mcp")

    session_service = InMemorySessionService()

    runner = Runner(
        agent=greenops_pipeline,
        app_name="greenops_agent",
        session_service=session_service
    )

    session = await session_service.create_session(
        app_name="greenops_agent",
        user_id="greenops_user"
    )

    print("\n" + "=" * 60)
    print("  GreenOps Agentic AI — Starting Pipeline")
    print(f"  Project: {PROJECT_ID}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")

    user_message = Content(
        role="user",
        parts=[Part(text=f"Run full GreenOps analysis and optimization for GCP project {PROJECT_ID}")]
    )

    print("Running pipeline... (this may take 1-2 minutes)\n")

    async for event in runner.run_async(
        user_id="greenops_user",
        session_id=session.id,
        new_message=user_message
    ):
        if hasattr(event, 'author') and hasattr(event, 'content') and event.content:
            if event.content.parts:
                text = event.content.parts[0].text
                if text and text.strip():
                    print(f"\n[{event.author.upper()}]")
                    print("-" * 40)
                    print(text[:2000])
                    if len(text) > 2000:
                        print("... [truncated, see full output above]")

        if hasattr(event, 'is_final_response') and event.is_final_response():
            if event.content and event.content.parts:
                final_text = event.content.parts[0].text
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"output/greenops_report_{timestamp}.md"
                os.makedirs("output", exist_ok=True)
                with open(output_file, "w") as f:
                    f.write(final_text)
                print(f"\n✅ Report saved to: {output_file}")

    print("\n" + "=" * 60)
    print("  GreenOps Pipeline Complete")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_greenops())
