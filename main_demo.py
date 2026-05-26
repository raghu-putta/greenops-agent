"""
GreenOps DEMO — Runs the full 4-agent pipeline with simulated GCP resources.
Shows the complete flow: scan → analyze → human approval → report.

Run: python main_demo.py
"""
import asyncio
import os
from datetime import datetime
from dotenv import load_dotenv
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai.types import Content, Part

load_dotenv()

api_key = os.getenv("GOOGLE_API_KEY")
if api_key:
    os.environ["GOOGLE_API_KEY"] = api_key


async def run_demo():
    from agents.greenops_pipeline_demo import greenops_pipeline_demo

    session_service = InMemorySessionService()

    runner = Runner(
        agent=greenops_pipeline_demo,
        app_name="greenops_demo",
        session_service=session_service
    )

    session = await session_service.create_session(
        app_name="greenops_demo",
        user_id="greenops_user"
    )

    print("\n" + "=" * 60)
    print("  🌱 GreenOps Agentic AI — DEMO MODE")
    print("  Simulated GCP project with idle resources")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")
    print("  Simulated resources:")
    print("  • 3 idle VMs (ml-training, staging-api, data-pipeline)")
    print("  • 2 unattached disks (500GB + 200GB)")
    print("  • 1 unused reserved IP")
    print("  • 1 rightsizing recommendation")
    print("\n" + "-" * 60 + "\n")

    user_message = Content(
        role="user",
        parts=[Part(text="Run full GreenOps analysis and optimization for GCP project greenops-demo-project")]
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
                    print(text[:3000])
                    if len(text) > 3000:
                        print("... [truncated]")

        if hasattr(event, 'is_final_response') and event.is_final_response():
            if event.content and event.content.parts:
                final_text = event.content.parts[0].text
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"output/greenops_DEMO_report_{timestamp}.md"
                os.makedirs("output", exist_ok=True)
                with open(output_file, "w") as f:
                    f.write(final_text)
                print(f"\n✅ Demo report saved to: {output_file}")

    print("\n" + "=" * 60)
    print("  🌱 GreenOps Demo Pipeline Complete")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_demo())
