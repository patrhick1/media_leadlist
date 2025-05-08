#!/usr/bin/env python
import logging
import asyncio
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Ensure the src directory is in the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir) 
sys.path.insert(0, project_root)

# Import necessary components AFTER adjusting path
from src.agents.vetting_agent import VettingAgent
from src.models.podcast_profile import EnrichedPodcastProfile

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('src.services.gemini_search').setLevel(logging.DEBUG) # Show Gemini interactions
logger = logging.getLogger(__name__)

async def run_vetting_test():
    """Runs the standalone vetting test."""
    logger.info("--- Starting Standalone Vetting Test --- ")
    load_dotenv()

    # --- Define Test Data --- 
    # 1. Enriched Profiles (Dummy Data)
    dummy_profiles = [
        EnrichedPodcastProfile(
            api_id="tech_podcast_01", 
            title="AI Forward", 
            description="Weekly discussions on the latest advancements in machine learning, deep learning, and artificial intelligence applications across industries.",
            keywords=["artificial intelligence", "machine learning", "deep learning", "tech news"],
            latest_episode_date=datetime.now() - timedelta(days=5),
            first_episode_date=datetime.now() - timedelta(days=370),
            total_episodes=55, 
            publishing_frequency_days=7 # Consistent weekly
        ),
        EnrichedPodcastProfile(
            api_id="history_podcast_02", 
            title="Echoes of Time", 
            description="Exploring pivotal moments and forgotten stories from world history, from ancient civilizations to modern events.",
            keywords=["history", "world history", "storytelling"],
            latest_episode_date=datetime.now() - timedelta(days=160), # Stale
            first_episode_date=datetime.now() - timedelta(days=800),
            total_episodes=40,
            publishing_frequency_days=20 # Semi-regular
        ),
         EnrichedPodcastProfile(
            api_id="finance_podcast_03", 
            title="Market Movers", 
            description="Daily updates and analysis on stock market trends, investment strategies, and global economic news.",
            keywords=["finance", "investing", "stocks", "economics"],
            latest_episode_date=datetime.now() - timedelta(days=1),
            first_episode_date=datetime.now() - timedelta(days=500),
            total_episodes=450,
            publishing_frequency_days=1 # Consistent daily
        ),
         EnrichedPodcastProfile(
            api_id="minimal_podcast_04", 
            title="Brief Thoughts", 
            description="Short episodes.", # Minimal description
            keywords=[],
            latest_episode_date=datetime.now() - timedelta(days=30),
            first_episode_date=datetime.now() - timedelta(days=60),
            total_episodes=3, # Not enough for good frequency calc
            publishing_frequency_days=20
        )
    ]

    # 2. Vetting Criteria
    test_ideal_desc = "An expert interview podcast focusing on the practical applications and ethical considerations of generative AI in business and creative fields. Target audience includes tech leaders, product managers, and AI practitioners."
    test_guest_bio = "Dr. Evelyn Reed is a leading AI ethicist and consultant with over 10 years of experience advising Fortune 500 companies on responsible AI implementation. She has published numerous papers on bias detection and mitigation in large language models."
    test_guest_tp = [
        "Real-world examples of generative AI transforming specific industries (e.g., marketing, software dev).",
        "Common ethical pitfalls when deploying generative AI and how to avoid them.",
        "The future workforce: How generative AI will change job roles and required skills.",
        "Balancing innovation speed with responsible AI development practices."
    ]

    # --- Instantiate Agent and Run --- 
    try:
        vetting_agent = VettingAgent()
        if not vetting_agent.vetting_service or not vetting_agent.vetting_service.gemini_service:
            logger.error("Failed to initialize VettingAgent or its required services. Exiting test.")
            return
            
        logger.info(f"Starting vetting for {len(dummy_profiles)} profiles...")
        
        results, csv_path = await vetting_agent.perform_standalone_vetting(
            enriched_profiles=dummy_profiles,
            ideal_podcast_description=test_ideal_desc,
            guest_bio=test_guest_bio,
            guest_talking_points=test_guest_tp,
            source_campaign_id="llm_vetting_test_run"
        )

        logger.info("--- Vetting Test Completed ---")
        print("\n================ RESULTS ================")
        if results:
            for i, res in enumerate(results):
                print(f"\n--- Profile {i+1}: {res.podcast_id} ---")
                print(f"  Tier: {res.quality_tier}")
                print(f"  Score: {res.composite_score:.1f}")
                print(f"  Consistency Passed: {res.programmatic_consistency_passed}")
                print(f"  Consistency Reason: {res.programmatic_consistency_reason}")
                print(f"  LLM Match Score: {res.llm_match_score}")
                print(f"  LLM Explanation: {res.llm_match_explanation}")
                print(f"  Final Explanation: {res.final_explanation}")
                if res.error:
                    print(f"  ERROR: {res.error}")
            if csv_path:
                print(f"\nCSV output available at web path: {csv_path}")
                # To get the local path, we need to reconstruct it (or modify agent to return it too)
                local_csv_path = os.path.join(project_root, csv_path.replace("/static/", "data/"))
                print(f"Local CSV path: {local_csv_path}")
            else:
                print("\nCSV file was not generated.")
        else:
            print("No vetting results were returned.")
        print("=======================================")
            
    except Exception as e:
        logger.exception("An unexpected error occurred during the vetting test run.")

# --- Run the Async Test --- 
if __name__ == "__main__":
    # For Windows compatibility with asyncio selector policy if needed
    # if sys.platform == 'win32':
    #      asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_vetting_test()) 