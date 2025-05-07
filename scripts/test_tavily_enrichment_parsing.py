import sys
from pathlib import Path
import asyncio
import json
import logging
import os
from dotenv import load_dotenv
from typing import Optional

# Ensure project root is on PYTHONPATH
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import the agent and the Pydantic model for type hinting/checking
from src.agents.enrichment_agent import EnrichmentAgent
from src.models.social import GeminiPodcastEnrichment # For type hint

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# You can set a more verbose level for specific modules if needed, e.g.:
# logging.getLogger('src.agents.enrichment_agent').setLevel(logging.INFO) # Will be set to DEBUG in main
logging.getLogger('src.services.tavily_search').setLevel(logging.INFO)
logging.getLogger('src.services.gemini_search').setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# Sample podcast data provided by the user
sample_podcast_data = [
  {
    "api_id": "834dc1648f718c00479348f4",
    "title": "Three Buddy Problem Security Conversations",
    "description": "<p>We discuss security topics that impact you and your family every week. We break down the topics in a way that is easy to digest and understand! If you want to be aware of the security risks in your daily life but don't have the time for complex technical deep dives this is the podcast for you!</p>",
    "website": "https://sec.naraine.ai/free"
  },
  {
    "api_id": "d116a055a79b73ed580c51c9",
    "title": "Everything AI And Law",
    "description": "Welcome",
    "website": "https://www.everythingaiandlaw.com/"
  },
  {
    "api_id": "12a8a8de9e26c126f401427e",
    "title": "Leveraging AI in Business",
    "description": "<p><b>Dive into the future of business</b> with 'Leveraging AI in Business' hosted by <b>Andreas Welsch.</b></p><p>This podcast is your essential guide to understanding and applying Artificial Intelligence (AI) to transform and grow your business. Andreas brings you expert interviews, case studies, and actionable insights on how AI is reshaping various industries, from optimizing operations and enhancing customer experiences to developing innovative products and strategies.</p><p>Whether you're a business leader, entrepreneur, or tech enthusiast, this podcast provides valuable knowledge on navigating the AI landscape. Discover the potential of machine learning, natural language processing, and other AI technologies to gain a competitive edge, increase efficiency, and drive significant value.</p><p>Tune in to 'Leveraging AI in Business' and unlock the power of AI to build a smarter, more prosperous future for your business.</p>",
    "website": "https://multiplai.ai/?utm_source=listennotes"
  },
  {
    "api_id": "1a4afcdc780774ed766d7efc",
    "title": "Weight and Healthcare",
    "description": "Examining",
    "website": "https://weightandhealthcare.com/podcast/"
  },
  {
    "api_id": "f107f4f8313c79b9edb81539",
    "title": "CRT - Culture Religion and Technology",
    "description": "The CRT - ( Culture , Religion and Technology) Podcast where we talk about how Culture Religion and Technology are all converging to create a new world. The show will be a mixture of solo ranting , interviews and conversations. The show has a Christian Worldview but open to discussing anything that would be interesting to our listeners. The podcast is hosted by Darrell Harrison and Virgil Walker with Revolver News.",
    "website": "https://revolver.news/"
  },
  {
    "api_id": "fb34e2ad77f8ef0b6d2c4374",
    "title": "Tech 4 Thought",
    "description": "<p> Inspiring innovative thinking in a rapidly evolving world.</p><p>Join host Ryan Naraine, a veteran cybersecurity journalist and founder of <a href=\"https://sec.naraine.ai/free\" rel=\"noopener noreferrer\" target=\"_blank\">Naraine.AI</a>, as he explores cutting-edge technology, cybersecurity, and the impact of AI on our future. Tech 4 Thought features insightful conversations with leading experts, providing a thought-provoking perspective on the digital landscape. Tune in for discussions that will challenge your assumptions and expand your understanding of the forces shaping our world.</p>",
    "website": "https://www.buzzsprout.com/2444636/share"
  },
  {
    "api_id": "1e63e32b43a52a4975401c35",
    "title": "The AI CEO",
    "description": "The AI",
    "website": "https://site.seemaalexander.com/seema-alexander-podcast"
  },
  {
    "api_id": "7dca3456c50059d68257f539",
    "title": "The DNAi Podcast",
    "description": "Join Naible-",
    "website": "https://poc.authoridrake.com/podcast/how-to-write-a-book"
  },
  {
    "api_id": "64927a6d62a20165b5572222",
    "title": "The Adventurous Entrepreneur",
    "description": "Hey, Time-",
    "website": "https://authoridrake.com/podcast/how-to-write-a-book"
  },
  {
    "api_id": "43c670c3bf08398ee4c33141",
    "title": "The AI Daily Brief (formerly The AI Daily)",
    "description": "A daily new",
    "website": "https://site.nlw.co/Whittaker"
  }
]

async def main():
    logger.info("--- Starting Tavily Enrichment Parsing Test ---")
    logger.info("--- This test verifies the EnrichmentAgent workflow using Tavily's 'include_answer=True' feature. ---")
    load_dotenv()

    # Set specific logger for enrichment_agent to DEBUG for this test run to see more details
    logging.getLogger('src.agents.enrichment_agent').setLevel(logging.DEBUG)

    # Check for necessary API keys
    google_api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    tavily_api_key = os.getenv("TAVILY_API_KEY")

    if not google_api_key:
        logger.error("GOOGLE_API_KEY or GEMINI_API_KEY not found in environment variables.")
        return
    if not tavily_api_key:
        logger.error("TAVILY_API_KEY not found in environment variables.")
        return

    agent = EnrichmentAgent()
    if not agent.gemini_service or not agent.gemini_service.llm:
        logger.error("EnrichmentAgent's Gemini service or LLM not initialized. Cannot proceed.")
        return

    for i, podcast_input_data in enumerate(sample_podcast_data):
        api_id = podcast_input_data.get('api_id', f'unknown_id_{i}')
        title = podcast_input_data.get('title', 'Unknown Title')
        
        logger.info(f"\n--- Processing Podcast {i+1}/{len(sample_podcast_data)} ---")
        logger.info(f"API ID: {api_id}, Title: {title}")

        try:
            # Ensure all necessary fields for _run_gemini_discovery_for_podcast are present, even if None
            # The method expects a Dict[str, Any] and will use .get() internally.
            # We just need 'api_id' and 'title' for logging here, and the method handles the rest.
            
            gemini_discovery_result: Optional[GeminiPodcastEnrichment] = await agent._run_gemini_discovery_for_podcast(podcast_input_data)
            
            if gemini_discovery_result:
                logger.info(f"Successfully ran Gemini discovery for: {title} (API ID: {api_id})")
                # Pretty print the Pydantic model using model_dump_json
                print(f"\nStructured Output for '{title}' (API ID: {api_id}):")
                print(gemini_discovery_result.model_dump_json(indent=2, exclude_none=True))
            else:
                logger.warning(f"Gemini discovery returned no structured output for: {title} (API ID: {api_id})")

        except Exception as e:
            logger.error(f"An error occurred while processing {title} (API ID: {api_id}): {e}", exc_info=True)
        
        # Add a small delay to avoid hitting API rate limits too quickly if any services are called rapidly
        await asyncio.sleep(2) # Adjust as needed, e.g., 1 second for Tavily free tier might be okay

    logger.info("\n--- Tavily Enrichment Parsing Test Finished ---")

if __name__ == "__main__":
    asyncio.run(main()) 