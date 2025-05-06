import sys
from pathlib import Path

# Ensure project root (two levels up) is on PYTHONPATH so `import src...` works
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import asyncio
import json
import os
from dotenv import load_dotenv

# Ensure env vars (.env contains APIFY_API_KEY)
load_dotenv()

from src.agents.enrichment.social_discovery import SocialMediaDiscovery

# Test with a known TikTok URL that should yield channel data
test_tiktok_urls = [
    "https://www.tiktok.com/@vozmediausa",
    "https://www.tiktok.com/@mrbeast" # Add another one for variety
]

async def main():
    print("--- Testing Apify TikTok Scraper Integration ---")
    if not os.getenv("APIFY_API_KEY"):
        print("Error: APIFY_API_KEY not found in environment variables.")
        print("Please create a .env file with your Apify API key.")
        return

    discovery = SocialMediaDiscovery()
    
    print(f"\nFetching TikTok data for URLs: {test_tiktok_urls}")
    # The get_tiktok_data_for_urls_batch method in SocialMediaDiscovery 
    # itself handles calling the actor and processing its dataset.
    # We are testing if this processing and mapping works correctly.
    results_map = await discovery.get_tiktok_data_for_urls_batch(test_tiktok_urls)

    print("\n=== PROCESSED RESULTS MAP (Original URL to Scraped Data) ===")
    if not results_map:
        print("No results map returned (None or empty).")
    else:
        print(json.dumps(results_map, indent=2))
        print(f"\nTotal URLs processed in map: {len(results_map)}")
        for url, data in results_map.items():
            if data:
                print(f"  Data for {url}: Followers = {data.get('followers_count')}, Username = {data.get('username')}")
            else:
                print(f"  No data found/mapped for {url}")

if __name__ == "__main__":
    asyncio.run(main()) 