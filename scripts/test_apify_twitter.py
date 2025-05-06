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
import logging # Import logging

# Ensure env vars (.env contains APIFY_API_KEY)
load_dotenv()

from src.agents.enrichment.social_discovery import SocialMediaDiscovery

# Configure logging for the test script to see debug messages from social_discovery
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('src.agents.enrichment.social_discovery').setLevel(logging.DEBUG)
logger = logging.getLogger(__name__) # Logger for this test script

test_urls = [
    "https://twitter.com/PodcastPrivacy", # Previously problematic
    "https://twitter.com/enoumen",        # Previously worked
    "https://twitter.com/elonmusk",
    "https://twitter.com/apify",          # Will be used for padding if list is short
    "https://twitter.com/nonexistentuser123xyzabc", # Likely won't be found by actor
    "http://twitter.com/billgates",       # Test normalization (http, no www)
    "https://x.com/nasa",                 # Test x.com normalization
    "https://x.com/notaurlatall",                       # Invalid URL
    "https://instagram.com/notatwitter"   # Wrong domain
]

# Expected padding URLs if the list above is filtered down to less than 5 valid twitter URLs
padding_profile_urls = [
    "https://twitter.com/nasa",
    "https://twitter.com/bbcworld",
    "https://twitter.com/github",
    "https://twitter.com/teslamotors",
    "https://twitter.com/apify"
]
MIN_URLS_FOR_ACTOR = 5

async def main():
    print("--- Testing Apify Twitter Scraper Integration (apidojo/twitter-user-scraper) ---")
    if not os.getenv("APIFY_API_KEY"):
        print("Error: APIFY_API_KEY not found in environment variables.")
        print("Please create a .env file with your Apify API key.")
        return

    discovery = SocialMediaDiscovery()

    print("\n--- Initial Test URLs & Pre-computation --- ")
    valid_normalized_urls_for_test = set()
    username_to_original_url_map_for_test = {}

    for i, url_in in enumerate(test_urls):
        normalized = discovery._normalize_url(url_in) # Test normalization
        username = discovery._extract_username_from_twitter_url(normalized) # Test username extraction
        logger.info(f"Test URL {i+1}: Input='{url_in}', Normalized='{normalized}', Extracted Username='{username}'")
        if username: # Only consider it valid for actor if username can be extracted
            valid_normalized_urls_for_test.add(normalized)
            if username.lower() not in username_to_original_url_map_for_test: # Keep first original URL for a username
                 username_to_original_url_map_for_test[username.lower()] = normalized

    urls_for_actor_input = list(valid_normalized_urls_for_test)
    actual_unique_urls_count = len(urls_for_actor_input)
    
    if actual_unique_urls_count < MIN_URLS_FOR_ACTOR:
        needed_padding = MIN_URLS_FOR_ACTOR - actual_unique_urls_count
        padding_to_add_urls = []
        for pad_url in padding_profile_urls:
            if len(padding_to_add_urls) >= needed_padding:
                break
            normalized_pad_url = discovery._normalize_url(pad_url)
            if normalized_pad_url not in urls_for_actor_input:
                padding_to_add_urls.append(normalized_pad_url)
        urls_for_actor_input.extend(padding_to_add_urls)
        while len(urls_for_actor_input) < MIN_URLS_FOR_ACTOR and padding_profile_urls:
            urls_for_actor_input.append(discovery._normalize_url(padding_profile_urls[-1]))
        logger.info(f"Test script: Padded startUrls list from {actual_unique_urls_count} to {len(urls_for_actor_input)}.")
    
    final_run_input_for_actor = {
        "startUrls": urls_for_actor_input,
        "getFollowers": True,
        "getFollowing": False,
        "getRetweeters": False,
        "includeUnavailableUsers": False,
        "maxItems": len(urls_for_actor_input)
    }
    logger.info(f"Test script: Final run_input for Apify actor:\n{json.dumps(final_run_input_for_actor, indent=2)}")

    print(f"\nFetching Twitter data for URLs (after pre-filtering & potential padding): {urls_for_actor_input}")
    results_map = await discovery.get_twitter_data_for_urls_batch(test_urls) # Pass original list here

    print("\n=== PROCESSED RESULTS MAP (Original Normalized URL to Scraped Data) ===")
    if not results_map:
        print("No results map returned (None or empty).")
    else:
        # Print all results for debugging
        print(json.dumps(results_map, indent=2))
        print(f"\nTotal original unique normalized URLs processed in map: {len(results_map)}")
        
        success_count = 0
        fail_count = 0
        print("\n--- Individual Results Summary ---")
        for original_norm_url in sorted(list(original_urls_set_from_method_internal_test_only)):
            # This original_urls_set_from_method_internal_test_only would be the one used inside get_twitter_data_for_urls_batch
            # We need to iterate over the keys we expect based on our input `test_urls` that were valid
            # The `results_map` should be keyed by the normalized versions of `test_urls` that had extractable usernames
            
            data = results_map.get(original_norm_url)
            if data:
                print(f"  Data for {original_norm_url}: Username={data.get('username')}, Followers={data.get('followers_count')}, Verified={data.get('is_verified')}")
                success_count +=1
            else:
                print(f"  No data found/mapped for {original_norm_url}")
                fail_count +=1
        print(f"\nSummary: {success_count} successful, {fail_count} failed/no data.")

# This is a bit tricky, we need to simulate the `original_urls_set` that `get_twitter_data_for_urls_batch` creates internally
# to iterate results correctly for the summary.
original_urls_set_from_method_internal_test_only = set()
_temp_discovery = SocialMediaDiscovery() # Temp instance for helper methods
for url in test_urls:
    norm_url = _temp_discovery._normalize_url(url)
    if _temp_discovery._extract_username_from_twitter_url(norm_url):
         original_urls_set_from_method_internal_test_only.add(norm_url)

if __name__ == "__main__":
    asyncio.run(main()) 