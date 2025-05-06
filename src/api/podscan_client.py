import os
import logging
import html
from typing import Dict, Any, Optional, List
import asyncio

from .base_client import PodcastAPIClient
from .exceptions import APIClientError, AuthenticationError

logger = logging.getLogger(__name__)

# Use the standardized environment variable name
PODSCAN_API_KEY = os.getenv("PODSCAN_API_KEY")
PODSCAN_BASE_URL = "https://podscan.fm/api/v1"

class PodscanAPIClient(PodcastAPIClient):
    """API Client for Podscan.fm, inheriting from PodcastAPIClient."""

    def __init__(self):
        if not PODSCAN_API_KEY:
            logger.error("PODSCAN_API_KEY environment variable not set.")
            raise AuthenticationError("Podscan API key not configured")
        super().__init__(api_key=PODSCAN_API_KEY, base_url=PODSCAN_BASE_URL)
        logger.info("PodscanAPIClient initialized.")

    def _set_auth_header(self):
        """Sets the Authorization Bearer token header for Podscan authentication."""
        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})
            logger.debug("Podscan API authentication header set.")

    def search_podcasts(self, query: str, **kwargs) -> Dict[str, Any]:
        """Searches for podcasts using the Podscan API.

        Args:
            query: The search term.
            **kwargs: Additional Podscan search parameters (e.g., category_id, page, per_page, language).
                      See Podscan API documentation.

        Returns:
            The raw response dictionary from the Podscan API, which includes
            podcast list and potentially pagination metadata.

        Raises:
            APIClientError: For issues during the API request or processing.
        """
        endpoint = "podcasts/search"
        params = {
            'query': query,
            'per_page': kwargs.get('per_page', 20),
            'language': kwargs.get('language', 'en'),
            # Add other known params from kwargs
            **{k: v for k, v in kwargs.items() if k in ['page', 'category_ids']}
        }
        # Rename category_id if passed
        if 'category_id' in params:
            params['category_ids'] = params.pop('category_id')

        logger.info(f"Searching Podscan for query: '{query}' with params: {params}")
        try:
            response_data = self._request("GET", endpoint, params=params)
            # Directly return the raw response
            return response_data
        except APIClientError as e:
            logger.error(f"Podscan API search failed: {e}")
            raise
        except Exception as e:
            logger.exception(f"An unexpected error occurred during Podscan search: {e}")
            raise APIClientError(f"Unexpected error in Podscan search: {e}")

    # --- Other Podscan Methods (Refactored) ---

    def get_categories(self) -> List[Dict[str, Any]]:
        """Retrieves the list of categories from Podscan."""
        endpoint = 'categories'
        logger.info("Fetching categories from Podscan.")
        try:
            response_data = self._request("GET", endpoint)
            categories = response_data.get("categories", [])
            if not isinstance(categories, list):
                 logger.warning(f"Podscan get_categories returned non-list: {type(categories)}")
                 return []

            results = []
            for category in categories:
                details = {
                    "category_id": category.get("category_id"),
                    "category_name": category.get("category_name"),
                    "category_display_name": category.get("category_display_name")
                }
                results.append(details)
            return results
        except APIClientError as e:
            logger.error(f"Podscan get_categories failed: {e}")
            raise
        except Exception as e:
            logger.exception(f"An unexpected error occurred during Podscan get_categories: {e}")
            raise APIClientError(f"Unexpected error in Podscan get_categories: {e}")

    def get_podcast_episodes(self, podcast_id: str, **kwargs) -> List[Dict[str, Any]]:
        """Retrieves episodes for a specific podcast ID from Podscan."""
        endpoint = f'podcasts/{podcast_id}/episodes'
        params = {
            'order_by': kwargs.get('order_by', 'posted_at'),
            'order_dir': kwargs.get('order_dir', 'desc'),
            'per_page': kwargs.get('per_page', 10)
        }
        logger.info(f"Fetching episodes for podcast {podcast_id} from Podscan with params: {params}")
        try:
            response_data = self._request("GET", endpoint, params=params)
            episodes = response_data.get("episodes", [])
            if not isinstance(episodes, list):
                 logger.warning(f"Podscan get_podcast_episodes returned non-list: {type(episodes)}")
                 return []

            results = []
            for episode in episodes:
                details = {
                    "episode_id": episode.get("episode_id"),
                    "episode_url": episode.get("episode_url"),
                    "episode_title": episode.get("episode_title"),
                    "episode_audio_url": episode.get("episode_audio_url"),
                    "posted_at": episode.get("posted_at"),
                    "episode_transcript": episode.get("episode_transcript"),
                    'episode_description': episode.get('episode_description')
                }
                results.append(details)
            return results
        except APIClientError as e:
            logger.error(f"Podscan get_podcast_episodes for {podcast_id} failed: {e}")
            raise
        except Exception as e:
            logger.exception(f"An unexpected error occurred during Podscan get_podcast_episodes: {e}")
            raise APIClientError(f"Unexpected error in Podscan get_podcast_episodes: {e}")

    def search_podcast_by_rss(self, rss_feed_url: str) -> Optional[Dict[str, Any]]:
        """Searches for a podcast by RSS feed URL using the Podscan API.
        Assumes an RSS feed uniquely identifies one podcast.

        Args:
            rss_feed_url: The RSS feed URL of the podcast.

        Returns:
            A dictionary containing the podcast data if found, otherwise None.
        
        Raises:
            APIClientError: For issues during the API request or processing.
        """
        endpoint = 'podcasts/search/by/RSS'
        params = {'rss_feed': rss_feed_url}
        logger.info(f"Searching Podscan by RSS: {rss_feed_url}")
        try:
            # Use the shared request method
            response_data = self._request("GET", endpoint, params=params)
            
            # --- Handle potential dictionary wrapper --- 
            podcast_list = []
            if isinstance(response_data, dict) and 'podcasts' in response_data and isinstance(response_data['podcasts'], list):
                podcast_list = response_data['podcasts']
                logger.debug("Podscan RSS search returned a dictionary, extracted 'podcasts' list.")
            elif isinstance(response_data, list):
                podcast_list = response_data
                logger.debug("Podscan RSS search returned a list directly.")
            else:
                logger.warning(f"Unexpected response format from Podscan RSS search for {rss_feed_url}. Expected list or dict with 'podcasts' key, got {type(response_data)}. Data: {response_data}")
                return None
            # --- End Handle dictionary wrapper ---

            # Check if the list contains exactly one podcast
            if len(podcast_list) == 1:
                podcast_data = podcast_list[0]
                # Optional: Add basic validation that it looks like podcast data
                if isinstance(podcast_data, dict) and podcast_data.get('podcast_id'):
                     # Can optionally unescape name here if needed, or keep it raw
                     # podcast_data['podcast_name'] = html.unescape(...) 
                     logger.debug(f"Found podcast {podcast_data.get('podcast_id')} via RSS {rss_feed_url}")
                     return podcast_data
                else:
                     logger.warning(f"Podscan RSS search for {rss_feed_url} found single item in list, but it was non-dict or missing podcast_id: {podcast_data}")
                     return None
            elif len(podcast_list) > 1:
                logger.warning(f"Podscan RSS search for {rss_feed_url} returned multiple ({len(podcast_list)}) results. Returning None.")
                return None
            else: # len(podcast_list) == 0
                 logger.info(f"Podscan RSS search for {rss_feed_url} returned no results in the list.")
                 return None

        except APIClientError as e:
            logger.error(f"Podscan search_by_rss for {rss_feed_url} failed: {e}")
            # Depending on the error (e.g., 404), might want to return None vs raising
            if e.status_code == 404:
                 logger.info(f"Podscan RSS {rss_feed_url} not found (404).")
                 return None
            raise # Re-raise other API client errors
        except Exception as e:
            logger.exception(f"An unexpected error occurred during Podscan search_by_rss for {rss_feed_url}: {e}")
            # Don't raise APIClientError here, just return None for unexpected issues during lookup
            return None 

    def search_podcast_by_itunes_id(self, itunes_id: int) -> Optional[Dict[str, Any]]:
        """Searches for a podcast by its iTunes ID using the Podscan API.

        Args:
            itunes_id: The iTunes ID of the podcast.

        Returns:
            A dictionary containing the podcast data if found, otherwise None.
        
        Raises:
            APIClientError: For other API-related issues.
        """
        if not itunes_id:
            return None
            
        endpoint = 'podcasts/search/by/itunesid'
        params = {'itunes_id': itunes_id}
        logger.info(f"Searching Podscan by iTunes ID: {itunes_id}")
        try:
            response_data = self._request("GET", endpoint, params=params)
            
            # Updated Logic: Expect a dict with a 'podcast' key
            if isinstance(response_data, dict) and 'podcast' in response_data:
                podcast_data = response_data.get('podcast')
                if isinstance(podcast_data, dict) and podcast_data.get('podcast_id'):
                    logger.debug(f"Found podcast for iTunes ID {itunes_id} (returned as dict).")
                    # Optional: Verify the iTunes ID in the response matches the query
                    if podcast_data.get('podcast_itunes_id') == str(itunes_id):
                        logger.debug(f"Podcast {podcast_data.get('podcast_id')} found via iTunes ID {itunes_id} matches requested ID.")
                        return podcast_data
                    else:
                        logger.warning(f"Podscan iTunes ID search for {itunes_id} returned a podcast, but its iTunes ID ({podcast_data.get('podcast_itunes_id')}) does not match.")
                        return None
                else:
                    logger.warning(f"Podscan iTunes ID search for {itunes_id} response contained 'podcast' key, but value was not a valid dict or missing 'podcast_id'. Data: {podcast_data}")
                    return None
            else:
                # Handle cases where the response isn't the expected dict format or is empty
                logger.warning(f"Unexpected response format or no podcast found from Podscan iTunes ID search for {itunes_id}. Got Type: {type(response_data)}. Data: {response_data}")
                return None

        except APIClientError as e:
            logger.error(f"Podscan search_by_itunes_id for {itunes_id} failed: {e}")
            if e.status_code == 404:
                 logger.info(f"Podscan iTunes ID {itunes_id} not found (404).")
                 return None
            raise # Re-raise other API client errors
        except Exception as e:
            logger.exception(f"An unexpected error occurred during Podscan search_by_itunes_id for {itunes_id}: {e}")
            return None
            
    def get_related_podcasts(self, podcast_id: str) -> Optional[List[Dict[str, Any]]]:
        """Fetches related podcasts for a given Podscan podcast ID.

        Args:
            podcast_id: The Podscan ID of the podcast.

        Returns:
            A list of related podcast dictionaries, or None if the request fails.
        """
        if not podcast_id:
            return None
            
        endpoint = f"podcasts/{podcast_id}/related_podcasts"
        
        logger.info(f"Fetching related podcasts for Podscan ID: {podcast_id}")
        try:
            response_data = self._request("GET", endpoint)
            # Updated logic: Handle dict with 'related_podcasts' key
            related_podcasts_list = []
            if isinstance(response_data, dict) and 'related_podcasts' in response_data:
                 related_podcasts_list = response_data.get('related_podcasts', [])
                 if not isinstance(related_podcasts_list, list):
                     logger.warning(f"Podscan get_related_podcasts for {podcast_id} returned dict but 'related_podcasts' key was not a list. Found type: {type(related_podcasts_list)}")
                     return None # Invalid format within the key
                 logger.debug(f"Extracted related podcasts list from dictionary response for {podcast_id}.")
            elif isinstance(response_data, list):
                 # Keep handling direct list response as a fallback/possibility
                 related_podcasts_list = response_data
                 logger.debug(f"Received related podcasts as a direct list for {podcast_id}.")
            else:
                 logger.warning(f"Podscan get_related_podcasts for {podcast_id} returned unexpected format. Expected list or dict with 'related_podcasts'. Got {type(response_data)}. Response: {response_data}")
                 return None

            # Process the extracted list
            logger.info(f"Successfully fetched {len(related_podcasts_list)} raw related items for Podscan ID {podcast_id}")
            # Basic check if items look like podcasts
            valid_results = [p for p in related_podcasts_list if isinstance(p, dict) and p.get('podcast_id')]
            if len(valid_results) != len(related_podcasts_list):
                logger.warning(f"Some items in related podcasts response for {podcast_id} were not valid podcast dicts.")
            return valid_results
        except APIClientError as e:
            logger.error(f"Podscan get_related_podcasts for {podcast_id} failed: {e}")
            # Return None on failure
            return None
        except Exception as e:
            logger.exception(f"An unexpected error occurred during Podscan get_related_podcasts for {podcast_id}: {e}")
            return None 

# Example Usage (for testing - requires APIFY_API_KEY in .env)
async def main_test(): # Make test function async
    if not os.getenv("APIFY_API_KEY"):
        # ... (API key check)
        pass # Keep existing test logic
        
    try:
        # --- ADDED: Test Podscan iTunes ID lookup --- #
        print("\n--- Testing Podscan iTunes ID Search --- ")
        test_itunes_id = 624693800 # ID provided by user
        client = PodscanAPIClient() # Instantiate Podscan client
        itunes_search_result = client.search_podcast_by_itunes_id(test_itunes_id)
        if itunes_search_result:
            print("Podscan iTunes Search Result (raw):")
            print(itunes_search_result)
        else:
            print(f"Failed to find podcast with iTunes ID {test_itunes_id} on Podscan.")
        # --- END ADDED TEST ---
        
        #discovery = SocialMediaDiscovery()
        # ... (Existing LinkedIn/Twitter tests remain)

    except ValueError as e:
        print(f"Initialization Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during testing: {e}")

if __name__ == "__main__":
    # Note: This test block now requires both APIFY_API_KEY and PODSCAN_API_KEY
    asyncio.run(main_test()) # Run the async test function 