import os
import logging
from typing import Dict, Any, Optional, List

from .base_client import PodcastAPIClient
from .exceptions import APIClientError, AuthenticationError

logger = logging.getLogger(__name__)

# Use the standardized environment variable name
LISTENNOTES_API_KEY = os.getenv("LISTENNOTES_API_KEY")
LISTENNOTES_BASE_URL = "https://listen-api.listennotes.com/api/v2"

class ListenNotesAPIClient(PodcastAPIClient):
    """API Client for Listen Notes, inheriting from PodcastAPIClient."""

    def __init__(self):
        if not LISTENNOTES_API_KEY:
            logger.error("LISTENNOTES_API_KEY environment variable not set.")
            raise AuthenticationError("Listen Notes API key not configured")
        super().__init__(api_key=LISTENNOTES_API_KEY, base_url=LISTENNOTES_BASE_URL)
        logger.info("ListenNotesAPIClient initialized.")

    def _set_auth_header(self):
        """Sets the X-ListenAPI-Key header for Listen Notes authentication."""
        if self.api_key:
            self.session.headers.update({"X-ListenAPI-Key": self.api_key})
            logger.debug("Listen Notes API authentication header set.")

    def _fetch_podcasts_batch(self, 
                             ids: Optional[List[str]] = None,
                             rsses: Optional[List[str]] = None,
                             itunes_ids: Optional[List[int]] = None,
                             spotify_ids: Optional[List[str]] = None,
                             show_latest_episodes: int = 0,
                             next_episode_pub_date: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Fetches podcast data in batch using the POST /podcasts endpoint.

        Args:
            ids: List of Listen Notes podcast IDs.
            rsses: List of RSS feed URLs.
            itunes_ids: List of iTunes IDs.
            spotify_ids: List of Spotify IDs.
            show_latest_episodes: Whether to fetch latest episodes (0 or 1).
            next_episode_pub_date: Epoch timestamp for episode pagination.

        Returns:
            The JSON response dictionary from the API, or None if the request fails.
        """
        endpoint = "podcasts"
        data = {}
        if ids: data['ids'] = ",".join(ids)
        if rsses: data['rsses'] = ",".join(rsses)
        if itunes_ids: data['itunes_ids'] = ",".join(map(str, itunes_ids))
        if spotify_ids: data['spotify_ids'] = ",".join(spotify_ids)
        if show_latest_episodes in [0, 1]: data['show_latest_episodes'] = str(show_latest_episodes)
        if next_episode_pub_date: data['next_episode_pub_date'] = str(next_episode_pub_date)

        if not data:
            logger.warning("No identifiers provided to _fetch_podcasts_batch.")
            return None

        logger.info(f"Fetching podcast batch data with params: {list(data.keys())}") # Log keys only
        try:
            # Use the shared request method, specifying 'data' for POST
            response_data = self._request("POST", endpoint, data=data) 
            return response_data
        except APIClientError as e:
            logger.error(f"Listen Notes POST /podcasts failed: {e}")
            return None
        except Exception as e:
            logger.exception(f"An unexpected error occurred during Listen Notes POST /podcasts: {e}")
            return None

    def search_podcasts(self, query: str, **kwargs) -> Dict[str, Any]:
        """Searches for podcasts using the Listen Notes API.

        Args:
            query: The search term.
            **kwargs: Additional Listen Notes search parameters (e.g., sort_by_date, type, offset, genre_ids, etc.).
                      See https://www.listennotes.com/api/docs/#get-api-v2-search

        Returns:
            A dictionary containing the search results from Listen Notes.

        Raises:
            APIClientError: For issues during the API request or processing.
        """
        endpoint = "search"
        # Parameters from the provided code + defaults
        params = {
            "q": query,
            "sort_by_date": kwargs.get("sort_by_date", 1),
            "type": kwargs.get("type", "podcast"),
            "offset": kwargs.get("offset", 0),
            "language": kwargs.get("language", "English"),
            # Add other relevant params from kwargs if needed
            **{k: v for k, v in kwargs.items() if k in ['genre_ids', 'published_after', 'ocid', 'safe_mode']}
        }
        # Handle 'interviews_only' specifically if present
        if kwargs.get('interviews_only') == 1:
             params['only_in'] = 'title,description' # Approximation based on docs

        logger.info(f"Searching Listen Notes for query: '{query}' with params: {params}")
        try:
            # Use the shared request method from the base class
            response_data = self._request("GET", endpoint, params=params)
            return response_data
        except APIClientError as e:
            logger.error(f"Listen Notes API search failed: {e}")
            # Log specific details if available (e.g., e.status_code)
            raise
        except Exception as e:
            logger.exception(f"An unexpected error occurred during Listen Notes search: {e}")
            raise APIClientError(f"Unexpected error in Listen Notes search: {e}")

    def lookup_podcast_by_rss(self, rss_feed_url: str) -> Optional[Dict[str, Any]]:
        """Looks up a podcast by RSS feed URL using the POST /podcasts endpoint.

        Args:
            rss_feed_url: The exact RSS feed URL.

        Returns:
            A dictionary containing the podcast data if found, otherwise None.
        """
        if not rss_feed_url:
            return None
            
        logger.info(f"Looking up Listen Notes by RSS (POST /podcasts): {rss_feed_url}")
        
        response_data = self._fetch_podcasts_batch(rsses=[rss_feed_url])

        if response_data and isinstance(response_data.get('podcasts'), list) and len(response_data['podcasts']) == 1:
            podcast_data = response_data['podcasts'][0]
            # Optional: Add a check here if the returned RSS *must* exactly match? 
            # The POST endpoint is likely more reliable, but verification might still be good.
            # if podcast_data.get('rss') == rss_feed_url:
            logger.info(f"Found podcast for RSS {rss_feed_url} via POST: ID {podcast_data.get('id')}")
            return podcast_data
            # else:
            #    logger.warning(f"POST /podcasts for {rss_feed_url} returned a result, but RSS did not match exactly. Found: {podcast_data.get('rss')}")
            #    return None
        elif response_data and isinstance(response_data.get('podcasts'), list) and len(response_data['podcasts']) > 1:
             logger.warning(f"POST /podcasts for {rss_feed_url} returned multiple ({len(response_data['podcasts'])}) results. This shouldn't happen with a single RSS query.")
             # Maybe return the first one if its RSS matches? For now, return None.
             return None
        else:
            logger.info(f"POST /podcasts for {rss_feed_url} returned no results or unexpected format. Response: {response_data}")
            return None
        # Note: Error logging happens within _fetch_podcasts_batch

    def lookup_podcast_by_itunes_id(self, itunes_id: int) -> Optional[Dict[str, Any]]:
        """Looks up a podcast by its iTunes ID using the POST /podcasts endpoint.

        Args:
            itunes_id: The Apple Podcasts (iTunes) ID.

        Returns:
            A dictionary containing the full podcast data if found, otherwise None.
        """
        if not itunes_id:
            return None

        logger.info(f"Looking up ListenNotes by iTunes ID (POST /podcasts): {itunes_id}")
        
        response_data = self._fetch_podcasts_batch(itunes_ids=[itunes_id])

        if response_data and isinstance(response_data.get('podcasts'), list) and len(response_data['podcasts']) == 1:
            podcast_data = response_data['podcasts'][0]
            # Verify the iTunes ID matches the one we requested
            # Ensure types match for comparison (both as int)
            try:
                found_itunes_id = int(podcast_data.get('itunes_id'))
                queried_itunes_id = int(itunes_id) # itunes_id is already int, but cast just in case
                
                if found_itunes_id == queried_itunes_id:
                    logger.info(f"Found podcast for iTunes ID {itunes_id} via POST: LN ID {podcast_data.get('id')}")
                    return podcast_data
                else:
                    # IDs genuinely don't match
                    logger.warning(f"POST /podcasts for iTunes ID {itunes_id} returned a result, but the ID didn't match. Expected {itunes_id}, Found: {found_itunes_id}")
                    return None
            except (TypeError, ValueError) as e:
                 # Handle cases where found ID is not a valid integer
                 logger.warning(f"Could not compare iTunes IDs for query {itunes_id}. Found value '{podcast_data.get('itunes_id')}' is not an integer. Error: {e}")
                 return None
        elif response_data and isinstance(response_data.get('podcasts'), list) and len(response_data['podcasts']) > 1:
             logger.warning(f"POST /podcasts for iTunes ID {itunes_id} returned multiple ({len(response_data['podcasts'])}) results. This shouldn't happen with a single ID query.")
             # Maybe return the first one if its ID matches? For now, return None.
             return None
        else:
            logger.info(f"POST /podcasts for iTunes ID {itunes_id} returned no results or unexpected format. Response: {response_data}")
            return None
        # Error logging happens within _fetch_podcasts_batch

    def get_recommendations(self, podcast_id: str, safe_mode: int = 0) -> Optional[List[Dict[str, Any]]]:
        """Fetches podcast recommendations for a given Listen Notes podcast ID.

        Args:
            podcast_id: The Listen Notes ID of the podcast.
            safe_mode: Whether to exclude podcasts with explicit language (0 or 1).

        Returns:
            A list of recommended podcast dictionaries, or None if the request fails.
        """
        if not podcast_id:
            return None
            
        endpoint = f"podcasts/{podcast_id}/recommendations"
        params = {"safe_mode": safe_mode}
        
        logger.info(f"Fetching recommendations for ListenNotes podcast ID: {podcast_id} with safe_mode={safe_mode}")
        try:
            response_data = self._request("GET", endpoint, params=params)
            recommendations = response_data.get('recommendations')
            if isinstance(recommendations, list):
                logger.info(f"Successfully fetched {len(recommendations)} recommendations for LN ID {podcast_id}")
                return recommendations
            else:
                logger.warning(f"ListenNotes get_recommendations for {podcast_id} returned non-list or missing key. Response: {response_data}")
                return None
        except APIClientError as e:
            logger.error(f"ListenNotes get_recommendations for {podcast_id} failed: {e}")
            # Return None on failure for lookup-like methods
            return None
        except Exception as e:
            logger.exception(f"An unexpected error occurred during ListenNotes get_recommendations for {podcast_id}: {e}")
            return None

    # Add other methods like get_podcast_by_id, get_genres etc. if needed

# Example Usage for testing RSS lookup
if __name__ == '__main__':
    import logging
    from dotenv import load_dotenv
    import pprint # Import pprint for better dict printing
    
    # Load .env for local testing (ensure LISTENNOTES_API_KEY is set)
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    
    # --- Test Data --- 
    test_rss_url = "https://www.thisamericanlife.org/podcast/rss.xml" # This American Life
    test_itunes_id = 1691157207 # Normal World iTunes ID
    # bad_rss_url = "https://feeds.captivate.fm/non-existent-feed-for-testing/"
    # bad_itunes_id = 9999999999 # Non-existent iTunes ID
    # --- 
    
    if not LISTENNOTES_API_KEY:
        logger.error("LISTENNOTES_API_KEY not found in environment. Cannot run test.")
    else:
        try:
            client = ListenNotesAPIClient()
            logger.info("--- Testing ListenNotes Lookups (POST /podcasts) ---")

            # Test 1: Successful iTunes ID Lookup (New Test)
            print(f"\n[TEST] Looking up iTunes ID: {test_itunes_id}")
            podcast_data_itunes = client.lookup_podcast_by_itunes_id(test_itunes_id)
            if podcast_data_itunes:
                print("  --- Found Podcast Data via iTunes ID ---")
                pprint.pprint(podcast_data_itunes)
            else:
                print(f"  --- Podcast Not Found via iTunes ID {test_itunes_id} ---")

            # --- Previous Tests Commented Out --- #
            # print(f"\n[TEST 1] Looking up RSS: {test_rss_url}")
            # podcast_data_rss = client.get_recommendations("25212ac3c53240a880dd5032e547047b")
            # print(f"RSS Lookup Result: {podcast_data_rss}")

        except APIClientError as e:
            print(f"\n--- API Client Error ---: {e}")
        except Exception as e:
            print(f"\n--- Unexpected Error ---: {e}") 