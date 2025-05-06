import logging
import abc
import json # Import json
from typing import Dict, Any, List, Optional
from dateutil import parser

# Remove PodcastLead import if no longer used directly here
# from ..models.lead import PodcastLead 
from ..api.exceptions import APIParsingError

logger = logging.getLogger(__name__)

# --- Define the target unified structure keys (as constants) --- 
# This helps avoid typos and makes the code clearer
SOURCE_API = "source_api"
API_ID = "api_id"
TITLE = "title"
DESCRIPTION = "description"
RSS_URL = "rss_url"
WEBSITE = "website"
EMAIL = "email"
ITUNES_ID = "itunes_id"
LATEST_EPISODE_ID = "latest_episode_id"
LATEST_PUB_DATE_MS = "latest_pub_date_ms"
EARLIEST_PUB_DATE_MS = "earliest_pub_date_ms"
TOTAL_EPISODES = "total_episodes"
UPDATE_FREQUENCY_HOURS = "update_frequency_hours"
LISTEN_SCORE = "listen_score"
LISTEN_SCORE_GLOBAL_RANK = "listen_score_global_rank"
# New Podscan fields
PODCAST_SPOTIFY_ID = "podcast_spotify_id"
AUDIENCE_SIZE = "audience_size"
ITUNES_RATING_AVG = "itunes_rating_average"
ITUNES_RATING_COUNT = "itunes_rating_count"
SPOTIFY_RATING_AVG = "spotify_rating_average"
SPOTIFY_RATING_COUNT = "spotify_rating_count"
SOCIAL_LINKS = "social_links"
LAST_POSTED_AT = "last_posted_at"
IMAGE_URL = "image_url"
# Add new social constants
INSTAGRAM_URL = "instagram_url"
TWITTER_URL = "twitter_url"
LINKEDIN_URL = "linkedin_url"
TIKTOK_URL = "tiktok_url"
YOUTUBE_URL = "youtube_url"
FACEBOOK_URL = "facebook_url"
OTHER_SOCIAL_URL = "other_social_url"

class BaseResultMapper(abc.ABC):
    """Abstract base class for API result mappers to a unified dictionary."""
    @abc.abstractmethod
    def map_to_unified_dict(self, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Maps a single API result dictionary to the unified dictionary format."""
        pass

    def map_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Maps a list of API result dictionaries to a list of unified dictionaries."""
        unified_dicts = []
        for result in results:
            try:
                unified_dict = self.map_to_unified_dict(result)
                if unified_dict:
                    unified_dicts.append(unified_dict)
            except APIParsingError as e:
                logger.warning(f"Skipping result due to parsing error: {e}. Raw result: {result}")
            except Exception as e:
                logger.exception(f"Unexpected error mapping result: {result}. Error: {e}")
        return unified_dicts

class ListenNotesResultMapper(BaseResultMapper):
    """Maps Listen Notes API search results to the unified dictionary format."""
    def map_to_unified_dict(self, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not result or not isinstance(result, dict):
            logger.warning(f"Invalid Listen Notes result format received: {result}")
            return None

        api_id = result.get('id')
        if not api_id:
            logger.warning(f"Listen Notes result missing required 'id': {result}")
            return None

        try:
            unified_data = {
                SOURCE_API: 'listennotes',
                API_ID: api_id,
                TITLE: result.get('title_original') or result.get('title'),
                DESCRIPTION: result.get('description_original') or result.get('description'),
                RSS_URL: result.get('rss'),
                WEBSITE: result.get('website'),
                EMAIL: result.get('email'),
                ITUNES_ID: result.get('itunes_id'),
                LATEST_EPISODE_ID: result.get('latest_episode_id'),
                LATEST_PUB_DATE_MS: result.get('latest_pub_date_ms'),
                EARLIEST_PUB_DATE_MS: result.get('earliest_pub_date_ms'),
                TOTAL_EPISODES: result.get('total_episodes'),
                UPDATE_FREQUENCY_HOURS: result.get('update_frequency_hours'),
                LISTEN_SCORE: result.get('listen_score'),
                LISTEN_SCORE_GLOBAL_RANK: result.get('listen_score_global_rank'),
                IMAGE_URL: result.get('image'),
                PODCAST_SPOTIFY_ID: None,
                AUDIENCE_SIZE: None,
                ITUNES_RATING_AVG: None,
                ITUNES_RATING_COUNT: None,
                SPOTIFY_RATING_AVG: None,
                SPOTIFY_RATING_COUNT: None,
                LAST_POSTED_AT: None,
                INSTAGRAM_URL: None,
                TWITTER_URL: None,
                LINKEDIN_URL: None,
                TIKTOK_URL: None,
                YOUTUBE_URL: None,
                FACEBOOK_URL: None,
                OTHER_SOCIAL_URL: None
            }
            return unified_data
        except Exception as e:
            logger.error(f"Error mapping ListenNotes data to unified dict: {e}. Data: {result}")
            raise APIParsingError(f"Failed to map ListenNotes result: {e}")

class PodscanResultMapper(BaseResultMapper):
    """Maps Podscan API search results to the unified dictionary format."""
    def map_to_unified_dict(self, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not result or not isinstance(result, dict):
            logger.warning(f"Invalid Podscan result format received: {result}")
            return None

        api_id = result.get('podcast_id')
        if not api_id:
            logger.warning(f"Podscan result missing required 'podcast_id': {result}")
            return None

        try:
            reach_data = result.get('reach', {}) or {} # Handle None for reach
            itunes_reach = reach_data.get('itunes', {}) or {}
            spotify_reach = reach_data.get('spotify', {}) or {}
            social_links_list = reach_data.get('social_links', [])
            
            # Initialize social fields
            social_urls = {
                INSTAGRAM_URL: None,
                TWITTER_URL: None,
                LINKEDIN_URL: None,
                TIKTOK_URL: None,
                YOUTUBE_URL: None,
                FACEBOOK_URL: None,
                OTHER_SOCIAL_URL: None
            }
            other_socials = []

            # Parse social links list
            if social_links_list and isinstance(social_links_list, list):
                for link in social_links_list:
                    if isinstance(link, dict):
                        platform = link.get('platform', '').lower()
                        url = link.get('url')
                        if not url: continue

                        if 'instagram' in platform and not social_urls[INSTAGRAM_URL]: social_urls[INSTAGRAM_URL] = url
                        elif 'twitter' in platform and not social_urls[TWITTER_URL]: social_urls[TWITTER_URL] = url
                        elif 'linkedin' in platform and not social_urls[LINKEDIN_URL]: social_urls[LINKEDIN_URL] = url
                        elif 'tiktok' in platform and not social_urls[TIKTOK_URL]: social_urls[TIKTOK_URL] = url
                        elif 'youtube' in platform and not social_urls[YOUTUBE_URL]: social_urls[YOUTUBE_URL] = url
                        elif 'facebook' in platform and not social_urls[FACEBOOK_URL]: social_urls[FACEBOOK_URL] = url
                        else: other_socials.append(url) # Collect others
            
            # Use the first "other" social link if available
            if other_socials: social_urls[OTHER_SOCIAL_URL] = other_socials[0]
            
            unified_data = {
                SOURCE_API: 'podscan',
                API_ID: api_id,
                TITLE: result.get('podcast_name'),
                DESCRIPTION: result.get('podcast_description'),
                RSS_URL: result.get('rss_url'),
                WEBSITE: result.get('podcast_url'),
                EMAIL: reach_data.get('email'),
                ITUNES_ID: result.get('podcast_itunes_id'),
                TOTAL_EPISODES: result.get('episode_count'),
                LAST_POSTED_AT: result.get('last_posted_at'),
                PODCAST_SPOTIFY_ID: result.get('podcast_spotify_id'),
                AUDIENCE_SIZE: reach_data.get('audience_size'),
                ITUNES_RATING_AVG: itunes_reach.get('itunes_rating_average'),
                ITUNES_RATING_COUNT: itunes_reach.get('itunes_rating_count'),
                SPOTIFY_RATING_AVG: spotify_reach.get('spotify_rating_average'),
                SPOTIFY_RATING_COUNT: spotify_reach.get('spotify_rating_count'),
                IMAGE_URL: result.get('podcast_image_url'),
                LATEST_EPISODE_ID: None,
                LATEST_PUB_DATE_MS: None,
                EARLIEST_PUB_DATE_MS: None,
                UPDATE_FREQUENCY_HOURS: None,
                LISTEN_SCORE: None,
                LISTEN_SCORE_GLOBAL_RANK: None,
                **social_urls
            }
            return unified_data
        except Exception as e:
            logger.error(f"Error mapping Podscan data to unified dict: {e}. Data: {result}")
            raise APIParsingError(f"Failed to map Podscan result: {e}")

# Optional: Keep the StandardizationService wrapper if complex logic is needed later
# class StandardizationService: ... 