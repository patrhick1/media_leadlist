import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import re # For duration parsing
import uuid # Import uuid

# Import the target model and source data structures
from ...models.podcast_profile import EnrichedPodcastProfile, EpisodeInfo, SocialProfileInfo
# Remove base_lead import, we now start from unified dict
# from ...models.lead import PodcastLead 

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s') # Set level to DEBUG and add formatter
logger = logging.getLogger(__name__)

class DataMerger:
    """Merges data from various enrichment sources into a standardized profile."""

    def __init__(self):
        """Initializes the DataMerger."""
        pass

    def _parse_duration(self, duration_str: Optional[str]) -> Optional[int]:
        """Parses various duration string formats (HH:MM:SS, MM:SS, seconds) into seconds."""
        if not duration_str:
            return None
        
        seconds = 0
        try:
            if ':' in duration_str:
                parts = list(map(int, duration_str.split(':')))
                if len(parts) == 3: # HH:MM:SS
                    seconds = parts[0] * 3600 + parts[1] * 60 + parts[2]
                elif len(parts) == 2: # MM:SS
                    seconds = parts[0] * 60 + parts[1]
                else:
                    logger.warning(f"Unexpected time format: {duration_str}")
                    return None
            else:
                # Assume it's just seconds
                seconds = int(duration_str)
            return seconds
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse duration string '{duration_str}': {e}")
            return None

    def merge_data(
        self,
        base_unified_data: Dict[str, Any], # Start from unified search result dict
        rss_data: Optional[Dict[str, Any]] = None,
        gemini_data: Optional[Dict[str, Any]] = None, 
        social_media_data: Optional[Dict[str, Any]] = None, # Holds structured data from Apify
        # Remove linkedin_data, twitter_data - subsumed into social_media_data
    ) -> Optional[EnrichedPodcastProfile]: # Return Optional in case of critical failure
        """
        Merges data from unified search result, RSS, Gemini, Social Media Scraping.

        Args:
            base_unified_data: The unified dictionary from the SearchAgent.
            rss_data: Parsed data from RssFeedParser.parse_feed.
            gemini_data: Dictionary containing host name & URLs discovered by Gemini.
            social_media_data: Dictionary containing structured data from Apify 
                               (e.g., {'twitter': {...}, 'host_linkedin': {...}}).

        Returns:
            An EnrichedPodcastProfile instance or None if base data is invalid.
        """
        if not base_unified_data or not isinstance(base_unified_data, dict):
             logger.error("Invalid base_unified_data provided to DataMerger. Cannot proceed.")
             return None
             
        podcast_id = base_unified_data.get('api_id') or f"unknown_{uuid.uuid4()}"
        logger.info(f"Starting data merge for podcast: {podcast_id} - {base_unified_data.get('title')}")
        
        # Initialize profile using the unified dictionary
        # Use `.get()` for safety, Pydantic model handles validation/defaults
        profile_data = {
            'source_api': base_unified_data.get('source_api'),
            'api_id': base_unified_data.get('api_id'),
            'title': base_unified_data.get('title'),
            'description': base_unified_data.get('description'),
            'image_url': base_unified_data.get('image_url'),
            'website': base_unified_data.get('website'),
            'language': base_unified_data.get('language'), # Might be overridden by RSS
            'podcast_spotify_id': base_unified_data.get('podcast_spotify_id'),
            'itunes_id': base_unified_data.get('itunes_id'),
            'total_episodes': base_unified_data.get('total_episodes'),
            'last_posted_at': base_unified_data.get('last_posted_at'), # Need parsing?
            'rss_feed_url': base_unified_data.get('rss_url'),
            # Metrics from initial search
            'listen_score': base_unified_data.get('listen_score'),
            'listen_score_global_rank': base_unified_data.get('listen_score_global_rank'),
            'audience_size': base_unified_data.get('audience_size'),
            'itunes_rating_average': base_unified_data.get('itunes_rating_average'),
            'itunes_rating_count': base_unified_data.get('itunes_rating_count'),
            'spotify_rating_average': base_unified_data.get('spotify_rating_average'),
            'spotify_rating_count': base_unified_data.get('spotify_rating_count'),
            # Base social URLs from search (might be overwritten by Gemini)
            'podcast_twitter_url': base_unified_data.get('twitter_url'),
            'podcast_linkedin_url': base_unified_data.get('linkedin_url'),
            'podcast_instagram_url': base_unified_data.get('instagram_url'),
            'podcast_facebook_url': base_unified_data.get('facebook_url'),
            'podcast_youtube_url': base_unified_data.get('youtube_url'),
            'podcast_tiktok_url': base_unified_data.get('tiktok_url'),
            'podcast_other_social_url': base_unified_data.get('other_social_url'),
        }

        # --- Pre-validation Cleanup --- 
        # Convert empty strings or clearly invalid URL strings to None 
        # for fields expected to be numeric or URL
        fields_to_clean = {
            # Numeric/Int fields where '' might appear
            'itunes_id': 'numeric', 
            'total_episodes': 'numeric',
            'listen_score': 'numeric',
            'audience_size': 'numeric',
            'itunes_rating_average': 'numeric',
            'itunes_rating_count': 'numeric',
            'spotify_rating_average': 'numeric',
            'spotify_rating_count': 'numeric',
            # URL fields where '' or invalid strings might appear
            'image_url': 'url',
            'website': 'url',
            'rss_feed_url': 'url',
            'podcast_twitter_url': 'url',
            'podcast_linkedin_url': 'url',
            'podcast_instagram_url': 'url',
            'podcast_facebook_url': 'url',
            'podcast_youtube_url': 'url',
            'podcast_tiktok_url': 'url',
            'podcast_other_social_url': 'url'
        }
        for field, field_type in fields_to_clean.items():
            value = profile_data.get(field)
            if value == '':
                profile_data[field] = None
                logger.debug(f"Converted empty string to None for field: {field}")
            elif field_type == 'url' and isinstance(value, str) and not value.startswith(('http://', 'https://')):
                logger.warning(f"Invalid string found for URL field '{field}': '{value}'. Setting to None.")
                profile_data[field] = None
            # Add elif for numeric if needed, e.g., checking if value is numeric string
                            
        # Convert specific fields (e.g., dates) if needed - Pydantic might handle some
        # Example: Convert last_posted_at string to datetime if needed
        if isinstance(profile_data.get('last_posted_at'), str):
            try:
                from dateutil import parser
                profile_data['last_posted_at'] = parser.parse(profile_data['last_posted_at'])
            except Exception:
                 logger.warning(f"Could not parse last_posted_at: {profile_data['last_posted_at']}")
                 profile_data['last_posted_at'] = None
                 
        # TODO: Add similar conversions for ratings (str to float/int) if needed
        # It might be better to handle this in the mappers or initial parsing
        # but the None conversion above should handle empty strings at least.

        # Attempt to create the base profile
        try:
            profile = EnrichedPodcastProfile(**profile_data)
            profile.data_sources = [f"search_{profile.source_api or 'unknown'}"] # Start with search source
        except Exception as pydantic_error:
             logger.error(f"Pydantic validation failed initializing profile for {podcast_id} from base data: {pydantic_error}")
             logger.debug(f"Base data causing validation error: {profile_data}")
             return None # Cannot proceed if base data fails validation

        # Ensure Gemini and Social Media data are dictionaries
        gemini_data = gemini_data or {}
        social_media_data = social_media_data or {}
        rss_data = rss_data or {} # Ensure rss_data is a dict for .get

        # --- Merge RSS Data --- #
        feed_info = rss_data.get('feed_info')
        if feed_info and isinstance(feed_info, dict):
            logger.debug("Merging RSS feed data...")
            profile.data_sources.append('rss')
            owner_info = feed_info.get('author_detail') or {}
            profile.rss_owner_name = owner_info.get('name') 
            profile.rss_owner_email = owner_info.get('email')
            profile.rss_explicit = feed_info.get('itunes_explicit')
            profile.language = feed_info.get('language') or profile.language
            profile.website = feed_info.get('link') or profile.website 
            if isinstance(feed_info.get('tags'), list):
                 profile.rss_categories = [tag.get('term') for tag in feed_info['tags'] if isinstance(tag, dict) and tag.get('term')]
                 
            # --- Process Episodes from RSS (PAUSED) --- #
            # if isinstance(rss_data.get('episodes'), list):
            #     episodes = rss_data['episodes']
            #     if not profile.total_episodes: # Fill if missing from API
            #         profile.total_episodes = len(episodes)
            #     profile.recent_episodes = []
            #     valid_dates = []
            #     total_duration_seconds = 0
            #     valid_durations = 0
            #     for ep_data in episodes[:10]: 
            #         # ... (episode parsing logic) ...
            #         profile.recent_episodes.append(episode_info)
            #     if valid_dates:
            #         valid_dates.sort()
            #         profile.first_episode_date = valid_dates[0]
            #         profile.latest_episode_date = valid_dates[-1]
            #     if valid_durations > 0:
            #         profile.average_duration_seconds = total_duration_seconds / valid_durations
            # --- End Paused Episode Processing --- #
        
        # --- Merge Gemini Data --- #
        if gemini_data:
            logger.debug("Merging Gemini discovery data...")
            # Only add host names if not already present?
            if gemini_data.get('host_names') and not profile.host_names: 
                 profile.host_names = gemini_data['host_names']
                 profile.data_sources.append('gemini_host')
            
            # Social URLs (Only fill if missing from initial data)
            if gemini_data.get('podcast_twitter_url') and not profile.podcast_twitter_url:
                 profile.podcast_twitter_url = gemini_data['podcast_twitter_url']
                 # Don't add gemini source here, source determined by which URL was *used* for scraping
            if gemini_data.get('host_linkedin_url') and not profile.host_linkedin_url:
                 profile.host_linkedin_url = gemini_data['host_linkedin_url']
            # Add checks for other Gemini URL fields if needed (podcast_linkedin, host_twitter etc.)
            # Ensure you check `not profile.<field_name>` before assigning

        # --- Merge Social Media Scraped Data --- #
        if social_media_data:
             logger.debug("Merging social media scraped data...")
             # --- Twitter Podcast Page Data --- 
             podcast_twitter_data = social_media_data.get('podcast_twitter') # Key matches EnrichmentAgent
             if isinstance(podcast_twitter_data, dict):
                  # --- Remove Debug Logging (or keep if needed) --- #
                  # followers_val = podcast_twitter_data.get('followers_count') or podcast_twitter_data.get('followersCount') or podcast_twitter_data.get('followers')
                  # following_val = podcast_twitter_data.get('following_count') or podcast_twitter_data.get('followingCount') or podcast_twitter_data.get('following')
                  # verified_val = podcast_twitter_data.get('is_verified') or podcast_twitter_data.get('verified')
                  # logger.debug(f"Merging Twitter data for {profile.api_id}. Raw Apify Data: {podcast_twitter_data}. Extracted Followers: {followers_val}, Following: {following_val}, Verified: {verified_val}")
                  
                  # --- MODIFIED: Directly access standardized keys from the processed Apify data --- #
                  profile.twitter_followers = podcast_twitter_data.get('followers_count') # Key used in SocialMediaDiscovery extraction
                  profile.twitter_following = podcast_twitter_data.get('following_count') # Key used in SocialMediaDiscovery extraction
                  profile.is_twitter_verified = podcast_twitter_data.get('is_verified') # Key used in SocialMediaDiscovery extraction
                  
                  profile.data_sources.append('apify_podcast_twitter')
             
             # --- LinkedIn Host Profile Data --- 
             host_linkedin_data = social_media_data.get('host_linkedin') # Key matches EnrichmentAgent
             if isinstance(host_linkedin_data, dict):
                  # --- Add Debug Logging --- #
                  followers_li = host_linkedin_data.get('followers_count')
                  connections_li = host_linkedin_data.get('connections_count')
                  logger.debug(f"Merging LinkedIn data for {profile.api_id}. Raw Apify Data: {host_linkedin_data}. Extracted Followers: {followers_li}, Connections: {connections_li}")
                  # --- End Debug Logging --- #
                  
                  # --- MODIFIED: Directly access standardized keys --- #
                  # Prioritize followers_count if available from SocialMediaDiscovery extraction
                  profile.linkedin_connections = host_linkedin_data.get('followers_count') if host_linkedin_data.get('followers_count') is not None else host_linkedin_data.get('connections_count') 
                  
                  profile.data_sources.append('apify_host_linkedin')
             
             # --- Instagram Podcast Page Data (NEW) ---
             podcast_instagram_data = social_media_data.get('podcast_instagram')
             if isinstance(podcast_instagram_data, dict):
                 logger.debug(f"Merging Instagram data for {profile.api_id}. Raw Apify Data: {podcast_instagram_data}")
                 scraped_ig_url = podcast_instagram_data.get('profile_url')
                 if scraped_ig_url:
                     profile.podcast_instagram_url = scraped_ig_url 
                 
                 profile.instagram_followers = podcast_instagram_data.get('followers_count') 
                 profile.data_sources.append('apify_podcast_instagram')

             # --- TikTok Podcast Page Data (NEW) ---
             podcast_tiktok_data = social_media_data.get('podcast_tiktok')
             if isinstance(podcast_tiktok_data, dict):
                 logger.debug(f"Merging TikTok data for {profile.api_id}. Raw Apify Data: {podcast_tiktok_data}")
                 # The social_discovery method should return a dictionary where 'profile_url' is the canonical channel URL
                 # and 'followers_count' is the extracted follower count.
                 scraped_tt_url = podcast_tiktok_data.get('profile_url') 
                 if scraped_tt_url:
                     profile.podcast_tiktok_url = scraped_tt_url # Update with the canonical URL from scraper
                 
                 # 'followers_count' should already be an int due to _safe_int_cast in social_discovery
                 profile.tiktok_followers = podcast_tiktok_data.get('followers_count') 
                 profile.data_sources.append('apify_podcast_tiktok')

             # --- Facebook Podcast Page Data (NEW) ---
             podcast_facebook_data = social_media_data.get('podcast_facebook')
             if isinstance(podcast_facebook_data, dict):
                 logger.debug(f"Merging Facebook data for {profile.api_id}. Raw Apify Data: {podcast_facebook_data}")
                 scraped_fb_url = podcast_facebook_data.get('profile_url')
                 if scraped_fb_url:
                     profile.podcast_facebook_url = scraped_fb_url
                 
                 profile.facebook_likes = podcast_facebook_data.get('likes_count') 
                 profile.data_sources.append('apify_podcast_facebook')

             # --- YouTube Podcast Channel Data (NEW) ---
             podcast_youtube_data = social_media_data.get('podcast_youtube')
             if isinstance(podcast_youtube_data, dict):
                 logger.debug(f"Merging YouTube data for {profile.api_id}. Raw Apify Data: {podcast_youtube_data}")
                 scraped_yt_url = podcast_youtube_data.get('channel_url') # Or 'profile_url'
                 if scraped_yt_url:
                     profile.podcast_youtube_url = scraped_yt_url
                 
                 profile.youtube_subscribers = podcast_youtube_data.get('subscriber_count') 
                 profile.data_sources.append('apify_podcast_youtube')

        # --- Final Processing --- #
        # Determine Primary Email (Prioritize RSS Owner)
        profile.primary_email = profile.rss_owner_email or base_unified_data.get('email')

        # Update timestamp
        profile.last_enriched_timestamp = datetime.utcnow()

        # Finalize data sources (remove duplicates, sort?)
        if profile.data_sources:
            profile.data_sources = sorted(list(set(profile.data_sources)))

        # --- NEW: Populate date/frequency fields from base_unified_data --- 
        # Convert ms timestamps to datetime objects
        try:
            latest_ms = base_unified_data.get('latest_pub_date_ms')
            if latest_ms and isinstance(latest_ms, (int, float)):
                profile.latest_episode_date = datetime.fromtimestamp(latest_ms / 1000.0)
            else:
                # Keep any value already set (e.g., from RSS if logic existed)
                profile.latest_episode_date = profile.latest_episode_date 
                
            earliest_ms = base_unified_data.get('earliest_pub_date_ms')
            if earliest_ms and isinstance(earliest_ms, (int, float)):
                profile.first_episode_date = datetime.fromtimestamp(earliest_ms / 1000.0)
            else:
                profile.first_episode_date = profile.first_episode_date
                
        except (ValueError, OSError, TypeError) as date_err:
            logger.warning(f"Error converting ms timestamps for {podcast_id}: {date_err}")
            # Keep profile dates as they are if conversion fails

        # Convert update frequency hours to days
        try:
            freq_hours = base_unified_data.get('update_frequency_hours')
            if freq_hours and isinstance(freq_hours, (int, float)) and freq_hours > 0:
                profile.publishing_frequency_days = freq_hours / 24.0
            else:
                 profile.publishing_frequency_days = profile.publishing_frequency_days # Keep existing if any
        except TypeError as freq_err:
             logger.warning(f"Error converting frequency hours for {podcast_id}: {freq_err}")
             # Keep profile frequency as it is if conversion fails
        # --- END NEW --- 

        logger.info(f"Data merge completed for {podcast_id}. Sources: {profile.data_sources}")
        return profile

# Example Usage (placeholder - needs updating)
# ... (Keep the old example commented out or update it later) ...

# Example Usage (placeholder - requires actual data from previous steps)
if __name__ == "__main__":
    merger = DataMerger()

    # Create dummy input data
    dummy_unified_data = {
        'api_id': 'test_123',
        'title': 'My Test Podcast',
        'description': 'Initial description.',
        'image_url': 'http://example.com/image.jpg',
        'website': 'http://example.com',
        'language': 'English',
        'podcast_spotify_id': 'spotify:podcast:123456789',
        'itunes_id': '123456789',
        'total_episodes': 10,
        'last_posted_at': '2023-01-01',
        'rss_url': 'http://rss.example.com',
        'listen_score': 85.5,
        'listen_score_global_rank': 1234,
        'audience_size': 100000,
        'itunes_rating_average': 4.5,
        'itunes_rating_count': 1000,
        'spotify_rating_average': 4.7,
        'spotify_rating_count': 5000,
        'twitter_url': 'https://twitter.com/testpodcast',
        'linkedin_url': 'https://linkedin.com/in/testhost',
        'instagram_url': 'https://instagram.com/testpodcast',
        'facebook_url': 'https://facebook.com/testpodcast',
        'youtube_url': 'https://youtube.com/testpodcast',
        'tiktok_url': 'https://tiktok.com/@testpodcast',
        'other_social_url': 'https://example.com/testpodcast'
    }
    
    dummy_rss = {
        'feed_info': {'title': 'RSS Title', 'link': 'http://rss.example.com', 'description': 'RSS Description.', 'author': 'RSS Author'},
        'episodes': [
            {'id': 'ep1', 'title': 'Episode 1', 'published_parsed': (2023, 1, 15, 10, 0, 0, 0, 15, 0), 'duration': '1805'},
            {'id': 'ep2', 'title': 'Episode 2', 'published_parsed': (2023, 1, 22, 10, 0, 0, 0, 22, 0), 'duration': '00:35:10'}
        ]
    }

    dummy_gemini = {
        'host_name': 'My Test Podcast',
        'podcast_twitter_url': 'https://twitter.com/testpodcast'
    }
    
    dummy_social_media = {
        'twitter': {
            'url': 'https://twitter.com/testpodcast',
            'followers_count': 1200,
            'description': 'Host of My Test Podcast'
        },
        'host_linkedin': {
            'followersCount': 500,
            'headline': 'Podcast Host at My Test Podcast'
        },
        'podcast_instagram': {
            'profile_url': 'https://instagram.com/testpodcast',
            'followers_count': 1000
        },
        'podcast_tiktok': {
            'profile_url': 'https://tiktok.com/@testpodcast',
            'followers_count': 500
        },
        'podcast_facebook': {
            'profile_url': 'https://facebook.com/testpodcast',
            'likes_count': 2000
        },
        'podcast_youtube': {
            'channel_url': 'https://youtube.com/testpodcast',
            'subscriber_count': 10000
        }
    }

    # Perform merge
    enriched_profile = merger.merge_data(
        base_unified_data=dummy_unified_data,
        rss_data=dummy_rss,
        gemini_data=dummy_gemini,
        social_media_data=dummy_social_media
    )

    # Print the result (using model_dump for cleaner output)
    print("\n--- Enriched Profile ---")
    print(enriched_profile.model_dump_json(indent=2)) 