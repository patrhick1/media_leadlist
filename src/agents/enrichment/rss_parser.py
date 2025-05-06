import feedparser
import logging
from typing import Optional, Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RssFeedParser:
    """Parses RSS feeds to extract podcast and episode metadata."""

    def __init__(self):
        """Initializes the RssFeedParser."""
        # Potentially initialize with configuration or session later
        pass

    def parse_feed(self, feed_url: str) -> Optional[Dict[str, Any]]:
        """
        Fetches and parses an RSS feed from the given URL.

        Args:
            feed_url: The URL of the RSS feed.

        Returns:
            A dictionary containing parsed feed data, or None if parsing fails.
            The structure might include:
            {
                'feed_info': { 'title': ..., 'link': ..., 'description': ..., ... },
                'episodes': [ { 'title': ..., 'published': ..., 'summary': ..., 'duration': ..., ... }, ... ]
            }
        """
        logger.info(f"Attempting to parse feed from URL: {feed_url}")
        try:
            # Fetch and parse the feed using feedparser
            # feedparser handles fetching via HTTP internally
            parsed_feed = feedparser.parse(feed_url)

            # Check for basic parsing errors indicated by feedparser
            if parsed_feed.bozo:
                # bozo=1 indicates a potential issue (e.g., not well-formed XML)
                bozo_exception = parsed_feed.get('bozo_exception', 'Unknown parsing issue')
                logger.warning(f"Feed at {feed_url} may be ill-formed. Bozo exception: {bozo_exception}")
                # Decide if we want to proceed despite potential issues or return None/raise
                # For now, we log a warning but proceed if basic data exists

            # Check if essential feed data is present
            if not parsed_feed.feed or not parsed_feed.entries:
                logger.error(f"Failed to extract essential feed or entry data from {feed_url}. Feed object: {parsed_feed.feed}")
                return None

            # --- Extract Feed Level Information --- #
            feed_info = {
                'title': parsed_feed.feed.get('title'),
                'link': parsed_feed.feed.get('link'),
                'subtitle': parsed_feed.feed.get('subtitle'),
                'description': parsed_feed.feed.get('summary') or parsed_feed.feed.get('description'),
                'author': parsed_feed.feed.get('author'),
                'language': parsed_feed.feed.get('language'),
                'image_url': parsed_feed.feed.get('image', {}).get('href'),
                # Add more fields as needed (e.g., categories, owner)
            }
            logger.debug(f"Extracted feed info for {feed_url}: {feed_info}")

            # --- Extract Episode Information --- #
            episodes: List[Dict[str, Any]] = []
            for entry in parsed_feed.entries:
                episode_data = {
                    'id': entry.get('id'),
                    'title': entry.get('title'),
                    'link': entry.get('link'),
                    'published': entry.get('published_parsed'), # feedparser provides parsed datetime tuple
                    'summary': entry.get('summary') or entry.get('description'),
                    'duration': entry.get('itunes_duration'), # Often in HH:MM:SS or seconds
                    'audio_url': next((link.href for link in entry.get('links', []) if link.rel == 'enclosure'), None),
                    # Add more episode fields (e.g., episode number, content)
                }
                episodes.append(episode_data)

            logger.info(f"Successfully parsed {len(episodes)} episodes from {feed_url}")

            return {
                'feed_info': feed_info,
                'episodes': episodes
            }

        except Exception as e:
            # Catch any other unexpected errors during parsing/fetching
            logger.error(f"An unexpected error occurred while parsing feed {feed_url}: {e}", exc_info=True)
            return None

    # TODO: Add methods for content pattern analysis (step 5 from task details)
    # def analyze_publishing_frequency(self, episodes: List[Dict[str, Any]]) -> Optional[str]:
    #     pass
    #
    # def analyze_average_duration(self, episodes: List[Dict[str, Any]]) -> Optional[float]:
    #     pass

# Example Usage (for testing)
if __name__ == "__main__":
    parser = RssFeedParser()
    # Replace with a real podcast RSS feed URL for testing
    test_url = "https://feeds.simplecast.com/54nAGcIl" # Example: PodRocket
    # test_url = "https://feeds.megaphone.fm/darknetdiaries"

    parsed_data = parser.parse_feed(test_url)

    if parsed_data:
        print("\n--- Feed Info ---")
        for key, value in parsed_data['feed_info'].items():
            print(f"{key}: {value}")

        print(f"\n--- Episodes ({len(parsed_data['episodes'])}) ---")
        # Print details of the first few episodes
        for i, episode in enumerate(parsed_data['episodes'][:3]):
            print(f"\nEpisode {i+1}:")
            for key, value in episode.items():
                print(f"  {key}: {value}")
    else:
        print(f"\nFailed to parse feed: {test_url}") 