import os
import logging
import asyncio
from apify_client import ApifyClient
from dotenv import load_dotenv
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse
import re # Add re import
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class SocialMediaDiscovery:
    """Discovers and analyzes social media profiles using Apify."""

    def __init__(self):
        """Initializes the Apify client."""
        self.api_key = os.getenv("APIFY_API_KEY")
        if not self.api_key:
            logger.error("APIFY_API_KEY not found in environment variables.")
            raise ValueError("APIFY_API_KEY must be set in .env file")
        
        try:
            self.client = ApifyClient(self.api_key)
            logger.info("ApifyClient initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize ApifyClient: {e}", exc_info=True)
            raise

    async def _run_actor_async(self, actor_id: str, run_input: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """Runs a specific Apify actor asynchronously and retrieves its dataset items."""
        logger.info(f"Running Apify actor asynchronously: {actor_id}")
        # logger.debug(f"Actor Input: {run_input}") # Be careful logging sensitive inputs
        try:
            # Run the actor asynchronously
            # Note: actor().call() is synchronous, we run it in a thread pool
            actor_call = await asyncio.to_thread(
                self.client.actor(actor_id).call, 
                run_input=run_input
            )
            logger.info(f"Actor run completed. Run ID: {actor_call.get('id')}, Dataset ID: {actor_call.get('defaultDatasetId')}")

            if not actor_call or not actor_call.get("defaultDatasetId"):
                logger.warning(f"Invalid response or missing dataset ID from actor {actor_id}.")
                return None

            # Fetch the results from the dataset asynchronously
            dataset = self.client.dataset(actor_call["defaultDatasetId"])
            # dataset.list_items() is sync, run in thread
            dataset_page = await asyncio.to_thread(dataset.list_items)
            
            # For simplicity, we'll just take items from the first page. 
            # TODO: Implement proper pagination if needed for actors returning many items.
            dataset_items = dataset_page.items if dataset_page else []
            
            # --- NEW DEBUG LOGGING: Print full raw dataset items to examine structure --- #
            logger.debug(
                "Raw dataset items retrieved from actor %s (type=%s): %s",
                actor_id,
                type(dataset_items).__name__,
                dataset_items,
            )
            
            logger.info(f"Retrieved {len(dataset_items)} items from dataset for actor {actor_id}.")
            return dataset_items
        except Exception as e:
            logger.error(f"Apify actor {actor_id} async run failed: {e}", exc_info=True)
            return None

    def _extract_username_from_twitter_url(self, url: str) -> Optional[str]:
        """Extracts the username from a Twitter/X URL."""
        if not isinstance(url, str):
            return None
        try:
            # Handle both twitter.com and x.com
            parsed_url = urlparse(url)
            if parsed_url.netloc.lower() in ['twitter.com', 'x.com', 'www.twitter.com', 'www.x.com']:
                # Path should be /username or /username/...
                path_parts = parsed_url.path.strip('/').split('/')
                if path_parts and path_parts[0]:
                    # Basic check for valid username characters (alphanumeric + underscore)
                    if re.match(r'^[A-Za-z0-9_]+$', path_parts[0]):
                         return path_parts[0]
        except Exception as e:
            logger.warning(f"Could not parse username from Twitter URL '{url}': {e}")
        return None

    def _canonicalize_twitter_url(self, url: str) -> str:
        """Returns a canonical https://twitter.com/<username> form for any Twitter/X URL."""
        if not isinstance(url, str) or not url:
            return url
        # Ensure scheme is present and https
        url = url.strip()
        if url.startswith("http://"):
            url = "https://" + url[len("http://"):]
        if url.startswith("www."):
            url = "https://" + url
        # Replace x.com variants with twitter.com
        url = re.sub(r"https://(?:www\.)?x\.com/", "https://twitter.com/", url, flags=re.IGNORECASE)
        url = re.sub(r"https://(?:www\.)?twitter\.com/", "https://twitter.com/", url, flags=re.IGNORECASE)
        # Strip query/fragment and trailing slash
        url = url.split("?")[0].split("#")[0].rstrip("/")
        return url

    # --- New URL-based Methods --- 
    async def get_linkedin_data_for_urls_batch(self, profile_urls: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """Extracts LinkedIn profile info for a batch of URLs using a single Apify run."""
        if not profile_urls:
            logger.info("No LinkedIn profile URLs provided for batch scraping.")
            return {}
            
        valid_urls = list(set(filter(lambda u: isinstance(u, str) and u.startswith('http'), profile_urls)))
        if not valid_urls:
            logger.info("No valid LinkedIn profile URLs after filtering.")
            return {url: None for url in profile_urls}
            
        logger.info(f"Fetching LinkedIn data for {len(valid_urls)} unique URLs in a batch...")
        
        linkedin_actor_id = 'supreme_coder/linkedin-profile-scraper' 
        
        # Format URLs for the actor input
        run_input_urls = [{"url": url, "method": "GET"} for url in valid_urls]
        
        run_input = {
            "findContacts": False,
            "scrapeCompany": False,
            "urls": run_input_urls
        }
        
        actor_results = await self._run_actor_async(linkedin_actor_id, run_input)
        
        results_by_url: Dict[str, Optional[Dict[str, Any]]] = {url: None for url in profile_urls}
        urls_found_by_actor = set()

        if actor_results and isinstance(actor_results, list):
            for item in actor_results:
                if not isinstance(item, dict):
                    logger.warning(f"Skipping non-dict item in LinkedIn Apify results: {item}")
                    continue
                
                input_url = item.get('inputUrl') # Key from example output
                if not input_url:
                    logger.warning(f"Skipping LinkedIn Apify result item missing inputUrl: {item}")
                    continue
                
                urls_found_by_actor.add(input_url)
                
                # Extract only the needed fields
                extracted_data = {
                    'profile_url': item.get('inputUrl'), # Use inputUrl as the reliable URL
                    'headline': item.get('headline') or item.get('occupation'), # Use occupation as fallback
                    'summary': item.get('summary'),
                    'followers_count': item.get('followersCount'),
                    'connections_count': item.get('connectionsCount')
                }
                # Store result keyed by the original input URL
                results_by_url[input_url] = extracted_data
                logger.debug(f"Mapped LinkedIn Apify result for '{input_url}'")

        # Log which URLs we requested but didn't get results for
        missing_urls = set(valid_urls) - urls_found_by_actor
        if missing_urls:
             logger.warning(f"Apify actor '{linkedin_actor_id}' did not return results for URLs: {missing_urls}")
             # The corresponding URLs in results_by_url will remain None

        logger.info(f"Finished batch LinkedIn data fetch. Returning results for {len(results_by_url)} original URLs.")
        return results_by_url

    async def get_twitter_data_for_urls_batch(self, twitter_urls: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """Fetches Twitter user data for a list of Twitter URLs using Apify batch processing."""
        if not self.client:
            logger.error("Apify client not initialized in SocialMediaDiscovery. Cannot fetch Twitter data.")
            return {url: None for url in twitter_urls}

        if not twitter_urls:
            logger.info("No Twitter URLs provided to fetch data for.")
            return {}
        
        logger.info(f"Fetching Twitter data for {len(twitter_urls)} unique URLs in a batch...")

        # Normalize URLs and extract usernames
        # Use _canonicalize_twitter_url for consistency with Twitter/X domains
        original_urls_set = {self._canonicalize_twitter_url(url) for url in twitter_urls if url} 
        usernames_to_fetch = []
        # url_to_username_map now maps original canonical URL to username
        url_to_username_map: Dict[str, Optional[str]] = {}

        for norm_url in original_urls_set:
            username = self._extract_username_from_twitter_url(norm_url) # _extract_username should work on canonical form
            if username:
                usernames_to_fetch.append(username) # Build list of unique usernames for actor input
                url_to_username_map[norm_url] = username # Map canonical URL to its username
            else:
                logger.warning(f"Could not extract username for Twitter URL (after canonicalization): {norm_url}")
        
        # Deduplicate usernames in case multiple canonical URLs resolved to the same username (e.g. http/https, x/twitter)
        # However, actor now takes URLs, so this is less critical, but good for logging.
        unique_usernames_for_logging = sorted(list(set(usernames_to_fetch)))
        if not unique_usernames_for_logging: # Check if we have any valid usernames to proceed with
            logger.info("No valid Twitter usernames extracted after canonicalization. Skipping Apify call.")
            return {url: None for url in original_urls_set}

        actor_name = "apidojo/twitter-user-scraper"
        results_by_url: Dict[str, Optional[Dict[str, Any]]] = {url: None for url in original_urls_set}

        urls_for_actor_run = list(original_urls_set) # Actor takes the set of canonical URLs

        padding_profile_urls = [
            self._canonicalize_twitter_url("https://twitter.com/nasa"), # Normalize padding URLs too
            self._canonicalize_twitter_url("https://twitter.com/bbcworld"),
            self._canonicalize_twitter_url("https://twitter.com/github"),
            self._canonicalize_twitter_url("https://twitter.com/teslamotors"),
            self._canonicalize_twitter_url("https://twitter.com/apify")
        ]
        MIN_URLS_FOR_ACTOR = 5
        actual_unique_urls_count = len(urls_for_actor_run)
        exclusively_added_for_padding = set() # Keep track of URLs added ONLY for padding

        if actual_unique_urls_count < MIN_URLS_FOR_ACTOR:
            needed_padding = MIN_URLS_FOR_ACTOR - actual_unique_urls_count
            padding_to_add_urls = []
            for pad_url in padding_profile_urls:
                if len(padding_to_add_urls) >= needed_padding:
                    break
                # pad_url is already canonicalized from padding_profile_urls definition
                if pad_url not in urls_for_actor_run: 
                    padding_to_add_urls.append(pad_url)
                    exclusively_added_for_padding.add(pad_url) # Mark as added for padding
            
            urls_for_actor_run.extend(padding_to_add_urls)
            while len(urls_for_actor_run) < MIN_URLS_FOR_ACTOR and padding_profile_urls:
                last_pad_url = padding_profile_urls[-1]
                urls_for_actor_run.append(last_pad_url) 
                # If we repeat a padding URL, it's still considered padding-only if not in original set
                if last_pad_url not in original_urls_set: # original_urls_set contains user's initial URLs
                    exclusively_added_for_padding.add(last_pad_url)

            logger.info(f"Padded Twitter startUrls list from {actual_unique_urls_count} to {len(urls_for_actor_run)}. Original unique count: {actual_unique_urls_count}")
        
        run_input = {
            "startUrls": urls_for_actor_run,
            "getFollowers": True,
            "getFollowing": False, 
            "getRetweeters": False,
            "includeUnavailableUsers": False,
            "maxItems": len(urls_for_actor_run)
        }

        logger.info(f"Using Twitter actor '{actor_name}'. Final startUrls count: {len(urls_for_actor_run)}. Example URLs: {urls_for_actor_run[:7]}...")

        try:
            actor_instance = self.client.actor(actor_name)
            actor_run_details = await asyncio.to_thread(
                actor_instance.call, 
                run_input=run_input
            )

            if not actor_run_details or 'defaultDatasetId' not in actor_run_details:
                logger.error(f"Apify actor '{actor_name}' call failed or did not return a dataset ID.")
                return results_by_url

            dataset_id = actor_run_details['defaultDatasetId']
            items_list = list(self.client.dataset(dataset_id).iterate_items())
            logger.info(f"Retrieved {len(items_list)} profile items for '{actor_name}' from dataset {dataset_id}.")

            if not items_list:
                logger.warning(f"Apify actor '{actor_name}' returned no items for startUrls: {urls_for_actor_run}")
                return results_by_url

            for item in items_list:
                item_canonical_url = self._canonicalize_twitter_url(item.get('url') or item.get('profile_url') or item.get('twitterUrl'))
                item_username = item.get('username') or item.get('screenName') or item.get('userName')
                
                # Skip actor items that are completely empty for matching
                if not item_canonical_url and not item_username:
                    logger.debug("TWITTER ITEM IGNORED: Actor item has no usable URL and no username.")
                    continue

                logger.debug(f"TWITTER ITEM PROCESSING: Actor item URL='{item.get('url')}', Canonicalized='{item_canonical_url}', Username='{item_username}'")
                # logger.debug(f"  Original URLs Set (for matching): {original_urls_set}") # Can be verbose

                matched_original_url = None
                if item_canonical_url and item_canonical_url in original_urls_set:
                    matched_original_url = item_canonical_url
                    logger.debug(f"  Matched by canonical URL: {matched_original_url}")
                elif item_username:
                    normalized_item_username_lower = item_username.lower()
                    for orig_norm_url_key, orig_username_val in url_to_username_map.items():
                        if orig_username_val and orig_username_val.lower() == normalized_item_username_lower:
                            # Ensure this orig_norm_url_key is from the original user-provided set, not just any padding URL
                            if orig_norm_url_key in original_urls_set:
                                matched_original_url = orig_norm_url_key
                                logger.debug(f"  Matched by username '{normalized_item_username_lower}' to original URL: {matched_original_url}")
                                break 
                
                # Process if matched AND the matched_original_url was NOT one added *exclusively* for padding
                if matched_original_url and matched_original_url not in exclusively_added_for_padding:
                    if results_by_url.get(matched_original_url) is None: # Process each original URL only once
                        logger.debug(f"TWITTER RAW ITEM for {matched_original_url}: {json.dumps(item, indent=2)}") 
                        profile_details = {
                            'profile_url': item_canonical_url,
                            'username': item_username,
                            'name': item.get('name'),
                            'description': item.get('description') or item.get('rawDescription'),
                            'followers_count': self._safe_int_cast(item.get('followers_count') or item.get('followers')),
                            'following_count': self._safe_int_cast(item.get('following_count') or item.get('following')),
                            'is_verified': item.get('isVerified') or item.get('verified') or item.get('isBlueVerified'),
                            'location': item.get('location'),
                            'profile_picture_url': item.get('profile_image_url_https') or item.get('profilePicture')
                        }
                        results_by_url[matched_original_url] = profile_details
                        logger.info(f"Stored Twitter profile for '{item_username}' (orig: '{matched_original_url}'). Followers: {profile_details.get('followers_count')}")
                    else:
                        logger.debug(f"Already processed data for original URL: {matched_original_url} (Username: {item_username})")
                elif matched_original_url and matched_original_url in exclusively_added_for_padding:
                    logger.debug(f"Ignoring data from actor item because it matched an exclusively padded URL: {matched_original_url}")
                else:
                    # This log means the item from Apify couldn't be matched to any of OUR original URLs
                    # (even if it was a valid twitter profile, it wasn't one we asked for originally)
                    logger.debug(f"Could not map item from '{actor_name}' (Item URL: '{item_canonical_url}', Item User: '{item_username}') to any original, non-padded input URL.")
            
            # Log any original URLs (non-padding) for which no data was ultimately mapped
            for orig_url in original_urls_set:
                if orig_url not in padding_profile_urls and results_by_url.get(orig_url) is None:
                    logger.warning(f"No profile data ultimately mapped by '{actor_name}' for original input URL: {orig_url}")

        except Exception as e:
            logger.error(f"Error in get_twitter_data_for_urls_batch running actor '{actor_name}': {e}", exc_info=True)

        logger.info(f"Finished batch Twitter data fetch using '{actor_name}'. Returning results for {len(original_urls_set)} original URLs.")
        return results_by_url

    async def get_linkedin_data_from_url(self, profile_url: str) -> Optional[Dict[str, Any]]:
        """DEPRECATED: Use get_linkedin_data_for_urls_batch instead.
           Extracts specific LinkedIn profile info using Apify given a single profile URL.
        """
        logger.warning("get_linkedin_data_from_url is deprecated. Use get_linkedin_data_for_urls_batch.")
        batch_result = await self.get_linkedin_data_for_urls_batch([profile_url])
        return batch_result.get(profile_url)

    async def get_twitter_data_from_url(self, profile_url: str) -> Optional[Dict[str, Any]]:
        """DEPRECATED: Use get_twitter_data_for_urls_batch instead. 
           Extracts specific Twitter profile info using Apify given a single profile URL.
        """
        logger.warning("get_twitter_data_from_url is deprecated. Use get_twitter_data_for_urls_batch.")
        batch_result = await self.get_twitter_data_for_urls_batch([profile_url])
        return batch_result.get(profile_url)

    # --- Deprecated Search-based Methods (Commented Out) ---
    # def find_linkedin_profile(self, search_query: str) -> Optional[Dict[str, Any]]:
    #     ...
    # def find_twitter_profile(self, search_query: str) -> Optional[Dict[str, Any]]:
    #     ...

    def _extract_username_from_linkedin_url(self, url: str) -> Optional[str]:
        # ... existing code ...
        return None

    def _extract_username_from_instagram_url(self, url: str) -> Optional[str]:
        """Extracts the username from an Instagram profile URL."""
        if not url:
            return None
        # Normalize URL first (optional, but good practice)
        url = self._normalize_url(url) # Assuming a generic normalizer or create one
        
        # Regex to capture username from various Instagram URL formats
        # Handles: instagram.com/username, instagram.com/username/, instagram.com/p/postid (not this one), etc.
        # Focus on profile URLs: instagram.com/username
        match = re.search(r"instagram\.com/([a-zA-Z0-9_.]+)/?", url)
        if match:
            username = match.group(1)
            # Avoid capturing 'p', 'reel', 'tv' etc. as usernames if they are path segments
            if username in ['p', 'reel', 'tv', 'explore', 'accounts', 'stories']:
                logger.debug(f"Extracted segment '{username}' looks like a path, not a username, from URL: {url}")
                return None
            logger.debug(f"Extracted Instagram username: {username} from URL: {url}")
            return username
        logger.warning(f"Could not extract Instagram username from URL: {url}")
        return None

    async def get_instagram_data_for_urls_batch(self, instagram_urls: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Fetches Instagram profile data for a list of Instagram URLs using Apify.
        Specifically targets 'apify/instagram-profile-scraper'.
        """
        if not self.client:
            logger.error("Apify client not initialized in SocialMediaDiscovery. Cannot fetch Instagram data.")
            return {url: None for url in instagram_urls}

        if not instagram_urls:
            logger.info("No Instagram URLs provided to fetch data for.")
            return {}

        logger.info(f"Fetching Instagram data for {len(instagram_urls)} unique URLs in a batch...")
        
        unique_normalized_urls = {self._normalize_url(url) for url in instagram_urls if url}
        usernames_to_fetch = []
        url_to_username_map = {}

        for norm_url in unique_normalized_urls:
            username = self._extract_username_from_instagram_url(norm_url)
            if username:
                usernames_to_fetch.append(username)
                url_to_username_map[username] = norm_url
            else:
                logger.warning(f"Could not extract username for Instagram URL: {norm_url}")
        
        if not usernames_to_fetch:
            logger.info("No valid Instagram usernames extracted. Skipping Apify call.")
            return {url: None for url in unique_normalized_urls}

        # Remove duplicate usernames before sending to Apify, though url_to_username_map handles the first URL for a username
        unique_usernames = sorted(list(set(usernames_to_fetch)))
        
        actor_input = {"usernames": unique_usernames}
        actor_name = "apify/instagram-profile-scraper"
        results_by_url: Dict[str, Optional[Dict[str, Any]]] = {url: None for url in unique_normalized_urls}

        try:
            logger.info(f"Running Apify actor '{actor_name}' with {len(unique_usernames)} unique usernames: {unique_usernames}")
            # Corrected actor call pattern
            actor_instance = self.client.actor(actor_name)
            actor_run_details = await asyncio.to_thread(
                actor_instance.call, 
                run_input=actor_input
            )

            if not actor_run_details or 'defaultDatasetId' not in actor_run_details:
                logger.error(f"Apify actor '{actor_name}' call failed or did not return a dataset ID.")
                return results_by_url # All values will be None

            dataset_id = actor_run_details['defaultDatasetId']
            logger.info(f"Apify actor '{actor_name}' run complete. Dataset ID: {dataset_id}. Fetching items...")
            
            dataset = self.client.dataset(dataset_id)
            # Paginate through dataset items if necessary (though for profiles, often one item per username)
            # For simplicity, assuming a reasonable number of results for now.
            # Apify's list().items might be an iterator or a list.
            items_list = list(dataset.iterate_items()) 
            
            logger.info(f"Retrieved {len(items_list)} items from dataset for actor {actor_name}.")

            if not items_list:
                logger.warning(f"Apify actor '{actor_name}' run for usernames {unique_usernames} returned no items.")
                return results_by_url

            for item in items_list:
                item_username = item.get('username')
                if item_username and item_username in url_to_username_map:
                    original_url_for_this_username = url_to_username_map[item_username]
                    # We store the whole item, the merger will pick out followersCount
                    results_by_url[original_url_for_this_username] = item 
                    logger.debug(f"Successfully mapped result for username '{item_username}' to URL '{original_url_for_this_username}'. Followers: {item.get('followersCount')}")
                else:
                    logger.warning(f"Result item from Apify had username '{item_username}' which was not in our lookup map or was None. Item data: {str(item)[:200]}...")
            
            # Log usernames for which no data was found
            missing_usernames = [uname for uname in unique_usernames if url_to_username_map[uname] not in results_by_url or results_by_url[url_to_username_map[uname]] is None]
            if missing_usernames:
                 logger.warning(f"Apify actor '{actor_name}' did not return results for usernames: {missing_usernames}")


        except Exception as e:
            logger.error(f"Error in get_instagram_data_for_urls_batch running actor '{actor_name}': {e}", exc_info=True)
            # results_by_url is already initialized with Nones, so it's fine to return as is.

        logger.info(f"Finished batch Instagram data fetch. Returning results for {len(unique_normalized_urls)} original URLs.")
        return results_by_url

    async def get_tiktok_data_for_urls_batch(self, tiktok_urls: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Fetches TikTok profile data for a list of TikTok profile URLs using Apify.
        Specifically targets 'apidojo/tiktok-scraper'.
        Calls the actor individually for each URL due to actor behavior with batch startUrls.
        The actor returns video data, from which we extract channel/profile information.
        """
        if not self.client:
            logger.error("Apify client not initialized in SocialMediaDiscovery. Cannot fetch TikTok data.")
            return {url: None for url in tiktok_urls}

        if not tiktok_urls:
            logger.info("No TikTok URLs provided to fetch data for.")
            return {}

        unique_normalized_urls = sorted(list(set(self._normalize_url(url) for url in tiktok_urls if url)))
        
        if not unique_normalized_urls:
            logger.info("No valid unique TikTok URLs after normalization.")
            return {url: None for url in tiktok_urls}

        logger.info(f"Fetching TikTok data individually for {len(unique_normalized_urls)} unique URLs: {unique_normalized_urls}")

        actor_name = "apidojo/tiktok-scraper"
        results_by_input_url: Dict[str, Optional[Dict[str, Any]]] = {url: None for url in unique_normalized_urls}

        for single_url in unique_normalized_urls:
            logger.info(f"Processing TikTok URL: {single_url}")
            run_input = {
                "startUrls": [single_url],
                "maxItems": 1, 
                "includeSearchKeywords": False
            }
            processed_channel_for_this_url = None
            try:
                logger.debug(f"Running Apify actor '{actor_name}' for URL: {single_url}")
                actor_instance = self.client.actor(actor_name)
                actor_run_details = await asyncio.to_thread(
                    actor_instance.call, 
                    run_input=run_input
                )

                if not actor_run_details or 'defaultDatasetId' not in actor_run_details:
                    logger.error(f"Apify actor '{actor_name}' call failed or did not return a dataset ID for URL: {single_url}.")
                    results_by_input_url[single_url] = None
                    continue # Next URL

                dataset_id = actor_run_details['defaultDatasetId']
                dataset = self.client.dataset(dataset_id)
                items_list = list(dataset.iterate_items())
                logger.debug(f"Retrieved {len(items_list)} items (videos) from dataset for actor '{actor_name}' for URL {single_url}.")

                # --- ADD DETAILED LOGGING OF FIRST ITEM --- #
                if items_list:
                    logger.debug(f"TIKTOK ACTOR OUTPUT - First item for {single_url}:\n{json.dumps(items_list[0], indent=2)}")
                # --- END DETAILED LOGGING --- #

                if not items_list:
                    logger.warning(f"Apify actor '{actor_name}' run for URL {single_url} returned no items.")
                    results_by_input_url[single_url] = None
                    continue # Next URL

                # Process items to extract channel data for THIS single_url
                # We only need the first valid channel data found for this input URL
                for item in items_list:
                    # --- TIKTOK DEBUG: Log each item's channel URL before and after normalization ---
                    logger.info(f"TIKTOK ITEM CHECK for input {single_url}, indented: {json.dumps(item, indent=2)}")
                    raw_item_channel_url = item.get('channel', {}).get('url')
                    print(f"TIKTOK ITEM CHECK for input {single_url}: Raw item channel.url: '{raw_item_channel_url}'")

                    normalized_item_channel_url = self._normalize_url(raw_item_channel_url) if raw_item_channel_url else None
                    logger.info(f"TIKTOK ITEM CHECK for input {single_url}: Raw item channel.url: '{raw_item_channel_url}', Normalized item channel.url: '{normalized_item_channel_url}'")
                    # --- END TIKTOK DEBUG --- 

                    channel_url_from_item = normalized_item_channel_url
                    
                    if channel_url_from_item: # Ensure we have a channel URL from the item
                        item_channel_data = item.get('channel', {}) # Get the channel object

                        # --- TIKTOK DEBUG: Check if normalized item channel URL matches the input single_url --- #
                        if channel_url_from_item == single_url:
                            logger.info(f"TIKTOK MATCH: Normalized item channel URL '{channel_url_from_item}' MATCHES normalized input URL '{single_url}'")
                        else:
                            # This case should be rare if normalization is consistent and input URLs are profile URLs
                            logger.warning(f"TIKTOK MISMATCH: Normalized item channel URL '{channel_url_from_item}' does NOT MATCH normalized input URL '{single_url}'. Data might be for a different channel than directly requested.")
                        # --- END TIKTOK DEBUG --- #

                        channel_followers = item_channel_data.get('followers') 
                        extracted_profile_data = {
                            'profile_url': channel_url_from_item, 
                            'username': item_channel_data.get('username'),
                            'name': item_channel_data.get('name'),
                            'followers_count': self._safe_int_cast(channel_followers),
                            'is_verified': item_channel_data.get('verified'),
                            'source_actor_item_example': item_channel_data # Store the whole channel object
                        }
                        processed_channel_for_this_url = extracted_profile_data
                        logger.info(f"Extracted TikTok channel data for input URL '{single_url}' (from item channel '{channel_url_from_item}'): Followers {channel_followers}")
                        break # Found channel data for this single_url, no need to check other videos from this run
                
                results_by_input_url[single_url] = processed_channel_for_this_url
                if not processed_channel_for_this_url:
                    logger.warning(f"No usable TikTok channel data extracted from items for input URL: {single_url}")

            except Exception as e:
                logger.error(f"Error in get_tiktok_data_for_urls_batch processing URL '{single_url}' with actor '{actor_name}': {e}", exc_info=True)
                results_by_input_url[single_url] = None
            
            await asyncio.sleep(1) # Small delay between individual actor calls for different URLs

        logger.info(f"Finished batch TikTok data fetch. Returning data for {len(unique_normalized_urls)} original URLs.")
        return results_by_input_url

    def _normalize_url(self, url: str) -> str:
        """Normalizes a URL: forces HTTPS, removes www. (unless it's critical like www.linkedin.com), removes query params, fragments, and trailing slashes."""
        if not url or not isinstance(url, str):
            return url # Or raise an error, or return empty string

        url = url.strip()
        
        # Prepend https if scheme is missing (handles cases like "domain.com/path")
        if '://' not in url:
            url = "https://" + url
        elif url.startswith("http://"):
            url = "https://" + url[len("http://"):]

        # Standardize to remove 'www.' unless it's a known site that might need it (e.g. linkedin often uses www)
        # For most social media, non-www is canonical or redirects.
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname
        if hostname and hostname.startswith("www."):
            # Specific sites where www is common or preferred in canonical URLs
            # For TikTok, twitter, instagram, facebook, youtube, usually non-www is fine or redirects.
            # LinkedIn is an exception where www is often part of the canonical form.
            if "linkedin.com" not in hostname.lower(): # Keep www for LinkedIn
                new_hostname = hostname[4:]
                url = url.replace(hostname, new_hostname, 1)
        
        # Strip URL fragment/query params
        url = url.split("?")[0].split("#")[0]
        
        # Remove trailing slash (except for root domain like https://twitter.com/)
        if url.endswith("/") and url.count("/") > 2: # url.count("/") > 2 ensures it's not just https://domain.com/
            url = url.rstrip("/")
            
        return url.lower() # Return lowercase for case-insensitive matching

    def _safe_int_cast(self, value: Any) -> Optional[int]:
        """Safely casts a value to an integer, returning None on failure."""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.debug(f"Could not cast value '{value}' (type: {type(value)}) to int.")
            return None

# Example Usage (for testing - requires APIFY_API_KEY in .env)
async def main_test(): # Make test function async
    if not os.getenv("APIFY_API_KEY"):
        print("Error: APIFY_API_KEY not found in environment variables.")
        print("Please create a .env file with your Apify API key.")
        return
        
    try:
        discovery = SocialMediaDiscovery()

        # --- Test LinkedIn URL --- 
        print("\n--- Testing LinkedIn URL Discovery ---")
        test_linkedin_url = "https://www.linkedin.com/in/williamhgates/" # Example URL
        linkedin_result = await discovery.get_linkedin_data_for_urls_batch([test_linkedin_url])
        if linkedin_result:
            print("LinkedIn Result (Extracted):")
            print(linkedin_result)
        else:
            print(f"Failed to get LinkedIn data for {test_linkedin_url}")

        # --- Test Twitter URL --- 
        print("\n--- Testing Twitter URL Discovery ---")
        test_twitter_url = "https://twitter.com/BillGates" # Example URL
        twitter_result = await discovery.get_twitter_data_for_urls_batch([test_twitter_url])
        if twitter_result:
            print("Twitter Result (Extracted):")
            print(twitter_result)
        else:
            print(f"Failed to get Twitter data for {test_twitter_url}")

    except ValueError as e:
        print(f"Initialization Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during testing: {e}")

if __name__ == "__main__":
    asyncio.run(main_test()) # Run the async test function 