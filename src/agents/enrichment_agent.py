import logging
from typing import Dict, Any, List, Optional, Tuple
import time # Import time
import re # Import re for regex
import asyncio # Import asyncio
# Allow nested event loops when LangGraph already has one running
import nest_asyncio
import os # Import os for file operations
import json # Import json for parsing
import csv # Import csv for CSV operations
from datetime import datetime # Import datetime for timestamp

# Import state and graph types
from ..graph.graph_types import GraphState # TypedDict wrapper
from ..models.state import AgentState # Pydantic model

# Import enrichment components
from .enrichment.rss_parser import RssFeedParser
from .enrichment.social_discovery import SocialMediaDiscovery
from .enrichment.data_merger import DataMerger
# Import output model
from ..models.podcast_profile import EnrichedPodcastProfile
# Import Gemini Search Service
from ..services.gemini_search import GeminiSearchService
# Import the new Pydantic model for Gemini output
from ..models.social import GeminiPodcastEnrichment
# Import the utility function
from ..utils.csv_utils import write_enriched_profiles_to_csv
# Import MetricsService
from ..services.metrics_service import MetricsService
# --- NEW: Import for targeted online search --- 
# from ..services.gemini_online_search import query_gemini_google_search # Remove this
from ..services.tavily_search import async_tavily_search # Add this
# --- END NEW ---
# --- NEW: Import for HttpUrl validation ---
from pydantic import HttpUrl, ValidationError
# --- END NEW --- 

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Apply patch once at import time; harmless if applied multiple times
nest_asyncio.apply()

class EnrichmentAgent:
    """Agent responsible for enriching podcast leads with additional metadata."""

    def __init__(self):
        """Initializes the enrichment components and Gemini service."""
        self.rss_parser = RssFeedParser()
        self.social_discovery = SocialMediaDiscovery() # Assumes APIFY_API_KEY is set
        self.data_merger = DataMerger()
        self.gemini_service = GeminiSearchService() # Initialize Gemini Service
        self.metrics_service = MetricsService() # Added metrics service initialization
        # Regex pattern for extracting URLs (simple version)
        self._url_pattern = re.compile(r'https?://[\w\-./?=&%#:]+')
        
        # --- New: Normalization pattern maps for known social hosts --- #
        self._twitter_pattern = re.compile(r'https?://(?:mobile\.|m\.)?(?:twitter\.com|x\.com)/', re.IGNORECASE)
        self._linkedin_pattern = re.compile(r'https?://(?:[a-z]{2,3}\.)?linkedin\.com/', re.IGNORECASE)

    # --- Helper Methods for Parsing Gemini Responses --- 
    def _normalize_social_url(self, url: str) -> str:
        """Normalizes social URLs so Apify actors receive the canonical form.

        - Forces HTTPS
        - Removes trailing slash and query params
        - Maps mobile sub-domains to www
        """
        if not url:
            return url
        url = url.strip()
        # Force https
        if url.startswith("http://"):
            url = "https://" + url[len("http://"):]
        # Prepend https if missing schema (rare)
        if url.startswith("www."):
            url = "https://" + url
        # Canonicalise Twitter
        url = self._twitter_pattern.sub("https://twitter.com/", url)
        # Canonicalise LinkedIn (keep www)
        url = self._linkedin_pattern.sub("https://www.linkedin.com/", url)
        # Strip URL fragment/query
        url = url.split("?")[0].split("#")[0]
        # Remove trailing slash (except root)
        if url.endswith("/") and url.count("/") > 2:
            url = url.rstrip("/")
        return url

    def _extract_url(self, text: Optional[str]) -> Optional[str]:
        """Extracts the first plausible HTTP/HTTPS URL from text and normalises it."""
        if not text:
            return None
        match = self._url_pattern.search(text)
        if match:
            url = match.group(0)
            # Basic cleanup: remove trailing punctuation often added by LLMs
            url = url.strip('.,;)\'')
            url = self._normalize_social_url(url)
            logger.debug(f"Extracted & normalized URL: {url} from text: '{text[:50]}...'")
            return url
        logger.debug(f"No URL found in text: '{text[:50]}...'")
        return None

    def _extract_host_names(self, text: Optional[str]) -> Optional[List[str]]:
        """Extracts and cleans potential host names from text."""
        if not text:
            return None
        
        # Basic cleanup: remove common introductory phrases
        text = re.sub(r'^(?:The host(?:s)? (?:is|are)|Hosted by)\s*:?\s*', '', text, flags=re.IGNORECASE).strip()
        # Remove trailing explanations
        text = text.split('.')[0] # Simplistic: take text before first period
        text = text.strip('.,;)\'')

        # Split potential multiple names (e.g., "John Doe and Jane Smith", "John Doe, Jane Smith")
        # This is basic, might need refinement based on common Gemini outputs
        names = []
        if ' and '.lower() in text.lower():
            names = [name.strip() for name in text.split(' and ')]
        elif ',' in text:
            names = [name.strip() for name in text.split(',')]
        else:
            names = [text] # Assume single name
            
        # Filter out empty strings
        cleaned_names = [name for name in names if name]
        
        if cleaned_names:
            logger.debug(f"Extracted host names: {cleaned_names} from text: '{text[:50]}...'")
            return cleaned_names
        logger.debug(f"No host names extracted from text: '{text[:50]}...'")
        return None
        
    # --- Main Enrichment Logic --- 
    async def run_enrichment(self, state: GraphState) -> Dict[str, Any]: 
        """Runs the enrichment process for leads in the current state (async)."""
        logger.info("--- Running Enrichment Agent ---")
        start_time = time.time()
        agent_state: AgentState = state['agent_state']
        campaign_id = agent_state.campaign_config.campaign_id if agent_state.campaign_config else "unknown_campaign"
        errors_encountered = 0
        initial_podcast_data_list = []
        updated_enriched_profiles = []

        try:
            initial_podcast_data_list = agent_state.leads

            if not initial_podcast_data_list:
                logger.warning("No initial podcast data found in state for enrichment. Skipping.")
                agent_state.current_step = "vetting"
                agent_state.execution_status = "enrichment_skipped_no_leads"
                return {"agent_state": agent_state}

            if not self.gemini_service.llm:
                logger.error("LangChain Gemini client not initialized in EnrichmentAgent. Cannot perform Gemini search.")
                # Proceeding without Gemini search
                
            # --- Step 1: Gemini Discovery for all podcasts (collect URLs) --- #
            logger.info("Starting Gemini discovery phase for all leads...")
            gemini_outputs: Dict[str, Optional[GeminiPodcastEnrichment]] = {}
            twitter_urls_to_scrape = set()
            linkedin_urls_to_scrape = set() # Keep LinkedIn separate for now
            
            # Use a temporary list to run Gemini calls concurrently
            gemini_tasks = [] 
            for initial_data in initial_podcast_data_list:
                 # Assuming _run_gemini_discovery_for_podcast exists or will be added
                 gemini_tasks.append(self._run_gemini_discovery_for_podcast(initial_data)) 
                 
            # Run Gemini calls concurrently
            gemini_results = await asyncio.gather(*gemini_tasks)
            
            # Process Gemini results and collect URLs
            for initial_data, gemini_output in zip(initial_podcast_data_list, gemini_results):
                 podcast_api_id = initial_data.get('api_id', 'Unknown ID')
                 gemini_outputs[podcast_api_id] = gemini_output # Store output keyed by ID
                 
                 # --- Collect URLs from BOTH initial data and Gemini results --- #
                 # Helper to add valid URLs to a set
                 def add_valid_url(url_set, url_value):
                     if url_value and isinstance(url_value, str) and url_value.startswith(('http', 'https')):
                         url_set.add(url_value)
                         
                 # Add from initial_data
                 add_valid_url(twitter_urls_to_scrape, initial_data.get('twitter_url'))
                 add_valid_url(linkedin_urls_to_scrape, initial_data.get('linkedin_url')) # Assuming host for now
                 
                 # Add from Gemini results
                 if gemini_output:
                      add_valid_url(twitter_urls_to_scrape, str(gemini_output.podcast_twitter_url) if gemini_output.podcast_twitter_url else None)
                      add_valid_url(twitter_urls_to_scrape, str(gemini_output.host_twitter_url) if gemini_output.host_twitter_url else None)
                      add_valid_url(linkedin_urls_to_scrape, str(gemini_output.host_linkedin_url) if gemini_output.host_linkedin_url else None)
                      # Add other platforms if needed
                           
            logger.info(f"Gemini discovery phase complete. Found {len(twitter_urls_to_scrape)} unique Twitter URLs and {len(linkedin_urls_to_scrape)} unique LinkedIn URLs to potentially scrape.")
            
            # --- Step 2: Batch Apify Calls --- #
            batch_twitter_results: Dict[str, Optional[Dict[str, Any]]] = {}
            batch_linkedin_results: Dict[str, Optional[Dict[str, Any]]] = {}
            
            apify_batch_tasks = []
            # Batch Twitter
            if twitter_urls_to_scrape:
                apify_batch_tasks.append(self.social_discovery.get_twitter_data_for_urls_batch(list(twitter_urls_to_scrape)))
            else:
                 apify_batch_tasks.append(asyncio.sleep(0, result={})) # Return empty dict if no URLs
                 
            # --- NEW: Batch LinkedIn --- #
            if linkedin_urls_to_scrape:
                 apify_batch_tasks.append(self.social_discovery.get_linkedin_data_for_urls_batch(list(linkedin_urls_to_scrape)))
            else:
                 apify_batch_tasks.append(asyncio.sleep(0, result={}))
                
            # Run Apify batch tasks concurrently
            apify_gathered_results = await asyncio.gather(*apify_batch_tasks)
            batch_twitter_results = apify_gathered_results[0] if len(apify_gathered_results) > 0 else {}
            batch_linkedin_results = apify_gathered_results[1] if len(apify_gathered_results) > 1 else {}
            logger.info(f"Apify batch calls complete. Got results for {len(batch_twitter_results)} Twitter URLs and {len(batch_linkedin_results)} LinkedIn URLs.")

            # --- Step 3: Enrich individual podcasts using pre-fetched data --- #
            enrichment_tasks = []
            for initial_data in initial_podcast_data_list:
                 podcast_api_id = initial_data.get('api_id', 'Unknown ID')
                 # Pass the specific gemini output and the relevant batch results
                 enrichment_tasks.append(
                     self._enrich_single_podcast( 
                         initial_data,
                         gemini_output=gemini_outputs.get(podcast_api_id), 
                         batch_twitter_results=batch_twitter_results, 
                         batch_linkedin_results=batch_linkedin_results # Pass LinkedIn results now
                     )
                 )
            
            logger.info(f"Starting final concurrent enrichment merging for {len(enrichment_tasks)} podcasts…")
            enriched_results = await asyncio.gather(*enrichment_tasks)
            logger.info("Finished final concurrent enrichment merging.")

            # --- Final State Update & CSV --- #
            updated_enriched_profiles = [r for r in enriched_results if r is not None]
            errors_encountered = len(initial_podcast_data_list) - len(updated_enriched_profiles)

            # Update agent state
            agent_state.leads = []
            agent_state.enriched_profiles = updated_enriched_profiles

            # Write Enriched CSV Output
            csv_path = self._save_enriched_profiles_to_csv(updated_enriched_profiles, campaign_id)

            agent_state.current_step = "vetting"
            agent_state.execution_status = "enrichment_complete" if errors_encountered == 0 else "enrichment_complete_with_errors"

        except Exception as e:
            logger.exception("Critical error during enrichment setup or gathering: %s", e)
            agent_state.current_step = "error"
            agent_state.execution_status = "enrichment_failed_unexpected"
            state['error_message'] = f"Enrichment failed unexpectedly: {e}"
            errors_encountered = len(initial_podcast_data_list)
            updated_enriched_profiles = []
            agent_state.enriched_profiles = []
            agent_state.leads = []

        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            if getattr(self, 'metrics_service', None):
                total_leads_processed = len(initial_podcast_data_list)
                successful_enrichments = len(updated_enriched_profiles)
                self.metrics_service.record_event(
                    event_name="agent_step_end",
                    agent_step="enrichment",
                    campaign_id=campaign_id,
                    duration_ms=duration_ms,
                    metadata={
                        "final_status": agent_state.execution_status,
                        "total_leads_processed": total_leads_processed,
                        "enriched_profiles_created": successful_enrichments,
                        "successful_enrichments": successful_enrichments,
                        "failed_enrichments": errors_encountered,
                        "errors_encountered": errors_encountered,
                    },
                )
            logger.info(
                f"--- Enrichment Agent Finished. Final Status: {agent_state.execution_status}. Next step: {agent_state.current_step} ---"
            )
        
        # Return the required dictionary update
        return {"agent_state": agent_state}
        
    # --- NEW Helper for Gemini Discovery (Refactored) --- #
    async def _run_gemini_discovery_for_podcast(self, initial_data: Dict[str, Any]) -> Optional[GeminiPodcastEnrichment]:
        """Runs Gemini discovery for a single podcast, using targeted online searches for missing info."""
        podcast_name = initial_data.get('title', 'Unknown Podcast')
        podcast_description = initial_data.get('description', '')
        podcast_api_id = initial_data.get('api_id', 'Unknown ID') 
        logger.info(f"Starting Gemini discovery for {podcast_api_id} ({podcast_name}) with targeted online searches.")

        discovery_targets = [
            ("Host Names", None, 'host_names', False),
            ("Podcast Twitter URL", 'twitter_url', 'podcast_twitter_url', False),
            ("Podcast LinkedIn URL", 'linkedin_url', 'podcast_linkedin_url', False),
            ("Podcast Instagram URL", 'instagram_url', 'podcast_instagram_url', False),
            ("Podcast Facebook URL", 'facebook_url', 'podcast_facebook_url', False),
            ("Podcast YouTube URL", 'youtube_url', 'podcast_youtube_url', False),
            ("Podcast TikTok URL", 'tiktok_url', 'podcast_tiktok_url', False),
            ("Primary Host LinkedIn URL", 'host_linkedin_url', 'host_linkedin_url', True),
            ("Primary Host Twitter URL", 'host_twitter_url', 'host_twitter_url', True)
        ]

        found_info_texts = []
        found_info_texts.append(f"Original Podcast Name: {podcast_name}")
        if podcast_description:
            found_info_texts.append(f"Original Podcast Description: {podcast_description[:500]}...")

        host_name_for_prompts = podcast_name

        for target_name, initial_key, gemini_key, needs_host in discovery_targets:
            if initial_key and initial_data.get(initial_key):
                logger.debug(f"Found '{target_name}' in initial_data for {podcast_api_id}: {initial_data.get(initial_key)}")
                found_info_texts.append(f"{target_name} (from initial data): {initial_data.get(initial_key)}")
                if gemini_key == 'host_names':
                    current_host_info = initial_data.get(initial_key)
                    if isinstance(current_host_info, list):
                        host_name_for_prompts = ", ".join(current_host_info)
                    elif isinstance(current_host_info, str):
                        host_name_for_prompts = current_host_info
                continue # Skip online search for this item
            
            # If info not in initial_data, perform targeted online search
            query_subject_name = host_name_for_prompts if needs_host and host_name_for_prompts != podcast_name else podcast_name
            query_for = f"the host of '{query_subject_name}'" if needs_host and host_name_for_prompts != podcast_name else f"the podcast '{query_subject_name}'"
            if target_name == "Host Names":
                 query_for = f"the podcast '{podcast_name}'"
            
            search_query = f"{target_name} for {query_for}"
            
            logger.debug(f"Performing targeted online search for '{target_name}' for {podcast_api_id}. Query: '{search_query}'")
            online_search_result_text = None
            try:
                # tavily_results = await async_tavily_search(search_query, max_results=2) # Old call
                tavily_response = await async_tavily_search(
                    search_query,
                    max_results=2,  # Keep max_results relatively low if primarily using the answer
                    search_depth="advanced", # Ensure advanced depth for better quality answer
                    include_answer=True    # Request the LLM-generated answer
                )
                await asyncio.sleep(0.2) # Small delay, can be adjusted

                if tavily_response and tavily_response.get("answer"):
                    online_search_result_text = tavily_response["answer"]
                    logger.info(f"Tavily search for '{target_name}' for {podcast_api_id} yielded an answer: '{online_search_result_text[:150]}...'")
                    # Append the answer. The query is included for context for the final parser.
                    found_info_texts.append(f"{target_name} (from online search answer - query: \"{search_query}\"):\n{online_search_result_text}")
                
                elif tavily_response and tavily_response.get("results"):
                    # Fallback: If no answer, but results (snippets) are available
                    results_snippets = tavily_response["results"]
                    logger.warning(f"Tavily search for '{target_name}' for {podcast_api_id} provided no answer, but found {len(results_snippets)} snippets. Using snippets.")
                    formatted_tavily_snippets = []
                    for i, res in enumerate(results_snippets):
                        title_str = f"Title {i+1}: {res.get('title', 'N/A')}\n" if res.get('title') else ""
                        url_str = f"URL {i+1}: {res.get('url', 'N/A')}\n"
                        content_str = f"Content {i+1}: {res.get('content', 'N/A')}" # Content might be long
                        # Limit snippet length for conciseness in the final prompt
                        formatted_tavily_snippets.append(f"{title_str}{url_str}{content_str[:500]}...") 
                    
                    online_search_result_text = "\n\n---\n".join(formatted_tavily_snippets)
                    found_info_texts.append(f"{target_name} (from online search snippets - query: \"{search_query}\"):\n{online_search_result_text}")
                else:
                    logger.warning(f"Tavily search for '{target_name}' for {podcast_api_id} returned no answer and no results, or an error occurred (response was: {tavily_response})")

            except Exception as e:
                logger.error(f"Error during targeted online search call (Tavily) for '{target_name}' for {podcast_api_id}: {e}")
            
            # This 'if online_search_result_text:' block was for the old structure, 
            # the new logic for appending to found_info_texts is now inside the 'if tavily_results:' block.
            # So, the original 'if online_search_result_text:' and its 'else' can be removed or commented.
            # For clarity, I will remove it as the logic is now self-contained above.

            # if online_search_result_text: # Old block, logic moved up
            #     logger.info(f"Found online search result for '{target_name}' for {podcast_api_id}: '{online_search_result_text[:100]}...'")
            #     found_info_texts.append(f"{target_name} (from online search - query: \"{search_query}\"): {online_search_result_text}")
            #     if target_name == "Host Names":
            #          cleaned_host_text = online_search_result_text.split(" is ")[-1].split(" are ")[-1].strip('. ').strip()
            #          if cleaned_host_text and len(cleaned_host_text) < 70 :
            #              host_name_for_prompts = cleaned_host_text
            # else: # Old block, logic moved up
            #     logger.warning(f"No online search result for '{target_name}' for {podcast_api_id}")
        
        combined_text_for_parsing = "\n\n---\n\n".join(found_info_texts)
        logger.debug(f"Combined text for final parsing for {podcast_api_id}:\n{combined_text_for_parsing}")

        if not self.gemini_service or not self.gemini_service.llm:
            logger.error(f"GeminiSearchService (for structured output) not available. Cannot parse for {podcast_api_id}.")
            return None

        output_schema = GeminiPodcastEnrichment.model_json_schema()
        schema_prompt = json.dumps(output_schema, indent=2)
        final_parser_prompt = f"""You are an expert data extraction assistant.
Based *only* on the information within the 'Provided Text' section below, extract the required information and structure it according to the 'JSON Schema'.

Key Instructions:
1.  If specific information for a field (e.g., a particular social media URL) is not explicitly found in the 'Provided Text', use null for that field in the output. Do not guess or infer.
2.  Prioritize information that is clearly labeled, such as text following 'Host Names (from initial data):' or 'Podcast Twitter URL (from online search - query: "Podcast Twitter URL for..."):'.
3.  For social media URLs, look for full, valid HTTP/HTTPS links in the text segments that follow query markers like "(from online search - query: \"Podcast Twitter URL for...\")". Extract the most direct and plausible URL.
4.  If the text for a specific URL search says "unable to find" or similar, then the value for that URL field in the JSON output should be null.
5.  Host names should be a list of strings. If multiple hosts are mentioned (e.g., "Host A and Host B"), list them as ["Host A", "Host B"].

Provided Text:
---
{combined_text_for_parsing}
---

JSON Schema:
```json
{schema_prompt}
```

Response JSON (strictly adhere to the schema, using null for missing fields):
"""
        gemini_structured_output: Optional[GeminiPodcastEnrichment] = None
        try:
            logger.debug(f"Sending combined text to Gemini for structured parsing: {podcast_name}")
            # Use the new generic method, passing the specific Pydantic model
            gemini_output_untyped = await asyncio.to_thread(
                self.gemini_service.get_structured_data, # Use generic method
                final_parser_prompt,
                GeminiPodcastEnrichment # Pass the target model
            )
            # Check if the returned object is of the expected type
            if isinstance(gemini_output_untyped, GeminiPodcastEnrichment):
                gemini_structured_output = gemini_output_untyped
                logger.info(f"Successfully parsed combined text into structured output for {podcast_name}")
                url_fields = [
                    'podcast_twitter_url', 'podcast_linkedin_url', 'podcast_instagram_url',
                    'podcast_facebook_url', 'podcast_youtube_url', 'podcast_tiktok_url',
                    'host_linkedin_url', 'host_twitter_url'
                ]
                for field in url_fields:
                    value = getattr(gemini_structured_output, field, None)
                    if isinstance(value, str):
                        cleaned_value = value.strip()
                        if cleaned_value.lower() == 'unknown' or cleaned_value == '' or cleaned_value.lower() == 'n/a':
                            setattr(gemini_structured_output, field, None)
                            continue
                        
                        # Attempt to construct full URLs for handles or partials
                        processed_url = None
                        if field.endswith('_twitter_url') and cleaned_value.startswith('@'):
                            processed_url = f"https://twitter.com/{cleaned_value[1:]}"
                        elif field.endswith('_tiktok_url') and cleaned_value.startswith('@'):
                            processed_url = f"https://www.tiktok.com/{cleaned_value}" # TikTok usually includes @
                        elif 'linkedin.com' in cleaned_value and not cleaned_value.startswith(('http', 'www.')):
                            processed_url = f"https://www.{cleaned_value}"
                        # --- NEW: General partial URL handling for common social/video sites ---
                        elif ( \
                            ('youtube.com/' in cleaned_value or 'youtu.be/' in cleaned_value) or \
                            ('facebook.com/' in cleaned_value) or \
                            ('instagram.com/' in cleaned_value) or \
                            ('tiktok.com/' in cleaned_value and not cleaned_value.startswith('@')) or \
                            ('twitter.com/' in cleaned_value and not cleaned_value.startswith('@')) or \
                            ('linkedin.com/' in cleaned_value) \
                           ) and not cleaned_value.startswith('http'):
                            # Prepend https://, www handling is tricky and site-specific, 
                            # but for many social sites, https://domain/path works or redirects.
                            # For youtube.com/channel/..., https://youtube.com/channel/... is correct.
                            if cleaned_value.startswith("www."):
                                processed_url = f"https://{cleaned_value}"
                            else:
                                # Check if it looks like a domain name is present
                                parts = cleaned_value.split('/')
                                if parts and '.' in parts[0]: # Basic check for a domain in the first part
                                    processed_url = f"https://{cleaned_value}"
                                else:
                                    logger.warning(f"Partial URL '{cleaned_value}' for {field} in {podcast_api_id} does not look like a standard domain/path. Not prepending https.")
                                    processed_url = cleaned_value # Let it fail validation if truly malformed
                        # --- END NEW ---                                
                        elif not cleaned_value.startswith('http'):
                            # If it's not a known handle pattern and doesn't start with http, it's likely invalid for direct use
                            logger.warning(f"Potentially invalid/partial URL string for {field} in {podcast_api_id}: '{cleaned_value}'. Setting to None after processing.")
                        else:
                            processed_url = cleaned_value # Already a full URL or unhandled partial

                        # Validate with Pydantic HttpUrl (even though field is str, this confirms validity)
                        if processed_url:
                            try:
                                HttpUrl(processed_url) # Validate the constructed/cleaned URL
                                setattr(gemini_structured_output, field, processed_url) # Store the processed string
                                logger.debug(f"Validated/processed URL for {field} in {podcast_api_id}: {processed_url}")
                            except ValidationError:
                                logger.warning(f"Validation failed for processed URL '{processed_url}' for {field} in {podcast_api_id}. Setting to None.")
                                setattr(gemini_structured_output, field, None)
                        else:
                            # If processed_url ended up being None (e.g. only an @ for non-twitter/tiktok)
                            setattr(gemini_structured_output, field, None)
                    elif value is not None: # Not a string, not None (e.g. if Pydantic somehow made it a non-string type)
                        logger.warning(f"Unexpected type for {field} in {podcast_api_id}: {type(value)}. Setting to None.")
                        setattr(gemini_structured_output, field, None)
            else:
                logger.warning(f"Gemini failed to parse combined text into structured output for {podcast_name}.")
        except Exception as e:
            logger.error(f"Error during final structured parsing for {podcast_api_id}: {e}", exc_info=True)
        
        # --- NEW DEBUG LOG --- #
        if gemini_structured_output:
            logger.info(f"GEMINI STRUCTURED OUTPUT for {podcast_api_id} ({podcast_name}):\n{gemini_structured_output.model_dump_json(indent=2, exclude_none=True)}")
        else:
            logger.warning(f"GEMINI STRUCTURED OUTPUT for {podcast_api_id} ({podcast_name}) is None.")
        # --- END NEW DEBUG LOG ---
            
        return gemini_structured_output
        
    # --- Modified Single Podcast Enrichment (Uses pre-fetched data) --- # 
    async def _enrich_single_podcast(
        self, 
        initial_data: Dict[str, Any],
        gemini_output: Optional[GeminiPodcastEnrichment], # Pass Gemini output
        batch_twitter_results: Dict[str, Optional[Dict[str, Any]]], # Pass Twitter results
        batch_linkedin_results: Dict[str, Optional[Dict[str, Any]]], # Pass LinkedIn results
        # Add new batch results as parameters
        batch_instagram_results: Dict[str, Optional[Dict[str, Any]]] = None,
        batch_facebook_results: Dict[str, Optional[Dict[str, Any]]] = None,
        batch_youtube_results: Dict[str, Optional[Dict[str, Any]]] = None,
        batch_tiktok_results: Dict[str, Optional[Dict[str, Any]]] = None
    ) -> Optional[EnrichedPodcastProfile]:
        """Asynchronous function to enrich a single podcast's data using pre-fetched info."""
        podcast_name = initial_data.get('title', 'Unknown Podcast')
        # podcast_description = initial_data.get('description', '') # Not needed here anymore?
        podcast_api_id = initial_data.get('api_id', 'Unknown ID') 
        logger.info(f"Starting final merge for: {podcast_api_id} - {podcast_name}")
        
        rss_data = None # Still paused
        social_media_data = {} 
        
        # Ensure batch results are dicts even if None initially from signature
        batch_instagram_results = batch_instagram_results or {}
        batch_facebook_results = batch_facebook_results or {}
        batch_youtube_results = batch_youtube_results or {}
        batch_tiktok_results = batch_tiktok_results or {}

        try: 
            # --- Step 1: Parse RSS Feed (Currently Paused) --- #
            rss_data = None 

            # --- Step 2: Gemini Discovery (Already done) --- #
            # gemini_structured_output is passed in argument `gemini_output`

            # --- Step 3: Apify Social Data Fetching (Use pre-fetched batch results and prioritize) --- #
            
            # --- Twitter Prioritization & Lookup --- #
            final_twitter_url = None
            initial_twitter_url = initial_data.get('twitter_url')
            gemini_podcast_twitter_url = str(gemini_output.podcast_twitter_url) if gemini_output and gemini_output.podcast_twitter_url else None
            gemini_host_twitter_url = str(gemini_output.host_twitter_url) if gemini_output and gemini_output.host_twitter_url else None
            
            # 1. Prioritize initial data URL if valid
            if initial_twitter_url and isinstance(initial_twitter_url, str) and initial_twitter_url.startswith(('http', 'https')):
                final_twitter_url = initial_twitter_url
                logger.debug(f"Prioritizing initial Twitter URL: {final_twitter_url}")
            # 2. Fallback to Gemini podcast URL if valid
            elif gemini_podcast_twitter_url and gemini_podcast_twitter_url.startswith(('http', 'https')):
                 final_twitter_url = gemini_podcast_twitter_url
                 logger.debug(f"Falling back to Gemini podcast Twitter URL: {final_twitter_url}")
            # 3. Fallback to Gemini host URL if valid
            elif gemini_host_twitter_url and gemini_host_twitter_url.startswith(('http', 'https')):
                 final_twitter_url = gemini_host_twitter_url
                 logger.debug(f"Falling back to Gemini host Twitter URL: {final_twitter_url}")
                 
            # Get data for the final selected URL from batch results
            twitter_data = None
            if final_twitter_url:
                twitter_data = batch_twitter_results.get(final_twitter_url)
                if twitter_data:
                     social_media_data['podcast_twitter'] = twitter_data
                     logger.info(f"Using pre-fetched Twitter data for prioritized URL: {final_twitter_url}")
                else:
                     username = self.social_discovery._extract_username_from_twitter_url(final_twitter_url)
                     logger.warning(f"No batch Twitter data found for prioritized URL {final_twitter_url} (Username: {username}), although a URL was selected.")
            else:
                 logger.debug(f"No valid Twitter URL found for {podcast_api_id} after checking initial data and Gemini.")

            # --- LinkedIn Prioritization & Lookup --- #
            final_linkedin_url = None
            initial_linkedin_url = initial_data.get('linkedin_url')
            gemini_host_linkedin_url = str(gemini_output.host_linkedin_url) if gemini_output and gemini_output.host_linkedin_url else None
            # Add gemini_podcast_linkedin_url if/when relevant

            # 1. Prioritize initial data URL if valid
            if initial_linkedin_url and isinstance(initial_linkedin_url, str) and initial_linkedin_url.startswith(('http', 'https')):
                final_linkedin_url = initial_linkedin_url
                logger.debug(f"Prioritizing initial LinkedIn URL: {final_linkedin_url}")
            # 2. Fallback to Gemini host URL if valid (priority for LinkedIn)
            elif gemini_host_linkedin_url and gemini_host_linkedin_url.startswith(('http', 'https')):
                 final_linkedin_url = gemini_host_linkedin_url
                 logger.debug(f"Falling back to Gemini host LinkedIn URL: {final_linkedin_url}")
            # Add fallback for Gemini podcast LinkedIn if needed later

            # --- MODIFIED: Use batch results instead of individual call --- #
            linkedin_data = None
            if final_linkedin_url:
                 linkedin_data = batch_linkedin_results.get(final_linkedin_url)
                 if linkedin_data:
                      social_media_data['host_linkedin'] = linkedin_data # Store under host_linkedin for now
                      logger.info(f"Using pre-fetched LinkedIn data for prioritized URL: {final_linkedin_url}")
                 else:
                      logger.warning(f"No batch LinkedIn data found for prioritized URL {final_linkedin_url}, although a URL was selected.")
            else:
                 logger.debug(f"No valid LinkedIn URL found for {podcast_api_id} after checking initial data and Gemini.")

            # --- Instagram Prioritization & Lookup (NEW) --- #
            final_instagram_url = None
            initial_instagram_url = initial_data.get('instagram_url')
            gemini_podcast_instagram_url = str(gemini_output.podcast_instagram_url) if gemini_output and gemini_output.podcast_instagram_url else None
            
            if initial_instagram_url and isinstance(initial_instagram_url, str) and initial_instagram_url.startswith(('http', 'https')):
                final_instagram_url = initial_instagram_url
            elif gemini_podcast_instagram_url and gemini_podcast_instagram_url.startswith(('http', 'https')):
                final_instagram_url = gemini_podcast_instagram_url
            
            if final_instagram_url:
                instagram_data = batch_instagram_results.get(self._normalize_social_url(final_instagram_url)) # Normalize before lookup
                if instagram_data:
                    social_media_data['podcast_instagram'] = instagram_data
                    logger.info(f"Using pre-fetched Instagram data for {final_instagram_url}")
                else: logger.debug(f"No batch Instagram data for {final_instagram_url}")
            else: logger.debug(f"No Instagram URL for {podcast_api_id}")

            # --- Facebook Prioritization & Lookup (NEW) --- #
            final_facebook_url = None
            initial_facebook_url = initial_data.get('facebook_url')
            gemini_podcast_facebook_url = str(gemini_output.podcast_facebook_url) if gemini_output and gemini_output.podcast_facebook_url else None
            if initial_facebook_url and isinstance(initial_facebook_url, str) and initial_facebook_url.startswith(('http', 'https')):
                final_facebook_url = initial_facebook_url
            elif gemini_podcast_facebook_url and gemini_podcast_facebook_url.startswith(('http', 'https')):
                final_facebook_url = gemini_podcast_facebook_url
            if final_facebook_url:
                facebook_data = batch_facebook_results.get(self._normalize_social_url(final_facebook_url))
                if facebook_data: social_media_data['podcast_facebook'] = facebook_data
                else: logger.debug(f"No batch Facebook data for {final_facebook_url}")
            else: logger.debug(f"No Facebook URL for {podcast_api_id}")

            # --- YouTube Prioritization & Lookup (NEW) --- #
            final_youtube_url = None
            initial_youtube_url = initial_data.get('youtube_url')
            gemini_podcast_youtube_url = str(gemini_output.podcast_youtube_url) if gemini_output and gemini_output.podcast_youtube_url else None
            if initial_youtube_url and isinstance(initial_youtube_url, str) and initial_youtube_url.startswith(('http', 'https')):
                final_youtube_url = initial_youtube_url
            elif gemini_podcast_youtube_url and gemini_podcast_youtube_url.startswith(('http', 'https')):
                final_youtube_url = gemini_podcast_youtube_url
            if final_youtube_url:
                youtube_data = batch_youtube_results.get(self._normalize_social_url(final_youtube_url))
                if youtube_data: social_media_data['podcast_youtube'] = youtube_data
                else: logger.debug(f"No batch YouTube data for {final_youtube_url}")
            else: logger.debug(f"No YouTube URL for {podcast_api_id}")

            # --- TikTok Prioritization & Lookup (NEW) --- #
            final_tiktok_url = None
            initial_tiktok_url = initial_data.get('tiktok_url')
            gemini_podcast_tiktok_url = str(gemini_output.podcast_tiktok_url) if gemini_output and gemini_output.podcast_tiktok_url else None
            if initial_tiktok_url and isinstance(initial_tiktok_url, str) and initial_tiktok_url.startswith(('http', 'https')):
                final_tiktok_url = initial_tiktok_url
            elif gemini_podcast_tiktok_url and gemini_podcast_tiktok_url.startswith(('http', 'https')):
                final_tiktok_url = gemini_podcast_tiktok_url
            if final_tiktok_url:
                tiktok_data = batch_tiktok_results.get(self._normalize_social_url(final_tiktok_url))
                if tiktok_data: social_media_data['podcast_tiktok'] = tiktok_data
                else: logger.debug(f"No batch TikTok data for {final_tiktok_url}")
            else: logger.debug(f"No TikTok URL for {podcast_api_id}")

            # --- Step 4: Merge Data --- #
            enriched_profile = self.data_merger.merge_data(
                base_unified_data=initial_data, 
                rss_data=rss_data,
                gemini_data=gemini_output.model_dump(exclude_none=True) if gemini_output else {}, # Use passed data
                social_media_data=social_media_data
            )
            
            if not isinstance(enriched_profile, EnrichedPodcastProfile):
                logger.error(f"DataMerger failed for {podcast_api_id}. Skipping.")
                return None 

            logger.info(f"Successfully merged enriched profile for {podcast_api_id}")
            return enriched_profile

        except Exception as e:
            logger.error(f"Critical error enriching podcast {podcast_api_id}: {e}", exc_info=True)
            return None 

    # --- NEW METHOD FOR STANDALONE API USAGE ---
    async def perform_standalone_enrichment(
        self,
        leads_to_enrich: List[Dict[str, Any]],
        campaign_id_prefix: str = "standalone_enrich", # Keep for fallback if no existing_campaign_id
        existing_campaign_id: Optional[str] = None # New parameter
    ) -> Tuple[List[EnrichedPodcastProfile], Optional[str]]:
        """
        Performs standalone enrichment on a list of provided podcast leads.
        Returns a list of enriched profiles and the path to an output CSV file.
        Uses existing_campaign_id for CSV naming if provided.
        """
        logger.info(f"--- Executing Standalone Enrichment for {len(leads_to_enrich)} leads ---")
        start_time = time.time()

        # Determine the campaign ID to use for this run
        if existing_campaign_id:
            temp_campaign_id = existing_campaign_id
            logger.info(f"Using existing campaign ID for output: {temp_campaign_id}")
        else:
            timestamp = time.strftime("%Y%m%d%H%M%S")
            temp_campaign_id = f"{campaign_id_prefix}_{timestamp}"
            logger.info(f"Generated new campaign ID for output: {temp_campaign_id}")

        updated_enriched_profiles: List[EnrichedPodcastProfile] = []
        errors_encountered = 0
        csv_path: Optional[str] = None

        if not leads_to_enrich:
            logger.warning("Standalone enrichment: No leads provided to enrich.")
            return [], None

        if not self.gemini_service or not self.gemini_service.llm:
            logger.warning("Standalone enrichment: Gemini service not available. Some enrichment steps might be skipped.")
            # Decide if this is a fatal error or if enrichment should proceed without Gemini

        try:
            # --- Step 1: Gemini Discovery for all provided leads --- #
            logger.info("Starting Gemini discovery phase for standalone enrichment...")
            gemini_outputs: Dict[str, Optional[GeminiPodcastEnrichment]] = {}
            twitter_urls_to_scrape = set()
            linkedin_urls_to_scrape = set()
            instagram_urls_to_scrape = set() # New
            facebook_urls_to_scrape = set()  # New
            youtube_urls_to_scrape = set()   # New
            tiktok_urls_to_scrape = set()    # New

            gemini_tasks = []
            for lead_data in leads_to_enrich:
                gemini_tasks.append(self._run_gemini_discovery_for_podcast(lead_data))
            
            gemini_results = await asyncio.gather(*gemini_tasks)

            for lead_data, gemini_output in zip(leads_to_enrich, gemini_results):
                podcast_api_id = lead_data.get('api_id', f"unknown_id_{leads_to_enrich.index(lead_data)}")
                gemini_outputs[podcast_api_id] = gemini_output

                def add_valid_url(url_set, url_value):
                    if url_value and isinstance(url_value, str) and url_value.startswith(('http', 'https')):                        
                        normalized = self._normalize_social_url(url_value) 
                        url_set.add(normalized)
                
                # Existing social platforms
                add_valid_url(twitter_urls_to_scrape, lead_data.get('twitter_url'))
                add_valid_url(linkedin_urls_to_scrape, lead_data.get('linkedin_url'))
                # New social platforms from initial_data (if mappers ever add them)
                add_valid_url(instagram_urls_to_scrape, lead_data.get('instagram_url'))
                add_valid_url(facebook_urls_to_scrape, lead_data.get('facebook_url'))
                add_valid_url(youtube_urls_to_scrape, lead_data.get('youtube_url'))
                add_valid_url(tiktok_urls_to_scrape, lead_data.get('tiktok_url'))

                if gemini_output:
                    # Existing from Gemini
                    add_valid_url(twitter_urls_to_scrape, str(gemini_output.podcast_twitter_url) if gemini_output.podcast_twitter_url else None)
                    add_valid_url(twitter_urls_to_scrape, str(gemini_output.host_twitter_url) if gemini_output.host_twitter_url else None)
                    add_valid_url(linkedin_urls_to_scrape, str(gemini_output.host_linkedin_url) if gemini_output.host_linkedin_url else None)
                    # New from Gemini (podcast specific)
                    add_valid_url(instagram_urls_to_scrape, str(gemini_output.podcast_instagram_url) if gemini_output.podcast_instagram_url else None)
                    add_valid_url(facebook_urls_to_scrape, str(gemini_output.podcast_facebook_url) if gemini_output.podcast_facebook_url else None)
                    add_valid_url(youtube_urls_to_scrape, str(gemini_output.podcast_youtube_url) if gemini_output.podcast_youtube_url else None)
                    add_valid_url(tiktok_urls_to_scrape, str(gemini_output.podcast_tiktok_url) if gemini_output.podcast_tiktok_url else None)
            
            logger.info(f"Standalone Gemini phase complete. Twitter URLs: {len(twitter_urls_to_scrape)}, LinkedIn URLs: {len(linkedin_urls_to_scrape)}, Instagram: {len(instagram_urls_to_scrape)}, Facebook: {len(facebook_urls_to_scrape)}, YouTube: {len(youtube_urls_to_scrape)}, TikTok: {len(tiktok_urls_to_scrape)}")

            # --- Step 2: Batch Apify Calls --- #
            batch_twitter_results: Dict[str, Optional[Dict[str, Any]]] = {}
            batch_linkedin_results: Dict[str, Optional[Dict[str, Any]]] = {}
            batch_instagram_results: Dict[str, Optional[Dict[str, Any]]] = {} # New
            batch_facebook_results: Dict[str, Optional[Dict[str, Any]]] = {}  # New
            batch_youtube_results: Dict[str, Optional[Dict[str, Any]]] = {}   # New
            batch_tiktok_results: Dict[str, Optional[Dict[str, Any]]] = {}    # New
            apify_batch_tasks = []

            # Twitter (existing)
            if twitter_urls_to_scrape:
                apify_batch_tasks.append(self.social_discovery.get_twitter_data_for_urls_batch(list(twitter_urls_to_scrape)))
            else:
                apify_batch_tasks.append(asyncio.sleep(0, result={}))
            
            # LinkedIn (existing)
            if linkedin_urls_to_scrape:
                apify_batch_tasks.append(self.social_discovery.get_linkedin_data_for_urls_batch(list(linkedin_urls_to_scrape)))
            else:
                apify_batch_tasks.append(asyncio.sleep(0, result={}))
            
            # Instagram (New - Placeholder Call)
            if instagram_urls_to_scrape:
                apify_batch_tasks.append(self.social_discovery.get_instagram_data_for_urls_batch(list(instagram_urls_to_scrape)))
            else:
                apify_batch_tasks.append(asyncio.sleep(0, result={}))

            # Facebook (New - Placeholder Call)
            if facebook_urls_to_scrape:
                # logger.warning("Facebook scraping not yet implemented in SocialMediaDiscovery. Skipping batch call.")
                apify_batch_tasks.append(asyncio.sleep(0, result={})) # Placeholder
            else:
                apify_batch_tasks.append(asyncio.sleep(0, result={}))

            # YouTube (New - Placeholder Call)
            if youtube_urls_to_scrape:
                # logger.warning("YouTube scraping not yet implemented in SocialMediaDiscovery. Skipping batch call.")
                apify_batch_tasks.append(asyncio.sleep(0, result={})) # Placeholder
            else:
                apify_batch_tasks.append(asyncio.sleep(0, result={}))

            # TikTok (New - Placeholder Call)
            if tiktok_urls_to_scrape:
                apify_batch_tasks.append(self.social_discovery.get_tiktok_data_for_urls_batch(list(tiktok_urls_to_scrape)))
            else:
                apify_batch_tasks.append(asyncio.sleep(0, result={}))
            
            apify_gathered_results = await asyncio.gather(*apify_batch_tasks)
            batch_twitter_results = apify_gathered_results[0] if len(apify_gathered_results) > 0 else {}
            batch_linkedin_results = apify_gathered_results[1] if len(apify_gathered_results) > 1 else {}
            batch_instagram_results = apify_gathered_results[2] if len(apify_gathered_results) > 2 else {} # New
            batch_facebook_results = apify_gathered_results[3] if len(apify_gathered_results) > 3 else {}  # New
            batch_youtube_results = apify_gathered_results[4] if len(apify_gathered_results) > 4 else {}   # New
            batch_tiktok_results = apify_gathered_results[5] if len(apify_gathered_results) > 5 else {}    # New
            
            logger.info(f"Standalone Apify batch calls complete. Twitter: {len(batch_twitter_results)}, LinkedIn: {len(batch_linkedin_results)}, Insta: {len(batch_instagram_results)}, FB: {len(batch_facebook_results)}, YT: {len(batch_youtube_results)}, TikTok: {len(batch_tiktok_results)}")

            # --- Step 3: Enrich individual podcasts --- #
            enrichment_tasks = []
            for lead_data in leads_to_enrich:
                podcast_api_id = lead_data.get('api_id', f"unknown_id_{leads_to_enrich.index(lead_data)}")
                enrichment_tasks.append(
                    self._enrich_single_podcast(
                        initial_data=lead_data,
                        gemini_output=gemini_outputs.get(podcast_api_id),
                        batch_twitter_results=batch_twitter_results,
                        batch_linkedin_results=batch_linkedin_results,
                        # Pass new batch results (will be empty dicts if scraping not implemented)
                        batch_instagram_results=batch_instagram_results, 
                        batch_facebook_results=batch_facebook_results,
                        batch_youtube_results=batch_youtube_results,
                        batch_tiktok_results=batch_tiktok_results
                    )
                )
            
            enriched_results_from_tasks = await asyncio.gather(*enrichment_tasks)
            updated_enriched_profiles = [res for res in enriched_results_from_tasks if res is not None]
            errors_encountered = len(leads_to_enrich) - len(updated_enriched_profiles)

            # --- Step 4: Write Enriched CSV Output --- #
            if updated_enriched_profiles:
                # try:
                #     output_dir = "search_outputs" # Consistent with SearchAgent # OLD
                #     os.makedirs(output_dir, exist_ok=True)
                #     csv_filename = f"enriched_output_{temp_campaign_id}.csv"
                #     csv_path = os.path.join(output_dir, csv_filename)
                #     write_enriched_profiles_to_csv(updated_enriched_profiles, csv_path)
                #     logger.info(f"Standalone enriched profiles CSV written to {csv_path}")
                # except Exception as csv_err:
                #     logger.error(f"Failed to write standalone enriched profiles CSV: {csv_err}")
                #     csv_path = None # Ensure path is None if write fails
                
                # NEW: Use the _save_enriched_profiles_to_csv helper method
                csv_path = self._save_enriched_profiles_to_csv(updated_enriched_profiles, temp_campaign_id)
                if csv_path:
                    logger.info(f"Standalone enriched profiles CSV generated. Web path: {csv_path}")
                else:
                    logger.error(f"Failed to generate standalone enriched profiles CSV using helper for campaign {temp_campaign_id}.")
            else:
                logger.info("No profiles were successfully enriched, skipping CSV output.")

        except Exception as e:
            logger.exception(f"Critical error during standalone enrichment: {e}")
            errors_encountered = len(leads_to_enrich)
            updated_enriched_profiles = []
            # csv_path will remain None or its last state
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            # Optional: Add metrics logging here if desired for standalone runs
            logger.info(
                f"--- Standalone Enrichment Finished in {duration_ms:.2f}ms. "
                f"Campaign ID: {temp_campaign_id}, Processed: {len(leads_to_enrich)}, Enriched: {len(updated_enriched_profiles)}, Errors: {errors_encountered}. CSV: {csv_path} --- "
            )
        
        return updated_enriched_profiles, csv_path

    def _save_enriched_profiles_to_csv(self, profiles_data: List[EnrichedPodcastProfile], campaign_id: str) -> Optional[str]:
        if not profiles_data:
            return None

        # Ensure the campaign directory exists
        campaign_dir = os.path.join("data", "campaigns", campaign_id, "enrichment_results")
        os.makedirs(campaign_dir, exist_ok=True)
        
        filename = f"enriched_profiles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = os.path.join(campaign_dir, filename)

        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                # Dynamically get headers from the Pydantic model
                headers = list(EnrichedPodcastProfile.model_fields.keys())
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                for profile in profiles_data:
                    # For complex fields like social_links, you might want to serialize them (e.g., to JSON string)
                    # For now, model_dump should handle it, but be mindful for very complex nested objects.
                    dumped_profile = profile.model_dump(exclude_none=True)
                    # Serialize dicts (like social_links or host_bios) to JSON strings for CSV
                    for key, value in dumped_profile.items():
                        if isinstance(value, dict) or isinstance(value, list):
                            dumped_profile[key] = json.dumps(value)
                    writer.writerow(dumped_profile)
            
            logger.info(f"Successfully saved {len(profiles_data)} enriched profiles to {filepath}")
            # Return a web-accessible path
            if filepath.startswith("data/") or filepath.startswith("data\\"):
                web_path = "/static/" + filepath[len("data/"):].replace(os.sep, '/')
            else: # Should not happen if base is 'data'
                web_path = "/static/" + filepath.replace(os.sep, '/') 
            return web_path
        except Exception as e:
            logger.exception(f"Error saving enriched profiles to CSV: {e}")
            return None

# --- Example Usage for Testing Gemini Discovery --- #
if __name__ == "__main__":
    import asyncio
    from dotenv import load_dotenv
    import pprint
    
    print("--- Testing Enrichment Agent: Standalone Full Enrichment Flow --- ") # Modified print
    load_dotenv() 
    
    # --- Sample Podcast Data (List of Dictionaries) --- # 
    sample_leads_to_enrich = [
        {
            'api_id': '834dc1648f718c00479348f4',
            'title': 'Three Buddy Problem Security Conversations',
            'description': '<p>We discuss security topics that impact you and your family every week. We break down the topics in a way that is easy to digest and understand! If you want to be aware of the security risks in your daily life but don\'t have the time for complex technical deep dives this is the podcast for you!</p>',
            'website': 'https://sec.naraine.ai/free',
            'twitter_url': None, 'linkedin_url': None, 'instagram_url': None, 'facebook_url': None, 'youtube_url': None, 'tiktok_url': None
        },
        {
            'api_id': '12a8a8de9e26c126f401427e',
            'title': 'Leveraging AI in Business',
            'description': '<p><b>Dive into the future of business</b> with \'Leveraging AI in Business\' hosted by <b>Andreas Welsch.</b></p><p>This podcast is your essential guide to understanding and applying Artificial Intelligence (AI) to transform and grow your business. Andreas brings you expert interviews, case studies, and actionable insights on how AI is reshaping various industries, from optimizing operations and enhancing customer experiences to developing innovative products and strategies.</p><p>Whether you\'re a business leader, entrepreneur, or tech enthusiast, this podcast provides valuable knowledge on navigating the AI landscape. Discover the potential of machine learning, natural language processing, and other AI technologies to gain a competitive edge, increase efficiency, and drive significant value.</p><p>Tune in to \'Leveraging AI in Business\' and unlock the power of AI to build a smarter, more prosperous future for your business.</p>',
            'website': 'https://multiplai.ai/?utm_source=listennotes',
            'twitter_url': None, 'linkedin_url': None, 'instagram_url': None, 'facebook_url': None, 'youtube_url': None, 'tiktok_url': None
        },
        {
            'api_id': 'f107f4f8313c79b9edb81539',
            'title': 'CRT - Culture Religion and Technology',
            'description': "The CRT - ( Culture , Religion and Technology) Podcast where we talk about how Culture Religion and Technology are all converging to create a new world. The show will be a mixture of solo ranting , interviews and conversations. The show has a Christian Worldview but open to discussing anything that would be interesting to our listeners. The podcast is hosted by Darrell Harrison and Virgil Walker with Revolver News.",
            'website': 'https://revolver.news/',
            'twitter_url': None, 'linkedin_url': None, 'instagram_url': None, 'facebook_url': None, 'youtube_url': None, 'tiktok_url': None
        },
        {
            'api_id': 'fb34e2ad77f8ef0b6d2c4374',
            'title': 'Tech 4 Thought',
            'description': '<p> Inspiring innovative thinking in a rapidly evolving world.</p><p>Join host Ryan Naraine, a veteran cybersecurity journalist and founder of <a href="https://sec.naraine.ai/free" rel="noopener noreferrer" target="_blank">Naraine.AI</a>, as he explores cutting-edge technology, cybersecurity, and the impact of AI on our future. Tech 4 Thought features insightful conversations with leading experts, providing a thought-provoking perspective on the digital landscape. Tune in for discussions that will challenge your assumptions and expand your understanding of the forces shaping our world.</p>',
            'website': 'https://www.buzzsprout.com/2444636/share',
            'twitter_url': None, 'linkedin_url': None, 'instagram_url': None, 'facebook_url': None, 'youtube_url': None, 'tiktok_url': None
        },
        {
            'api_id': '9019ae17421d0c4e06f61642',
            'title': 'Serious Privacy',
            'description': '<p>For those who take privacy seriously.</p><p>Hosted by Paul Breitbarth (CIPP/E, CIPM, CIPT, FIP) and K Royal (CIPP/E, CIPP/US, CIPM, FIP), this podcast explores the latest changes and updates in data protection and privacy. Paul and K interview thought leaders and practitioners from around the globe to understand the challenges and opportunities that abound as privacy continues its evolution. Serious Privacy is a weekly show, published every Monday morning at 8 AM CET (2 AM ET).</p>',
            'website': 'https://podcast.seriousprivacy.eu/',
            'twitter_url': None, 'linkedin_url': None, 'instagram_url': None, 'facebook_url': None, 'youtube_url': None, 'tiktok_url': None
        },
        {
            'api_id': 'bf51d72a1b83487279335c86',
            'title': 'AI Unraveled: Latest AI News, Trends, and ChatGPT Insights',
            'description': '<p>In this podcast, we unravel the complexities of Artificial Intelligence, bringing you the latest news, hottest trends, and deepest insights into technologies like ChatGPT. Join us as we explore the cutting edge of AI, making sense of its impact on our world and future. Perfect for tech enthusiasts, professionals, and anyone curious about AI!</p>',
            'website': 'https://djamgatech.com/',
            'twitter_url': None, 'linkedin_url': None, 'instagram_url': None, 'facebook_url': None, 'youtube_url': None, 'tiktok_url': None
        },
        {
            'api_id': '21e636372b17481f14a0814b',
            'title': 'SunDeep Talks AI, Digitalization & Leadership',
            'description': '<p><strong>Welcome</strong></p>',
            'website': 'https://sundeepmehra.com/',
            'twitter_url': None, 'linkedin_url': None, 'instagram_url': None, 'facebook_url': None, 'youtube_url': None, 'tiktok_url': None
        }
    ]
    
    test_campaign_id = "my_test_enrich_run_social_discovery"

    # --- Instantiate Agent and Run Test --- #
    try:
        agent = EnrichmentAgent()
        
        print(f"\nRunning standalone enrichment for {len(sample_leads_to_enrich)} leads...")
        print(f"Test Campaign ID to be used (if passed): {test_campaign_id}\n")
        
        # Run the async method
        enriched_profiles, csv_path = asyncio.run(agent.perform_standalone_enrichment(
            leads_to_enrich=sample_leads_to_enrich,
            existing_campaign_id=test_campaign_id # Test with an existing ID
            # To test without, comment out the line above or pass None
        ))
        
        print("\n--- Standalone Enrichment Results --- ")
        if enriched_profiles:
            print(f"Successfully enriched {len(enriched_profiles)} profiles.")
            if csv_path:
                print(f"Output CSV generated at: {csv_path}")
            else:
                print("CSV file was not generated (or path not returned).")
            
            print("\n--- Enriched Profile Summaries (First few) ---")
            for i, profile in enumerate(enriched_profiles[:3]): # Print summary of first 3
                print(f"\nProfile {i+1} (ID: {profile.unified_profile_id}):")
                pprint.pprint(profile.model_dump(exclude_none=True), indent=2, width=100)
        else:
            print("Standalone enrichment returned no profiles or failed.")
            if csv_path:
                 print(f"A CSV path was returned even with no profiles: {csv_path}")
            
    except ValueError as e:
        print(f"\nERROR during agent instantiation or setup: {e}") 
    except Exception as e:
        print(f"\nUNEXPECTED ERROR during test execution: {e}")
        import traceback
        traceback.print_exc()

    print("\n--- Test Finished --- ")