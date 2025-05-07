import logging
from typing import Dict, Any, List, Set, Tuple, Optional # Added List, Set, Tuple, Optional
import time # Import time module
import os # For creating output directory
import re # For safe campaign_id creation
import csv # Added import for csv module
from datetime import datetime # Added import for datetime

# Import Graph State and Services
from ..graph.graph_types import GraphState
from ..services.search_service import SearchService
from ..services.keyword_service import KeywordGenerationService # Added import
from ..models.state import AgentState # To update the state model
from ..models.lead import PodcastLead # Added import
# Import MetricsService
from ..services.metrics_service import MetricsService
# Import Result Mappers
from ..services.result_standardizer import ListenNotesResultMapper, PodscanResultMapper
# Import CSV Utility
from ..utils.csv_utils import write_dicts_to_csv

logger = logging.getLogger(__name__)

# Define output directory for search results
# SEARCH_OUTPUT_DIR = "search_outputs" # OLD: Remove this or change to data based directory

class SearchAgent:
    """Agent responsible for searching for podcast leads using generated keywords."""

    def __init__(self):
        """Initializes the Search Agent with necessary services."""
        try:
            self.search_service = SearchService()
            self.keyword_service = KeywordGenerationService() # Initialize Keyword Service
            self.metrics_service = MetricsService() # Initialize metrics service
            # Instantiate mappers here for reuse
            self.ln_mapper = ListenNotesResultMapper()
            self.ps_mapper = PodscanResultMapper()
            logger.info("SearchAgent initialized with SearchService, KeywordGenerationService, and MetricsService.")
        except Exception as e:
            logger.exception("Failed to initialize services in SearchAgent.")
            self.search_service = None
            self.keyword_service = None # Add keyword service init failure handling
            self.metrics_service = None # Set to None on failure
            self.ln_mapper = None
            self.ps_mapper = None

    # --- NEW Helper: CSV Writing ---
    def _write_unified_results_to_csv(self, results: List[Dict[str, Any]], campaign_id: str, search_type: str) -> Optional[str]:
        """Writes unified search results to a CSV file inside the 'data/campaigns' directory."""
        csv_web_path = None # Changed variable name for clarity (web path vs file path)
        if results:
            try:
                timestamp = time.strftime("%Y%m%d-%H%M%S")
                safe_campaign_id = re.sub(r'[^\w\-]+', '_', campaign_id)
                filename_prefix = "search_output" if search_type == "topic" else "related_search_output"
                
                # NEW: Define base path relative to 'data' directory for static serving
                # e.g., data/campaigns/campaign_xyz/search_type/
                campaign_specific_dir_name = f"{filename_prefix}_{safe_campaign_id}_{timestamp}"
                # Filesystem path for saving
                filesystem_save_dir = os.path.join("data", "campaigns", safe_campaign_id, search_type)
                os.makedirs(filesystem_save_dir, exist_ok=True)
                
                csv_filename = f"{campaign_specific_dir_name}.csv" # Simpler filename, directory is specific
                filesystem_filepath = os.path.join(filesystem_save_dir, csv_filename)
                
                logger.info(f"Writing {len(results)} unified results to {filesystem_filepath}")
                write_dicts_to_csv(results, filesystem_filepath) # Assuming this utility works with full path

                # Construct the web-accessible path
                # Example: /static/campaigns/campaign_xyz/search_type/search_output_campaign_xyz_timestamp.csv
                relative_path_for_web = os.path.join("campaigns", safe_campaign_id, search_type, csv_filename).replace(os.sep, '/')
                csv_web_path = f"/static/{relative_path_for_web}"
                logger.info(f"CSV web path: {csv_web_path}")

            except Exception as e:
                logger.error(f"Failed to write unified search results CSV: {e}")
                csv_web_path = None
        else:
            logger.info(f"No {search_type} results found to write to CSV.")
        return csv_web_path

    # --- NEW Helper: Cross-API Enrichment --- 
    def _perform_cross_api_enrichment(self, unified_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.info(f"Starting targeted cross-API enrichment for {len(unified_list)} records...")
        enriched_list = []
        processed_rss = set() 
        enrichment_lookups = {"podscan_attempted": 0, "podscan_found": 0, "listennotes_attempted": 0, "listennotes_found": 0}

        for record in unified_list:
            rss_url = record.get("rss_url")
            itunes_id = record.get("itunes_id")
            identifier = rss_url
            if not identifier or identifier in processed_rss:
                enriched_list.append(record)
                continue
            enriched_record = record.copy()
            try:
                if record.get("source_api") == 'listennotes':
                    podscan_data = None
                    if itunes_id and enriched_record.get("audience_size") is None:
                        logger.debug(f"Attempting Podscan enrichment for LN record via iTunes ID: {itunes_id}")
                        enrichment_lookups["podscan_attempted"] += 1
                        time.sleep(0.5)
                        podscan_data = self.search_service.podscan_client.search_podcast_by_itunes_id(itunes_id) 
                        if podscan_data: enrichment_lookups["podscan_found"] += 1
                        else: logger.debug(f"Podscan lookup via iTunes ID {itunes_id} failed.")
                        
                    if podscan_data is None and rss_url and enriched_record.get("audience_size") is None: 
                        logger.debug(f"Attempting Podscan enrichment for LN record via RSS: {rss_url}")
                        if not itunes_id: enrichment_lookups["podscan_attempted"] += 1 
                        time.sleep(0.5) 
                        podscan_data = self.search_service.podscan_client.search_podcast_by_rss(rss_url)
                        if podscan_data:
                            if not itunes_id: enrichment_lookups["podscan_found"] += 1 
                        else: logger.debug(f"Podscan lookup via RSS {rss_url} also failed.")
                        
                    if podscan_data:
                        ps_reach = podscan_data.get('reach', {}) or {}
                        if enriched_record.get("audience_size") is None: enriched_record["audience_size"] = ps_reach.get('audience_size')
                        if enriched_record.get("podcast_spotify_id") is None: enriched_record["podcast_spotify_id"] = podscan_data.get('podcast_spotify_id')
                        ps_mapped_socials = self.ps_mapper.map_to_unified_dict(podscan_data) or {}
                        if enriched_record.get("itunes_rating_average") is None: enriched_record["itunes_rating_average"] = (ps_reach.get('itunes',{}) or {}).get('itunes_rating_average')
                        if enriched_record.get("itunes_rating_count") is None: enriched_record["itunes_rating_count"] = (ps_reach.get('itunes',{}) or {}).get('itunes_rating_count')
                        if enriched_record.get("spotify_rating_average") is None: enriched_record["spotify_rating_average"] = (ps_reach.get('spotify',{}) or {}).get('spotify_rating_average')
                        if enriched_record.get("spotify_rating_count") is None: enriched_record["spotify_rating_count"] = (ps_reach.get('spotify',{}) or {}).get('spotify_rating_count')
                        if enriched_record.get("instagram_url") is None: enriched_record["instagram_url"] = ps_mapped_socials.get("instagram_url")
                        if enriched_record.get("twitter_url") is None: enriched_record["twitter_url"] = ps_mapped_socials.get("twitter_url")
                        if enriched_record.get("linkedin_url") is None: enriched_record["linkedin_url"] = ps_mapped_socials.get("linkedin_url")
                        if enriched_record.get("tiktok_url") is None: enriched_record["tiktok_url"] = ps_mapped_socials.get("tiktok_url")
                        if enriched_record.get("youtube_url") is None: enriched_record["youtube_url"] = ps_mapped_socials.get("youtube_url")
                        if enriched_record.get("facebook_url") is None: enriched_record["facebook_url"] = ps_mapped_socials.get("facebook_url")
                        if enriched_record.get("other_social_url") is None: enriched_record["other_social_url"] = ps_mapped_socials.get("other_social_url")
                        logger.debug(f"Successfully merged Podscan data into LN record {record.get('api_id')}.")
                                
                elif record.get("source_api") == 'podscan':
                    listennotes_data = None
                    lookup_success = False
                    if itunes_id and enriched_record.get("listen_score") is None: 
                        logger.debug(f"Attempting ListenNotes enrichment for PS record via iTunes ID: {itunes_id}")
                        enrichment_lookups["listennotes_attempted"] += 1
                        time.sleep(0.5) 
                        listennotes_data = self.search_service.listennotes_client.lookup_podcast_by_itunes_id(itunes_id)
                        if listennotes_data:
                            lookup_success = True
                            enrichment_lookups["listennotes_found"] += 1
                        else: logger.debug(f"ListenNotes lookup via iTunes ID {itunes_id} failed.")

                    if not lookup_success and rss_url and enriched_record.get("listen_score") is None:
                         logger.debug(f"Attempting ListenNotes enrichment for PS record via RSS: {rss_url}")
                         if not itunes_id: enrichment_lookups["listennotes_attempted"] += 1
                         time.sleep(0.5) 
                         listennotes_data = self.search_service.listennotes_client.lookup_podcast_by_rss(rss_url)
                         if listennotes_data:
                             if not itunes_id: enrichment_lookups["listennotes_found"] += 1
                             lookup_success = True 
                         else: logger.debug(f"ListenNotes lookup via RSS {rss_url} also failed.")
                    
                    if listennotes_data: 
                        if enriched_record.get("listen_score") is None: enriched_record["listen_score"] = listennotes_data.get('listen_score')
                        if enriched_record.get("listen_score_global_rank") is None: enriched_record["listen_score_global_rank"] = listennotes_data.get('listen_score_global_rank')
                        if enriched_record.get("update_frequency_hours") is None: enriched_record["update_frequency_hours"] = listennotes_data.get('update_frequency_hours')
                        if enriched_record.get("latest_episode_id") is None: enriched_record["latest_episode_id"] = listennotes_data.get('latest_episode_id')
                        if enriched_record.get("latest_pub_date_ms") is None: enriched_record["latest_pub_date_ms"] = listennotes_data.get('latest_pub_date_ms')
                        if enriched_record.get("earliest_pub_date_ms") is None: enriched_record["earliest_pub_date_ms"] = listennotes_data.get('earliest_pub_date_ms')
                        logger.debug(f"Successfully merged ListenNotes data into PS record {record.get('api_id')}.")

            except Exception as lookup_err:
                 logger.warning(f"Error during cross-API enrichment lookup for record {record.get('api_id', identifier)}: {lookup_err}")
            
            enriched_list.append(enriched_record)
            processed_rss.add(identifier)

        logger.info(f"Cross-API enrichment loop finished. Podscan Lookups: {enrichment_lookups['podscan_attempted']} attempted, {enrichment_lookups['podscan_found']} found. ListenNotes Lookups: {enrichment_lookups['listennotes_attempted']} attempted, {enrichment_lookups['listennotes_found']} found.")
        return enriched_list

    # --- Existing run_search method --- 
    def run_search(self, state: GraphState) -> GraphState:
        """
        Runs the podcast search process: generates keywords, fetches results,
        unifies them, and saves to CSV.

        Args:
            state: The current graph state dictionary.

        Returns:
            The updated graph state dictionary.
        """
        logger.info("--- Executing Search Agent (Topic Search) --- ") # Clarify search type
        start_time = time.time() # Record start time
        agent_state: AgentState = state['agent_state']
        campaign_config = agent_state.campaign_config # Get config
        campaign_id = campaign_config.campaign_id if campaign_config else "unknown_campaign"
        
        # Reset relevant state fields
        agent_state.leads = [] # Clear old leads
        agent_state.raw_listennotes_results = [] # Add field for raw results
        agent_state.raw_podscan_results = []   # Add field for raw results
        unified_results = [] # Initialize unified results list
        csv_filepath = None # Initialize csv path
        generated_keywords = [] # Initialize keywords list
        
        # Record start event
        if self.metrics_service:
            self.metrics_service.record_event(
                event_name="agent_step_start", 
                agent_step="topic_search", # Use distinct step name
                campaign_id=campaign_id
            )
            
        try:
            # Check required services and mappers
            if not all([self.search_service, self.keyword_service, self.ln_mapper, self.ps_mapper]):
                error_msg = "SearchAgent dependencies (services or mappers) not available. Skipping search."
                logger.error(error_msg)
                agent_state.current_step = "error"
                agent_state.execution_status = "search_failed_dependency_missing"
                state['error_message'] = error_msg
                state['agent_state'] = agent_state
                return state
            
            # --- Keyword Generation --- 
            # Combine target_audience and key_messages for richer context
            target_audience = campaign_config.target_audience # Already checked in node
            key_messages = campaign_config.key_messages or [] # Default to empty list if None
            
            search_prompt_parts = [target_audience]
            if key_messages:
                search_prompt_parts.append("\nKey Messages:")
                search_prompt_parts.extend(key_messages)
            search_prompt = "\n".join(search_prompt_parts)
            
            logger.info(f"Generating keywords based on combined prompt: '{search_prompt[:150]}...'")
            
            try:
                # Pass combined prompt to keyword service
                generated_keywords = self.keyword_service.generate_keywords(search_prompt, num_keywords=20) 
                if not generated_keywords:
                    logger.warning("Keyword generation yielded no results. Skipping API searches.")
                    agent_state.current_step = "enrichment" # Go to next step in graph
                    agent_state.execution_status = "search_complete_no_keywords"
                    state['agent_state'] = agent_state
                    logger.info(f"--- Search Agent Finished (No Keywords). Next step: {agent_state.current_step} --- ")
                    return state 
                else:
                    logger.info(f"Successfully generated {len(generated_keywords)} keywords: {generated_keywords}")
            except Exception as e:
                logger.exception(f"Search Agent failed during keyword generation: {e}")
                agent_state.current_step = "error"
                agent_state.execution_status = "search_failed_keyword_gen"
                state['error_message'] = f"Keyword generation failed: {e}"
                if self.metrics_service:
                    self.metrics_service.record_event(
                        event_name="error", 
                        agent_step="topic_search", # Use distinct step name
                        campaign_id=campaign_id,
                        metadata={"error_type": "KeywordGen", "error_message": str(e)} # Simplified metadata
                    )
                state['agent_state'] = agent_state
                return state
            # --- End Keyword Generation --- 
            
            # --- API Search & Pagination --- 
            all_listennotes_raw_data = []
            all_podscan_raw_data = []
            total_ln_results_fetched = 0
            total_ps_results_fetched = 0
            
            for keyword in generated_keywords:
                logger.info(f"--- Searching APIs for keyword: '{keyword}' ---")
                
                # ListenNotes Pagination
                current_offset = 0
                while True:
                    logger.debug(f"Fetching ListenNotes page for '{keyword}' at offset {current_offset}")
                    ln_page_data, ln_pagination = self.search_service.search_listennotes_paginated(keyword, offset=current_offset)
                    
                    if ln_page_data:
                        page_results = ln_page_data.get('results', [])
                        all_listennotes_raw_data.extend(page_results)
                        count_this_page = len(page_results)
                        total_ln_results_fetched += count_this_page
                        logger.debug(f"Got {count_this_page} results from ListenNotes page for '{keyword}'.")
                    else:
                        logger.warning(f"ListenNotes search failed for '{keyword}' at offset {current_offset}. Stopping LN search for this keyword.")
                        break # Stop LN search for this keyword if a page fails
                        
                    if ln_pagination and ln_pagination.get("has_next"):
                        current_offset = ln_pagination.get("next_offset")
                        if current_offset is None: # Safety check
                            logger.warning(f"ListenNotes reported has_next=True but no next_offset for '{keyword}'. Stopping.")
                            break
                    else:
                        logger.debug(f"No more ListenNotes pages for keyword '{keyword}'.")
                        break # Exit while loop for this keyword

                # Podscan Pagination
                current_page = 1
                while True:
                    logger.debug(f"Fetching Podscan page {current_page} for '{keyword}'")
                    ps_page_data, ps_pagination = self.search_service.search_podscan_paginated(keyword, page=current_page)
                    if ps_page_data:
                        page_results = ps_page_data.get('podcasts', []) # Assuming list is under 'podcasts' key
                        all_podscan_raw_data.extend(page_results)
                        count_this_page = len(page_results)
                        total_ps_results_fetched += count_this_page
                        logger.debug(f"Got {count_this_page} results from Podscan page for '{keyword}'.")
                    else:
                        logger.warning(f"Podscan search failed for '{keyword}' at page {current_page}. Stopping PS search for this keyword.")
                        break # Stop Podscan search for this keyword if a page fails
                    if ps_pagination and ps_pagination.get("has_next"):
                        next_page = ps_pagination.get("next_page")
                        if next_page is None: # Safety check
                            logger.warning(f"Podscan reported has_next=True but no next_page for '{keyword}'. Stopping.")
                            break
                        current_page = next_page
                    else:
                        logger.debug(f"No more Podscan pages for keyword '{keyword}'.")
                        break # Exit while loop for this keyword
            
            logger.info(f"Finished API searches for all keywords. Total ListenNotes raw results: {len(all_listennotes_raw_data)}, Total Podscan raw results: {len(all_podscan_raw_data)}")
            # --- End API Search & Pagination --- 
            
            # --- Initial Unification --- 
            logger.info("Performing initial unification of search results...")
            initial_unified_list = []
            initial_unified_list.extend(self.ln_mapper.map_results(all_listennotes_raw_data))
            initial_unified_list.extend(self.ps_mapper.map_results(all_podscan_raw_data))
            logger.info(f"Total initial unified results (before enrichment/dedup): {len(initial_unified_list)}")

            # --- NEW: Call Cross-API Enrichment Helper --- 
            enriched_list = self._perform_cross_api_enrichment(initial_unified_list)

            # --- Deduplication & Merging --- 
            logger.info("Performing deduplication and merging...")
            if hasattr(self.search_service, 'deduplicator') and hasattr(self.search_service.deduplicator, 'deduplicate_and_merge'):
                 final_results = self.search_service.deduplicator.deduplicate_and_merge(
                     enriched_list, 
                     key_field="rss_url", 
                     priority_source="listennotes"
                 )
                 logger.info(f"Final results count after dedup/merge: {len(final_results)}")
            else:
                 logger.warning("DeduplicationService or deduplicate_and_merge method not found. Using enriched list without merging duplicates.")
                 final_results = enriched_list 
            
            # --- CSV Output (TEMP Enabled for Testing) --- #
            csv_filepath = self._write_unified_results_to_csv(final_results, campaign_id, "topic")
            
            # --- Update State --- 
            agent_state.search_results_csv_path = csv_filepath 
            agent_state.leads = final_results # Pass unified dicts
            agent_state.current_step = "enrichment"
            agent_state.execution_status = "search_complete_csv_generated" if csv_filepath else "search_complete_no_results" # Update status based on CSV creation
            state['error_message'] = None

        except Exception as e:
            # Catch any unexpected errors during the API search loops or processing
            logger.exception(f"Search Agent failed during API search execution: {e}")
            agent_state.current_step = "error"
            agent_state.execution_status = "search_failed_api_search_or_processing"
            state['error_message'] = f"API search/processing failed: {e}"
            if self.metrics_service:
                self.metrics_service.record_event(
                    event_name="error",
                    agent_step="topic_search", # Use distinct step name
                    campaign_id=campaign_id,
                    metadata={"error_type": "APIErrorOrProcessing", "error_message": str(e)}
                )
            state['agent_state'] = agent_state
            return state
        # --- End Outer Try ---
            
        finally:
            # Record end event with duration
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            if self.metrics_service:
                keywords_generated_count = len(generated_keywords)
                # Safely access counts, defaulting to 0 if variables don't exist (e.g., early exit)
                ln_results_count = len(all_listennotes_raw_data) if 'all_listennotes_raw_data' in locals() else 0
                ps_results_count = len(all_podscan_raw_data) if 'all_podscan_raw_data' in locals() else 0
                final_results_count = len(final_results) if 'final_results' in locals() else 0

                self.metrics_service.record_event(
                    event_name="agent_step_end", 
                    agent_step="topic_search", # Use distinct step name
                    campaign_id=campaign_id,
                    duration_ms=duration_ms,
                    metadata={
                        "final_status": agent_state.execution_status, 
                        "keywords_generated": keywords_generated_count,
                        "listennotes_raw_results": ln_results_count,
                        "podscan_raw_results": ps_results_count,
                        "final_merged_count": final_results_count,
                        "output_csv_path": csv_filepath
                        } 
                )
        state['agent_state'] = agent_state
        logger.info(f"--- Search Agent Finished (Topic Search). Next step: {agent_state.current_step} --- ")
        return state

    # --- NEW METHOD for Related Search --- 
    def run_related_search(self, state: GraphState) -> GraphState:
        """
        Runs the related podcast search process: fetches related podcasts based on a seed RSS,
        unifies them, deduplicates, and saves to CSV.

        Args:
            state: The current graph state dictionary.

        Returns:
            The updated graph state dictionary.
        """
        logger.info("--- Executing Related Search --- ")
        start_time = time.time() # Record start time
        agent_state: AgentState = state['agent_state']
        campaign_id = agent_state.campaign_config.campaign_id if agent_state.campaign_config else "unknown_campaign"
        seed_rss_url = agent_state.campaign_config.seed_rss_url
        
        # Clear previous results if any
        agent_state.leads = []
        agent_state.raw_listennotes_results = []
        agent_state.raw_podscan_results = []
        final_results = []
        csv_filepath = None
        
        # Record start event
        if self.metrics_service:
            self.metrics_service.record_event(
                event_name="agent_step_start", 
                agent_step="related_search", # Distinguish from topic search
                campaign_id=campaign_id,
                metadata={"seed_rss": seed_rss_url}
            )
            
        try:
            # Check required services and mappers
            if not all([self.search_service, self.ln_mapper, self.ps_mapper]):
                error_msg = "SearchAgent dependencies (services or mappers) not available for related search. Skipping."
                logger.error(error_msg)
                agent_state.current_step = "error"
                agent_state.execution_status = "related_search_failed_dependency_missing"
                state['error_message'] = error_msg
                state['agent_state'] = agent_state
                return state

            if not seed_rss_url:
                error_msg = "Seed RSS URL not provided in campaign config for related search."
                logger.error(error_msg)
                agent_state.current_step = "error"
                agent_state.execution_status = "related_search_failed_no_seed_rss"
                state['error_message'] = error_msg
                state['agent_state'] = agent_state
                return state

            logger.info(f"Starting deep related podcast search for seed RSS: {seed_rss_url}")
            
            # --- Perform Deep Related Search --- 
            # Use reasonable defaults or get from config if needed later
            max_depth = 2 
            max_total_results = 100 # Increased default limit for related search
            
            initial_final_results = self.find_related_podcasts_by_rss_deep(
                seed_rss_url=seed_rss_url,
                max_depth=max_depth,
                max_total_results=max_total_results 
            )
            logger.info(f"Deep related search yielded {len(initial_final_results)} initial unique podcasts.")
            # --- End Deep Related Search --- 

            # --- NEW: Call Cross-API Enrichment Helper --- 
            enriched_results = self._perform_cross_api_enrichment(initial_final_results)
            
            # --- Deduplicate & Merge (Potentially Optional after Enrichment) --- 
            # Depending on enrichment strategy, deduplication might happen within _perform_cross_api_enrichment 
            # or still be needed here. Assuming it's still needed for now.
            logger.info("Performing deduplication and merging on enriched related results...")
            if hasattr(self.search_service, 'deduplicator') and hasattr(self.search_service.deduplicator, 'deduplicate_and_merge'):
                 final_results = self.search_service.deduplicator.deduplicate_and_merge(
                     enriched_results, 
                     key_field="rss_url", 
                     priority_source="listennotes"
                 )
                 logger.info(f"Final related results count after enrichment & dedup/merge: {len(final_results)}")
            else:
                 final_results = enriched_results
            
            # --- CSV Output Step (TEMP Enabled for Testing) --- #
            csv_filepath = self._write_unified_results_to_csv(final_results, campaign_id, "related")

            # --- Update State --- 
            agent_state.search_results_csv_path = csv_filepath
            agent_state.leads = final_results # Assign the final enriched List[Dict[str, Any]]
            agent_state.current_step = "enrichment"
            # ... (determine execution_status based on final_results and csv_filepath) ...
            if final_results:
                if csv_filepath:
                    agent_state.execution_status = "related_search_complete_csv_generated"
                else:
                    agent_state.execution_status = "related_search_complete"
            else:
                agent_state.execution_status = "related_search_complete_no_results"
            state['error_message'] = None

        except Exception as e:
            logger.exception(f"Related Search failed during execution: {e}")
            agent_state.current_step = "error"
            agent_state.execution_status = "related_search_failed_execution"
            state['error_message'] = f"Related search failed: {e}"
            # Record error metric
            if self.metrics_service:
                self.metrics_service.record_event(
                    event_name="error",
                    agent_step="related_search",
                    campaign_id=campaign_id,
                    metadata={"error_type": "ExecutionError", "error_message": str(e), "seed_rss": seed_rss_url}
                )
            state['agent_state'] = agent_state
            return state
        # --- End Outer Try ---
            
        finally:
            # Record end event with duration
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            if self.metrics_service:
                final_results_count = len(final_results) if 'final_results' in locals() else 0
                self.metrics_service.record_event(
                    event_name="agent_step_end", 
                    agent_step="related_search",
                    campaign_id=campaign_id,
                    duration_ms=duration_ms,
                    metadata={
                        "final_status": agent_state.execution_status,
                        "final_related_count": final_results_count,
                        "output_csv_path": csv_filepath,
                        "seed_rss": seed_rss_url
                        }
                )
                
        state['agent_state'] = agent_state
        logger.info(f"--- Related Search Finished. Next step: {agent_state.current_step} --- ")
        return state
    # --- END NEW METHOD --- 

    def find_related_podcasts_by_rss(self, rss_url: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """Finds related/recommended podcasts starting from a given RSS feed.

        Args:
            rss_url: The RSS feed URL of the seed podcast.
            max_results: The approximate maximum number of related podcasts to return.

        Returns:
            A list of unified podcast dictionaries representing related podcasts.
        """
        logger.info(f"--- Finding related podcasts for RSS: {rss_url} ---")
        related_podcasts = []
        listennotes_id = None
        podscan_id = None

        # Check dependencies
        if not all([self.search_service, self.search_service.listennotes_client, self.search_service.podscan_client, self.ln_mapper, self.ps_mapper]):
            logger.error("Related podcast search dependencies not available.")
            return []
            
        # 1. Get IDs from RSS
        try:
            logger.debug(f"Looking up ListenNotes ID for RSS: {rss_url}")
            ln_data = self.search_service.listennotes_client.lookup_podcast_by_rss(rss_url)
            if ln_data:
                listennotes_id = ln_data.get('id')
                logger.info(f"Found ListenNotes ID: {listennotes_id}")
            else:
                 logger.info(f"Could not find ListenNotes ID for RSS: {rss_url}")
        except Exception as e:
             logger.warning(f"Error looking up ListenNotes ID for {rss_url}: {e}")
             
        try:
            logger.debug(f"Looking up Podscan ID for RSS: {rss_url}")
            ps_data = self.search_service.podscan_client.search_podcast_by_rss(rss_url)
            if ps_data:
                podscan_id = ps_data.get('podcast_id')
                logger.info(f"Found Podscan ID: {podscan_id}")
            else:
                 logger.info(f"Could not find Podscan ID for RSS: {rss_url}")
        except Exception as e:
            logger.warning(f"Error looking up Podscan ID for {rss_url}: {e}")

        if not listennotes_id and not podscan_id:
            logger.warning(f"Could not find any ID for RSS {rss_url}. Cannot find related podcasts.")
            return []

        # 2. Fetch Related/Recommended
        raw_ln_recommendations = []
        raw_ps_related = []
        
        if listennotes_id:
            try:
                logger.debug(f"Fetching ListenNotes recommendations for ID: {listennotes_id}")
                time.sleep(0.5) # API delay
                raw_ln_recommendations = self.search_service.listennotes_client.get_recommendations(listennotes_id) or []
                logger.info(f"Got {len(raw_ln_recommendations)} recommendations from ListenNotes.")
            except Exception as e:
                 logger.warning(f"Error fetching ListenNotes recommendations for {listennotes_id}: {e}")
                 
        if podscan_id:
            try:
                logger.debug(f"Fetching Podscan related podcasts for ID: {podscan_id}")
                time.sleep(0.5) # API delay
                raw_ps_related = self.search_service.podscan_client.get_related_podcasts(podscan_id) or []
                logger.info(f"Got {len(raw_ps_related)} related podcasts from Podscan.")
            except Exception as e:
                 logger.warning(f"Error fetching Podscan related podcasts for {podscan_id}: {e}")

        # 3. Unify Results
        unified_list = []
        if raw_ln_recommendations:
            unified_list.extend(self.ln_mapper.map_results(raw_ln_recommendations))
        if raw_ps_related:
            unified_list.extend(self.ps_mapper.map_results(raw_ps_related))
            
        logger.info(f"Total unified related podcasts before deduplication: {len(unified_list)}")

        # 4. Deduplicate & Merge
        final_results = []
        if unified_list:
            if hasattr(self.search_service, 'deduplicator') and hasattr(self.search_service.deduplicator, 'deduplicate_and_merge'):
                # Use RSS URL as the key for deduplication if available and reliable
                # Might need fallback keys like itunes_id or even title/publisher if RSS is missing
                final_results = self.search_service.deduplicator.deduplicate_and_merge(
                    unified_list, 
                    key_field="rss_url", 
                    priority_source="listennotes" # Or choose another priority
                )
                logger.info(f"Final related podcasts count after dedup/merge: {len(final_results)}")
            else:
                logger.warning("DeduplicationService or deduplicate_and_merge method not found. Using unified list without merging duplicates.")
                final_results = unified_list
        
        # Limit results if necessary (deduplication might already reduce the count)
        # Note: The APIs themselves might limit results (e.g., LN recommends up to 8)
        final_results = final_results[:max_results]
        
        logger.info(f"--- Returning {len(final_results)} related podcasts for RSS: {rss_url} --- ")
        return final_results

    def find_related_podcasts_by_rss_deep(
        self, 
        seed_rss_url: str, 
        max_depth: int = 2, # Depth of search (1 = direct related, 2 = related of related)
        max_total_results: int = 100, # Max final results
    ) -> List[Dict[str, Any]]:
        """
        Finds related podcasts up to a certain depth, starting from a seed RSS.

        Args:
            seed_rss_url: The RSS feed URL of the starting podcast.
            max_depth: How many degrees of separation to search (e.g., 2 for related of related).
            max_total_results: Approximate maximum number of unique podcasts to return in the final list.

        Returns:
            A deduplicated list of unified podcast dictionaries.
        """
        logger.info(f"--- Starting deep related search (depth={max_depth}) for RSS: {seed_rss_url} ---")
        
        all_found_podcasts: Dict[str, Dict[str, Any]] = {} # Use RSS as key for deduplication
        processed_rss: Set[str] = {seed_rss_url} # Start with seed to avoid processing it
        # Queue stores tuples of (rss_url, current_depth)
        queue: List[Tuple[str, int]] = [(seed_rss_url, 1)] 
        
        current_index = 0
        # Process the queue using index to allow adding items during iteration (BFS)
        while current_index < len(queue):
            current_rss, current_depth = queue[current_index]
            current_index += 1

            # Stop searching deeper if max_depth is reached for the current node
            if current_depth >= max_depth:
                logger.debug(f"Reached max depth ({max_depth}) for {current_rss}. Not searching further from this node.")
                continue 

            logger.debug(f"Processing depth {current_depth+1} search based on RSS: {current_rss}")

            # Fetch related podcasts for the current RSS
            try:
                # Use the existing 1st-degree search method
                related_results = self.find_related_podcasts_by_rss(
                    current_rss, 
                )

                if not related_results:
                    logger.debug(f"No related podcasts found for {current_rss}. Skipping.")
                    continue
                    
                logger.debug(f"Found {len(related_results)} potential related podcasts for {current_rss}. Adding to queue/results.")
                
                newly_added_to_queue_count = 0
                for related_podcast in related_results:
                    related_rss = related_podcast.get("rss_url")
                    if related_rss and isinstance(related_rss, str): # Basic validation
                        # Add/Update in main dictionary (handles deduplication)
                        # Check if we haven't already exceeded max results BEFORE adding
                        if related_rss not in all_found_podcasts and len(all_found_podcasts) >= max_total_results:
                            logger.info(f"Reached max_total_results ({max_total_results}) limit. Skipping addition of {related_rss}.")
                            continue # Skip adding more if limit is hit
                            
                        all_found_podcasts[related_rss] = related_podcast
                        
                        # If not processed, add to queue for the next level
                        if related_rss not in processed_rss:
                            processed_rss.add(related_rss)
                            # Only add to queue if we haven't hit the total result cap
                            if len(all_found_podcasts) <= max_total_results: 
                                queue.append((related_rss, current_depth + 1))
                                newly_added_to_queue_count += 1
                            else:
                                 logger.debug(f"Max results reached, not adding {related_rss} to queue for further processing.")
                                 
                    else:
                        logger.warning(f"Skipping related podcast result due to missing or invalid RSS URL: {related_podcast.get('podcast_title')}")
                
                logger.debug(f"Added {newly_added_to_queue_count} new podcasts to the processing queue from {current_rss}.")

            except Exception as e:
                logger.warning(f"Error fetching/processing related podcasts for {current_rss} at depth {current_depth}: {e}")
                continue # Skip to next item in queue if error occurs

        # Convert final dictionary to list
        final_list = list(all_found_podcasts.values())
        
        # Optional: Sort results? E.g., by score if available, or prioritize depth 1?
        # For now, just return the limited list.
        
        # Final limit enforcement (in case queue processing slightly overshot)
        final_list = final_list[:max_total_results]
        
        logger.info(f"--- Deep related search complete. Found {len(final_list)} unique podcasts (processed up to depth {max_depth}, limited to {max_total_results}). ---")
        return final_list

    # --- NEW METHODS FOR STANDALONE API USAGE ---

    def perform_standalone_topic_search(
        self,
        target_audience: str,
        key_messages: Optional[List[str]] = None,
        num_keywords_to_generate: int = 10,
        max_results_per_keyword: int = 50, # New parameter
        campaign_id_prefix: str = "standalone_topic"
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Performs a standalone topic-based search, returning results and CSV path.
        Does not interact with or require GraphState.
        """
        logger.info(f"--- Executing Standalone Topic Search for Target Audience: '{target_audience[:100]}...' ---")
        logger.info(f"Max results per keyword API call set to: {max_results_per_keyword}") # Log new param
        start_time = time.time()
        
        # Generate a unique campaign_id for this standalone run (for CSV naming, logging)
        timestamp = time.strftime("%Y%m%d%H%M%S")
        temp_campaign_id = f"{campaign_id_prefix}_{timestamp}"

        all_listennotes_raw_data: List[Dict[str, Any]] = []
        all_podscan_raw_data: List[Dict[str, Any]] = []
        final_results: List[Dict[str, Any]] = []
        csv_filepath: Optional[str] = None
        generated_keywords: List[str] = []

        try:
            if not all([self.search_service, self.keyword_service, self.ln_mapper, self.ps_mapper]):
                logger.error("SearchAgent dependencies not available for standalone topic search.")
                return [], None

            search_prompt_parts = [target_audience]
            if key_messages:
                search_prompt_parts.append("\nKey Messages:")
                search_prompt_parts.extend(key_messages)
            search_prompt = "\n".join(search_prompt_parts)

            logger.info(f"Generating keywords for standalone search (prompt: '{search_prompt[:100]}...')")
            generated_keywords = self.keyword_service.generate_keywords(search_prompt, num_keywords=num_keywords_to_generate)
            if not generated_keywords:
                logger.warning("Standalone search: Keyword generation yielded no results.")
                return [], None
            logger.info(f"Standalone search: Generated {len(generated_keywords)} keywords: {generated_keywords}")

            for keyword in generated_keywords:
                logger.info(f"--- Standalone search: Searching APIs for keyword: '{keyword}' ---")
                current_offset = 0
                ln_results_for_keyword = 0 # Counter for ListenNotes results for this keyword
                while True:
                    if ln_results_for_keyword >= max_results_per_keyword:
                        logger.info(f"Reached max_results_per_keyword ({max_results_per_keyword}) for ListenNotes on keyword '{keyword}'. Moving to next API/keyword.")
                        break
                        
                    ln_page_data, ln_pagination = self.search_service.search_listennotes_paginated(keyword, offset=current_offset)
                    if ln_page_data and ln_page_data.get('results'):
                        results_on_page = ln_page_data['results']
                        # Check how many can be added without exceeding the limit
                        can_add_count = max_results_per_keyword - ln_results_for_keyword
                        to_add = results_on_page[:can_add_count]
                        all_listennotes_raw_data.extend(to_add)
                        ln_results_for_keyword += len(to_add)
                        
                    if not ln_pagination or not ln_pagination.get("has_next") or ln_pagination.get("next_offset") is None or ln_results_for_keyword >= max_results_per_keyword:
                        break
                    current_offset = ln_pagination["next_offset"]
                
                current_page = 1
                ps_results_for_keyword = 0 # Counter for Podscan results for this keyword
                while True:
                    # Stop if combined LN and PS results reach the per-keyword maximum
                    if ln_results_for_keyword + ps_results_for_keyword >= max_results_per_keyword:
                        logger.info(f"Reached combined max_results_per_keyword ({max_results_per_keyword}) for keyword '{keyword}'. Moving to next keyword.")
                        break

                    ps_page_data, ps_pagination = self.search_service.search_podscan_paginated(keyword, page=current_page)
                    if ps_page_data and ps_page_data.get('podcasts'):
                        results_on_page = ps_page_data['podcasts']
                        # Calculate remaining quota across both APIs
                        remaining_quota = max_results_per_keyword - (ln_results_for_keyword + ps_results_for_keyword)
                        to_add = results_on_page[:remaining_quota]
                        all_podscan_raw_data.extend(to_add)
                        ps_results_for_keyword += len(to_add)
                    else:
                        break

                    # Stop if no further pages
                    if not ps_pagination or not ps_pagination.get('has_next') or ps_pagination.get('next_page') is None:
                        break
                    current_page = ps_pagination['next_page']
            
            logger.info(f"Standalone search: Total LN raw: {len(all_listennotes_raw_data)}, Total PS raw: {len(all_podscan_raw_data)}")

            initial_unified_list = []
            initial_unified_list.extend(self.ln_mapper.map_results(all_listennotes_raw_data))
            initial_unified_list.extend(self.ps_mapper.map_results(all_podscan_raw_data))
            logger.info(f"Standalone search: Initial unified count: {len(initial_unified_list)}")

            enriched_list = self._perform_cross_api_enrichment(initial_unified_list)
            logger.info(f"Standalone search: Enriched count: {len(enriched_list)}")

            if hasattr(self.search_service, 'deduplicator') and hasattr(self.search_service.deduplicator, 'deduplicate_and_merge'):
                final_results = self.search_service.deduplicator.deduplicate_and_merge(
                    enriched_list, key_field="rss_url", priority_source="listennotes"
                )
                logger.info(f"Standalone search: Final count after dedup: {len(final_results)}")
            else:
                logger.warning("Standalone search: Deduplicator not found. Using enriched list.")
                final_results = enriched_list

            csv_filepath = self._write_unified_results_to_csv(final_results, temp_campaign_id, "standalone_topic")

        except Exception as e:
            logger.exception(f"Error during standalone topic search for '{target_audience}': {e}")
            return [], None # Return empty list and None path on error
        finally:
            end_time = time.time()
            logger.info(f"--- Standalone Topic Search for '{target_audience}' finished in {end_time - start_time:.2f}s. Found {len(final_results)} leads. CSV: {csv_filepath} ---")
        
        return final_results, csv_filepath

    def perform_standalone_related_search(
        self,
        seed_rss_url: str,
        max_depth: int = 2,
        max_total_results: int = 50, # Reduced default for standalone API call
        campaign_id_prefix: str = "standalone_related"
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Performs a standalone related podcast search, returning results and CSV path.
        Does not interact with or require GraphState.
        """
        logger.info(f"--- Executing Standalone Related Search for RSS: {seed_rss_url} (depth={max_depth}, max_results={max_total_results}) ---")
        start_time = time.time()

        timestamp = time.strftime("%Y%m%d%H%M%S")
        temp_campaign_id = f"{campaign_id_prefix}_{timestamp}"

        final_results: List[Dict[str, Any]] = []
        csv_filepath: Optional[str] = None

        try:
            if not all([self.search_service, self.ln_mapper, self.ps_mapper]):
                logger.error("SearchAgent dependencies not available for standalone related search.")
                return [], None
            
            if not seed_rss_url:
                logger.error("Standalone related search: Seed RSS URL is required.")
                return [], None

            # 1. Perform the deep related search (gets List[Dict])
            # This method itself handles primary deduplication based on RSS during its BFS traversal.
            initial_deep_search_results = self.find_related_podcasts_by_rss_deep(
                seed_rss_url=seed_rss_url,
                max_depth=max_depth,
                max_total_results=max_total_results
            )
            logger.info(f"Standalone related search: Deep search yielded {len(initial_deep_search_results)} initial unique podcasts.")

            # 2. Perform cross-API enrichment on the results
            enriched_results = self._perform_cross_api_enrichment(initial_deep_search_results)
            logger.info(f"Standalone related search: Enriched count: {len(enriched_results)}")

            # 3. Final Deduplication (Optional but good practice after enrichment)
            # Enrichment might bring in more data that allows for better merging.
            if hasattr(self.search_service, 'deduplicator') and hasattr(self.search_service.deduplicator, 'deduplicate_and_merge'):
                final_results = self.search_service.deduplicator.deduplicate_and_merge(
                    enriched_results, 
                    key_field="rss_url", 
                    priority_source="listennotes" # Or your preferred priority
                )
                logger.info(f"Standalone related search: Final results count after enrichment & dedup/merge: {len(final_results)}")
            else:
                logger.warning("Standalone related search: Deduplicator not found. Using enriched list.")
                final_results = enriched_results

            # 4. Write to CSV
            csv_filepath = self._write_unified_results_to_csv(final_results, temp_campaign_id, "standalone_related")

        except Exception as e:
            logger.exception(f"Error during standalone related search for '{seed_rss_url}': {e}")
            return [], None # Return empty list and None path on error
        finally:
            end_time = time.time()
            logger.info(f"--- Standalone Related Search for '{seed_rss_url}' finished in {end_time - start_time:.2f}s. Found {len(final_results)} leads. CSV: {csv_filepath} ---")

        return final_results, csv_filepath


# Example Usage (within the context of a LangGraph node)
# def search_agent_node(state: GraphState) -> GraphState:
#     agent = SearchAgent()
#     return agent.run_search(state) 
    
# --- Test Block for find_related_podcasts_by_rss_deep ---
if __name__ == '__main__':
    import logging
    from dotenv import load_dotenv
    import pprint
    import time 
    
    # Load .env for local testing (ensure API keys are set)
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # Lower log level for specific modules if needed
    logging.getLogger('src.api.base_client').setLevel(logging.WARNING) # Quiet base client unless error
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    logging.getLogger('src.services.search_service').setLevel(logging.INFO)
    
    # --- Test Config ---
    seed_rss = "https://filmthreat.libsyn.com/rss"
    test_depth = 2
    test_max_total = 50 # Increase this limit
    # test_max_per_fetch = 8 # Removed in previous step
    # ---
    
    logger.info("--- Initializing SearchAgent for Deep Search Test ---")
    agent = SearchAgent()
    
    if not agent.search_service or not agent.search_service.listennotes_client or not agent.search_service.podscan_client:
        logger.error("Failed to initialize necessary services. Cannot run test.")
    else:
        logger.info(f"--- Starting Deep Related Podcast Search ---")
        logger.info(f"Seed RSS: {seed_rss}")
        logger.info(f"Max Depth: {test_depth}")
        logger.info(f"Max Total Results: {test_max_total}")
        
        start_test_time = time.time()
        try:
            # 1. Perform the deep related search (gets List[Dict])
            related_podcast_dicts = agent.find_related_podcasts_by_rss_deep(
                seed_rss_url=seed_rss,
                max_depth=test_depth,
                max_total_results=test_max_total
            )
            
            # 2. Perform cross-API enrichment on the results
            enriched_related_podcasts = agent._perform_cross_api_enrichment(related_podcast_dicts)

            # 3. Write the *enriched* results to CSV (Using the helper)
            test_campaign_id_for_csv = "test_direct_related_search"
            csv_path = agent._write_unified_results_to_csv(enriched_related_podcasts, test_campaign_id_for_csv, "related")

            end_test_time = time.time()
            logger.info(f"--- Deep Search Test Completed in {end_test_time - start_test_time:.2f} seconds ---")
            
            if csv_path:
                print(f"\nEnriched related podcasts saved to: {csv_path}")
                print(f"Found {len(enriched_related_podcasts)} podcasts.")
            else:
                print(f"\nSearch completed but failed to write CSV. Found {len(enriched_related_podcasts)} podcasts.")

            # Optional: Print summary to console
            print(f"\n--- Enriched Results Summary (First 20) ---")
            for i, podcast in enumerate(enriched_related_podcasts[:20]): # Limit console output
                title_str = podcast.get('title')
                rss_str = podcast.get('rss_url')
                source_str = podcast.get('source_api')
                ln_score = podcast.get('listen_score')
                ps_aud_size = podcast.get('audience_size')
                
                title_display = (title_str[:50] if title_str else 'N/A')
                # rss_display = (rss_str[:70] if rss_str else 'N/A')
                api_id_display = podcast.get('api_id', 'N/A')
                
                print(f"  {i+1}. {title_display} (ID: {api_id_display}, Src: {source_str}, LN_Score: {ln_score}, PS_Aud: {ps_aud_size})")

        except Exception as e:
            logger.exception("An error occurred during the deep search test.") 