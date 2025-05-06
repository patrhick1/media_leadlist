import logging
from typing import List, Dict, Any, Optional, Tuple

# Import API clients
from ..api.listennotes_client import ListenNotesAPIClient
from ..api.podscan_client import PodscanAPIClient
from ..api.exceptions import APIClientError
# Import Mappers
from .result_standardizer import ListenNotesResultMapper, PodscanResultMapper
# Import Deduplication Service
from .deduplication_service import DeduplicationService
# Import Target Model
from ..models.lead import PodcastLead
# Import Metrics Service
from .metrics_service import MetricsService

logger = logging.getLogger(__name__)

class SearchService:
    """Service to coordinate searches, standardize results, and deduplicate."""

    def __init__(self):
        """Initializes the service, API clients, mappers, deduplicator, and metrics service."""
        self.listennotes_client: Optional[ListenNotesAPIClient] = None
        self.podscan_client: Optional[PodscanAPIClient] = None
        self.ln_mapper: Optional[ListenNotesResultMapper] = None
        self.ps_mapper: Optional[PodscanResultMapper] = None
        self.deduplicator = DeduplicationService()
        self.metrics_service = MetricsService()

        try:
            self.listennotes_client = ListenNotesAPIClient()
            self.ln_mapper = ListenNotesResultMapper()
            logger.info("ListenNotes client and mapper initialized in SearchService.")
        except APIClientError as e:
            logger.error(f"Failed to initialize ListenNotes client: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error initializing ListenNotes components: {e}")

        try:
            self.podscan_client = PodscanAPIClient()
            self.ps_mapper = PodscanResultMapper()
            logger.info("Podscan client and mapper initialized in SearchService.")
        except APIClientError as e:
            logger.error(f"Failed to initialize Podscan client: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error initializing Podscan components: {e}")

        if not self.listennotes_client and not self.podscan_client:
            logger.error("SearchService failed to initialize ANY API clients.")
            if self.metrics_service:
                self.metrics_service.record_event(event_name="initialization_error", agent_step="search", metadata={"component": "API Clients", "error": "No clients initialized"})

    def search_listennotes_paginated(self, keyword: str, offset: int = 0, **kwargs) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """ 
        Performs a paginated search for a single keyword using ListenNotes.

        Args:
            keyword: The search keyword.
            offset: The starting offset for this page of results.
            **kwargs: Additional Listen Notes search parameters.

        Returns:
            A tuple containing:
            - The raw API response dictionary for the current page (or None if error).
            - Pagination info dictionary (e.g., {"has_next": bool, "next_offset": int}) or None.
        """
        if not self.listennotes_client:
            logger.warning("ListenNotes client not available for search.")
            return None, None
        
        try:
            # Pass the specific offset and other kwargs to the client
            raw_response = self.listennotes_client.search_podcasts(keyword, offset=offset, **kwargs)
            
            # Extract pagination info (assuming structure based on LN docs)
            pagination_info = {
                "has_next": raw_response.get("has_next", False),
                "next_offset": raw_response.get("next_offset"),
                "total_results": raw_response.get("total"), # Optional but useful
                "count_this_page": raw_response.get("count", 0) # Optional
            }
            
            logger.debug(f"ListenNotes page search for '{keyword}' (offset {offset}) returned {pagination_info.get('count_this_page')} results. Has Next: {pagination_info['has_next']}")
            return raw_response, pagination_info
            
        except APIClientError as e:
            logger.error(f"Error searching ListenNotes page for '{keyword}' (offset {offset}): {e}")
            # Optionally record metrics here for page-level errors
            return None, None
        except Exception as e:
            logger.exception(f"Unexpected error during ListenNotes paginated search for '{keyword}' (offset {offset}): {e}")
            return None, None

    def search_podscan_paginated(self, keyword: str, page: int = 1, per_page: int = 20, **kwargs) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """ 
        Performs a paginated search for a single keyword using Podscan.

        Args:
            keyword: The search keyword.
            page: The page number to fetch.
            per_page: The number of results per page.
            **kwargs: Additional Podscan search parameters.

        Returns:
            A tuple containing:
            - The raw API response dictionary for the current page (or None if error).
            - Pagination info dictionary (e.g., {"has_next": bool, "next_page": int, "total_results": int}) or None.
        """
        if not self.podscan_client:
            logger.warning("Podscan client not available for search.")
            return None, None
        
        try:
            # Pass the specific page and other kwargs to the client
            raw_response = self.podscan_client.search_podcasts(keyword, page=page, per_page=per_page, **kwargs)
            
            # Extract pagination info (requires knowing Podscan response structure)
            # --- ASSUMING Podscan provides these fields --- 
            # Replace with actual field names if different
            total_results = raw_response.get("total_results") # e.g., 150
            current_page = raw_response.get("current_page", page) # e.g., 1
            results_per_page = raw_response.get("per_page", per_page) # e.g., 20
            podcasts_on_page = len(raw_response.get("podcasts", []))
            
            has_next = False
            next_page = None
            if total_results is not None and current_page is not None and results_per_page is not None:
                total_pages = (total_results + results_per_page - 1) // results_per_page
                if current_page < total_pages:
                    has_next = True
                    next_page = current_page + 1
            # --- End Assumption Block --- 
            
            pagination_info = {
                "has_next": has_next,
                "next_page": next_page,
                "total_results": total_results, 
                "count_this_page": podcasts_on_page 
            }
            
            logger.debug(f"Podscan page search for '{keyword}' (page {page}) returned {pagination_info.get('count_this_page')} results. Has Next: {pagination_info['has_next']}")
            return raw_response, pagination_info
            
        except APIClientError as e:
            logger.error(f"Error searching Podscan page for '{keyword}' (page {page}): {e}")
            # Optionally record metrics here for page-level errors
            return None, None
        except Exception as e:
            logger.exception(f"Unexpected error during Podscan paginated search for '{keyword}' (page {page}): {e}")
            return None, None

# Example Usage (optional)
# if __name__ == '__main__':
#     from dotenv import load_dotenv
#     load_dotenv()
#     logging.basicConfig(level=logging.INFO)
#     search_service = SearchService()
#     if search_service:
#         # Make sure API keys are in .env for this example
#         standardized_results = search_service.search("data science podcast", genre_ids="133") # Example with LN genre filter
#         print(f"\n--- Combined & Standardized Search Results ({len(standardized_results)}) ---")
#         for i, lead in enumerate(standardized_results[:5]): # Print first 5 standardized leads
#             print(f"{i+1}. ID: {lead.podcast_id}, Name: {lead.name}, Desc: {lead.description[:50]}..., Contact: {lead.contact_details}")
#             # print(lead.model_dump_json(indent=2))
#             print("---") 