import logging
import math # Add math import
from typing import List, Optional, Dict, Set # Added Set
from datetime import datetime, date, timedelta

# Models
from ..models.guests import Guest, GuestAppearance
from ..models.lead import PodcastLead
from ..models.discovery import DiscoveryResult, GuestShadowingFilterConfig, NeighborhoodFilterConfig # Added filter models

# Persistence
from ..persistence.mongodb import (
    find_guests, 
    get_guest,
    save_guest, 
    get_appearances,
    # Need functions to get leads by podcast_ids
    get_podcast_leads, # Existing function for querying leads
    get_podcast_lead # Added function for single lead
)

# Utilities
from ..utils.name_matching import normalize_name, match_guest_by_name

# Configure logging
logger = logging.getLogger(__name__)

class GuestDiscoveryService:
    """Service for finding guests and their podcast appearances."""

    def __init__(self):
        # Persistence layer access is handled by calling the functions directly for now
        # Could implement dependency injection later if needed
        pass

    def find_guest_entity(self, name: str, threshold: int = 85) -> Optional[Guest]:
        """Finds the best matching Guest entity for a given name.

        Uses text search first, then falls back to fuzzy matching if needed.
        """
        logger.info(f"Searching for guest entity matching name: '{name}'")
        normalized_name = normalize_name(name)
        if not normalized_name:
            return None

        # Option 1: Use MongoDB text search (requires text index on name/aliases)
        try:
            text_search_filter = {"$text": {"$search": normalized_name}}
            # MongoDB text search automatically scores, sort by score
            potential_matches = find_guests(text_search_filter, limit=10) # Limit initial candidates
            
            if potential_matches:
                # Simple approach: return the top text search result if reasonable
                # More complex: Could combine text score with fuzzy score below
                logger.info(f"Found potential matches via text search for '{name}'")
                # For now, let's refine with fuzzy matching on these candidates
                guest_dict = {g.guest_id: g for g in potential_matches}
                best_match_id = match_guest_by_name(name, guest_dict, threshold=threshold)
                if best_match_id:
                    logger.info(f"Refined text search result for '{name}' to guest_id: {best_match_id}")
                    return guest_dict.get(best_match_id)
                else:
                    logger.warning(f"Text search found candidates for '{name}', but fuzzy match failed threshold.")
                    # Fall through to broader fuzzy search if desired, or return None
                    # return None # Stricter: Only return if refined match is good

        except Exception as e:
            # Handle cases where text index might not exist or other DB errors
            logger.warning(f"MongoDB text search failed for name '{name}': {e}. Falling back.")

        # Option 2: Fallback or broad fuzzy match (can be slow on large datasets)
        # Avoid loading ALL guests if possible. 
        # If text search failed or didn't yield results, maybe return None or 
        # implement a more targeted query if possible (e.g., regex on normalized name).
        # For now, we'll skip the full fuzzy scan for performance reasons.
        logger.warning(f"No definitive guest match found for '{name}' via initial search.")
        return None

    def get_appearances_for_guest(self, guest_id: str, sort_by: str = '-appearance_date', limit: int = 50) -> List[GuestAppearance]:
        """Retrieves appearances for a specific guest ID."""
        logger.info(f"Getting appearances for guest_id: {guest_id}, sort: {sort_by}, limit: {limit}")
        filter_criteria = {"guest_id": guest_id}
        appearances = get_appearances(filter_criteria, sort_by=sort_by, limit=limit)
        logger.info(f"Found {len(appearances)} appearances for guest_id: {guest_id}")
        return appearances

    def _calculate_relevancy_score(self, appearance: GuestAppearance) -> float:
        """Calculates a relevancy score for an appearance based on recency, prominence, and position."""
        
        # Base score starts at 1.0
        score = 1.0
        
        # Recency factor (higher for newer appearances, 1 year decay)
        recency_factor = 0.5 # Default penalty if no date
        if appearance.appearance_date:
            try:
                # Use datetime.now().date() for comparison with date object
                days_since = (datetime.now().date() - appearance.appearance_date).days
                if days_since < 0: days_since = 0 # Handle future dates
                recency_factor = max(0.1, 1.0 - (days_since / 365.0))
            except (TypeError, ValueError):
                logger.warning(f"Could not calculate recency for appearance {appearance.appearance_id} date {appearance.appearance_date}")
                recency_factor = 0.5 # Keep penalty if date is invalid
        
        # Prominence factor (how featured the guest was)
        # Use 0.7 as default if missing, ensure minimum of 0.1
        prominence_factor = max(0.1, appearance.prominence_score or 0.7) 
        
        # Episode position factor (earlier = more important)
        position_factor = 1.0  # Default
        if appearance.episode_position is not None and appearance.episode_position > 0:
            position_factor = 1.2 if appearance.episode_position == 1 else 1.0
            
        return score * recency_factor * prominence_factor * position_factor

    def find_podcasts_by_guest_name(self, guest_name: str, threshold: int = 85, appearance_limit: int = 50, podcast_limit: int = 10) -> List[DiscoveryResult]:
        """Finds podcasts where a guest (matched by name) has appeared.

        Returns a list of DiscoveryResult objects containing the podcast lead
        and context about how it was found (guest shadowing).
        """
        logger.info(f"Finding podcasts for guest name: '{guest_name}' via guest shadowing")
        guest = self.find_guest_entity(guest_name, threshold=threshold)
        if not guest:
            logger.warning(f"Could not find guest entity for name '{guest_name}'")
            return []

        appearances = self.get_appearances_for_guest(guest.guest_id, limit=appearance_limit)
        if not appearances:
            logger.info(f"No appearances found for guest {guest.guest_id} ('{guest_name}')")
            return []

        # Score appearances
        scored_appearances = [
            (app, self._calculate_relevancy_score(app))
            for app in appearances
        ]
        # Sort by score (descending)
        scored_appearances.sort(key=lambda x: x[1], reverse=True)

        # Get unique podcast IDs and track the triggering appearance/score
        podcast_discovery_info: Dict[str, tuple[float, GuestAppearance]] = {}
        seen_podcast_ids = set()
        for app, score in scored_appearances:
            if len(seen_podcast_ids) >= podcast_limit and app.podcast_id in seen_podcast_ids:
                continue # Already have enough podcasts or this one is already included
                
            if app.podcast_id not in seen_podcast_ids:
                 if len(seen_podcast_ids) < podcast_limit:
                    seen_podcast_ids.add(app.podcast_id)
                    # Store the highest score and the first appearance that added this podcast
                    podcast_discovery_info[app.podcast_id] = (score, app)
                 else:
                     # Stop processing further appearances once we hit the limit
                     break 
            
        if not seen_podcast_ids:
             logger.info(f"No unique podcast IDs found from appearances for guest {guest.guest_id}")
             return []

        # Fetch PodcastLead data for these IDs
        unique_podcast_ids_list = list(seen_podcast_ids)
        logger.info(f"Fetching {len(unique_podcast_ids_list)} podcasts for guest {guest.guest_id}")
        podcast_filter = {"podcast_id": {"$in": unique_podcast_ids_list}}
        podcasts = get_podcast_leads(podcast_filter)
        
        # Create DiscoveryResult objects (Updated structure)
        results: List[DiscoveryResult] = []
        podcast_map = {p.podcast_id: p for p in podcasts}
        
        for podcast_id in unique_podcast_ids_list:
             if podcast_id in podcast_map:
                podcast_lead = podcast_map[podcast_id]
                score, appearance = podcast_discovery_info[podcast_id]
                results.append(
                    DiscoveryResult(
                        podcast_lead=podcast_lead,
                        discovery_methods=["guest_shadowing"],
                        method_scores={"guest_shadowing": score}, # Dict with score for this method
                        source_guests=[guest] if guest else [],
                        triggering_appearances=[appearance],
                        source_podcasts=[],
                        matched_attributes=[]
                    )
                )
             else:
                 logger.warning(f"Podcast lead data not found for ID: {podcast_id} during guest shadowing discovery for guest {guest.guest_id}")

        # No need to re-sort here, sorting happens after merging
        logger.info(f"Returning {len(results)} initial DiscoveryResult objects found via guest shadowing for '{guest_name}'")
        return results

    def find_podcasts_by_guest_ids(self, guest_ids: List[str], limit_per_guest: int = 10, appearance_limit_per_guest: int = 50) -> Dict[str, List[DiscoveryResult]]:
        """Finds top podcasts for a list of guest IDs using guest shadowing.
        
        Args:
            guest_ids: A list of guest IDs to search for.
            limit_per_guest: The maximum number of podcasts to return for each guest.
            appearance_limit_per_guest: The maximum number of appearances to consider per guest.

        Returns:
            A dictionary where keys are guest_ids and values are lists of DiscoveryResult objects.
        """
        logger.info(f"Finding podcasts for {len(guest_ids)} guest IDs: {guest_ids}")
        # TODO: Optimize with batch DB queries for appearances and podcasts
        results: Dict[str, List[DiscoveryResult]] = {}
        
        for guest_id in guest_ids:
            guest = get_guest(guest_id)
            if not guest:
                logger.warning(f"Guest ID {guest_id} not found during batch processing.")
                results[guest_id] = []
                continue

            # Directly get appearances for the guest ID
            appearances = self.get_appearances_for_guest(guest_id, limit=appearance_limit_per_guest)
            if not appearances:
                logger.info(f"No appearances found for guest {guest_id}.")
                results[guest_id] = []
                continue

            # Score and sort appearances
            scored_appearances = [
                (app, self._calculate_relevancy_score(app))
                for app in appearances
            ]
            scored_appearances.sort(key=lambda x: x[1], reverse=True)

            # Get unique podcast IDs and track triggering info
            podcast_discovery_info: Dict[str, tuple[float, GuestAppearance]] = {}
            seen_podcast_ids = set()
            for app, score in scored_appearances:
                if len(seen_podcast_ids) >= limit_per_guest and app.podcast_id in seen_podcast_ids:
                    continue
                if app.podcast_id not in seen_podcast_ids:
                    if len(seen_podcast_ids) < limit_per_guest:
                        seen_podcast_ids.add(app.podcast_id)
                        podcast_discovery_info[app.podcast_id] = (score, app)
                    else:
                        break
            
            if not seen_podcast_ids:
                results[guest_id] = []
                continue
                
            # Fetch PodcastLead data
            unique_podcast_ids_list = list(seen_podcast_ids)
            podcast_filter = {"podcast_id": {"$in": unique_podcast_ids_list}}
            podcasts = get_podcast_leads(podcast_filter)
            podcast_map = {p.podcast_id: p for p in podcasts}

            # Construct DiscoveryResult objects for this guest
            guest_results: List[DiscoveryResult] = []
            for podcast_id in unique_podcast_ids_list:
                if podcast_id in podcast_map:
                    podcast_lead = podcast_map[podcast_id]
                    score, appearance = podcast_discovery_info[podcast_id]
                    guest_results.append(
                        DiscoveryResult(
                            podcast_lead=podcast_lead,
                            discovery_methods=["guest_shadowing"],
                            method_scores={"guest_shadowing": score},
                            source_guests=[guest],
                            triggering_appearances=[appearance],
                            source_podcasts=[],
                            matched_attributes=[]
                        )
                    )
                else:
                    logger.warning(f"Podcast lead data not found for ID: {podcast_id} during discovery for guest {guest_id}")
            
            # Sort results for this specific guest
            guest_results.sort(key=lambda x: x.method_scores.get("guest_shadowing", 0), reverse=True)
            results[guest_id] = guest_results
            logger.info(f"Found {len(guest_results)} podcasts for guest {guest_id}.")
            
        logger.info(f"Completed finding podcasts for {len(guest_ids)} guest IDs.")
        return results

    def _calculate_neighborhood_similarity(self, source_podcast: PodcastLead, neighbor_podcast: PodcastLead) -> tuple[float, List[str]]:
        """Calculates a similarity score between two podcasts based on shared attributes."""
        score = 0.0
        max_score = 0.001 # Avoid division by zero
        matched_attrs: List[str] = []

        # 1. Categories (Weight: 1 per match)
        source_cats_set = set(source_podcast.categories)
        neighbor_cats_set = set(neighbor_podcast.categories)
        shared_cats = source_cats_set.intersection(neighbor_cats_set)
        score += len(shared_cats) * 1.0
        max_score += len(source_cats_set) * 1.0
        for cat in shared_cats:
            matched_attrs.append(f"Category: {cat}")

        # 2. Tags (Weight: 0.5 per match)
        source_tags_set = set(source_podcast.tags)
        neighbor_tags_set = set(neighbor_podcast.tags)
        shared_tags = source_tags_set.intersection(neighbor_tags_set)
        score += len(shared_tags) * 0.5
        max_score += len(source_tags_set) * 0.5
        for tag in shared_tags:
             matched_attrs.append(f"Tag: {tag}")

        # 3. Network (Weight: 2 if match)
        if source_podcast.network:
            max_score += 2.0
            if source_podcast.network == neighbor_podcast.network:
                score += 2.0
                matched_attrs.append(f"Network: {source_podcast.network}")

        # Normalize score (0 to 1 range roughly)
        normalized_score = score / max_score if max_score > 0 else 0
        # Clamp score just in case
        normalized_score = max(0.0, min(1.0, normalized_score))

        return normalized_score, matched_attrs

    def find_podcasts_by_neighborhood(
        self, 
        source_podcast_id: str, 
        similarity_threshold: float = 0.5, 
        limit: int = 10,
        max_candidates: int = 100 # Limit initial DB query size
    ) -> List[DiscoveryResult]:
        """Finds podcasts similar to a source podcast based on shared attributes."""
        logger.info(f"Finding neighborhood for source_podcast_id: {source_podcast_id}")

        # 1. Get source_podcast details
        source_podcast = get_podcast_lead(source_podcast_id)
        if not source_podcast:
            logger.error(f"Source podcast {source_podcast_id} not found.")
            return []

        # 2. Identify key attributes
        source_categories = source_podcast.categories
        source_tags = source_podcast.tags
        source_network = source_podcast.network

        query_parts = []
        if source_categories:
            query_parts.append({"categories": {"$in": source_categories}})
        if source_tags:
            query_parts.append({"tags": {"$in": source_tags}})
        if source_network:
            query_parts.append({"network": source_network})
        
        if not query_parts:
            logger.warning(f"Source podcast {source_podcast_id} has no categories, tags, or network to find neighbors.")
            return []

        # 3. Query for other podcasts with matching attributes (excluding self)
        mongo_filter = {
            "podcast_id": {"$ne": source_podcast_id}, # Exclude self
            "$or": query_parts
        }
        
        # Limit the number of candidates fetched from DB for performance
        potential_neighbors = get_podcast_leads(mongo_filter) # Assuming this defaults to a reasonable limit or we need to add limit param
        if len(potential_neighbors) > max_candidates:
             logger.warning(f"Found {len(potential_neighbors)} potential neighbors for {source_podcast_id}, limiting initial calculation to {max_candidates}")
             # Ideally, DB query should support limit. If not, slice here.
             potential_neighbors = potential_neighbors[:max_candidates] 

        if not potential_neighbors:
            logger.info(f"No potential neighbors found for {source_podcast_id} based on attributes.")
            return []
        
        logger.info(f"Calculating similarity for {len(potential_neighbors)} potential neighbors of {source_podcast_id}.")
        
        # 4. Calculate similarity scores & 6. Construct initial DiscoveryResult objects
        results: List[DiscoveryResult] = []
        for neighbor in potential_neighbors:
            similarity_score, matched_attrs = self._calculate_neighborhood_similarity(source_podcast, neighbor)
            
            # 5. Filter by threshold
            if similarity_score >= similarity_threshold:
                results.append(
                     DiscoveryResult(
                          podcast_lead=neighbor, 
                          discovery_methods=["neighborhood_mapping"], 
                          method_scores={"neighborhood_mapping": similarity_score}, # Dict with score
                          source_guests=[],
                          triggering_appearances=[],
                          source_podcasts=[source_podcast],
                          matched_attributes=matched_attrs 
                     )
                )
        
        # 7. Sort and limit results
        results.sort(key=lambda x: x.method_scores["neighborhood_mapping"], reverse=True)
        final_results = results[:limit]

        logger.info(f"Returning {len(final_results)} podcasts found via neighborhood mapping for {source_podcast_id}.")
        return final_results

    def discover_podcast_leads(
        self, 
        guest_name: Optional[str] = None, 
        source_podcast_id: Optional[str] = None, 
        guest_threshold: int = 85,
        neighborhood_threshold: float = 0.5,
        guest_shadowing_filters: Optional[GuestShadowingFilterConfig] = None, # Added guest filters
        neighborhood_filters: Optional[NeighborhoodFilterConfig] = None, # Added neighborhood filters
        limit: int = 20 # Overall limit for combined results
    ) -> List[DiscoveryResult]:
        """Orchestrates discovery methods, merges/combines results, filters, and ranks."""
        
        all_results: List[DiscoveryResult] = []
        
        # --- Call Individual Discovery Methods --- 
        if guest_name:
            logger.info(f"Running guest shadowing discovery for: {guest_name}")
            guest_results = self.find_podcasts_by_guest_name(
                guest_name=guest_name, 
                threshold=guest_threshold, 
                podcast_limit=limit # Use overall limit for now, could refine
            )
            all_results.extend(guest_results)
            logger.info(f"Found {len(guest_results)} results via guest shadowing.")
            
        if source_podcast_id:
            logger.info(f"Running neighborhood mapping discovery for: {source_podcast_id}")
            neighborhood_results = self.find_podcasts_by_neighborhood(
                source_podcast_id=source_podcast_id,
                similarity_threshold=neighborhood_threshold,
                limit=limit # Use overall limit for now
            )
            all_results.extend(neighborhood_results)
            logger.info(f"Found {len(neighborhood_results)} results via neighborhood mapping.")
            
        if not all_results:
            logger.info("No results found from any discovery method.")
            return []
            
        # --- Merge & Combine Results --- 
        merged_results: Dict[str, DiscoveryResult] = {}
        for result in all_results:
            podcast_id = result.podcast_lead.podcast_id
            if podcast_id not in merged_results:
                merged_results[podcast_id] = result 
            else:
                # Duplicate podcast found, combine context and scores
                existing_result = merged_results[podcast_id]
                
                # Add scores from the new method finding this podcast
                for method, score in result.method_scores.items():
                    # Add/update the score for this method
                    # (Could use max() if a method somehow ran twice, but shouldn't happen here)
                    existing_result.method_scores[method] = score 
                
                # Add new discovery method(s) if not already present
                for method in result.discovery_methods:
                     if method not in existing_result.discovery_methods:
                         existing_result.discovery_methods.append(method)
                         
                # Append context 
                existing_result.source_guests.extend(result.source_guests)
                existing_result.triggering_appearances.extend(result.triggering_appearances)
                existing_result.source_podcasts.extend(result.source_podcasts)
                # Use set to avoid duplicate attribute strings
                existing_result.matched_attributes = list(set(existing_result.matched_attributes + result.matched_attributes))

        unique_results = list(merged_results.values())
        logger.info(f"Merged results down to {len(unique_results)} unique podcasts, combining context and scores.")

        # --- Apply Method-Specific Filtering --- 
        filtered_results: List[DiscoveryResult] = []
        for result in unique_results:
            passes_filter = True 
            
            # Apply guest shadowing filters
            if "guest_shadowing" in result.discovery_methods and guest_shadowing_filters:
                gs_score = result.method_scores.get("guest_shadowing", -1.0) # Get score for this method
                if guest_shadowing_filters.min_method_score is not None and gs_score < guest_shadowing_filters.min_method_score:
                    passes_filter = False
                if guest_shadowing_filters.min_guest_popularity is not None:
                    # Check if *any* source guest meets the criteria
                    guest_passes = False
                    for guest in result.source_guests:
                        if guest and guest.popularity_score is not None and guest.popularity_score >= guest_shadowing_filters.min_guest_popularity:
                             guest_passes = True
                             break
                    if not guest_passes:
                         passes_filter = False
            
            # Apply neighborhood mapping filters
            if passes_filter and "neighborhood_mapping" in result.discovery_methods and neighborhood_filters:
                nm_score = result.method_scores.get("neighborhood_mapping", -1.0)
                if neighborhood_filters.min_method_score is not None and nm_score < neighborhood_filters.min_method_score:
                    passes_filter = False
                
            if passes_filter:
                filtered_results.append(result)
                
        logger.info(f"Filtered results down to {len(filtered_results)} podcasts based on method-specific criteria.")

        # --- Apply Unified Scoring & Ranking --- 
        # Define weights and bonus
        WEIGHT_GUEST = 1.0
        WEIGHT_NEIGHBOR = 0.8
        BONUS_FACTOR = 1.2

        for result in filtered_results:
            guest_score = result.method_scores.get("guest_shadowing", 0.0) * WEIGHT_GUEST
            neighbor_score = result.method_scores.get("neighborhood_mapping", 0.0) * WEIGHT_NEIGHBOR
            
            is_guest = "guest_shadowing" in result.discovery_methods
            is_neighbor = "neighborhood_mapping" in result.discovery_methods
            
            unified_score = 0.0
            if is_guest and is_neighbor:
                # Found by both: use max weighted score and apply bonus
                unified_score = max(guest_score, neighbor_score) * BONUS_FACTOR
            elif is_guest:
                unified_score = guest_score
            elif is_neighbor:
                unified_score = neighbor_score
                
            # Ensure score is capped at a reasonable maximum (e.g., if bonus pushes > 1)
            result.unified_relevance_score = min(1.0, unified_score) # Cap at 1.0 for now
             
        filtered_results.sort(key=lambda x: x.unified_relevance_score or 0, reverse=True)
        
        # --- Apply Overall Limit --- 
        final_results = filtered_results[:limit]
        
        logger.info(f"Returning final {len(final_results)} discovery results.")
        return final_results

    def update_guest_popularity(self, guest_id: str) -> Optional[float]:
        """Calculates and updates the popularity score for a guest based on appearance count and recency."""
        logger.info(f"Updating popularity for guest_id: {guest_id}")
        guest = get_guest(guest_id)
        if not guest:
            logger.warning(f"Cannot update popularity, guest {guest_id} not found.")
            return None

        appearances = self.get_appearances_for_guest(guest_id, limit=1000) # Get more appearances for calculation
        num_appearances = len(appearances)

        if num_appearances == 0:
            popularity_score = 0.0
        else:
            # 1. Logarithmic count of appearances
            log_count_score = math.log1p(num_appearances) # log1p(x) = log(1+x)

            # 2. Recency-weighted count
            recency_weighted_score = 0.0
            max_recency_score = 0.0 # To normalize later
            today = datetime.now().date()
            for app in appearances:
                weight = 0.5 # Default weight if no date
                if app.appearance_date:
                    try:
                        days_since = (today - app.appearance_date).days
                        if days_since < 0: days_since = 0
                        # Use a longer decay period (e.g., 3 years) than relevancy score
                        weight = max(0.1, 1.0 - (days_since / (365.0 * 3)))
                    except (TypeError, ValueError):
                        weight = 0.5 # Keep penalty
                recency_weighted_score += weight
                max_recency_score += 1.0 # Max possible score is 1 per appearance
            
            # Normalize recency score (0 to 1 range approx)
            normalized_recency_score = (recency_weighted_score / max_recency_score) if max_recency_score > 0 else 0
            
            # Combine scores (Example: 50% log count, 50% recency)
            # Note: log_count_score isn't normalized to 0-1, need a better combination strategy potentially
            # For now, let's scale log score heuristically. log1p(10) ~ 2.4, log1p(100) ~ 4.6
            # Let's try scaling log score by a factor to bring it closer to 0-1 range for combination
            # Max expected appearances? 1000? log1p(1000) ~ 6.9. Divide by 7?
            scaled_log_score = log_count_score / 7.0 
            
            popularity_score = 0.5 * scaled_log_score + 0.5 * normalized_recency_score
            # Ensure score is within a reasonable range (e.g., 0-1 or higher if desired)
            popularity_score = max(0, min(1.0, popularity_score)) # Clamp to 0-1 for now

        guest.popularity_score = popularity_score
        try:
            save_guest(guest)
            logger.info(f"Updated popularity for guest {guest_id} to {popularity_score:.4f}")
            return popularity_score
        except Exception as e:
            logger.error(f"Failed to save updated popularity for guest {guest_id}: {e}")
            return None

    def _rank_and_filter_results(self, results_map: Dict[str, DiscoveryResult], limit: int = 10) -> List[DiscoveryResult]:
        """
        Ranks and filters a dictionary of discovery results based on their scores.
        
        Args:
            results_map: Dictionary mapping podcast IDs to DiscoveryResult objects
            limit: Maximum number of results to return
            
        Returns:
            List of DiscoveryResult objects, sorted by score (highest first) and limited to the specified count
        """
        # Convert dictionary to list
        results_list = list(results_map.values())
        
        # Sort by the guest_shadowing score (or other method score as needed)
        # Using the first discovery method found in each result
        def get_score(result):
            if not result.method_scores or not result.discovery_methods:
                return 0.0
            # Use the first discovery method's score
            method = result.discovery_methods[0]
            return result.method_scores.get(method, 0.0)
        
        # Sort results by score, highest first
        results_list.sort(key=get_score, reverse=True)
        
        # Apply limit
        return results_list[:limit]

# Example Usage (Dependency Injection would be better)
# guest_service = GuestDiscoveryService()
# discovery_results = guest_service.find_podcasts_by_guest_name("Some Guest Name") 