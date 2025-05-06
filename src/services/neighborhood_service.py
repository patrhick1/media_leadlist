import logging
from typing import List, Dict, Optional, Tuple, Set
from collections import defaultdict
from functools import lru_cache
import os

# Use cachetools for timed caching if needed, basic lru_cache for simplicity now
# from cachetools import cached, TTLCache, LRUCache 

from ..models.lead import PodcastLead
from ..models.podcast_graph import RelatedPodcast
from ..persistence.mongodb import get_podcast_leads # Assuming this function allows flexible querying

# Configure logging
logger = logging.getLogger(__name__)

# --- Configuration ---
# Load from environment variables or a config file later
DEFAULT_NEIGHBORHOOD_SIZE = int(os.getenv("NEIGHBORHOOD_SIZE", "10"))
DEFAULT_WEIGHTS = {
    'category': float(os.getenv("NEIGHBORHOOD_WEIGHT_CATEGORY", "0.4")),
    'network': float(os.getenv("NEIGHBORHOOD_WEIGHT_NETWORK", "0.2")),
    'topic': float(os.getenv("NEIGHBORHOOD_WEIGHT_TOPIC", "0.4")) 
    # Ensure weights sum to 1.0
}
# Basic validation for weights
if abs(sum(DEFAULT_WEIGHTS.values()) - 1.0) > 1e-9:
    logger.warning(f"Neighborhood weights do not sum to 1.0: {DEFAULT_WEIGHTS}. Normalizing.")
    total = sum(DEFAULT_WEIGHTS.values())
    if total > 0:
        DEFAULT_WEIGHTS = {k: v / total for k, v in DEFAULT_WEIGHTS.items()}
    else: # Reset to defaults if total is zero
        DEFAULT_WEIGHTS = {'category': 0.4, 'network': 0.2, 'topic': 0.4}


# --- Helper Functions ---
def _calculate_jaccard_similarity(set1: Set[str], set2: Set[str]) -> float:
    """Calculate Jaccard similarity between two sets (intersection over union)."""
    if not set1 or not set2:
        return 0.0
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union > 0 else 0.0

# --- Service Class ---

class NeighborhoodService:
    """Service to find related podcasts based on shared attributes."""

    # Simple LRU cache for find_neighborhood results
    # Cache size can be configured
    CACHE_SIZE = int(os.getenv("NEIGHBORHOOD_CACHE_SIZE", "128"))

    @lru_cache(maxsize=CACHE_SIZE)
    def find_neighborhood(
        self, 
        podcast_id: str, 
        neighborhood_size: int = DEFAULT_NEIGHBORHOOD_SIZE, 
        weights_tuple: Optional[Tuple[Tuple[str, float], ...]] = None
    ) -> List[RelatedPodcast]:
        """
        Finds related podcasts (neighborhood) for a given podcast ID.

        Args:
            podcast_id: The ID of the target podcast.
            neighborhood_size: The maximum number of related podcasts to return.
            weights_tuple: A tuple of (key, value) pairs for weights, e.g., (('category', 0.4), ...).
                           Defaults to DEFAULT_WEIGHTS if None.

        Returns:
            A list of RelatedPodcast objects, sorted by relevance score.
        """
        weights = dict(weights_tuple) if weights_tuple else DEFAULT_WEIGHTS
        if abs(sum(weights.values()) - 1.0) > 1e-9:
            logger.warning(f"Neighborhood weights do not sum to 1.0: {weights}. Normalizing.")
            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
            else: # Reset to defaults if total is zero
                weights = {'category': 0.4, 'network': 0.2, 'topic': 0.4}

        logger.info(f"Finding neighborhood for podcast_id: {podcast_id} with weights {weights}")
        
        # 1. Get target podcast data
        target_podcast_list = get_podcast_leads({"podcast_id": podcast_id})
        if not target_podcast_list:
            logger.warning(f"Target podcast not found: {podcast_id}")
            return []
        target_podcast = target_podcast_list[0]

        # 2. Find potential neighbors by each factor
        query_limit = neighborhood_size * 3
        category_neighbors = self._find_by_category(target_podcast, query_limit)
        network_neighbors = self._find_by_network(target_podcast, query_limit)
        topic_neighbors = self._find_by_topic(target_podcast, query_limit)

        # 3. Combine and score neighbors
        combined_neighbors: Dict[str, Tuple[float, List[str], Dict[str, List[str]]]] = defaultdict(
            lambda: (0.0, [], defaultdict(list)) 
        )

        all_neighbors_data = {
            'category': category_neighbors,
            'network': network_neighbors,
            'topic': topic_neighbors
        }

        for relationship_type, neighbors_list in all_neighbors_data.items():
            weight = weights.get(relationship_type, 0.0)
            if weight == 0: continue

            for neighbor_id, score, common in neighbors_list:
                current_score, types, common_elements_dict = combined_neighbors[neighbor_id]
                
                new_score = current_score + (score * weight)
                if relationship_type not in types:
                    types.append(relationship_type)
                
                if common: 
                    common_elements_dict[relationship_type].extend(common)
                    common_elements_dict[relationship_type] = list(set(common_elements_dict[relationship_type]))
                
                combined_neighbors[neighbor_id] = (new_score, types, common_elements_dict)

        # 4. Format and sort results
        related_podcasts = [
            RelatedPodcast(
                podcast_id=pid, 
                score=data[0], 
                relationship_types=data[1],
                common_elements=dict(data[2]) 
            ) 
            for pid, data in combined_neighbors.items()
        ]
        
        related_podcasts.sort(key=lambda p: p.score, reverse=True)

        # 5. Limit results
        logger.info(f"Found {len(related_podcasts)} potential neighbors for {podcast_id}, returning top {neighborhood_size}")
        return related_podcasts[:neighborhood_size]

    def _find_by_category(self, target_podcast: PodcastLead, limit: int) -> List[Tuple[str, float, List[str]]]:
        """Finds podcasts sharing categories with the target podcast."""
        target_categories = set(target_podcast.categories)
        if not target_categories:
            return []

        # Query MongoDB for podcasts that have at least one category in common
        # This requires an index on 'categories'
        query = {
            "categories": {"$in": list(target_categories)},
            "podcast_id": {"$ne": target_podcast.podcast_id} # Exclude self
        }
        # Fetch candidates - potentially more than limit initially for better scoring
        candidate_leads = get_podcast_leads(query) # This might need limit support in persistence layer

        scored_neighbors = []
        for lead in candidate_leads:
            common_categories = target_categories.intersection(set(lead.categories))
            if not common_categories: continue # Should not happen due to query, but safe check

            # Score based on Jaccard similarity of categories
            score = _calculate_jaccard_similarity(target_categories, set(lead.categories))
            
            scored_neighbors.append((lead.podcast_id, score, list(common_categories)))

        # Sort by score and limit
        scored_neighbors.sort(key=lambda x: x[1], reverse=True)
        return scored_neighbors[:limit]

    def _find_by_network(self, target_podcast: PodcastLead, limit: int) -> List[Tuple[str, float, List[str]]]:
        """Finds podcasts within the same network."""
        target_network = target_podcast.network
        if not target_network:
            return []

        # Query MongoDB for podcasts in the same network
        # Requires index on 'network'
        query = {
            "network": target_network,
            "podcast_id": {"$ne": target_podcast.podcast_id} # Exclude self
        }
        candidate_leads = get_podcast_leads(query) # Might need limit

        # Simple scoring: score 1.0 if in the same network
        neighbors = [(lead.podcast_id, 1.0, [target_network]) for lead in candidate_leads]
        
        return neighbors[:limit] # Limit results (though network results might be fewer)

    def _find_by_topic(self, target_podcast: PodcastLead, limit: int) -> List[Tuple[str, float, List[str]]]:
        """Finds podcasts with similar topics/tags."""
        target_tags = set(target_podcast.tags)
        if not target_tags:
            return []

        # Query MongoDB for podcasts sharing at least one tag
        # Requires index on 'tags'
        query = {
            "tags": {"$in": list(target_tags)},
            "podcast_id": {"$ne": target_podcast.podcast_id} # Exclude self
        }
        candidate_leads = get_podcast_leads(query) # Might need limit

        scored_neighbors = []
        for lead in candidate_leads:
            common_tags = target_tags.intersection(set(lead.tags))
            if not common_tags: continue

            # Score based on Jaccard similarity of tags
            score = _calculate_jaccard_similarity(target_tags, set(lead.tags))
            scored_neighbors.append((lead.podcast_id, score, list(common_tags)))
            
        # Sort by score and limit
        scored_neighbors.sort(key=lambda x: x[1], reverse=True)
        return scored_neighbors[:limit]

# Example instantiation (dependency injection would handle this normally)
# neighborhood_service = NeighborhoodService() 