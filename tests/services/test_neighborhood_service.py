import pytest
from unittest.mock import patch, MagicMock
from typing import List, Dict, Optional, Set, Tuple

# Adjust imports based on your project structure
from src.models.lead import PodcastLead
from src.models.podcast_graph import RelatedPodcast
from src.services.neighborhood_service import (
    NeighborhoodService, 
    _calculate_jaccard_similarity, 
    DEFAULT_WEIGHTS,
    DEFAULT_NEIGHBORHOOD_SIZE
)

# --- Test Data ---

# Define the base mock data
BASE_MOCK_LEADS = {
    "target1": PodcastLead(
        podcast_id="target1", name="Target Podcast 1", description="Desc 1",
        categories=["Tech", "AI"], network="PodcastNetworkA", tags=["python", "ml", "web"]
    ),
    "cat_neighbor1": PodcastLead(
        podcast_id="cat1", name="Category Neighbor 1", description="Desc C1",
        categories=["Tech", "Cloud"], network="OtherNet", tags=["java", "cloud"]
    ),
    "cat_neighbor2": PodcastLead(
        podcast_id="cat2", name="Category Neighbor 2", description="Desc C2",
        categories=["AI", "Data"], network="OtherNet", tags=["r", "stats"]
    ),
    "net_neighbor1": PodcastLead(
        podcast_id="net1", name="Network Neighbor 1", description="Desc N1",
        categories=["Business"], network="PodcastNetworkA", tags=["finance"]
    ),
    "tag_neighbor1": PodcastLead(
        podcast_id="tag1", name="Tag Neighbor 1", description="Desc T1",
        categories=["Science"], network="OtherNet", tags=["ml", "research", "web"]
    ),
    "tag_neighbor2": PodcastLead(
        podcast_id="tag2", name="Tag Neighbor 2", description="Desc T2",
        categories=["Culture"], network="OtherNet", tags=["python", "history"]
    ),
    "full_match": PodcastLead(
        podcast_id="full1", name="Full Match", description="Desc F1",
        categories=["Tech", "AI"], network="PodcastNetworkA", tags=["python", "ml", "web"]
    ), 
    "no_match": PodcastLead(
        podcast_id="none1", name="No Match", description="Desc None",
        categories=["Sports"], network="SportsNet", tags=["football"]
    ),
}

# Mock function using a copy of the base data
def mock_get_podcast_leads_factory(mock_data: Dict[str, PodcastLead]):
    def mock_get_podcast_leads(filter_criteria: Optional[dict] = None) -> List[PodcastLead]:
        local_mock_leads = mock_data # Use the provided data
        if not filter_criteria:
            return list(local_mock_leads.values())

        podcast_id_filter = filter_criteria.get("podcast_id")
        target_id_ne = None
        specific_target_id = None

        if isinstance(podcast_id_filter, dict) and "$ne" in podcast_id_filter:
            target_id_ne = podcast_id_filter["$ne"]
        elif isinstance(podcast_id_filter, str):
            specific_target_id = podcast_id_filter

        if specific_target_id:
            return [local_mock_leads[specific_target_id]] if specific_target_id in local_mock_leads else []

        candidate_pool = {pid: lead for pid, lead in local_mock_leads.items() if pid != target_id_ne}
        results = list(candidate_pool.values())

        if "categories" in filter_criteria and "$in" in filter_criteria["categories"]:
            target_cats = set(filter_criteria["categories"]["$in"])
            results = [lead for lead in results if target_cats.intersection(set(lead.categories))]
        if "network" in filter_criteria and isinstance(filter_criteria["network"], str):
            target_net = filter_criteria["network"]
            results = [lead for lead in results if lead.network == target_net]
        if "tags" in filter_criteria and "$in" in filter_criteria["tags"]:
            target_tags_list = set(filter_criteria["tags"]["$in"])
            results = [lead for lead in results if target_tags_list.intersection(set(lead.tags))]
        return results
    return mock_get_podcast_leads

# --- Test Suite ---

@pytest.fixture
def service_with_mock_data():
    "Fixture providing service instance and a fresh copy of mock data."
    # Create a deep copy for each test run to ensure isolation
    current_mock_data = {k: v.model_copy(deep=True) for k, v in BASE_MOCK_LEADS.items()}
    mock_db_call = mock_get_podcast_leads_factory(current_mock_data)
    
    with patch('src.services.neighborhood_service.get_podcast_leads', side_effect=mock_db_call):
        instance = NeighborhoodService()
        instance.find_neighborhood.cache_clear()
        # Yield both the service and the isolated data copy
        yield instance, current_mock_data 

# --- Test Helper Functions ---

def test_calculate_jaccard_similarity():
    assert _calculate_jaccard_similarity({1, 2, 3}, {2, 3, 4}) == pytest.approx(2 / 4)
    assert _calculate_jaccard_similarity({1, 2}, {3, 4}) == 0.0
    assert _calculate_jaccard_similarity({1, 2, 3}, {1, 2, 3}) == 1.0
    assert _calculate_jaccard_similarity(set(), {1, 2}) == 0.0
    assert _calculate_jaccard_similarity({1, 2}, set()) == 0.0
    assert _calculate_jaccard_similarity(set(), set()) == 0.0

# Helper to convert dict to sorted tuple for caching
def dict_to_sorted_tuple(d: Dict) -> Optional[Tuple[Tuple[str, float], ...]]:
    if not d: return None
    return tuple(sorted(d.items()))

DEFAULT_WEIGHTS_TUPLE = dict_to_sorted_tuple(DEFAULT_WEIGHTS)

# --- Test Service Methods ---

class TestNeighborhoodService:

    def test_find_by_category(self, service_with_mock_data):
        service, mock_leads = service_with_mock_data # Unpack fixture
        target = mock_leads["target1"]
        neighbors = service._find_by_category(target, limit=5)
        
        assert len(neighbors) <= 5
        neighbor_ids = [n[0] for n in neighbors]
        # Check expected neighbors based on shared categories ("Tech", "AI")
        assert "cat1" in neighbor_ids # Shares Tech
        assert "cat2" in neighbor_ids # Shares AI
        assert "full1" in neighbor_ids # Shares Tech, AI
        assert "net1" not in neighbor_ids # No shared cats
        assert "tag1" not in neighbor_ids # No shared cats
        # Check sorting (full_match should have higher score)
        neighbor_map = {n[0]: n[1] for n in neighbors}
        assert neighbor_map["full1"] > neighbor_map["cat1"] # 2/2 vs 1/3
        assert neighbor_map["full1"] > neighbor_map["cat2"] # 2/2 vs 1/3

    def test_find_by_category_no_target_cats(self, service_with_mock_data):
        service, mock_leads = service_with_mock_data
        target = mock_leads["target1"].model_copy(update={"categories": []})
        neighbors = service._find_by_category(target, limit=5)
        assert neighbors == []

    def test_find_by_network(self, service_with_mock_data):
        service, mock_leads = service_with_mock_data
        target = mock_leads["target1"]
        neighbors = service._find_by_network(target, limit=5)
        neighbor_ids = [n[0] for n in neighbors]
        # Check expected neighbors based on network="PodcastNetworkA"
        assert "net1" in neighbor_ids
        assert "full1" in neighbor_ids
        assert "cat1" not in neighbor_ids
        # Check scores are 1.0
        assert all(n[1] == 1.0 for n in neighbors)

    def test_find_by_network_no_target_network(self, service_with_mock_data):
        service, mock_leads = service_with_mock_data
        target = mock_leads["target1"].model_copy(update={"network": None})
        neighbors = service._find_by_network(target, limit=5)
        assert neighbors == []

    def test_find_by_topic(self, service_with_mock_data):
        service, mock_leads = service_with_mock_data
        target = mock_leads["target1"]
        neighbors = service._find_by_topic(target, limit=5)
        neighbor_ids = [n[0] for n in neighbors]
        # Check expected neighbors based on tags=["python", "ml", "web"]
        assert "tag1" in neighbor_ids # Shares ml, web
        assert "tag2" in neighbor_ids # Shares python
        assert "full1" in neighbor_ids # Shares all 3
        assert "cat1" not in neighbor_ids # No shared tags
        # Check sorting (full_match should have score 1.0)
        neighbor_map = {n[0]: n[1] for n in neighbors}
        assert neighbor_map["full1"] == 1.0
        assert neighbor_map["tag1"] > neighbor_map["tag2"] # 2/4 vs 1/4

    def test_find_by_topic_no_target_tags(self, service_with_mock_data):
        service, mock_leads = service_with_mock_data
        target = mock_leads["target1"].model_copy(update={"tags": []})
        neighbors = service._find_by_topic(target, limit=5)
        assert neighbors == []

    # --- Tests for find_neighborhood --- 

    def test_find_neighborhood_target_not_found(self, service_with_mock_data):
        service, _ = service_with_mock_data # Don't need mock_leads here
        results = service.find_neighborhood("nonexistent_id", weights_tuple=None)
        assert results == []

    def test_find_neighborhood_no_matches(self, service_with_mock_data):
        service, mock_leads = service_with_mock_data # Use the isolated mock_leads
        temp_target_id = "temp_no_neighbor_target"
        # Add the temp lead to the *isolated* mock_leads for this test
        mock_leads[temp_target_id] = PodcastLead(
            podcast_id=temp_target_id, name="Isolated", description="None",
            categories=["UniqueCat"], network="UniqueNet", tags=["unique_tag"]
        )
        
        service.find_neighborhood.cache_clear()
        # The mock factory passed to the patch will use this modified mock_leads
        results = service.find_neighborhood(temp_target_id, weights_tuple=DEFAULT_WEIGHTS_TUPLE)
        assert results == [], f"Expected no neighbors for isolated target {temp_target_id}"
        # No cleanup needed as the mock_leads copy is discarded after the test

    def test_find_neighborhood_combines_scores(self, service_with_mock_data):
        service, mock_leads = service_with_mock_data # Use isolated mock_leads
        results = service.find_neighborhood("target1", weights_tuple=DEFAULT_WEIGHTS_TUPLE)
        assert len(results) <= DEFAULT_NEIGHBORHOOD_SIZE
        
        result_map = {r.podcast_id: r for r in results}
        
        # --- Debugging --- 
        # print("\nDEBUG: Keys in mock_leads for combines_scores test:", mock_leads.keys())
        # --- End Debugging ---
        
        # Ensure the keys we expect are present before trying to access them
        assert "target1" in mock_leads 
        assert "cat_neighbor1" in mock_leads # Use correct key
        
        target_lead = mock_leads["target1"]
        cat1_lead = mock_leads["cat_neighbor1"] # Use correct key

        # Check 'full1'
        assert "full1" in result_map
        full1_score = result_map["full1"].score
        cat_score = 1.0 * DEFAULT_WEIGHTS['category']
        net_score = 1.0 * DEFAULT_WEIGHTS['network']
        tag_score = 1.0 * DEFAULT_WEIGHTS['topic']
        assert full1_score == pytest.approx(cat_score + net_score + tag_score)
        assert set(result_map["full1"].relationship_types) == {'category', 'network', 'topic'}
        expected_common_full1 = {
            "category": {"AI", "Tech"},
            "network": {"PodcastNetworkA"},
            "topic": {"ml", "python", "web"}
        }
        actual_common_full1 = { key: set(value) for key, value in result_map["full1"].common_elements.items() }
        assert actual_common_full1 == expected_common_full1
        
        # Check 'cat_neighbor1' (using its actual podcast_id 'cat1')
        assert "cat1" in result_map # Check using the podcast_id from results
        cat1_score = result_map["cat1"].score
        # Use lead data fetched using the correct key
        expected_cat1_score = _calculate_jaccard_similarity(set(target_lead.categories), set(cat1_lead.categories)) * DEFAULT_WEIGHTS['category']
        assert result_map["cat1"].score == pytest.approx(expected_cat1_score)
        assert result_map["cat1"].relationship_types == ['category']
        assert result_map["cat1"].common_elements == {"category": ["Tech"]}

        # Check sorting
        assert results[0].podcast_id == "full1"
        
    def test_find_neighborhood_limit(self, service_with_mock_data):
        service, _ = service_with_mock_data
        limit = 2
        results = service.find_neighborhood("target1", neighborhood_size=limit, weights_tuple=DEFAULT_WEIGHTS_TUPLE)
        assert len(results) == limit
        assert results[0].podcast_id == "full1"
        assert results[1].podcast_id in ["cat1", "cat2", "net1", "tag1", "tag2"]

    def test_find_neighborhood_custom_weights(self, service_with_mock_data):
        service, _ = service_with_mock_data
        custom_weights_dict = {'category': 0.1, 'network': 0.8, 'topic': 0.1}
        custom_weights_tuple = dict_to_sorted_tuple(custom_weights_dict)
        results = service.find_neighborhood("target1", weights_tuple=custom_weights_tuple)
        
        result_map = {r.podcast_id: r for r in results}

        assert "full1" in result_map
        full1_score = result_map["full1"].score
        expected_full1_score = (1.0 * 0.1) + (1.0 * 0.8) + (1.0 * 0.1)
        assert full1_score == pytest.approx(expected_full1_score)

        assert "net1" in result_map
        net1_score = result_map["net1"].score
        expected_net1_score = (1.0 * 0.8)
        assert net1_score == pytest.approx(expected_net1_score)

        assert results[0].podcast_id == "full1"
        assert results[1].podcast_id == "net1"

    def test_find_neighborhood_caching(self, service_with_mock_data):
        service, _ = service_with_mock_data
        # Use default weights (passed as tuple)
        results1 = service.find_neighborhood("target1", weights_tuple=DEFAULT_WEIGHTS_TUPLE)
        cache_info1 = service.find_neighborhood.cache_info()
        assert cache_info1.hits == 0
        assert cache_info1.misses == 1
        assert cache_info1.currsize == 1

        results2 = service.find_neighborhood("target1", weights_tuple=DEFAULT_WEIGHTS_TUPLE)
        cache_info2 = service.find_neighborhood.cache_info()
        assert cache_info2.hits == 1
        assert cache_info2.misses == 1
        assert cache_info2.currsize == 1
        assert results1 == results2

        results3 = service.find_neighborhood("target1", neighborhood_size=5, weights_tuple=DEFAULT_WEIGHTS_TUPLE)
        cache_info3 = service.find_neighborhood.cache_info()
        assert cache_info3.hits == 1
        assert cache_info3.misses == 2 
        assert cache_info3.currsize == 2
        assert results1 != results3

        # Use different weights (as tuple)
        custom_weights_dict = {'category': 0.1, 'network': 0.8, 'topic': 0.1}
        custom_weights_tuple = dict_to_sorted_tuple(custom_weights_dict)
        results4 = service.find_neighborhood("target1", weights_tuple=custom_weights_tuple)
        cache_info4 = service.find_neighborhood.cache_info()
        assert cache_info4.hits == 1
        assert cache_info4.misses == 3
        assert cache_info4.currsize == 3
        assert results1 != results4 # Results should differ due to different weights 