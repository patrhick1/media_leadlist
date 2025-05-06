import pytest
import math  # Import math
from unittest.mock import patch, MagicMock, call
from typing import List, Optional, Dict
from datetime import date, timedelta

# Models
from src.models.guests import Guest, GuestAppearance
from src.models.lead import PodcastLead
from src.models.discovery import DiscoveryResult, GuestShadowingFilterConfig, NeighborhoodFilterConfig

# Service being tested
from src.services.guest_discovery_service import GuestDiscoveryService

# --- Test Data ---

MOCK_GUESTS = {
    "guest1": Guest(guest_id="guest1", name="Alice Expert", aliases=["Ally Expert"], popularity_score=10.0),
    "guest2": Guest(guest_id="guest2", name="Bob Notable", aliases=[], popularity_score=5.0),
    "guest3": Guest(guest_id="guest3", name="Charlie Frequent", aliases=["Chuck F."], popularity_score=25.0),
}

MOCK_APPEARANCES = {
    "app1_g1": GuestAppearance(
        appearance_id="app1_g1", guest_id="guest1", podcast_id="pod1", 
        appearance_date=date.today() - timedelta(days=10), episode_title="Episode A", prominence_score=0.9
    ),
    "app2_g1": GuestAppearance(
        appearance_id="app2_g1", guest_id="guest1", podcast_id="pod2", 
        appearance_date=date.today() - timedelta(days=200), episode_title="Episode B", prominence_score=0.7
    ),
    "app3_g1": GuestAppearance(
        appearance_id="app3_g1", guest_id="guest1", podcast_id="pod3", 
        appearance_date=date.today() - timedelta(days=15), episode_title="Episode F", prominence_score=0.8
    ),
    "app1_g3": GuestAppearance(
        appearance_id="app1_g3", guest_id="guest3", podcast_id="pod1", 
        appearance_date=date.today() - timedelta(days=30), episode_title="Episode C", prominence_score=None
    ),
    "app2_g3": GuestAppearance(
        appearance_id="app2_g3", guest_id="guest3", podcast_id="pod3", 
        appearance_date=date.today() - timedelta(days=5), episode_title="Episode D", prominence_score=1.0
    ),
    "app_nodate_g3": GuestAppearance(
        appearance_id="app_nodate_g3", guest_id="guest3", podcast_id="pod4", 
        appearance_date=None, episode_title="Episode E"
    ),
}

MOCK_PODCASTS = {
    "pod1": {
        "podcast_id": "pod1", 
        "name": "Podcast One", 
        "description": "Desc P1", 
        "categories": ["Tech"], 
        "network": "NetA", 
        "tags": ["ai"]
    },
    "pod2": {
        "podcast_id": "pod2", 
        "name": "Podcast Two", 
        "description": "Desc P2", 
        "categories": ["Business"], 
        "network": "NetB", 
        "tags": ["startup"]
    },
    "pod3": {
        "podcast_id": "pod3", 
        "name": "Podcast Three", 
        "description": "Desc P3", 
        "categories": ["Tech"], 
        "network": "NetA", 
        "tags": ["web"]
    },
    "pod4": {
        "podcast_id": "pod4", 
        "name": "Podcast Four", 
        "description": "Desc P4", 
        "categories": ["News"], 
        "network": "NetC", 
        "tags": ["current"]
    },
}

# --- Pytest Fixture ---

@pytest.fixture
def mock_find_guests():
    return MagicMock()

@pytest.fixture
def mock_match_guest():
    return MagicMock()

@pytest.fixture
def mock_get_guest():
    return MagicMock()

@pytest.fixture
def mock_get_appearances():
    return MagicMock()

@pytest.fixture
def mock_save_guest():
    return MagicMock()

@pytest.fixture
def mock_get_podcast_leads():
    return MagicMock()

@pytest.fixture
def mock_milvus_search_guests():
    return MagicMock()

@pytest.fixture
def mock_hnsw_query_guests():
    return MagicMock()

@pytest.fixture
def guest_service_with_mocks(mock_find_guests, mock_match_guest, mock_get_guest, 
                              mock_get_appearances, mock_save_guest, mock_get_podcast_leads, 
                              mock_milvus_search_guests, mock_hnsw_query_guests):
    """
    Fixture providing a GuestDiscoveryService instance with all external 
    dependencies mocked.
    Yields the service instance and a dictionary of the mocks.
    """
    # Instantiate the service (dependencies don't matter here as they'll be patched)
    service = GuestDiscoveryService()
    
    # Bundle mocks for potential use in tests (though patching is preferred)
    mocks = {
        "find_guests": mock_find_guests,
        "match_guest_by_name": mock_match_guest,
        "get_guest": mock_get_guest,
        "get_appearances": mock_get_appearances,
        "save_guest": mock_save_guest,
        "get_podcast_leads": mock_get_podcast_leads,
        "milvus_search_guests": mock_milvus_search_guests,
        "hnsw_query_guests": mock_hnsw_query_guests
    }

    # Use patch context managers for broader scope if needed,
    # but prefer patching within individual tests for clarity.
    # Example (if you needed mocks across multiple tests using this fixture):
    # with patch('src.services.guest_discovery_service.find_guests', mock_find_guests),
    #      patch('src.services.guest_discovery_service.match_guest_by_name', mock_match_guest):
    #     yield service # Mocks active only within this context

    # If not patching here, just yield the service
    yield service  # Return only the service instance


# --- Test Class ---
class TestGuestDiscoveryService:

    # --- Tests for find_guest_entity ---
    # Depends on: find_guests, match_guest_by_name
    def test_find_guest_entity_success(self, guest_service_with_mocks):
        service = guest_service_with_mocks  # Get the service instance
        query_name = "Alice Expert"
        potential_matches = [MOCK_GUESTS["guest1"]]
        guest_dict = {"guest1": MOCK_GUESTS["guest1"]}  # Corrected guest_dict
        expected_guest = MOCK_GUESTS["guest1"]

        # Use patch context manager for dependencies of this method
        with patch('src.services.guest_discovery_service.find_guests', return_value=potential_matches) as mock_find, \
             patch('src.services.guest_discovery_service.match_guest_by_name', return_value=expected_guest.guest_id) as mock_match:
            result = service.find_guest_entity(query_name)

            assert result is not None
            assert result == expected_guest  # Check the whole object
            # Match actual implementation that uses MongoDB text search filter
            expected_filter = {"$text": {"$search": "alice expert"}}
            mock_find.assert_called_once_with(expected_filter, limit=10)  # Verify find_guests call
            mock_match.assert_called_once_with(query_name, guest_dict, threshold=85)

    def test_find_guest_entity_no_text_match(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        query_name = "Unknown Person"
        with patch('src.services.guest_discovery_service.find_guests', return_value=[]) as mock_find, \
             patch('src.services.guest_discovery_service.match_guest_by_name') as mock_match:
            result = service.find_guest_entity(query_name)
            assert result is None
            # Match actual implementation that uses MongoDB text search filter
            expected_filter = {"$text": {"$search": "unknown person"}}
            mock_find.assert_called_once_with(expected_filter, limit=10)
            mock_match.assert_not_called()

    def test_find_guest_entity_text_match_fuzzy_fail(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        query_name = "Alice Ex"
        potential_matches = [MOCK_GUESTS["guest1"]]
        guest_dict = {"guest1": MOCK_GUESTS["guest1"]}
        with (
            patch('src.services.guest_discovery_service.find_guests', return_value=potential_matches) as mock_find,
            patch('src.services.guest_discovery_service.match_guest_by_name', return_value=None) as mock_match
        ):
            result = service.find_guest_entity(query_name)
            assert result is None
            # Match actual implementation that uses MongoDB text search filter
            expected_filter = {"$text": {"$search": "alice ex"}}
            mock_find.assert_called_once_with(expected_filter, limit=10)
            mock_match.assert_called_once_with(query_name, guest_dict, threshold=85)

    def test_find_guest_entity_text_search_error(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        query_name = "Alice Expert"
        with (
            patch('src.services.guest_discovery_service.find_guests', side_effect=Exception("DB text search error")) as mock_find,
            patch('src.services.guest_discovery_service.match_guest_by_name') as mock_match
        ):
            result = service.find_guest_entity(query_name)
            assert result is None
            # Match actual implementation that uses MongoDB text search filter
            expected_filter = {"$text": {"$search": "alice expert"}}
            mock_find.assert_called_once_with(expected_filter, limit=10)
            mock_match.assert_not_called()

    def test_find_guest_entity_empty_name(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        # No DB calls expected, but patch to be safe if logic changes
        with (
            patch('src.services.guest_discovery_service.find_guests') as mock_find,
            patch('src.services.guest_discovery_service.match_guest_by_name') as mock_match
        ):
            result = service.find_guest_entity("")
            assert result is None
            mock_find.assert_not_called()
            mock_match.assert_not_called()

    # --- Tests for get_appearances_for_guest ---
    # Depends on: get_appearances
    def test_get_appearances_success(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        guest_id = "guest3"
        # Use the objects directly - they're already GuestAppearance instances
        expected_apps = [
            MOCK_APPEARANCES["app2_g3"], 
            MOCK_APPEARANCES["app1_g3"],
            MOCK_APPEARANCES["app_nodate_g3"]
        ]
        # Patch the specific dependency needed
        with patch('src.services.guest_discovery_service.get_appearances', return_value=expected_apps) as mock_get_apps:
            results = service.get_appearances_for_guest(guest_id, sort_by='-appearance_date', limit=5)
            assert results == expected_apps
            # Check correct filter object is passed to get_appearances
            mock_get_apps.assert_called_once_with({"guest_id": guest_id}, sort_by='-appearance_date', limit=5)
        
    def test_get_appearances_none_found(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        guest_id = "guest2"
        with patch('src.services.guest_discovery_service.get_appearances', return_value=[]) as mock_get_apps:
            results = service.get_appearances_for_guest(guest_id)
            assert results == []
            # Check correct filter object is passed to get_appearances
            mock_get_apps.assert_called_once_with({"guest_id": guest_id}, sort_by='-appearance_date', limit=50)

    # --- Tests for find_podcasts_by_guest_name ---
    # Depends on: find_guest_entity, get_appearances, get_podcast_leads, _calculate_relevancy_score
    def test_find_podcasts_by_guest_name_success(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        
        # Set up test data
        guest1 = MOCK_GUESTS["guest1"]  # Alice Expert
        guest2 = MOCK_GUESTS["guest2"]  # Bob Professional
        
        # Set up appearances
        app1_g1 = MOCK_APPEARANCES["app1_g1"]  # guest1 on pod1  
        app3_g1 = MOCK_APPEARANCES["app3_g1"]  # guest1 on pod3
        
        # Define appearance relationships  
        guest1_appearances = [app1_g1, app3_g1]
        
        # Set up podcast leads with pod1 having higher metrics than pod3
        pod1 = MOCK_PODCASTS["pod1"].copy()
        pod3 = MOCK_PODCASTS["pod3"].copy()
        
        # Create PodcastLead instances with only allowed fields
        # Set relevance_score instead of download_count and review_count
        podcast_lead1 = PodcastLead(**pod1)
        podcast_lead1.relevance_score = 0.9  # Higher score to make pod1 rank higher
        
        podcast_lead3 = PodcastLead(**pod3)
        podcast_lead3.relevance_score = 0.5  # Lower score
        
        podcast_leads = [podcast_lead1, podcast_lead3]
        
        # Set up mocks
        with patch.object(service, 'find_guest_entity', return_value=guest1) as mock_find_guest, \
             patch.object(service, 'get_appearances_for_guest', return_value=guest1_appearances) as mock_get_apps, \
             patch('src.services.guest_discovery_service.get_podcast_leads', 
                  return_value=podcast_leads) as mock_get_leads, \
             patch.object(service, '_calculate_relevancy_score', side_effect=[0.9, 0.8]) as mock_calc_score:
            
            # Execute
            results = service.find_podcasts_by_guest_name("Alice Expert")
            
            # Verify that we have the right number of results and the expected podcasts
            assert len(results) == 2
            
            # Check that both podcast IDs are in the results, ignoring order
            podcast_ids = {result.podcast_lead.podcast_id for result in results}
            assert "pod1" in podcast_ids
            assert "pod3" in podcast_ids
            
            # Check source_guests instead of guest attribute
            for result in results:
                assert len(result.source_guests) == 1
                assert result.source_guests[0].guest_id == "guest1"
            
            # Verify correct function calls - include all default parameters
            mock_find_guest.assert_called_once_with("Alice Expert", threshold=85)
            mock_get_apps.assert_called_once_with("guest1", limit=50)
            mock_get_leads.assert_called_once()
            assert mock_calc_score.call_count == 2

    def test_find_podcasts_by_guest_name_guest_not_found(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        with patch.object(service, 'find_guest_entity', return_value=None) as mock_find_entity, \
             patch('src.services.guest_discovery_service.get_appearances') as mock_get_apps, \
             patch('src.services.guest_discovery_service.get_podcast_leads') as mock_get_leads:
            results = service.find_podcasts_by_guest_name("Nobody Known")
            assert results == []
            mock_find_entity.assert_called_once_with("Nobody Known", threshold=85)
            mock_get_apps.assert_not_called()
            mock_get_leads.assert_not_called()

    def test_find_podcasts_by_guest_name_no_appearances(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        guest_name = "Bob Notable"
        guest_obj = MOCK_GUESTS["guest2"]
        with patch.object(service, 'find_guest_entity', return_value=guest_obj) as mock_find_entity, \
             patch('src.services.guest_discovery_service.get_appearances', return_value=[]) as mock_get_apps, \
             patch('src.services.guest_discovery_service.get_podcast_leads') as mock_get_leads:
            results = service.find_podcasts_by_guest_name(guest_name)
            assert results == []
            mock_find_entity.assert_called_once_with(guest_name, threshold=85)
            # Check get_appearances called with correct filter and sort_by parameter
            mock_get_apps.assert_called_once_with({"guest_id": guest_obj.guest_id}, sort_by='-appearance_date', limit=50)
            mock_get_leads.assert_not_called()
        
    # --- Tests for find_podcasts_by_guest_ids ---
    # Depends on: get_guest, get_appearances, get_podcast_leads, _calculate_relevancy_score
    def test_find_podcasts_by_guest_ids_success(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        
        # Set up test data
        guest1 = MOCK_GUESTS["guest1"]
        guest2 = MOCK_GUESTS["guest2"]
        guest_ids = [guest1.guest_id, guest2.guest_id]
        
        # Set up appearances
        app1_g1 = MOCK_APPEARANCES["app1_g1"] 
        # Create a mock appearance for guest2 since it doesn't exist in MOCK_APPEARANCES
        app2_g2 = GuestAppearance(
            appearance_id="app2_g2", 
            guest_id=guest2.guest_id, 
            podcast_id="pod2", 
            appearance_date=date.today() - timedelta(days=20), 
            episode_title="Episode G", 
            prominence_score=0.6
        )
        
        # Set up podcast leads
        pod1 = MOCK_PODCASTS["pod1"].copy()
        pod2 = MOCK_PODCASTS["pod2"].copy() 
        
        # Create PodcastLead instances with allowed fields
        podcast_lead1 = PodcastLead(**pod1)
        podcast_lead1.relevance_score = 0.9  # Higher score for pod1
        podcast_lead2 = PodcastLead(**pod2)
        podcast_lead2.relevance_score = 0.6
        
        podcast_leads = [podcast_lead1, podcast_lead2]
        
        # Define mock returns for get_guest
        def mock_get_guest(guest_id):
            if guest_id == guest1.guest_id:
                return guest1
            elif guest_id == guest2.guest_id:
                return guest2
            return None
        
        # Define mock returns for get_appearances_for_guest with correct parameters
        def mock_get_appearances(guest_id, **kwargs):
            # Accept any keyword arguments
            if guest_id == guest1.guest_id:
                return [app1_g1]
            elif guest_id == guest2.guest_id:
                return [app2_g2]
            return []
        
        # Set up mocks
        with patch('src.services.guest_discovery_service.get_guest', 
                   side_effect=mock_get_guest) as mock_get_guest, \
             patch.object(service, 'get_appearances_for_guest', 
                   side_effect=mock_get_appearances) as mock_get_apps, \
             patch('src.services.guest_discovery_service.get_podcast_leads', 
                   return_value=podcast_leads) as mock_get_leads, \
             patch.object(service, '_calculate_relevancy_score', return_value=0.9) as mock_calc_score:
            
            # Execute
            results = service.find_podcasts_by_guest_ids(guest_ids)
            
            # Verify - note: results is a Dict[str, List[DiscoveryResult]] now
            assert len(results) == 2  # Two guest IDs
            assert guest1.guest_id in results
            assert guest2.guest_id in results
            assert len(results[guest1.guest_id]) == 1
            assert len(results[guest2.guest_id]) == 1
            
            # Check individual results
            assert results[guest1.guest_id][0].podcast_lead.podcast_id == "pod1"
            assert results[guest2.guest_id][0].podcast_lead.podcast_id == "pod2"
            
            # Verify mock calls
            assert mock_get_guest.call_count == 2
            mock_get_apps.assert_any_call(guest1.guest_id, limit=50)
            mock_get_apps.assert_any_call(guest2.guest_id, limit=50)
            
            # get_podcast_leads is called once per guest ID, so it's called twice
            assert mock_get_leads.call_count == 2
            mock_get_leads.assert_any_call({"podcast_id": {"$in": ["pod1"]}})
            mock_get_leads.assert_any_call({"podcast_id": {"$in": ["pod2"]}})

    def test_find_podcasts_by_guest_ids_guest_not_found(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        
        # Set up test data
        guest1 = MOCK_GUESTS["guest1"]
        nonexistent_id = "nonexistent"
        guest_ids = [guest1.guest_id, nonexistent_id]
        
        # Set up appearances and podcasts
        app1_g1 = MOCK_APPEARANCES["app1_g1"]
        pod1 = MOCK_PODCASTS["pod1"].copy()
        
        # Create PodcastLead instance with allowed fields
        podcast_lead1 = PodcastLead(**pod1)
        podcast_lead1.relevance_score = 0.8
        podcast_leads = [podcast_lead1]
        
        # Define mock returns for get_guest
        def mock_get_guest(guest_id):
            if guest_id == guest1.guest_id:
                return guest1
            return None
        
        # Set up mocks
        with patch('src.services.guest_discovery_service.get_guest',
                   side_effect=mock_get_guest) as mock_get_guest, \
             patch.object(service, 'get_appearances_for_guest',
                   return_value=[app1_g1]) as mock_get_apps, \
             patch('src.services.guest_discovery_service.get_podcast_leads',
                   return_value=podcast_leads) as mock_get_leads, \
             patch.object(service, '_calculate_relevancy_score', return_value=0.8) as mock_calc_score:
            
            # Execute
            results = service.find_podcasts_by_guest_ids(guest_ids)
            
            # Verify - results is a Dict[str, List[DiscoveryResult]]
            assert len(results) == 2  # Two keys in the dictionary for both guest IDs
            assert guest1.guest_id in results
            assert nonexistent_id in results
            
            # Guest1 should have one result
            assert len(results[guest1.guest_id]) == 1
            assert results[guest1.guest_id][0].podcast_lead.podcast_id == "pod1"
            
            # Check source_guests instead of guest attribute
            assert len(results[guest1.guest_id][0].source_guests) == 1
            assert results[guest1.guest_id][0].source_guests[0].guest_id == guest1.guest_id
            
            # Nonexistent guest should have empty list
            assert results[nonexistent_id] == []
            
            # Verify mock calls
            assert mock_get_guest.call_count == 2
            mock_get_apps.assert_called_once_with(guest1.guest_id, limit=50)
            assert mock_get_leads.call_count == 1  # Only called for guest1

    # --- Tests for update_guest_popularity ---
    # Depends on: get_guest, get_appearances, save_guest
    def test_update_guest_popularity_success(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        guest_id = "guest3"
        # Use the guest object directly
        guest_obj = MOCK_GUESTS["guest3"].model_copy(deep=True)
        # We'll set an initial popularity score for testing
        guest_obj.popularity_score = 0.5
        
        # Recent apps with high prominence will increase score
        recent_apps = [
            GuestAppearance(appearance_id="app_recent1", guest_id=guest_id, podcast_id="pod_recent1", 
                appearance_date=date.today() - timedelta(days=5), prominence_score=0.9),
            GuestAppearance(appearance_id="app_recent2", guest_id=guest_id, podcast_id="pod_recent2", 
                appearance_date=date.today() - timedelta(days=7), prominence_score=0.8)
        ]
        
        # Patch the dependencies
        with patch('src.services.guest_discovery_service.get_guest', return_value=guest_obj) as mock_get_g, \
             patch('src.services.guest_discovery_service.get_appearances', return_value=recent_apps) as mock_get_a, \
             patch('src.services.guest_discovery_service.save_guest', return_value=guest_obj) as mock_save_g:
            
            # --- Execute ---
            result_score = service.update_guest_popularity(guest_id)
            
            # --- Assertions ---
            assert result_score is not None
            # The score is calculated and bounded between 0 and 1 in the implementation
            assert 0 <= result_score <= 1
            mock_get_g.assert_called_once_with(guest_id)
            mock_get_a.assert_called_once_with({"guest_id": guest_id}, sort_by='-appearance_date', limit=1000)
            mock_save_g.assert_called_once()
            # Check that the saved guest has updated score
            saved_guest = mock_save_g.call_args[0][0]  # First positional arg
            assert saved_guest.popularity_score == result_score
            
    def test_update_guest_popularity_zero_appearances(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        guest_id = "guest2"
        # Use the guest object directly
        guest_obj = MOCK_GUESTS["guest2"].model_copy(deep=True)
        # Set an initial non-zero score for testing
        guest_obj.popularity_score = 0.5
        
        # No appearances should result in a zero score
        with patch('src.services.guest_discovery_service.get_guest', return_value=guest_obj) as mock_get_g, \
             patch('src.services.guest_discovery_service.get_appearances', return_value=[]) as mock_get_a, \
             patch('src.services.guest_discovery_service.save_guest', return_value=guest_obj) as mock_save_g:
            
            result_score = service.update_guest_popularity(guest_id)
            
            assert result_score is not None
            assert result_score == 0.0  # Score should be exactly 0.0 with no appearances
            mock_get_g.assert_called_once_with(guest_id)
            mock_get_a.assert_called_once_with({"guest_id": guest_id}, sort_by='-appearance_date', limit=1000)
            mock_save_g.assert_called_once()
            saved_guest = mock_save_g.call_args[0][0]
            assert saved_guest.popularity_score == result_score

    def test_update_guest_popularity_guest_not_found(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        guest_id = "unknown_guest"
        with patch('src.services.guest_discovery_service.get_guest', return_value=None) as mock_get_g, \
             patch('src.services.guest_discovery_service.get_appearances') as mock_get_a, \
             patch('src.services.guest_discovery_service.save_guest') as mock_save_g:

            # --- Execute ---
            result_score = service.update_guest_popularity(guest_id)

            # --- Assertions ---
            assert result_score is None
            mock_get_g.assert_called_once_with(guest_id)
            mock_get_a.assert_not_called()
            mock_save_g.assert_not_called()

    # --- Tests for Helper: _calculate_relevancy_score ---
    # This is a private method, testing it directly is okay, but usually
    # we test its effect through public methods like find_podcasts_by_guest_name
    def test_calculate_relevancy_score_recent_date(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        today = date.today()
        recent_app = GuestAppearance(
            appearance_id="app_recent", podcast_id="pod_recent", guest_id="g_recent",
            appearance_date=today - timedelta(days=10), topic_keywords=["ai", "ml"],
            prominence_score=0.9
        )
        
        # Based on implementation: score * recency_factor * prominence_factor * position_factor
        # recency_factor = 1.0 - (days_since / 365.0) = 1.0 - (10/365) ≈ 0.973
        # prominence_factor = 0.9 (from the app)
        # position_factor = 1.0 (default)
        # Final score = 1.0 * 0.973 * 0.9 * 1.0 ≈ 0.876

        score = service._calculate_relevancy_score(recent_app)
        # Allow some flexibility for differences in calculation
        assert 0.85 <= score <= 0.89

    def test_calculate_relevancy_score_old_date(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        today = date.today()
        old_app = GuestAppearance(
            appearance_id="app_old", podcast_id="pod_old", guest_id="g_old",
            appearance_date=today - timedelta(days=730),  # 2 years old
            prominence_score=0.7
        )
        
        # Based on implementation: 
        # recency_factor = 1.0 - (days_since / 365.0) = 1.0 - (730/365) ≈ -1.0, clamped to 0.1
        # prominence_factor = 0.7
        # position_factor = 1.0 (default)
        # Final score = 1.0 * 0.1 * 0.7 * 1.0 = 0.07

        score = service._calculate_relevancy_score(old_app)
        assert 0.06 <= score <= 0.075

    def test_calculate_relevancy_score_no_date(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        no_date_app = GuestAppearance(
            appearance_id="app_nodate", podcast_id="pod_nodate", guest_id="g_nodate",
            appearance_date=None,
            prominence_score=0.7
        )
        
        # Based on implementation: 
        # recency_factor = 0.5 (default for no date)
        # prominence_factor = 0.7
        # position_factor = 1.0 (default)
        # Final score = 1.0 * 0.5 * 0.7 * 1.0 = 0.35

        score = service._calculate_relevancy_score(no_date_app)
        assert score == pytest.approx(0.35)

    # Add more tests for _calculate_relevancy_score, 
    # e.g., considering keywords, guest popularity if implemented.

    # --- Tests for Helper: _rank_and_filter_results ---
    # Also a private method. Test its effects via public methods, or directly.
    def test_rank_and_filter_results_simple(self, guest_service_with_mocks):
        service = guest_service_with_mocks
        # Dummy data
        guest = Guest(guest_id="g1", name="G One")
        pod1 = PodcastLead(
            podcast_id="p1", 
            name="Pod One",
            description="Description for Pod One"
        )
        pod2 = PodcastLead(
            podcast_id="p2", 
            name="Pod Two",
            description="Description for Pod Two"
        )
        app1 = GuestAppearance(appearance_id="a1", podcast_id="p1", guest_id="g1")
        app2 = GuestAppearance(appearance_id="a2", podcast_id="p2", guest_id="g1")
        
        results_map = {
            "p1": DiscoveryResult(
                podcast_lead=pod1, 
                discovery_methods=["guest_shadowing"],
                method_scores={"guest_shadowing": 0.9}, 
                source_guests=[guest],
                triggering_appearances=[app1]
            ),
            "p2": DiscoveryResult(
                podcast_lead=pod2, 
                discovery_methods=["guest_shadowing"],
                method_scores={"guest_shadowing": 0.7}, 
                source_guests=[guest],
                triggering_appearances=[app2]
            )
        }
        
        ranked_results = service._rank_and_filter_results(results_map, limit=1)
        assert len(ranked_results) == 1
        assert ranked_results[0].podcast_lead.podcast_id == "p1"  # Highest score
        
        ranked_results_all = service._rank_and_filter_results(results_map, limit=5)
        assert len(ranked_results_all) == 2
        assert ranked_results_all[0].podcast_lead.podcast_id == "p1"
        assert ranked_results_all[1].podcast_lead.podcast_id == "p2"

    # Add more complex tests for _rank_and_filter_results if needed
    # (e.g., tie-breaking, multiple discovery methods)