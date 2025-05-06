import unittest
from unittest.mock import patch, MagicMock, call
import os
import sys

# Target for testing
from src.services.search_service import SearchService

# Dependencies to mock
from src.api_clients.listennotes_client import ListenNotesAPIClient
from src.api_clients.podscan_client import PodscanAPIClient
from src.services.deduplication_service import DeduplicationService
from src.services.metrics_service import MetricsService
from src.utils.podcast_mapping import map_listennotes_result_to_lead, map_podscan_result_to_lead

# Models used
from src.models.lead import PodcastLead
from src.models.campaign import CampaignConfiguration


class TestSearchService(unittest.TestCase):
    """Unit tests for the SearchService."""

    def setUp(self):
        """Set up test environment before each test."""
        # Mock dependencies that are instantiated within SearchService
        self.mock_listennotes_client = MagicMock(spec=ListenNotesAPIClient)
        self.mock_podscan_client = MagicMock(spec=PodscanAPIClient)
        self.mock_dedup_service = MagicMock(spec=DeduplicationService)
        self.mock_metrics_service = MagicMock(spec=MetricsService)

        # Create the service instance with mocked dependencies
        self.search_service = SearchService(
            listennotes_client=self.mock_listennotes_client,
            podscan_client=self.mock_podscan_client,
            dedup_service=self.mock_dedup_service,
            metrics_service=self.mock_metrics_service
        )

        # Example campaign config for context
        self.campaign_config = CampaignConfiguration(
            campaign_id="test-campaign-123",
            target_audience="tech startups",
            key_messages=["boost productivity"],
            listennotes_query="python programming",
            podscan_query="ai ethics"
        )

    # --- Test Cases --- #

    @patch('src.services.search_service.map_listennotes_result_to_lead')
    @patch('src.services.search_service.map_podscan_result_to_lead')
    def test_search_success_both_sources(self, mock_map_podscan, mock_map_listennotes):
        """Test successful search with results from both ListenNotes and Podscan."""
        # Arrange: Mock API client responses
        mock_listennotes_results = [{"id": "ln1", "title_original": "LN Podcast 1"}]
        mock_podscan_results = [{"id": "ps1", "title": "PS Podcast 1"}]
        self.mock_listennotes_client.search_podcasts.return_value = mock_listennotes_results
        self.mock_podscan_client.search_podcasts.return_value = mock_podscan_results

        # Arrange: Mock mapper functions
        mock_mapped_ln_lead = PodcastLead(podcast_id="ln_std_1", name="LN Podcast 1 Mapped")
        mock_mapped_ps_lead = PodcastLead(podcast_id="ps_std_1", name="PS Podcast 1 Mapped")
        mock_map_listennotes.return_value = mock_mapped_ln_lead
        mock_map_podscan.return_value = mock_mapped_ps_lead

        # Arrange: Mock deduplication service
        # Assume deduplication removes nothing in this case
        deduped_leads = [mock_mapped_ln_lead, mock_mapped_ps_lead]
        self.mock_dedup_service.deduplicate.return_value = deduped_leads

        # Act: Run the search
        found_leads = self.search_service.search(self.campaign_config)

        # Assert: Check API calls
        self.mock_listennotes_client.search_podcasts.assert_called_once_with("python programming")
        self.mock_podscan_client.search_podcasts.assert_called_once_with("ai ethics")

        # Assert: Check mapping calls
        mock_map_listennotes.assert_called_once_with(mock_listennotes_results[0])
        mock_map_podscan.assert_called_once_with(mock_podscan_results[0])

        # Assert: Check deduplication call
        self.mock_dedup_service.deduplicate.assert_called_once_with([mock_mapped_ln_lead, mock_mapped_ps_lead])

        # Assert: Check final result
        self.assertEqual(found_leads, deduped_leads)
        self.assertEqual(len(found_leads), 2)

        # Assert: Check metrics calls (basic structure)
        self.mock_metrics_service.record_event.assert_any_call(
            event_name="api_results", agent_step="search", count=1, metadata={"source": "ListenNotes"}, campaign_id="test-campaign-123"
        )
        self.mock_metrics_service.record_event.assert_any_call(
            event_name="api_results", agent_step="search", count=1, metadata={"source": "Podscan"}, campaign_id="test-campaign-123"
        )
        # Add more specific metric checks as needed (e.g., mapped_results, deduplication counts)


if __name__ == '__main__':
    unittest.main() 