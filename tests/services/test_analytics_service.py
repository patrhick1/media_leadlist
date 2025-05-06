import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Adjust path to import from src
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.services.analytics_service import AnalyticsService, METRICS_COLLECTION

class TestAnalyticsService(unittest.TestCase):

    @patch('src.services.analytics_service._get_collection')
    def setUp(self, mock_get_collection):
        """Set up mock collection before each test."""
        self.mock_collection = MagicMock()
        mock_get_collection.return_value = self.mock_collection
        # Instantiate the service, it will now use the mock collection
        self.analytics_service = AnalyticsService()
        # Ensure the mock was used
        mock_get_collection.assert_called_with(METRICS_COLLECTION)
        # Reset mock for aggregate calls before each test method
        self.mock_collection.aggregate.reset_mock() 

    def test_get_step_durations_basic(self):
        """Test calculating step durations with sample aggregated data."""
        # Sample data returned by the mocked aggregate call
        mock_aggregate_result = [
            {
                "agent_step": "search",
                "avg_duration_ms": 1500.5,
                "all_durations": [1000.0, 2000.0, 1501.0]
            },
            {
                "agent_step": "vetting",
                "avg_duration_ms": 5000.0,
                "all_durations": [4000.0, 5000.0, 6000.0]
            }
        ]
        self.mock_collection.aggregate.return_value = mock_aggregate_result

        # Call the method under test
        durations = self.analytics_service.get_step_durations()

        # Assertions
        self.assertIn("search", durations)
        self.assertIn("vetting", durations)
        self.assertEqual(durations["search"]["avg_duration_ms"], 1500.5)
        self.assertEqual(durations["vetting"]["avg_duration_ms"], 5000.0)
        # Check median calculation (requires statistics module)
        self.assertAlmostEqual(durations["search"]["median_duration_ms"], 1501.0) 
        self.assertAlmostEqual(durations["vetting"]["median_duration_ms"], 5000.0)
        
        # Verify the aggregate pipeline structure (basic check)
        self.mock_collection.aggregate.assert_called_once()
        call_args = self.mock_collection.aggregate.call_args[0][0] # Get the pipeline argument
        self.assertIsInstance(call_args, list)
        self.assertEqual(call_args[0]['$match']['event_name'], 'agent_step_end')
        self.assertNotIn('campaign_id', call_args[0]['$match']) # No campaign filter applied
        self.assertEqual(call_args[1]['$group']['_id'], '$agent_step')

    def test_get_step_durations_with_campaign_filter(self):
        """Test calculating step durations with a campaign ID filter."""
        mock_aggregate_result = [
            {
                "agent_step": "search",
                "avg_duration_ms": 1200.0,
                "all_durations": [1100.0, 1300.0]
            }
        ]
        self.mock_collection.aggregate.return_value = mock_aggregate_result
        test_campaign_id = "campaign_xyz"

        durations = self.analytics_service.get_step_durations(campaign_id=test_campaign_id)

        self.assertIn("search", durations)
        self.assertEqual(len(durations), 1)
        self.assertEqual(durations["search"]["avg_duration_ms"], 1200.0)
        self.assertAlmostEqual(durations["search"]["median_duration_ms"], 1200.0)

        # Verify the aggregate pipeline included the campaign_id match
        self.mock_collection.aggregate.assert_called_once()
        call_args = self.mock_collection.aggregate.call_args[0][0]
        self.assertEqual(call_args[0]['$match']['campaign_id'], test_campaign_id)
        
    def test_get_step_durations_no_data(self):
        """Test case where aggregation returns no data."""
        self.mock_collection.aggregate.return_value = [] # Simulate no matching metrics

        durations = self.analytics_service.get_step_durations()

        self.assertEqual(durations, {})
        self.mock_collection.aggregate.assert_called_once()

    # --- Add tests for other AnalyticsService methods below --- #
    # test_get_vetting_tier_distribution_basic
    # test_get_search_source_performance_basic
    # test_get_crm_sync_summary_basic
    # test_generate_performance_suggestions_no_issues
    # test_generate_performance_suggestions_with_issues
    # test_compare_campaigns_basic

if __name__ == '__main__':
    unittest.main() 