import unittest
from unittest.mock import MagicMock, patch
import logging

# Mock the PodcastLead model
class MockPodcastLead:
    def __init__(self, podcast_id, name, categories=None, description=None, network=None):
        self.podcast_id = podcast_id
        self.name = name
        self.categories = categories if categories is not None else []
        self.description = description
        self.network = network

# Mock the RelatedPodcastResult model
class MockRelatedPodcastResult:
     def __init__(self, related_lead, score, match_type):
        self.related_lead = related_lead
        self.score = score
        self.match_type = match_type

# Mock BaseModel
class BaseModel:
    pass

# --- IMPORTANT: Import the class *after* defining mocks ---
# This ensures the mocks are defined before the module is loaded
from src.agents.search.discovery_service import AdvancedDiscoveryService
from src.models.lead import PodcastLead

# Disable logging for tests
logging.disable(logging.CRITICAL)

# Remove class decorators if they exist
# @patch(...)
class TestAdvancedDiscoveryService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Patch dependencies before tests run
        cls.podcast_lead_patcher = patch('src.agents.search.discovery_service.PodcastLead', new=MockPodcastLead)
        cls.base_model_patcher = patch('src.agents.search.discovery_service.BaseModel', new=BaseModel)
        cls.result_patcher = patch('src.agents.search.discovery_service.RelatedPodcastResult', new=MockRelatedPodcastResult)
        
        cls.MockPodcastLead = cls.podcast_lead_patcher.start()
        cls.MockBaseModel = cls.base_model_patcher.start()
        cls.MockRelatedPodcastResult = cls.result_patcher.start()

    @classmethod
    def tearDownClass(cls):
        # Stop patchers
        cls.podcast_lead_patcher.stop()
        cls.base_model_patcher.stop()
        cls.result_patcher.stop()

    def setUp(self):
        # Use the *original* (now mocked) names for creating instances if needed,
        # or directly use the mock classes assigned in setUpClass.
        # Using self.MockPodcastLead might be clearer if needed.
        self.podcast1 = MockPodcastLead("p1", "Podcast One", categories=["Tech", "News"], description="Tech news and reviews")
        self.podcast2 = MockPodcastLead("p2", "Podcast Two", categories=["Business", "News"], description="Business insights and interviews")
        self.podcast3 = MockPodcastLead("p3", "Podcast Three", categories=["Tech", "Gadgets"], description="Gadget reviews and tech talk")
        self.podcast4 = MockPodcastLead("p4", "Podcast Four", categories=["Comedy"], description="Funny talk show")
        self.podcast5 = MockPodcastLead("p5", "Podcast Five", categories=["Tech"], description="Deep tech analysis")

        self.all_podcasts = [self.podcast1, self.podcast2, self.podcast3, self.podcast4, self.podcast5]
        # Instantiate the service - patches are active
        self.service = AdvancedDiscoveryService(self.all_podcasts)

    # --- Test methods (no changes needed inside them) ---
    def test_initialization(self):
        self.assertEqual(len(self.service.podcast_data_map), 5)
        self.assertIn("p1", self.service.podcast_data_map)
        self.assertEqual(self.service.podcast_data_map["p1"].name, "Podcast One")

    def test_calculate_category_similarity(self):
        # p1 vs p3 (Tech)
        sim_1_3 = self.service._calculate_category_similarity(self.podcast1, self.podcast3)
        self.assertAlmostEqual(sim_1_3, 1/3) # Intersection=1 (Tech), Union=3 (Tech, News, Gadgets)
        # p1 vs p2 (News)
        sim_1_2 = self.service._calculate_category_similarity(self.podcast1, self.podcast2)
        self.assertAlmostEqual(sim_1_2, 1/3) # Intersection=1 (News), Union=3 (Tech, News, Business)
        # p1 vs p4 (None)
        sim_1_4 = self.service._calculate_category_similarity(self.podcast1, self.podcast4)
        self.assertEqual(sim_1_4, 0.0)
        # p1 vs p5 (Tech)
        sim_1_5 = self.service._calculate_category_similarity(self.podcast1, self.podcast5)
        self.assertAlmostEqual(sim_1_5, 1/2) # Intersection=1 (Tech), Union=2 (Tech, News)

    def test_calculate_network_similarity(self):
        # Placeholder test - currently always returns 0
        podcast_net1 = MockPodcastLead("pn1", "Net One", network="AlphaNet")
        podcast_net2 = MockPodcastLead("pn2", "Net Two", network="AlphaNet")
        podcast_net3 = MockPodcastLead("pn3", "Net Three", network="BetaNet")
        self.assertEqual(self.service._calculate_network_similarity(podcast_net1, podcast_net2), 0.0)
        self.assertEqual(self.service._calculate_network_similarity(podcast_net1, podcast_net3), 0.0)

    def test_calculate_topic_similarity(self):
        # Basic keyword overlap test
        # p1 vs p3 (Tech, reviews)
        sim_1_3 = self.service._calculate_topic_similarity(self.podcast1, self.podcast3)
        self.assertAlmostEqual(sim_1_3, 2.0/100.0)
        # p1 vs p2 (None)
        sim_1_2 = self.service._calculate_topic_similarity(self.podcast1, self.podcast2)
        self.assertEqual(sim_1_2, 0.0)
        # p1 vs p4 (talk)
        p1_talk = MockPodcastLead("p1t", "P1", description="Tech talk news")
        p4_talk = MockPodcastLead("p4t", "P4", description="Funny talk show")
        sim_talk = self.service._calculate_topic_similarity(p1_talk, p4_talk)
        self.assertAlmostEqual(sim_talk, 1.0/100.0)

    def test_find_neighborhood_target_not_found(self):
        results = self.service.find_podcast_neighborhood("p_unknown")
        self.assertEqual(results, [])

    def test_find_neighborhood_success_default_weights(self):
        results = self.service.find_podcast_neighborhood("p1", limit=3)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].related_lead.podcast_id, "p5")
        self.assertAlmostEqual(results[0].score, 0.252)
        self.assertIn('category', results[0].match_type)
        self.assertIn('topic', results[0].match_type)
        self.assertEqual(results[1].related_lead.podcast_id, "p3")
        self.assertAlmostEqual(results[1].score, 0.1706, places=4)
        self.assertIn('category', results[1].match_type)
        self.assertIn('topic', results[1].match_type)
        self.assertEqual(results[2].related_lead.podcast_id, "p2")
        self.assertAlmostEqual(results[2].score, 0.1666, places=4)
        self.assertIn('category', results[2].match_type)
        self.assertNotIn('topic', results[2].match_type)

    def test_find_neighborhood_different_weights(self):
         results = self.service.find_podcast_neighborhood("p1", limit=3, category_weight=0.1, topic_weight=0.9)
         self.assertEqual(len(results), 3)
         self.assertEqual(results[0].related_lead.podcast_id, "p5")
         self.assertAlmostEqual(results[0].score, 0.059)
         self.assertEqual(results[1].related_lead.podcast_id, "p3")
         self.assertAlmostEqual(results[1].score, 0.0513, places=4)
         self.assertEqual(results[2].related_lead.podcast_id, "p2")
         self.assertAlmostEqual(results[2].score, 0.0333, places=4)

    def test_find_neighborhood_limit(self):
        results = self.service.find_podcast_neighborhood("p1", limit=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].related_lead.podcast_id, "p5")

    def test_find_neighborhood_threshold(self):
         results = self.service.find_podcast_neighborhood("p1", limit=3, score_threshold=0.2)
         self.assertEqual(len(results), 1)
         self.assertEqual(results[0].related_lead.podcast_id, "p5")

if __name__ == '__main__':
    unittest.main()