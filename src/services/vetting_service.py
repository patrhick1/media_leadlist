import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import statistics
from dateutil import parser # Added for parsing dates in calculate_quality_metrics

# Import the result model
from ..models.vetting import VettingResult
# Import Metrics Service
from .metrics_service import MetricsService

logger = logging.getLogger(__name__)

# Placeholder for potential configuration later
VETTING_CONFIG = {
    "recency_threshold_days": 90,
    "consistency_min_episodes": 5,
    "consistency_max_dev_days": 7, # Max deviation from median interval for high consistency
    "episode_count_tiers": {
        "low": 10,
        "medium": 50,
        "high": 100
    },
    "weights": {
        "episode_count_score": 0.3,
        "consistency_score": 0.4,
        "recency_score": 0.3
    },
    # Tier Thresholds (Score: 0-100)
    "tier_thresholds": {
        "A": 85,
        "B": 65,
        "C": 40,
        # Anything below C is D
    }
}

# Validate weights sum to 1.0
weight_sum = sum(VETTING_CONFIG["weights"].values())
if abs(weight_sum - 1.0) > 1e-6: # Use tolerance for float comparison
    logger.error(f"Vetting config weights do not sum to 1.0 (sum: {weight_sum}). Adjust VETTING_CONFIG.")
    # Optionally raise an error or try to normalize weights
    # raise ValueError("Vetting config weights must sum to 1.0")

class VettingService:
    """Service to perform quality assessment, scoring, tiering, and explanation."""

    def __init__(self):
        """Initializes the VettingService and MetricsService."""
        self.metrics_service = MetricsService()
        logger.info("VettingService initialized.")

    def assess_episode_count(self, episode_count: int) -> float:
        """Assess quality based on the total number of episodes.

        Returns:
            Score between 0.0 and 1.0.
        """
        if not isinstance(episode_count, int) or episode_count < 0:
            logger.warning(f"Invalid episode_count received: {episode_count}")
            return 0.0

        tiers = VETTING_CONFIG["episode_count_tiers"]
        if episode_count >= tiers["high"]:
            return 1.0
        elif episode_count >= tiers["medium"]:
            return 0.7
        elif episode_count >= tiers["low"]:
            return 0.4
        else:
            return 0.1

    def assess_publishing_consistency(self, publish_dates: List[datetime]) -> float:
        """Assess quality based on publishing consistency.

        Args:
            publish_dates: A list of episode publication datetimes, sorted ascending.

        Returns:
            Score between 0.0 and 1.0.
        """
        min_episodes = VETTING_CONFIG["consistency_min_episodes"]
        if not publish_dates or len(publish_dates) < min_episodes:
            logger.debug(f"Not enough episodes ({len(publish_dates)}) for consistency check (min: {min_episodes}).")
            return 0.0 # Not enough data

        # Calculate intervals between episodes in days
        intervals = []
        for i in range(1, len(publish_dates)):
            # Ensure dates are datetime objects
            if isinstance(publish_dates[i], datetime) and isinstance(publish_dates[i-1], datetime):
                delta = publish_dates[i] - publish_dates[i-1]
                intervals.append(delta.days)
            else:
                logger.warning(f"Skipping invalid date type in consistency check: {publish_dates[i]}, {publish_dates[i-1]}")
                return 0.0 # Treat invalid data as inconsistent

        if not intervals:
             return 0.0

        try:
            median_interval = statistics.median(intervals)
            # Avoid division by zero if median is 0 (multiple episodes on same day)
            if median_interval <= 0: median_interval = 1

            # Calculate deviation from median
            deviations = [abs(interval - median_interval) for interval in intervals]
            mean_deviation = statistics.mean(deviations)

            # Score inversely based on mean deviation relative to median interval
            # Lower deviation = higher score
            relative_deviation = mean_deviation / median_interval

            # Simple scoring logic (can be refined)
            if relative_deviation <= 0.1: # Very consistent (e.g., daily/weekly within a day)
                return 1.0
            elif relative_deviation <= 0.3: # Moderately consistent
                return 0.7
            elif relative_deviation <= 0.6: # Somewhat inconsistent
                return 0.4
            else: # Highly inconsistent
                return 0.1

        except statistics.StatisticsError:
            logger.warning("Could not calculate statistics for consistency.")
            return 0.0
        except Exception as e:
            logger.exception(f"Unexpected error during consistency assessment: {e}")
            return 0.0

    def assess_recency(self, last_publish_date: Optional[datetime]) -> float:
        """Assess quality based on the recency of the last episode.

        Returns:
            Score between 0.0 and 1.0.
        """
        if not last_publish_date or not isinstance(last_publish_date, datetime):
            logger.debug("No valid last_publish_date provided for recency check.")
            return 0.0

        now = datetime.now(last_publish_date.tzinfo) # Use timezone of input if available
        days_since_last = (now - last_publish_date).days
        threshold = VETTING_CONFIG["recency_threshold_days"]

        if days_since_last < 0: # Published in the future? Treat as recent.
            return 1.0
        if days_since_last <= (threshold / 3): # Very recent
            return 1.0
        elif days_since_last <= threshold: # Moderately recent
            return 0.7
        elif days_since_last <= (threshold * 2): # Stale
            return 0.3
        else: # Very stale / inactive
            return 0.0

    def calculate_quality_metrics(self, podcast_data: Dict[str, Any]) -> Dict[str, float]:
        """Calculates all quality metrics for a given podcast.

        Args:
            podcast_data: A dictionary potentially containing keys like
                          'episode_count', 'publish_dates', 'last_publish_date'.
                          These ideally come from an Enrichment step.

        Returns:
            A dictionary with scores for each assessed metric.
        """
        scores = {}
        logger.info(f"Calculating quality metrics for podcast_id: {podcast_data.get('podcast_id', 'N/A')}")

        # Assess Episode Count
        episode_count = podcast_data.get('episode_count')
        if episode_count is not None:
            scores['episode_count_score'] = self.assess_episode_count(episode_count)
        else:
            logger.debug("Missing 'episode_count' for quality assessment.")
            scores['episode_count_score'] = 0.0

        # Assess Recency
        last_publish_date_val = podcast_data.get('last_publish_date')
        last_publish_date = None
        if isinstance(last_publish_date_val, datetime):
            last_publish_date = last_publish_date_val
        elif isinstance(last_publish_date_val, str):
             try:
                 last_publish_date = parser.parse(last_publish_date_val)
             except ValueError:
                 logger.warning(f"Could not parse last_publish_date string: {last_publish_date_val}")

        scores['recency_score'] = self.assess_recency(last_publish_date)

        # Assess Consistency
        publish_dates_val = podcast_data.get('publish_dates', []) # Expect list of dates/datestrings
        publish_dates = []
        if isinstance(publish_dates_val, list):
            for date_val in sorted(publish_dates_val): # Sort dates first
                 if isinstance(date_val, datetime):
                     publish_dates.append(date_val)
                 elif isinstance(date_val, str):
                      try:
                          publish_dates.append(parser.parse(date_val))
                      except ValueError:
                          logger.warning(f"Could not parse publish_date string: {date_val}")
                          # Skip invalid date
                 else:
                      logger.warning(f"Invalid type in publish_dates list: {type(date_val)}")

        scores['consistency_score'] = self.assess_publishing_consistency(publish_dates)

        logger.debug(f"Calculated scores: {scores}")
        return scores

    def calculate_composite_score(self, metric_scores: Dict[str, float]) -> float:
        """Calculates a composite quality score based on weighted metrics.

        Args:
            metric_scores: A dictionary containing the individual metric scores
                           (e.g., output from calculate_quality_metrics).

        Returns:
            A composite score, typically scaled 0-100.
        """
        composite_score = 0.0
        weights = VETTING_CONFIG["weights"]
        missing_metrics = []

        for metric, weight in weights.items():
            score = metric_scores.get(metric)
            if score is not None:
                # Scores from assessment functions are 0.0-1.0
                composite_score += score * weight
            else:
                # Handle missing metrics - currently assigns 0 score contribution
                logger.warning(f"Missing metric '{metric}' for composite scoring. Assigning 0 score contribution.")
                missing_metrics.append(metric)

        # Scale score to 0-100 and round
        final_score = round(composite_score * 100)

        logger.debug(f"Calculated composite score: {final_score}/100. Based on metrics: {metric_scores}. Missing: {missing_metrics}")
        return final_score

    def assign_quality_tier(self, composite_score: float) -> str:
        """Assigns a quality tier based on the composite score."""
        thresholds = VETTING_CONFIG["tier_thresholds"]
        if composite_score >= thresholds["A"]:
            return "A"
        elif composite_score >= thresholds["B"]:
            return "B"
        elif composite_score >= thresholds["C"]:
            return "C"
        else:
            return "D"

    def _get_metric_summary(self, metric_name: str, score: Optional[float]) -> str:
        """Generates a short summary phrase for a single metric score (0.0-1.0)."""
        if score is None:
            return f"{metric_name.replace('_score', '').capitalize()}: Not available" # Metric score missing
        if score >= 0.8:
            level = "Excellent"
        elif score >= 0.6:
            level = "Good"
        elif score >= 0.4:
            level = "Fair"
        elif score > 0.0:
            level = "Poor"
        else:
             level = "Very Poor / N/A" # Explicitly handle 0 or potentially missing data cases

        # Clean up metric name for display
        display_name = metric_name.replace('_score', '').replace('_', ' ').capitalize()
        return f"{display_name}: {level} ({score:.2f})"

    def generate_assessment_explanation(self, metric_scores: Dict[str, float], composite_score: float, quality_tier: str) -> str:
        """Generates a natural language explanation for the assessment."""
        explanation_parts = []
        explanation_parts.append(f"Overall Quality Tier: {quality_tier} (Score: {composite_score:.0f}/100)")
        explanation_parts.append("Breakdown:")

        # Generate summary for each metric contributing to the score
        weights = VETTING_CONFIG["weights"]
        for metric, weight in weights.items():
            score = metric_scores.get(metric)
            summary = self._get_metric_summary(metric, score)
            explanation_parts.append(f"- {summary}")

        # Add context based on tier or specific scores (optional enhancement)
        if quality_tier == "A":
            explanation_parts.append("\nAssessment: Strong overall quality indicators.")
        elif quality_tier == "D":
             explanation_parts.append("\nAssessment: Significant concerns regarding quality indicators (recency, consistency, or episode count).")
        # Add more nuanced explanations based on score combinations if needed

        return " | ".join(explanation_parts)

    def vet_podcast(self, podcast_data: Dict[str, Any], campaign_id: Optional[str] = None) -> Optional[VettingResult]:
        """Performs the full vetting process for a single podcast.

        Args:
            podcast_data: Dictionary containing the podcast lead data, 
                          including fields needed for metric calculations.
            campaign_id: Optional campaign ID for metrics context.

        Returns:
            A VettingResult object containing scores, tier, and explanation,
            or None if vetting fails.
        """
        podcast_id = podcast_data.get('podcast_id', 'N/A')
        logger.info(f"Starting vetting process for podcast_id: {podcast_id}")
        
        # Record start metric for this specific podcast vetting
        self.metrics_service.record_event(
            event_name="vetting_start",
            agent_step="vetting",
            campaign_id=campaign_id,
            metadata={"podcast_id": podcast_id}
        )
        
        try:
            # 1. Calculate individual metric scores
            metric_scores = self.calculate_quality_metrics(podcast_data)

            # 2. Calculate composite score
            composite_score = self.calculate_composite_score(metric_scores)

            # 3. Assign quality tier
            quality_tier = self.assign_quality_tier(composite_score)

            # 4. Generate explanation
            explanation = self.generate_assessment_explanation(metric_scores, composite_score, quality_tier)

            result = VettingResult(
                podcast_id=podcast_id,
                composite_score=composite_score,
                quality_tier=quality_tier,
                explanation=explanation,
                metric_scores=metric_scores
            )
            
            logger.info(f"Successfully vetted podcast {podcast_id}. Tier: {quality_tier}, Score: {composite_score}")
            # Record success metric
            self.metrics_service.record_event(
                event_name="vetting_success",
                agent_step="vetting",
                campaign_id=campaign_id,
                metadata={
                    "podcast_id": podcast_id, 
                    "composite_score": composite_score,
                    "quality_tier": quality_tier
                }
            )
            return result

        except Exception as e:
            logger.exception(f"Vetting failed for podcast_id {podcast_id}: {e}")
            # Record error metric
            self.metrics_service.record_event(
                event_name="vetting_error",
                agent_step="vetting",
                campaign_id=campaign_id,
                metadata={"podcast_id": podcast_id, "error": str(e)}
            )
            return None # Indicate failure

    def vet_podcasts_batch(self, podcast_list: List[Dict[str, Any]], campaign_id: Optional[str] = None) -> List[Optional[VettingResult]]:
        """Performs vetting on a batch of podcasts.

        Args:
            podcast_list: A list of podcast data dictionaries.
            campaign_id: Optional campaign ID for metrics context.
            
        Returns:
            A list of VettingResult objects or None for each podcast.
        """
        # Note: This batch method primarily calls the single vet_podcast method.
        # The agent calling this batch method should record the overall start/end/duration metrics.
        logger.info(f"Starting batch vetting for {len(podcast_list)} podcasts. Campaign ID: {campaign_id}")
        results = []
        for podcast_data in podcast_list:
            # Pass campaign_id down for individual metrics
            result = self.vet_podcast(podcast_data, campaign_id=campaign_id) 
            results.append(result)
        
        successful_vets = sum(1 for r in results if r is not None)
        logger.info(f"Batch vetting complete. Successfully vetted: {successful_vets}/{len(podcast_list)}")
        return results

# Example Usage (Optional)
# if __name__ == '__main__':
#     logging.basicConfig(level=logging.INFO)
#     vetting_service = VettingService()
#
#     # Example podcast data
#     test_podcast_data = {
#         "podcast_id": "test1234",
#         "episode_count": 150,
#         "last_publish_date": datetime.now() - timedelta(days=10),
#         "publish_dates": [(datetime.now() - timedelta(days=x*7)) for x in range(20)]
#     }
#     vetting_result = vetting_service.vet_podcast(test_podcast_data)
#     print("--- Vetting Result 1 ---")
#     print(vetting_result.model_dump_json(indent=2))
#
#     test_podcast_data_inactive = {
#         "podcast_id": "inactive567",
#         "episode_count": 25,
#         "last_publish_date": datetime.now() - timedelta(days=300),
#         "publish_dates": [(datetime.now() - timedelta(days=300 + x*30)) for x in range(5)]
#     }
#     vetting_result_inactive = vetting_service.vet_podcast(test_podcast_data_inactive)
#     print("\n--- Vetting Result 2 ---")
#     print(vetting_result_inactive.model_dump_json(indent=2))
#
#     test_podcast_data_missing = {
#         "podcast_id": "missing_data_890"
#         # Missing count, dates
#     }
#     vetting_result_missing = vetting_service.vet_podcast(test_podcast_data_missing)
#     print("\n--- Vetting Result 3 (Missing Data) ---")
#     print(vetting_result_missing.model_dump_json(indent=2)) 