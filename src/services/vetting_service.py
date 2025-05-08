import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import statistics
from dateutil import parser
import asyncio
from typing import Literal

# Import the result model
from ..models.vetting import VettingResult
# Import EnrichedPodcastProfile for type hinting
from ..models.podcast_profile import EnrichedPodcastProfile
# Import Metrics Service
from .metrics_service import MetricsService
# Import GeminiSearchService for LLM vetting
from .gemini_search import GeminiSearchService
# Pydantic model for LLM's structured output for vetting
from ..models.llm_outputs import LLMVettingOutput

logger = logging.getLogger(__name__)

# Configuration for vetting parameters (can be moved to a config file or settings model later)
PROGRAMMATIC_VETTING_CONFIG = {
    "recency_max_days": 120,  # Approx 4 months
    "frequency_ideal_max_days": 30, # Ideal: at least one episode per month
    "frequency_acceptable_max_days": 60, # Acceptable: at least one episode every two months
    "min_episodes_for_frequency_calc": 5,
    "weights": {
        "programmatic_consistency_score": 0.4, # Weight for recency/frequency
        "llm_match_score": 0.6 # Weight for content match
    },
    "tier_thresholds": { # Based on final composite_score (0-100)
        "A": 85,
        "B": 70,
        "C": 50,
        # Anything below C is D
    }
}

class VettingService:
    """Service to perform programmatic consistency checks and LLM-based content matching."""

    def __init__(self):
        """Initializes the VettingService and MetricsService."""
        self.metrics_service = MetricsService()
        self.gemini_service = GeminiSearchService() # For LLM-based vetting
        logger.info("VettingService initialized with MetricsService and GeminiSearchService.")

    def _calculate_programmatic_consistency(self, profile: EnrichedPodcastProfile) -> Tuple[bool, str, Optional[int], Optional[float], Dict[str, float]]:
        """Calculates programmatic consistency based on recency and frequency.

        Returns:
            Tuple: (passed: bool, reason: str, days_since_last: Optional[int], avg_freq_days: Optional[float], metric_scores: Dict[str, float])
        """
        days_since_last: Optional[int] = None
        avg_freq_days: Optional[float] = None
        recency_score = 0.0
        frequency_score = 0.0
        
        # Recency Check
        last_episode_date = profile.latest_episode_date or profile.last_posted_at # Prioritize more precise date
        if last_episode_date:
            # Ensure last_episode_date is a datetime object and naive
            if isinstance(last_episode_date, str):
                try:
                    # Parse string, make naive immediately
                    parsed_date = parser.parse(last_episode_date)
                    last_episode_date = parsed_date.replace(tzinfo=None) 
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse last_episode_date string: {last_episode_date} for {profile.api_id}")
                    last_episode_date = None
            elif isinstance(last_episode_date, datetime) and last_episode_date.tzinfo is not None:
                 # If it's already a datetime object but aware, make it naive
                 logger.debug(f"Converting aware datetime {last_episode_date} to naive for consistency.")
                 last_episode_date = last_episode_date.replace(tzinfo=None)
            
            # Now last_episode_date should be a naive datetime or None
            if isinstance(last_episode_date, datetime):
                # Use datetime.now() which is naive
                days_since_last = (datetime.now() - last_episode_date).days
                if days_since_last <= PROGRAMMATIC_VETTING_CONFIG["recency_max_days"] / 2:
                    recency_score = 1.0
                elif days_since_last <= PROGRAMMATIC_VETTING_CONFIG["recency_max_days"]:
                    recency_score = 0.6
                elif days_since_last <= PROGRAMMATIC_VETTING_CONFIG["recency_max_days"] * 1.5:
                    recency_score = 0.3
                else:
                    recency_score = 0.1 # Penalize but don't make it 0 if other factors are good
        else:
            logger.info(f"No last_episode_date found for {profile.api_id} for recency check.")
            recency_score = 0.0 # No data for recency

        # Frequency Check
        if profile.total_episodes and profile.total_episodes >= PROGRAMMATIC_VETTING_CONFIG["min_episodes_for_frequency_calc"] and \
           profile.first_episode_date and profile.latest_episode_date:
            
            first_date = profile.first_episode_date
            last_date = profile.latest_episode_date # Use the one already parsed for recency

            if isinstance(first_date, str):
                try: first_date = parser.parse(first_date).replace(tzinfo=None) 
                except (ValueError, TypeError): first_date = None
            
            if isinstance(last_date, str): # Should be datetime if parsed above, but check again
                try: last_date = parser.parse(last_date).replace(tzinfo=None)
                except(ValueError, TypeError): last_date = None

            if first_date and last_date and first_date < last_date:
                duration_days = (last_date - first_date).days
                if duration_days > 0 and profile.total_episodes > 1:
                    avg_freq_days = duration_days / (profile.total_episodes -1) # -1 because it's intervals
                    if avg_freq_days <= PROGRAMMATIC_VETTING_CONFIG["frequency_ideal_max_days"]:
                        frequency_score = 1.0
                    elif avg_freq_days <= PROGRAMMATIC_VETTING_CONFIG["frequency_acceptable_max_days"]:
                        frequency_score = 0.7
                    else:
                        frequency_score = 0.3
                elif profile.total_episodes == 1 and duration_days <= 0: # Single episode, recently published
                    avg_freq_days = 0 # Consider as very frequent
                    frequency_score = 0.5 # Neutral score for single recent episode
                else:
                    frequency_score = 0.1 # Edge case, likely very few episodes or bad date data
            else:
                logger.info(f"Could not calculate duration for frequency for {profile.api_id}. First: {first_date}, Last: {last_date}")
                frequency_score = 0.1 # Cannot determine frequency
        elif profile.publishing_frequency_days is not None: # Fallback to pre-calculated one if available
            avg_freq_days = profile.publishing_frequency_days
            if avg_freq_days <= PROGRAMMATIC_VETTING_CONFIG["frequency_ideal_max_days"]:
                frequency_score = 1.0
            elif avg_freq_days <= PROGRAMMATIC_VETTING_CONFIG["frequency_acceptable_max_days"]:
                frequency_score = 0.7
            else:
                frequency_score = 0.3
        else:
            logger.info(f"Not enough data for frequency calculation for {profile.api_id} (Episodes: {profile.total_episodes})")
            frequency_score = 0.0 # Not enough data for frequency
        
        # Determine overall programmatic consistency
        passed = recency_score >= 0.5 and frequency_score >= 0.5 # Example threshold for passing
        
        reasons = []
        if recency_score < 0.5:
            reasons.append(f"Last episode {days_since_last or 'N/A'} days ago (Threshold: <{PROGRAMMATIC_VETTING_CONFIG['recency_max_days']}).")
        else:
            reasons.append(f"Recent (Last ep: {days_since_last or 'N/A'} days ago).")

        if frequency_score < 0.5:
            # Handle None case for avg_freq_days before formatting
            freq_reason = f"Frequency avg {avg_freq_days:.1f} days" if avg_freq_days is not None else "Frequency calculation failed or N/A"
            reasons.append(f"{freq_reason} (Ideal: <{PROGRAMMATIC_VETTING_CONFIG['frequency_ideal_max_days']}).")
        else:
            # Handle None case here too for consistency
            freq_reason = f"Frequent (Avg: {avg_freq_days:.1f} days)" if avg_freq_days is not None else "Frequency OK (No avg calculated)"
            reasons.append(freq_reason)
        
        reason_str = " | ".join(reasons) if reasons else "Consistency checks passed programmatically."
        if not passed and not reasons:
            reason_str = "Programmatic consistency checks did not pass due to low scores."
        elif passed and not reasons:
             reason_str = "Passed programmatic consistency (default reason)." # Fallback if reasons somehow empty

        metric_scores = {
            "recency_score": recency_score, 
            "frequency_score": frequency_score
        }
        return passed, reason_str, days_since_last, avg_freq_days, metric_scores

    async def _perform_llm_vetting(self, profile: EnrichedPodcastProfile, ideal_podcast_description: str, guest_bio: str, guest_talking_points: List[str]) -> Tuple[Optional[float], Optional[str]]:
        """Performs LLM-based vetting for content match.
        Returns: Tuple (llm_match_score: Optional[float], llm_match_explanation: Optional[str])
        """
        logger.info(f"Performing LLM vetting for podcast: {profile.title} (ID: {profile.api_id})")
        
        if not self.gemini_service or not self.gemini_service.llm:
            logger.error(f"Gemini service not available for LLM vetting of {profile.api_id}.")
            return None, "LLM service not available."

        podcast_keywords_raw = getattr(profile, 'keywords', None)
        podcast_keywords_str = ', '.join(podcast_keywords_raw) if isinstance(podcast_keywords_raw, list) else (podcast_keywords_raw or 'N/A')
        podcast_summary = f"Title: {profile.title or 'N/A'}\nDescription: {profile.description or 'N/A'}\nKeywords/Categories: {podcast_keywords_str}"
        
        prompt = f"""You are an expert podcast vetting assistant. Your task is to evaluate how well the following podcast aligns with a guest's profile, their talking points, and an ideal podcast description.

        Podcast Information:
        {podcast_summary}

        Guest Profile:
        - Bio: {guest_bio}
        - Key Talking Points: {', '.join(guest_talking_points)}

        Ideal Podcast Description for this Guest:
        {ideal_podcast_description}

        Based on all the above information, please provide:
        1. A match score (integer from 0 to 100), where 0 indicates no match and 100 indicates a perfect match. The score should reflect how suitable this podcast is for this specific guest to discuss their talking points, considering the podcast's typical content and the ideal podcast description.
        2. A brief explanation (1-3 sentences) for your score, highlighting key reasons for or against the match. Focus on content alignment, audience suitability (if inferable), and thematic relevance.

        Return ONLY a JSON object with the keys "match_score" (integer) and "explanation" (string).
        Example JSON:
        {{
          "match_score": 75,
          "explanation": "The podcast focuses on technology startups, which aligns well with the guest's background in AI. The description suggests an audience open to technical discussions, suitable for the guest's talking points."
        }}
        """
        
        try:
            # Use the get_structured_data method from GeminiSearchService
            # Ensure LLMVettingOutput model is defined in src/models/llm_outputs.py
            # and imported here.
            llm_output: Optional[LLMVettingOutput] = await asyncio.to_thread(
                self.gemini_service.get_structured_data, # Assuming this method exists and takes prompt + Pydantic model
                prompt,
                LLMVettingOutput # The Pydantic model for structured output
            )

            if llm_output and isinstance(llm_output, LLMVettingOutput):
                logger.info(f"LLM vetting for {profile.api_id} successful. Score: {llm_output.match_score}, Explanation: {llm_output.explanation}")
                return llm_output.match_score, llm_output.explanation
            else:
                logger.warning(f"LLM vetting for {profile.api_id} did not return expected structured output. Output: {llm_output}")
                error_explanation = "LLM did not return a valid structured response."
                if isinstance(llm_output, str): # If Gemini service returned raw string on failure
                    error_explanation += f" Raw LLM Output: {llm_output[:200]}..." # Include part of raw output for debugging
                return None, error_explanation
        except Exception as e:
            logger.exception(f"Error during LLM vetting for {profile.api_id}: {e}")
            return None, f"LLM vetting failed with error: {str(e)[:100]}"

    async def vet_single_profile(
        self,
        profile: EnrichedPodcastProfile,
        ideal_podcast_description: str,
        guest_bio: str,
        guest_talking_points: List[str],
        campaign_id: Optional[str] = None
    ) -> VettingResult:
        """Performs full vetting (programmatic + LLM) for a single enriched profile."""
        podcast_id = profile.api_id or profile.unified_profile_id or "unknown_profile"
        logger.info(f"Starting full vetting for profile: {podcast_id}")
        self.metrics_service.record_event(
            event_name="vetting_profile_start",
            agent_step="vetting",
            campaign_id=campaign_id,
            metadata={"profile_id": podcast_id}
        )

        error_message: Optional[str] = None
        try:
            # 1. Programmatic Consistency Checks
            consistency_passed, consistency_reason, days_last_ep, avg_freq, prog_metric_scores = \
                self._calculate_programmatic_consistency(profile)
            
            # 2. LLM-based Content Match Vetting
            llm_score, llm_explanation = await self._perform_llm_vetting(
                profile, ideal_podcast_description, guest_bio, guest_talking_points
            )

            # 3. Combine Scores and Determine Tier
            # Programmatic score (0-1, based on pass/fail or could be more nuanced)
            prog_consistency_contribution = 1.0 if consistency_passed else 0.3 # Give some points even if minor programmatic fail if LLM is good
            
            # Ensure LLM score is float for calculation, default to 0 if None
            llm_match_score_value = llm_score if llm_score is not None else 0.0

            # Weighted composite score (0-100)
            composite_score = (
                prog_consistency_contribution * PROGRAMMATIC_VETTING_CONFIG["weights"]["programmatic_consistency_score"] + \
                (llm_match_score_value / 100.0) * PROGRAMMATIC_VETTING_CONFIG["weights"]["llm_match_score"] 
            ) * 100
            composite_score = round(max(0, min(composite_score, 100))) # Clamp to 0-100

            # Determine Quality Tier
            tier_thresholds = PROGRAMMATIC_VETTING_CONFIG["tier_thresholds"]
            quality_tier: Literal["A", "B", "C", "D", "Unvetted"]
            if not llm_score: # If LLM vetting failed or was skipped
                quality_tier = "Unvetted"
            elif composite_score >= tier_thresholds["A"]:
                quality_tier = "A"
            elif composite_score >= tier_thresholds["B"]:
                quality_tier = "B"
            elif composite_score >= tier_thresholds["C"]:
                quality_tier = "C"
            else:
                quality_tier = "D"
            
            # Final Explanation
            final_explanation_parts = [
                f"Overall Tier: {quality_tier} (Score: {composite_score:.0f}/100).",
                f"Programmatic Consistency: {'Passed' if consistency_passed else 'Failed'} - {consistency_reason}",
                f"LLM Content Match: Score {llm_score:.0f}/100 - {llm_explanation or 'Not available.'}"
            ]
            final_explanation = " | ".join(final_explanation_parts)
            
            all_metric_scores = {
                **prog_metric_scores,
                "llm_match_score": llm_match_score_value # Store the actual score used
            }

            vet_result = VettingResult(
                podcast_id=podcast_id,
                programmatic_consistency_passed=consistency_passed,
                programmatic_consistency_reason=consistency_reason,
                last_episode_date=profile.latest_episode_date or profile.last_posted_at,
                days_since_last_episode=days_last_ep,
                average_frequency_days=avg_freq,
                llm_match_score=llm_score,
                llm_match_explanation=llm_explanation,
                composite_score=composite_score,
                quality_tier=quality_tier,
                final_explanation=final_explanation,
                metric_scores=all_metric_scores,
                error=None
            )
            self.metrics_service.record_event("vetting_profile_success", agent_step="vetting", campaign_id=campaign_id, metadata=vet_result.model_dump(exclude_none=True))
            return vet_result

        except Exception as e:
            logger.exception(f"Error vetting profile {podcast_id}: {e}")
            error_message = f"Failed to vet profile {podcast_id}: {str(e)[:150]}"
            self.metrics_service.record_event("vetting_profile_error", agent_step="vetting", campaign_id=campaign_id, metadata={"profile_id": podcast_id, "error": error_message})
            # Return a VettingResult indicating error
            return VettingResult(
                podcast_id=podcast_id,
                programmatic_consistency_passed=False,
                programmatic_consistency_reason="Vetting process failed.",
                llm_match_score=None,
                llm_match_explanation="Vetting process failed.",
                composite_score=0,
                quality_tier="D", # Or a specific error tier
                final_explanation=error_message,
                error=error_message
            )

    # --- Batch Vetting Method (Calls vet_single_profile) ---
    async def vet_profiles_batch(
        self,
        enriched_profiles: List[EnrichedPodcastProfile],
        ideal_podcast_description: str,
        guest_bio: str,
        guest_talking_points: List[str],
        campaign_id: Optional[str] = None
    ) -> List[VettingResult]: # Return type is now non-optional VettingResult (includes error state)
        """Performs full vetting on a batch of enriched profiles asynchronously."""
        logger.info(f"Starting batch vetting for {len(enriched_profiles)} profiles. Campaign ID: {campaign_id}")
        
        tasks = []
        for profile in enriched_profiles:
            tasks.append(self.vet_single_profile(
                profile, ideal_podcast_description, guest_bio, guest_talking_points, campaign_id
            ))
        
        results = await asyncio.gather(*tasks)
        
        successful_vets = sum(1 for r in results if r and not r.error)
        logger.info(f"Batch vetting complete. Successfully vetted (no errors): {successful_vets}/{len(enriched_profiles)}")
        return results

    # --- Deprecated/Old methods (can be removed or refactored if still used elsewhere) ---
    # def assess_episode_count(self, episode_count: int) -> float: ...
    # def assess_publishing_consistency(self, publish_dates: List[datetime]) -> float: ...
    # def assess_recency(self, last_publish_date: Optional[datetime]) -> float: ...
    # def calculate_quality_metrics(self, podcast_data: Dict[str, Any]) -> Dict[str, float]: ...
    # def calculate_composite_score(self, metric_scores: Dict[str, float]) -> float: ...
    # def assign_quality_tier(self, composite_score: float) -> str: ...
    # def generate_assessment_explanation(...) -> str: ...
    # def vet_podcast(self, podcast_data: Dict[str, Any], campaign_id: Optional[str] = None) -> Optional[VettingResult]: ...
    # def vet_podcasts_batch(self, podcast_list: List[Dict[str, Any]], campaign_id: Optional[str] = None) -> List[Optional[VettingResult]]: ...

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