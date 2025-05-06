import logging
from typing import Optional, Dict, Any, List
from collections import defaultdict
import statistics # Import statistics for median calculation

from ..persistence.mongodb import _get_collection
from ..models.metrics import MetricRecord # May not be needed directly if using aggregation

logger = logging.getLogger(__name__)

METRICS_COLLECTION = "metrics"

class AnalyticsService:
    """
    Service responsible for analyzing collected metrics and providing insights.
    """

    def __init__(self):
        """Initializes the AnalyticsService."""
        try:
            self.metrics_collection = _get_collection(METRICS_COLLECTION)
            # Ensure metrics collection has necessary indexes
            self._ensure_indexes()
            logger.info("AnalyticsService initialized.")
        except Exception as e:
            logger.exception("Failed to initialize AnalyticsService collection.")
            self.metrics_collection = None
            # Decide if initialization failure should prevent operation
            # raise

    def _ensure_indexes(self):
        """Ensures necessary indexes exist on the metrics collection."""
        if not self.metrics_collection:
            return
        try:
            # Index for common filtering/grouping fields
            self.metrics_collection.create_index([("event_name", 1), ("agent_step", 1)])
            self.metrics_collection.create_index("campaign_id")
            self.metrics_collection.create_index("timestamp")
            logger.info("Ensured indexes on metrics collection.")
        except Exception as e:
            logger.error(f"Failed to create indexes on metrics collection: {e}")

    # --- Analysis Methods will be added below --- #

    def get_step_durations(self, campaign_id: Optional[str] = None) -> Dict[str, Dict[str, Optional[float]]]:
        """
        Calculates average and median durations for each agent step.
        
        Args:
            campaign_id: Optional campaign ID to filter metrics.
            
        Returns:
            A dictionary mapping agent_step to {avg_duration_ms, median_duration_ms}.
        """
        if not self.metrics_collection:
            logger.error("Metrics collection not available.")
            return {}
        
        pipeline = []
        match_filter: Dict[str, Any] = {
            "event_name": "agent_step_end",
            "duration_ms": {"$exists": True, "$type": "number"} # Ensure duration exists and is numeric
        }
        
        # Add campaign_id match if provided
        if campaign_id:
            match_filter["campaign_id"] = campaign_id 
            
        pipeline.append({"$match": match_filter})
        
        # Group by agent step and calculate stats
        pipeline.append({"$group": {
            "_id": "$agent_step", # Group by agent_step field
            "durations": {"$push": "$duration_ms"}, # Collect all durations for median calculation
            "avg_duration": {"$avg": "$duration_ms"} # Calculate average directly in DB
        }})
        
        # Project for cleaner output
        pipeline.append({"$project": {
            "_id": 0, # Exclude the default _id field
            "agent_step": "$_id", # Rename _id to agent_step
            "avg_duration_ms": "$avg_duration",
            "all_durations": "$durations" # Fetch all durations to calculate median in Python
        }})
        
        results = {}
        try:
            logger.debug(f"Running step duration aggregation pipeline: {pipeline}")
            aggregated_data = list(self.metrics_collection.aggregate(pipeline))
            logger.debug(f"Aggregation result: {aggregated_data}")
            
            for item in aggregated_data:
                agent_step = item.get("agent_step")
                if not agent_step: continue # Skip if grouping key is missing
                
                durations = item.get('all_durations', [])
                median_duration = None
                if durations:
                    try:
                        median_duration = statistics.median(durations)
                    except statistics.StatisticsError as median_err:
                         logger.warning(f"Could not calculate median duration for step '{agent_step}': {median_err}")
                         
                results[agent_step] = {
                    "avg_duration_ms": item.get("avg_duration_ms"),
                    "median_duration_ms": median_duration
                }
                
        except Exception as e:
            logger.exception(f"Error during metrics aggregation for step durations: {e}")
            
        logger.info(f"Calculated step durations. Steps found: {list(results.keys())}")
        return results

    def get_vetting_tier_distribution(self, campaign_id: Optional[str] = None) -> Dict[str, int]:
        """
        Calculates the distribution of quality tiers for successfully vetted leads.
        
        Args:
            campaign_id: Optional campaign ID to filter metrics.
            
        Returns:
            A dictionary mapping quality_tier (A, B, C, D, Error) to the count of leads.
        """
        if not self.metrics_collection:
            logger.error("Metrics collection not available.")
            return {}
        
        pipeline = []
        match_filter: Dict[str, Any] = {
            # Match only the success events for vetting
            "event_name": "vetting_success", 
            "agent_step": "vetting",
            # Ensure the relevant metadata exists
            "metadata.quality_tier": {"$exists": True}
        }
        
        # Add campaign_id match if provided
        if campaign_id:
            match_filter["campaign_id"] = campaign_id
            
        pipeline.append({"$match": match_filter})
        
        # Group by quality tier and count
        pipeline.append({"$group": {
            "_id": "$metadata.quality_tier", # Group by the tier in metadata
            "count": {"$sum": 1} # Count occurrences of each tier
        }})
        
        # Project for cleaner output
        pipeline.append({"$project": {
            "_id": 0,
            "quality_tier": "$_id",
            "count": "$count"
        }})
        
        tier_distribution = defaultdict(int) # Initialize with 0 counts
        try:
            logger.debug(f"Running vetting tier distribution aggregation pipeline: {pipeline}")
            aggregated_data = list(self.metrics_collection.aggregate(pipeline))
            logger.debug(f"Aggregation result: {aggregated_data}")
            
            for item in aggregated_data:
                tier = item.get("quality_tier")
                count = item.get("count", 0)
                if tier:
                    tier_distribution[tier] = count
                
        except Exception as e:
            logger.exception(f"Error during metrics aggregation for vetting tier distribution: {e}")
            
        # Ensure all standard tiers (A, B, C, D) are present, even if count is 0
        for tier in ["A", "B", "C", "D"]:
            tier_distribution.setdefault(tier, 0)

        logger.info(f"Calculated vetting tier distribution: {dict(tier_distribution)}")
        return dict(tier_distribution)

    def get_search_source_performance(self, campaign_id: Optional[str] = None) -> Dict[str, Dict[str, int]]:
        """
        Analyzes search performance metrics grouped by data source.
        
        Args:
            campaign_id: Optional campaign ID to filter metrics.
            
        Returns:
            A dictionary mapping data source (e.g., ListenNotes, Podscan) to 
            performance stats {raw_results, mapped_results, api_errors, unexpected_errors}.
        """
        if not self.metrics_collection:
            logger.error("Metrics collection not available.")
            return {}

        pipeline = []
        match_filter: Dict[str, Any] = {
            "agent_step": "search", # Focus on search step metrics
            "event_name": {"$in": ["api_results", "mapped_results", "error"]}, # Relevant events
            "metadata.source": {"$exists": True} # Ensure source is present
        }
        
        if campaign_id:
            match_filter["campaign_id"] = campaign_id
            
        pipeline.append({"$match": match_filter})
        
        # Group by source and event type to sum counts/collect errors
        pipeline.append({"$group": {
            "_id": {
                "source": "$metadata.source", 
                "event": "$event_name"
            },
            "total_count": {"$sum": "$count"}, # Sum counts for api_results, mapped_results
            "error_count": {"$sum": 1}, # Count occurrences for error events
             # Store error types for more detail (optional)
            "error_types": {"$addToSet": "$metadata.error_type"} 
        }})
        
        # Regroup by source only to structure the final output
        pipeline.append({"$group": {
            "_id": "$_id.source",
            "metrics": {"$push": {
                "event": "$_id.event",
                "total_count": "$total_count",
                "error_count": "$error_count",
                "error_types": "$error_types"
            }}
        }})
        
        # Project for final naming
        pipeline.append({"$project": {
            "_id": 0,
            "source": "$_id",
            "metrics": "$metrics"
        }})
        
        source_performance = defaultdict(lambda: defaultdict(int))
        try:
            logger.debug(f"Running search source performance aggregation pipeline: {pipeline}")
            aggregated_data = list(self.metrics_collection.aggregate(pipeline))
            logger.debug(f"Aggregation result: {aggregated_data}")

            for source_data in aggregated_data:
                source = source_data.get("source")
                if not source: continue
                
                for metric_item in source_data.get("metrics", []):
                    event = metric_item.get("event")
                    if event == "api_results":
                        source_performance[source]["raw_results"] += metric_item.get("total_count", 0)
                    elif event == "mapped_results":
                        source_performance[source]["mapped_results"] += metric_item.get("total_count", 0)
                    elif event == "error":
                        # Distinguish API vs Unexpected errors if needed using error_types
                        # For simplicity, just count total errors for now
                        source_performance[source]["total_errors"] += metric_item.get("error_count", 0)
                        # Example: Check error_types
                        # if "APIClientError" in metric_item.get("error_types", []):
                        #      source_performance[source]["api_errors"] += metric_item.get("error_count", 0)
                        # elif "Unexpected" in metric_item.get("error_types", []):
                        #      source_performance[source]["unexpected_errors"] += metric_item.get("error_count", 0)
                        
        except Exception as e:
            logger.exception(f"Error during metrics aggregation for search source performance: {e}")
            
        logger.info(f"Calculated search source performance: {dict(source_performance)}")
        # Convert defaultdict back to regular dict for return
        return {k: dict(v) for k, v in source_performance.items()}

    def get_crm_sync_summary(self, campaign_id: Optional[str] = None) -> Dict[str, int]:
        """
        Calculates a summary of CRM synchronization attempts (based on single sync events).
        
        Args:
            campaign_id: Optional campaign ID to filter metrics.
            
        Returns:
            A dictionary containing counts for {attempts, successes, errors}.
        """
        if not self.metrics_collection:
            logger.error("Metrics collection not available.")
            return {"attempts": 0, "successes": 0, "errors": 0}

        pipeline = []
        match_filter: Dict[str, Any] = {
            "agent_step": "crm_sync",
            "event_name": {"$in": ["crm_sync_single_start", "crm_sync_single_success", "crm_sync_single_error"]}
        }
        
        if campaign_id:
            match_filter["campaign_id"] = campaign_id
            
        pipeline.append({"$match": match_filter})
        
        # Group by event name and count occurrences
        pipeline.append({"$group": {
            "_id": "$event_name",
            "count": {"$sum": 1}
        }})
        
        # Project for cleaner output
        pipeline.append({"$project": {
            "_id": 0,
            "event_name": "$_id",
            "count": "$count"
        }})
        
        summary = {"attempts": 0, "successes": 0, "errors": 0}
        try:
            logger.debug(f"Running CRM sync summary aggregation pipeline: {pipeline}")
            aggregated_data = list(self.metrics_collection.aggregate(pipeline))
            logger.debug(f"Aggregation result: {aggregated_data}")

            for item in aggregated_data:
                event = item.get("event_name")
                count = item.get("count", 0)
                if event == "crm_sync_single_start":
                    summary["attempts"] = count
                elif event == "crm_sync_single_success":
                     # Note: Success is derived from crm_sync_single_end metadata in CRMAgent
                     # Aggregating crm_sync_single_error might be more direct for errors.
                     # Let's adjust - count errors directly.
                    pass # We'll count errors directly
                elif event == "crm_sync_single_error":
                    summary["errors"] = count
            
            # Calculate successes = attempts - errors
            summary["successes"] = summary["attempts"] - summary["errors"]
            if summary["successes"] < 0: summary["successes"] = 0 # Sanity check
                
        except Exception as e:
            logger.exception(f"Error during metrics aggregation for CRM sync summary: {e}")
            # Return 0 counts on error
            return {"attempts": 0, "successes": 0, "errors": 0}
            
        logger.info(f"Calculated CRM sync summary: {summary}")
        return summary

    def generate_performance_suggestions(self, campaign_id: Optional[str] = None) -> List[str]:
        """
        Analyzes various performance metrics and generates actionable suggestions.
        
        Args:
            campaign_id: Optional campaign ID to focus the analysis.
            
        Returns:
            A list of suggestion strings.
        """
        if not self.metrics_collection:
            logger.error("Metrics collection not available. Cannot generate suggestions.")
            return ["Error: Metrics database unavailable."]

        logger.info(f"Generating performance suggestions for campaign: {campaign_id or 'All Campaigns'}")
        suggestions = []

        try:
            # 1. Gather data from analysis functions
            step_durations = self.get_step_durations(campaign_id)
            vetting_dist = self.get_vetting_tier_distribution(campaign_id)
            search_perf = self.get_search_source_performance(campaign_id)
            crm_summary = self.get_crm_sync_summary(campaign_id)

            # 2. Apply rules/heuristics

            # Rule: High search errors for a source?
            for source, metrics in search_perf.items():
                if metrics.get("total_errors", 0) > 5: # Arbitrary threshold
                    suggestions.append(f"High error rate ({metrics.get('total_errors')}) observed for search source '{source}'. Consider checking API key or service status.")
            
            # Rule: Low raw results yield from a source?
            for source, metrics in search_perf.items():
                if metrics.get("raw_results", 0) < 10 and metrics.get("total_errors", 0) == 0: # Arbitrary threshold
                     suggestions.append(f"Low number of raw results ({metrics.get('raw_results')}) from search source '{source}' despite no errors. Consider broadening search criteria for this source if possible.")

            # Rule: Skewed vetting distribution?
            total_vetted = sum(vetting_dist.values()) # Excludes errors if any
            if total_vetted > 10: # Only suggest if enough data
                low_tiers_count = vetting_dist.get("C", 0) + vetting_dist.get("D", 0)
                if (low_tiers_count / total_vetted) > 0.6: # If > 60% are C/D tier
                    suggestions.append(f"High percentage ({low_tiers_count / total_vetted:.1%}) of vetted leads are C/D tier. Consider refining search targeting or vetting criteria.")
                
                high_tiers_count = vetting_dist.get("A", 0)
                if (high_tiers_count / total_vetted) < 0.1: # If < 10% are A tier
                    suggestions.append(f"Low percentage ({high_tiers_count / total_vetted:.1%}) of vetted leads are A tier. Review vetting score weights or search strategy.")

            # Rule: High CRM sync error rate?
            if crm_summary.get("attempts", 0) > 5: # Only if enough attempts
                error_rate = crm_summary.get("errors", 0) / crm_summary["attempts"]
                if error_rate > 0.2: # If > 20% error rate
                    suggestions.append(f"High CRM sync error rate ({error_rate:.1%}). Check CRM connection details, API status, or individual sync errors.")
            
            # Rule: Specific step taking too long?
            # Find step with max average duration
            max_duration = 0
            slowest_step = None
            for step, durations in step_durations.items():
                avg_duration = durations.get("avg_duration_ms")
                if avg_duration is not None and avg_duration > max_duration:
                    max_duration = avg_duration
                    slowest_step = step
            
            if slowest_step and max_duration > 300000: # If slowest step takes > 5 mins on average
                suggestions.append(f"The '{slowest_step}' step has a high average duration ({max_duration/1000:.1f}s). Consider investigating for optimization opportunities.")

            # Add more rules here...

            if not suggestions:
                suggestions.append("No specific performance issues detected based on current rules.")

        except Exception as e:
            logger.exception(f"Error during suggestion generation: {e}")
            suggestions.append("Error: Failed to generate suggestions due to an internal error.")

        logger.info(f"Generated {len(suggestions)} suggestion(s).")
        return suggestions

    def compare_campaigns(self, campaign_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Retrieves key analytics for a list of campaign IDs to allow comparison.
        
        Args:
            campaign_ids: A list of campaign IDs to compare.
            
        Returns:
            A dictionary where keys are campaign IDs and values are dictionaries
            containing various analytics summaries for that campaign.
        """
        if not self.metrics_collection:
            logger.error("Metrics collection not available. Cannot compare campaigns.")
            return {}
            
        if not campaign_ids:
             logger.warning("No campaign IDs provided for comparison.")
             return {}

        comparison_data = {}
        logger.info(f"Generating comparison data for campaigns: {campaign_ids}")

        for campaign_id in campaign_ids:
            logger.debug(f"Fetching analytics for campaign: {campaign_id}")
            campaign_metrics = {}
            try:
                # Gather metrics for this specific campaign
                campaign_metrics["step_durations"] = self.get_step_durations(campaign_id)
                campaign_metrics["vetting_distribution"] = self.get_vetting_tier_distribution(campaign_id)
                campaign_metrics["search_performance"] = self.get_search_source_performance(campaign_id)
                campaign_metrics["crm_sync_summary"] = self.get_crm_sync_summary(campaign_id)
                # Add calls to other relevant analysis functions here if needed
                
                comparison_data[campaign_id] = campaign_metrics
                
            except Exception as e:
                logger.exception(f"Error fetching analytics for campaign {campaign_id} during comparison: {e}")
                # Store partial data or indicate error for this campaign
                comparison_data[campaign_id] = {"error": f"Failed to fetch full analytics: {e}"}

        logger.info(f"Finished generating comparison data for {len(comparison_data)} campaigns.")
        return comparison_data

    # ---------------------------------------------------------------------
    # NEW: Generic Time‑Series Helper
    # ---------------------------------------------------------------------

    def get_time_series(
        self,
        metric_filter: Dict[str, Any],
        bucket: str = "day",
        match_extra: Optional[Dict[str, Any]] = None,
        campaign_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Returns bucketed counts for any metric filter.

        Args:
            metric_filter: A base `$match` filter dict to select metric docs.
            bucket: One of "hour", "day", "week", "month".
            match_extra: Additional `$match` conditions to AND with *after* base filter.
            campaign_id: Optional campaign filter.

        Returns:
            A list of dicts with keys {"bucket", "count"} sorted by bucket asc.
        """
        if not self.metrics_collection:
            logger.error("Metrics collection not available.")
            return []

        # Construct match stage
        match_stage: Dict[str, Any] = metric_filter.copy()
        if campaign_id:
            match_stage["campaign_id"] = campaign_id
        if match_extra:
            match_stage.update(match_extra)

        # Project a bucket timestamp field
        if bucket == "hour":
            date_format = "%Y-%m-%dT%H:00:00Z"
            group_format = {"$dateToString": {"format": date_format, "date": "$timestamp"}}
        elif bucket == "week":
            # %Y‑W%V for ISO week number, keep as string
            group_format = {"$dateToString": {"format": "%G-W%V", "date": "$timestamp"}}
        elif bucket == "month":
            group_format = {"$dateToString": {"format": "%Y-%m-01", "date": "$timestamp"}}
        else:
            # Default daily buckets
            group_format = {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}}

        pipeline = [
            {"$match": match_stage},
            {"$group": {"_id": group_format, "count": {"$sum": 1}}},
            {"$project": {"_id": 0, "bucket": "$_id", "count": "$count"}},
            {"$sort": {"bucket": 1}},
        ]

        try:
            logger.debug(f"Running time‑series aggregation pipeline: {pipeline}")
            results = list(self.metrics_collection.aggregate(pipeline))
            logger.info(f"Time‑series query returned {len(results)} buckets.")
            return results
        except Exception as e:
            logger.exception(f"Error while generating time‑series analytics: {e}")
            return []

    # --- Add other analysis methods below --- #

    # Example structure for an analysis method:
    # def get_step_durations(self, campaign_id: Optional[str] = None) -> Dict[str, Dict[str, float]]:
    #     """
    #     Calculates average and median durations for each agent step.
    #     Args:
    #         campaign_id: Optional campaign ID to filter metrics.
    #     Returns:
    #         A dictionary mapping agent_step to {avg_duration_ms, median_duration_ms}.
    #     """
    #     if not self.metrics_collection:
    #         logger.error("Metrics collection not available.")
    #         return {}
    #     
    #     pipeline = [
    #         # Match relevant end events with durations
    #         {"$match": {
    #             "event_name": "agent_step_end",
    #             "duration_ms": {"$exists": True}
    #             # Add campaign_id match if provided
    #             # if campaign_id: pipeline[0]["$match"]["campaign_id"] = campaign_id 
    #         }},
    #         # Group by agent step and calculate stats
    #         {"$group": {
    #             "_id": "$agent_step",
    #             "durations": {"$push": "$duration_ms"},
    #             "avg_duration": {"$avg": "$duration_ms"}
    #         }},
    #         # Project for cleaner output
    #         {"$project": {
    #             "_id": 0,
    #             "agent_step": "$_id",
    #             "avg_duration_ms": "$avg_duration",
    #             # Median requires more complex handling or fetching all durations
    #             "all_durations": "$durations" # Fetch all to calculate median in Python
    #         }}
    #     ]
    #     
    #     # Add campaign_id match to the initial $match stage if needed
    #     if campaign_id:
    #         pipeline[0]["$match"]["campaign_id"] = campaign_id
    #         
    #     results = {}    
    #     try:
    #         aggregated_data = list(self.metrics_collection.aggregate(pipeline))
    #         for item in aggregated_data:
    #             # Calculate median here if needed (requires numpy or statistics module)
    #             # import statistics
    #             # median_duration = statistics.median(item.get('all_durations', []))
    #             results[item["agent_step"]] = {
    #                 "avg_duration_ms": item.get("avg_duration_ms"),
    #                 # "median_duration_ms": median_duration
    #             }
    #     except Exception as e:
    #         logger.exception(f"Error during metrics aggregation for step durations: {e}")
    #         
    #     return results 