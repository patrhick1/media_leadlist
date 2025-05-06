import logging
from typing import Optional, Dict, Any

# --- Import commented out to pause DB interaction --- #
# from ..persistence.mongodb import _get_collection
from ..models.metrics import MetricRecord

logger = logging.getLogger(__name__)

METRICS_COLLECTION = "metrics"

class MetricsService:
    """
    Service responsible for recording system metrics.
    Database interaction is currently PAUSED.
    """

    def __init__(self):
        """Initializes the MetricsService."""
        self.metrics_collection = None # Initialize as None
        logger.info("MetricsService initialized (DB interaction is paused).")
        # try:
        #     # --- DB Interaction Commented Out --- #
        #     # self.metrics_collection = _get_collection(METRICS_COLLECTION)
        #     # logger.info("MetricsService initialized.")
        #     pass # Placeholder if other init needed
        # except Exception as e:
        #     logger.exception("Failed to initialize MetricsService collection.")
        #     self.metrics_collection = None

    def record_event(
        self,
        event_name: str,
        campaign_id: Optional[str] = None,
        agent_step: Optional[str] = None,
        duration_ms: Optional[float] = None,
        count: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Records a metric event (currently logs only, DB is paused).
        Args:
            event_name: The name of the event being recorded.
            campaign_id: The ID of the campaign associated with the event.
            agent_step: The agent step during which the event occurred.
            duration_ms: The duration of the event in milliseconds.
            count: A count associated with the event (e.g., number of errors).
            metadata: Additional key-value pairs for context.
        """
        # --- Check if collection exists (using correct check) AND log warning --- #
        if self.metrics_collection is None:
            logger.debug(f"Metrics DB collection is None. Logging event locally: {event_name}, Campaign: {campaign_id}, Step: {agent_step}, Meta: {metadata}")
            return # Exit early as DB is paused

        # --- The following DB interaction code will not run while paused --- #
        try:
            metric = MetricRecord(
                event_name=event_name,
                campaign_id=campaign_id,
                agent_step=agent_step,
                duration_ms=duration_ms,
                count=count,
                metadata=metadata
            )
            
            metric_dict = metric.model_dump(mode='json') 
            
            insert_result = self.metrics_collection.insert_one(metric_dict)
            logger.debug(f"Recorded metric event '{event_name}'. DB ID: {insert_result.inserted_id}")
            
        except Exception as e:
            logger.exception(f"Failed to record metric event '{event_name}' to DB: {e}")

# Optional: Provide a global instance or a factory function if preferred
# metrics_service_instance = MetricsService()
# def get_metrics_service():
#    return metrics_service_instance 