import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
import time # Import time module

# Import Graph State and Services/Models
from ..graph.graph_types import GraphState
from ..services.vetting_service import VettingService
from ..models.state import AgentState
from ..models.lead import PodcastLead
from ..models.vetting import VettingResult
# Import MetricsService
from ..services.metrics_service import MetricsService

logger = logging.getLogger(__name__)

class VettingAgent:
    """Agent responsible for vetting podcast leads based on quality criteria."""

    def __init__(self):
        """Initializes the Vetting Agent with necessary services."""
        try:
            self.vetting_service = VettingService()
            self.metrics_service = MetricsService() # Initialize metrics service
            logger.info("VettingAgent initialized with VettingService and MetricsService.")
        except Exception as e:
            logger.exception("Failed to initialize services in VettingAgent.")
            self.vetting_service = None
            self.metrics_service = None # Set to None on failure

    def run_vetting(self, state: GraphState) -> GraphState:
        """Runs the podcast vetting process for all leads in the current state.

        Args:
            state: The current graph state dictionary.

        Returns:
            The updated graph state dictionary.
        """
        logger.info("--- Executing Vetting Agent --- ")
        start_time = time.time() # Record start time
        agent_state: AgentState = state['agent_state']
        campaign_id = agent_state.campaign_config.campaign_id if agent_state.campaign_config else None
        processed_count = 0
        error_count = 0
        final_status = "vetting_failed" # Default status
        
        # Record start event
        if self.metrics_service:
            self.metrics_service.record_event(
                event_name="agent_step_start", 
                agent_step="vetting",
                campaign_id=campaign_id
            )

        try:
            if not self.vetting_service:
                logger.error("VettingService not available. Skipping vetting.")
                agent_state.current_step = "enrichment"
                state['error_message'] = "VettingService initialization failed."
                # Record init error metric
                if self.metrics_service:
                    self.metrics_service.record_event(
                        event_name="error", 
                        agent_step="vetting",
                        campaign_id=campaign_id,
                        metadata={"error_type": "Initialization", "component": "VettingService"}
                    )
                return state

            leads_to_vet = agent_state.leads
            results: List[VettingResult] = []

            if not leads_to_vet:
                logger.info("No leads found to vet.")
                agent_state.current_step = "enrichment"
                agent_state.execution_status = "vetting_skipped_no_leads"
                final_status = agent_state.execution_status
                state['agent_state'] = agent_state
                state['error_message'] = None
                return state

            logger.info(f"Vetting {len(leads_to_vet)} leads...")
            # --- Use batch vetting --- #
            # Note: vet_podcasts_batch now handles calling vet_podcast 
            # and recording individual vet_start/success/error metrics.
            # It requires podcast data dicts, not PodcastLead objects directly.
            # We still need the Enrichment step to provide real data.
            # For now, construct dummy data based on leads.
            
            podcast_data_list = []
            for lead in leads_to_vet:
                 podcast_data_list.append({
                     "podcast_id": lead.podcast_id,
                     # Add other known fields from lead if needed by dummy logic
                     # --- Placeholder Enrichment --- #
                     "episode_count": 55, # Placeholder
                     "last_publish_date": datetime.now() - timedelta(days=30), # Placeholder
                     "publish_dates": [(datetime.now() - timedelta(days=x*10)) for x in range(10)] # Placeholder
                     # --- End Placeholder --- #
                 })
            
            # Pass campaign_id to the batch method
            results = self.vetting_service.vet_podcasts_batch(podcast_data_list, campaign_id=campaign_id)
            
            # --- Filter out None results before assigning to state --- #
            valid_results = [r for r in results if r is not None]
            error_count = len(results) - len(valid_results)
            processed_count = len(results) # Total attempted
            
            # Update state with only the valid results
            agent_state.vetting_results = valid_results # Store list of VettingResult
            agent_state.current_step = "enrichment"
            agent_state.execution_status = "vetting_complete" # Consider adding nuance if errors occurred?
            final_status = agent_state.execution_status
            state['error_message'] = None # Clear previous errors if vetting was successful overall
            state['agent_state'] = agent_state

            logger.info(f"--- Vetting Agent Finished. Processed {processed_count} leads ({error_count} errors). Next step: {agent_state.current_step} --- ")
            return state
            
        except Exception as e:
            # Catch unexpected errors during the overall agent execution
            logger.exception(f"Vetting Agent failed unexpectedly: {e}")
            agent_state.current_step = "error"
            agent_state.execution_status = "vetting_failed_agent_error"
            final_status = agent_state.execution_status
            state['error_message'] = f"Vetting Agent failed: {e}"
            state['agent_state'] = agent_state
            # Record agent error metric
            if self.metrics_service:
                self.metrics_service.record_event(
                    event_name="error", 
                    agent_step="vetting",
                    campaign_id=campaign_id,
                    metadata={"error_type": "AgentExecution", "error_message": str(e)}
                )
            return state
            
        finally:
            # Record end event with duration
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            if self.metrics_service:
                self.metrics_service.record_event(
                    event_name="agent_step_end", 
                    agent_step="vetting",
                    campaign_id=campaign_id,
                    duration_ms=duration_ms,
                    metadata={
                        "final_status": final_status, 
                        "leads_processed": processed_count, 
                        "vetting_errors": error_count
                    } 
                )

    # pass # Methods will be added 