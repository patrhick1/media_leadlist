import logging
from typing import Dict, Any, List, Optional, Tuple
import time
import os
import csv # For CSV writing
import json # For serializing complex fields in CSV
from datetime import datetime, timedelta # Import timedelta here
import re # For safe campaign ID in CSV filename
import asyncio # For running async service methods

# Import Graph State and Services/Models
from ..graph.graph_types import GraphState
from ..services.vetting_service import VettingService
from ..models.state import AgentState
from ..models.lead import PodcastLead
from ..models.vetting import VettingResult
from ..models.podcast_profile import EnrichedPodcastProfile
# Import MetricsService
from ..services.metrics_service import MetricsService
# Import CSV utility for consistency, or implement directly
from ..utils.csv_utils import write_vetting_results_to_csv # Changed from write_dicts_to_csv

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

    def _save_vetting_results_to_csv(self, vetting_results: List[VettingResult], campaign_id: str) -> Optional[str]:
        """Saves the list of VettingResult objects to a CSV file."""
        if not vetting_results:
            logger.warning(f"No vetting results to save for campaign {campaign_id}.")
            return None

        # Construct filename using original logic
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        # Ensure campaign_id is safe for filenames
        safe_campaign_id = re.sub(r'[^\w\-]+', '_', campaign_id) 
        filename_prefix = "vetting_output" # Consistent prefix
        
        # Original path construction logic
        filesystem_save_dir = os.path.join("data", "campaigns", safe_campaign_id, "vetting_results")
        os.makedirs(filesystem_save_dir, exist_ok=True) # Ensure directory exists
            
        csv_filename = f"{filename_prefix}_{safe_campaign_id}_{timestamp}.csv"
        filesystem_filepath = os.path.join(filesystem_save_dir, csv_filename)

        try:
            # Use the new dedicated CSV writer function
            write_vetting_results_to_csv(vetting_results, filesystem_filepath)
            logger.info(f"Successfully wrote {len(vetting_results)} vetting results to {filesystem_filepath}")
            
            # Construct web-accessible path (assuming 'data' is served under '/static/')
            # This part needs to be robust if base path changes
            relative_path_for_web = os.path.join("campaigns", safe_campaign_id, "vetting_results", csv_filename).replace(os.sep, '/')
            csv_web_path = f"/static/{relative_path_for_web}"
            logger.info(f"Vetting results CSV web path: {csv_web_path}")
            return csv_web_path # Return web path for frontend
            
        except Exception as e:
            logger.error(f"Failed to write vetting results CSV for campaign {campaign_id}: {e}")
            logger.exception(e) # Log full traceback for debugging
            return None # Return None on failure

    async def perform_standalone_vetting(
        self,
        enriched_profiles: List[EnrichedPodcastProfile],
        ideal_podcast_description: str,
        guest_bio: str,
        guest_talking_points: List[str],
        campaign_id_prefix: str = "standalone_vetting",
        source_campaign_id: Optional[str] = None # To link to previous run
    ) -> Tuple[List[VettingResult], Optional[str]]:
        """Performs standalone vetting on a list of enriched profiles.
        
        Args:
            enriched_profiles: List of profiles to vet.
            ideal_podcast_description: Ideal podcast description for the guest.
            guest_bio: Bio of the guest.
            guest_talking_points: Guest's talking points.
            campaign_id_prefix: Prefix for generating a new campaign ID if source_campaign_id is not provided.
            source_campaign_id: Optional ID of a previous campaign run (e.g., search or enrichment).
            
        Returns:
            Tuple containing a list of VettingResult objects and an optional web path to the CSV output.
        """
        logger.info(f"--- Executing Standalone Vetting for {len(enriched_profiles)} profiles --- ")
        start_time = time.time()
        
        # Determine campaign ID for this vetting run
        if source_campaign_id:
            run_campaign_id = source_campaign_id
            logger.info(f"Using source campaign ID for vetting run: {run_campaign_id}")
        else:
            timestamp = time.strftime("%Y%m%d%H%M%S")
            run_campaign_id = f"{campaign_id_prefix}_{timestamp}"
            logger.info(f"Generated new campaign ID for vetting run: {run_campaign_id}")

        vetting_results: List[VettingResult] = []
        csv_web_path: Optional[str] = None

        if not self.vetting_service:
            logger.error("VettingService not initialized. Cannot perform standalone vetting.")
            # Create error VettingResult for each profile
            for profile in enriched_profiles:
                vetting_results.append(VettingResult(
                    podcast_id=profile.api_id or profile.unified_profile_id or "unknown",
                    programmatic_consistency_passed=False,
                    programmatic_consistency_reason="Vetting service unavailable.",
                    llm_match_score=None,
                    llm_match_explanation="Vetting service unavailable.",
                    composite_score=0,
                    quality_tier="D",
                    final_explanation="Vetting service unavailable.",
                    error="Vetting service unavailable."
                ))
            return vetting_results, None

        if not enriched_profiles:
            logger.warning("Standalone vetting: No enriched profiles provided to vet.")
            return [], None
        
        # Record overall batch start metric (optional, VettingService records per-profile)
        if self.metrics_service:
            self.metrics_service.record_event(
                event_name="standalone_vetting_batch_start", 
                agent_step="vetting", 
                campaign_id=run_campaign_id,
                metadata={"profile_count": len(enriched_profiles)}
            )

        try:
            vetting_results = await self.vetting_service.vet_profiles_batch(
                enriched_profiles=enriched_profiles,
                ideal_podcast_description=ideal_podcast_description,
                guest_bio=guest_bio,
                guest_talking_points=guest_talking_points,
                campaign_id=run_campaign_id # Pass campaign_id for per-profile metrics
            )
            
            if vetting_results:
                csv_web_path = self._save_vetting_results_to_csv(vetting_results, run_campaign_id)
            else:
                logger.info("No results returned from batch vetting, skipping CSV output.")

        except Exception as e:
            logger.exception(f"Critical error during standalone vetting batch for campaign {run_campaign_id}: {e}")
            # Populate results with error objects if batch call itself fails
            if not vetting_results: # If results list is empty due to early batch failure
                for profile in enriched_profiles:
                    vetting_results.append(VettingResult(
                        podcast_id=profile.api_id or profile.unified_profile_id or "unknown",
                        programmatic_consistency_passed=False,
                        programmatic_consistency_reason=f"Batch vetting failed: {str(e)[:100]}",
                        llm_match_score=None,
                        llm_match_explanation=f"Batch vetting failed: {str(e)[:100]}",
                        composite_score=0,
                        quality_tier="D",
                        final_explanation=f"Batch vetting failed: {str(e)[:100]}",
                        error=f"Batch vetting failed: {str(e)[:100]}"
                    ))
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            successful_vets = sum(1 for r in vetting_results if r and not r.error)
            if self.metrics_service:
                 self.metrics_service.record_event(
                    event_name="standalone_vetting_batch_end", 
                    agent_step="vetting", 
                    campaign_id=run_campaign_id,
                    duration_ms=duration_ms,
                    metadata={
                        "total_processed": len(enriched_profiles),
                        "successful_vets": successful_vets,
                        "output_csv_path": csv_web_path
                    }
                )
            logger.info(
                f"--- Standalone Vetting Finished in {duration_ms:.2f}ms. "
                f"Campaign ID: {run_campaign_id}, Processed: {len(enriched_profiles)}, Valid Results: {successful_vets}. CSV: {csv_web_path} ---"
            )
        
        return vetting_results, csv_web_path

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

    # --- Graph-based run_vetting (Example of how it might be adapted if needed) ---
    async def run_vetting_graph_mode(self, state: GraphState) -> Dict[str, Any]:
        """Runs the podcast vetting process for enriched_profiles in the current AgentState."""
        logger.info("--- Executing Vetting Agent (Graph Mode) --- ")
        start_time = time.time()
        agent_state: AgentState = state['agent_state']
        campaign_config = agent_state.campaign_config
        campaign_id = campaign_config.campaign_id if campaign_config else "unknown_graph_campaign"

        final_status = "vetting_failed_graph_mode"
        updated_vetting_results: List[VettingResult] = []

        # Get vetting criteria from CampaignConfiguration within the state
        ideal_desc = getattr(campaign_config, 'ideal_podcast_description', None)
        guest_b = getattr(campaign_config, 'guest_bio', None)
        guest_tp = getattr(campaign_config, 'guest_talking_points', [])

        # Validate that required vetting criteria are present in the config for graph mode
        if not all([ideal_desc, guest_b, guest_tp]):
            error_msg = "Missing required vetting criteria (ideal_podcast_description, guest_bio, guest_talking_points) in CampaignConfiguration for graph mode."
            logger.error(error_msg)
            agent_state.current_step = "error"
            agent_state.execution_status = "vetting_failed_missing_criteria"
            state['error_message'] = error_msg
            state['agent_state'] = agent_state
            # Record error metric if desired
            if self.metrics_service:
                self.metrics_service.record_event("vetting_error", agent_step="vetting_graph_mode", campaign_id=campaign_id, metadata={"error": error_msg})
            return state

        if not self.vetting_service:
            logger.error("VettingService not available. Skipping vetting in graph mode.")
            agent_state.current_step = "crm_export" # Or next appropriate step
            agent_state.execution_status = "vetting_skipped_service_unavailable"
            state['agent_state'] = agent_state
            state['error_message'] = "VettingService unavailable."
            return state

        profiles_to_vet = agent_state.enriched_profiles
        if not profiles_to_vet:
            logger.info("No enriched profiles to vet in graph mode.")
            agent_state.current_step = "crm_export"
            agent_state.execution_status = "vetting_skipped_no_profiles"
            state['agent_state'] = agent_state
            return state
        
        logger.info(f"Vetting {len(profiles_to_vet)} profiles in graph mode for campaign {campaign_id}...")
        
        try:
            updated_vetting_results = await self.vetting_service.vet_profiles_batch(
                enriched_profiles=profiles_to_vet,
                ideal_podcast_description=ideal_desc,
                guest_bio=guest_b,
                guest_talking_points=guest_tp,
                campaign_id=campaign_id
            )
            agent_state.vetting_results = updated_vetting_results
            # TODO: Implement logic to decide the next step based on results (e.g., human_review, crm_export, END)
            # For now, just setting to completed
            agent_state.current_step = "completed" 
            agent_state.execution_status = "vetting_complete_graph_mode"
            final_status = agent_state.execution_status
            state['error_message'] = None 
            state['agent_state'] = agent_state

        except Exception as e:
            logger.exception(f"Vetting Agent (graph mode) failed unexpectedly: {e}")
            agent_state.current_step = "error"
            agent_state.execution_status = final_status = "vetting_failed_agent_error_graph_mode"
            state['error_message'] = f"Vetting Agent (graph mode) failed: {e}"
            state['agent_state'] = agent_state
            # Record error metric
            if self.metrics_service:
                self.metrics_service.record_event("vetting_error", agent_step="vetting_graph_mode", campaign_id=campaign_id, metadata={"error": str(e)})
            
        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            successful_vets = sum(1 for r in updated_vetting_results if r and not r.error)
            if self.metrics_service:
                self.metrics_service.record_event(
                    event_name="agent_step_end", 
                    agent_step="vetting_graph_mode",
                    campaign_id=campaign_id,
                    duration_ms=duration_ms,
                    metadata={
                        "final_status": final_status, 
                        "profiles_processed": len(profiles_to_vet) if profiles_to_vet else 0,
                        "successful_vets": successful_vets
                    }
                )
        logger.info(f"--- Vetting Agent (Graph Mode) Finished. Processed {len(profiles_to_vet) if profiles_to_vet else 0} profiles. Next step: {agent_state.current_step} --- ")
        return state # Return the GraphState dictionary

# --- Example for standalone testing (optional) ---
# async def main_test():
# ... (rest of the file) ...

    # pass # Methods will be added 