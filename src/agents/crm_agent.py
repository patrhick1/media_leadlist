import logging
from typing import Dict, Any, List, Optional
import os
import time # Import time

from src.api.attio_client import AttioClient
# Import specific exceptions used
from src.api.exceptions import APIRequestError 
from src.utils.attio_converter import podcast_to_attio_company, vetting_status_to_attio_tag, attio_company_to_podcast
# Import MetricsService
from ..services.metrics_service import MetricsService 

logger = logging.getLogger(__name__)

class CRMAgent:
    """
    Agent responsible for managing podcast data in the Attio CRM system.
    Handles synchronization between internal podcast database and Attio.
    """
    
    def __init__(self):
        """Initialize the CRM agent with Attio client and MetricsService"""
        try:
            self.client = AttioClient()
            self.metrics_service = MetricsService()
            self.setup_required_tags()
            logger.info("CRMAgent initialized.")
        except Exception as e:
            logger.exception("Failed to initialize CRMAgent components.")
            self.client = None
            self.metrics_service = None
            # Consider re-raising or handling initialization failure
    
    def setup_required_tags(self):
        """Ensure all required tags exist in Attio for podcast classification"""
        required_tags = [
            "Vetting: New",
            "Vetting: In Progress",
            "Vetting: Approved",
            "Vetting: Rejected",
            "Vetting: Pending Review",
            "Outreach: Contacted",
            "Outreach: Scheduled",
            "Outreach: Completed"
        ]
        
        # Get existing tags to avoid creating duplicates
        existing_tags = self.client.get_tags()
        existing_tag_names = [tag.get("attributes", {}).get("name", "") for tag in existing_tags]
        
        # Create missing tags
        for tag_name in required_tags:
            if tag_name not in existing_tag_names:
                logger.info(f"Creating required tag: {tag_name}")
                try:
                    self.client.create_tag(tag_name)
                except Exception as e:
                    logger.exception(f"Failed to create required Attio tag '{tag_name}': {e}")
                    # Record error metric for tag creation failure
                    if self.metrics_service:
                        self.metrics_service.record_event(
                            event_name="error", 
                            agent_step="crm_sync_setup",
                            metadata={"error_type": "AttioTagCreation", "tag_name": tag_name, "error_message": str(e)}
                        )
    
    def sync_podcast_to_crm(self, podcast_data: Dict[str, Any], campaign_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Synchronizes a podcast to the Attio CRM. Creates or updates the podcast record.
        Also records metrics for the sync attempt.
        
        Args:
            podcast_data: Dictionary containing podcast information.
            campaign_id: Optional campaign ID for metrics and tagging.
            
        Returns:
            Updated podcast data including Attio ID, or None if sync fails.
        """
        podcast_id = podcast_data.get("podcast_id", "N/A")
        podcast_name = podcast_data.get("podcast_name", podcast_id)
        start_time = time.time()
        sync_status = "unknown"
        created = False
        attio_id_result = None
        
        # Record start
        if self.metrics_service:
            self.metrics_service.record_event(
                event_name="crm_sync_single_start", 
                agent_step="crm_sync",
                campaign_id=campaign_id,
                metadata={"podcast_id": podcast_id, "podcast_name": podcast_name}
            )
            
        try:
            # Basic Validation
            if not podcast_name or podcast_name == "N/A":
                logger.error(f"Skipping sync: Missing podcast_name/podcast_id in data: {podcast_data}")
                sync_status = "validation_error"
                raise ValueError("Missing essential identifier (podcast_name or podcast_id)")
            
            # Check if podcast already exists in Attio (by internal ID if available)
            attio_id = podcast_data.get("attio_id")
            company_exists = False
            
            if attio_id:
                try:
                    # Try to get the existing company
                    logger.debug(f"Checking existence for Attio ID: {attio_id}")
                    company = self.client.get_company(attio_id)
                    if company and company.get("id"):
                        company_exists = True
                        logger.debug(f"Found existing company with Attio ID: {attio_id}")
                except Exception as e:
                    # Attio might return 404 which client handles as APIRequestError
                    logger.warning(f"Could not retrieve existing company for Attio ID {attio_id} (may not exist): {e}. Proceeding with creation/update flow.")
                    # Ensure attio_id is cleared if lookup failed definitively, forcing create path
                    if isinstance(e, APIRequestError) and e.status_code == 404:
                        attio_id = None
                        company_exists = False # Ensure we try to create
                    # Otherwise, might be a transient error, proceed cautiously (update might fail)
            
            # Convert podcast data to Attio format
            attio_company_data = podcast_to_attio_company(podcast_data)
            
            # Create or update the company in Attio
            if company_exists and attio_id:
                logger.info(f"Updating existing company in Attio: {podcast_name} (ID: {attio_id})")
                company = self.client.update_company(attio_id, attio_company_data)
                created = False
            else:
                logger.info(f"Creating new company in Attio: {podcast_name}")
                company = self.client.create_company(attio_company_data)
                created = True
            
            # Update the podcast data with the Attio ID
            updated_podcast = podcast_data.copy()
            if company and company.get("id"):
                attio_id_result = company["id"]
                updated_podcast["attio_id"] = attio_id_result
                logger.info(f"Successfully {'created' if created else 'updated'} record for {podcast_name}. Attio ID: {attio_id_result}")
                
                # Update tags based on vetting status
                if "vetting_status" in podcast_data:
                    self._update_status_tags(attio_id_result, podcast_data["vetting_status"])
                
                # Add Campaign Tag
                if campaign_id:
                    self._apply_campaign_tag(attio_id_result, campaign_id)
            
            sync_status = "success"
            return updated_podcast

        except Exception as e:
            logger.exception(f"Failed to sync podcast {podcast_id} ({podcast_name}) to CRM: {e}")
            # Use status set in block if specific error, else generic error
            sync_status = sync_status if sync_status not in ["unknown", "success"] else "error"
            # Record error metric
            if self.metrics_service:
                self.metrics_service.record_event(
                    event_name="crm_sync_single_error", 
                    agent_step="crm_sync",
                    campaign_id=campaign_id,
                    metadata={"podcast_id": podcast_id, "podcast_name": podcast_name, "error": str(e), "sync_status_code": sync_status}
                )
            return None # Indicate failure
            
        finally:
             # Record end metric
            if self.metrics_service:
                duration_ms = (time.time() - start_time) * 1000
                self.metrics_service.record_event(
                    event_name="crm_sync_single_end", 
                    agent_step="crm_sync",
                    campaign_id=campaign_id,
                    duration_ms=duration_ms,
                    metadata={
                        "podcast_id": podcast_id, 
                        "podcast_name": podcast_name,
                        "status": sync_status, 
                        "attio_id": attio_id_result,
                        "action": "create" if created and sync_status=="success" else ("update" if company_exists and sync_status=="success" else "none")
                    }
                )

    def batch_sync_podcasts_to_crm(self, podcasts: List[Dict[str, Any]], campaign_id: Optional[str] = None) -> List[Optional[Dict[str, Any]]]:
        """
        Synchronizes multiple podcasts to Attio, attempting batch creation first.
        Records batch-level metrics.
        
        Args:
            podcasts: List of podcast data dictionaries.
            campaign_id: Optional campaign ID for metrics.
            
        Returns:
            List containing updated podcast data (dict) or None for each input podcast.
        """
        start_time = time.time()
        total_leads = len(podcasts)
        new_podcasts_count = 0
        existing_podcasts_count = 0
        batch_api_errors = 0
        processed_results = [] # To store results in order
        
        # Record start
        if self.metrics_service:
            self.metrics_service.record_event(
                event_name="crm_batch_start", 
                agent_step="crm_sync",
                campaign_id=campaign_id,
                metadata={"total_leads_to_sync": total_leads}
            )

        if not podcasts:
            # No need to record end event if nothing to process
            return []
        
        try:
            # For podcasts without Attio IDs, create them in a batch
            new_podcasts = [p for p in podcasts if not p.get("attio_id") and p.get("podcast_name")] # Basic validation
            invalid_new_podcasts = [p for p in podcasts if not p.get("attio_id") and not p.get("podcast_name")]
            existing_podcasts = [p for p in podcasts if p.get("attio_id")]
            new_podcasts_count = len(new_podcasts)
            existing_podcasts_count = len(existing_podcasts)
            
            # Pre-process results for invalid leads
            invalid_indices = {podcasts.index(p): None for p in invalid_new_podcasts}
            for p in invalid_new_podcasts:
                logger.error(f"Skipping sync: Missing podcast_name in data for batch create: {p.get('podcast_id', 'Unknown ID')}")
                if self.metrics_service:
                    self.metrics_service.record_event(
                        event_name="crm_sync_single_error", agent_step="crm_sync", campaign_id=campaign_id,
                        metadata={"podcast_id": p.get('podcast_id', 'Unknown ID'), "error": "Missing podcast_name", "sync_status_code": "validation_error"}
                    )
            
            # Process new podcasts in a batch if there are any
            created_podcast_map = {} # Map original index to result
            if new_podcasts:
                attio_companies = [podcast_to_attio_company(p) for p in new_podcasts]
                try:
                    # Create companies in batch
                    logger.info(f"Attempting batch create for {len(attio_companies)} new leads.")
                    created_companies_response = self.client.batch_create_records(attio_companies)
                    created_companies = created_companies_response.get("records", []) # Adjust key if needed
                    
                    # Map created companies back to podcasts
                    if len(created_companies) == len(new_podcasts):
                        for i, company in enumerate(created_companies):
                            podcast_index = podcasts.index(new_podcasts[i]) # Get original index
                            if company and company.get("id"):
                                updated_podcast = new_podcasts[i].copy()
                                attio_id_result = company["id"]
                                updated_podcast["attio_id"] = attio_id_result
                                logger.info(f"Successfully batch-created record for {updated_podcast.get('podcast_name')}. Attio ID: {attio_id_result}")
                                # Update tags based on vetting status
                                if "vetting_status" in updated_podcast:
                                    try:
                                        self._update_status_tags(attio_id_result, updated_podcast["vetting_status"])
                                    except Exception as tag_err:
                                        logger.error(f"Failed to update status tags for newly batch-created Attio ID {attio_id_result}: {tag_err}")
                                # Add Campaign Tag
                                if campaign_id:
                                    try:
                                        self._apply_campaign_tag(attio_id_result, campaign_id)
                                    except Exception as camp_tag_err:
                                        logger.error(f"Failed to apply campaign tag for newly batch-created Attio ID {attio_id_result}: {camp_tag_err}")
                                # Store successful batch-created lead
                                created_podcast_map[podcast_index] = updated_podcast 
                            else:
                                logger.error(f"Batch create response item {i} invalid or missing ID. Original: {new_podcasts[i].get('podcast_id')}")
                                # Mark original index as None
                                created_podcast_map[podcast_index] = None 
                    else:
                         logger.error(f"Batch create response count ({len(created_companies)}) mismatch with request count ({len(new_podcasts)}). Cannot reliably map results.")
                         batch_api_errors += 1 # Count as one major batch error
                         # Mark all new podcasts as failed for this batch
                         for p in new_podcasts: created_podcast_map[podcasts.index(p)] = None

                except Exception as batch_error:
                    logger.exception(f"Batch create operation failed: {batch_error}")
                    batch_api_errors += 1
                    # Mark all new podcasts as failed for this batch
                    for p in new_podcasts: created_podcast_map[podcasts.index(p)] = None
            
            # Process existing podcasts individually (or fallbacks from failed batch)
            processed_indices = set(created_podcast_map.keys()) | set(invalid_indices.keys())
            individual_sync_map = {} # Map original index to result
            for i, podcast in enumerate(podcasts):
                 if i not in processed_indices:
                      # Pass campaign_id for individual metrics and tagging
                      result = self.sync_podcast_to_crm(podcast, campaign_id=campaign_id) 
                      individual_sync_map[i] = result
            
            # Combine results in the original order
            for i in range(total_leads):
                 if i in created_podcast_map:
                      processed_results.append(created_podcast_map[i])
                 elif i in individual_sync_map:
                      processed_results.append(individual_sync_map[i])
                 elif i in invalid_indices:
                      processed_results.append(invalid_indices[i]) # Append None for invalid
                 else:
                      logger.error(f"Logic error: index {i} was not processed in batch, individually, or marked invalid.")
                      processed_results.append(None) # Add None if missed somehow

        except Exception as e:
            # Catch errors during the setup/looping of batch sync
            logger.exception(f"Error during batch sync orchestration: {e}")
            # Record a general batch error metric
            if self.metrics_service:
                 self.metrics_service.record_event(
                    event_name="crm_batch_error", 
                    agent_step="crm_sync",
                    campaign_id=campaign_id,
                    metadata={"error_type": "Orchestration", "error_message": str(e)}
                )
            # Ensure the result list has the correct length with Nones for failures
            if len(processed_results) < total_leads:
                 processed_results.extend([None] * (total_leads - len(processed_results)))

        finally:
            # Record end metric
            if self.metrics_service:
                duration_ms = (time.time() - start_time) * 1000
                success_count = sum(1 for r in processed_results if r is not None)
                self.metrics_service.record_event(
                    event_name="crm_batch_end", 
                    agent_step="crm_sync",
                    campaign_id=campaign_id,
                    duration_ms=duration_ms,
                    metadata={
                        "total_processed": total_leads, 
                        "new_records_batch_attempted": new_podcasts_count, 
                        "existing_records_individual_sync": existing_podcasts_count,
                        "batch_api_errors": batch_api_errors,
                        "final_success_count": success_count
                    }
                )
        
        return processed_results
    
    def _update_status_tags(self, company_id: str, status: str):
        """
        Updates the status tags for a company in Attio.
        
        Args:
            company_id: Attio company ID
            status: Current vetting status
        """
        tag_name = vetting_status_to_attio_tag(status)
        
        # Remove existing status tags first
        existing_tags = self.client.get_tags()
        for tag in existing_tags:
            tag_id = tag.get("id")
            tag_name = tag.get("attributes", {}).get("name", "")
            
            # Check if it's a status tag
            if tag_name.startswith("Vetting:") or tag_name.startswith("Outreach:"):
                try:
                    # Remove this tag if it exists on the record
                    self.client.remove_tag_from_record(record_id=company_id, tag_id=tag_id)
                except Exception as e:
                    # Ignore errors for tags that weren't applied
                    logger.warning(f"Failed to remove tag {tag_id} from company {company_id}, likely wasn't present: {e}")
        
        # Add the current status tag
        status_tag = next((t for t in existing_tags if t.get("attributes", {}).get("name") == tag_name), None)
        if status_tag:
            self.client.add_tag_to_record(status_tag["id"], company_id)
        else:
            # Create the tag if it doesn't exist
            new_tag = self.client.create_tag(tag_name)
            if new_tag and new_tag.get("id"):
                self.client.add_tag_to_record(new_tag["id"], company_id)
    
    def _apply_campaign_tag(self, company_id: str, campaign_id: str):
        """
        Applies a campaign-specific tag to a company record.
        Creates the tag if it doesn't exist.
        Args:
            company_id: Attio company ID.
            campaign_id: The campaign identifier to use for the tag.
        """
        tag_name = f"Campaign: {campaign_id}"
        tag_id_to_apply = None
        
        try:
            # Check if tag exists
            existing_tags = self.client.get_tags() # Consider caching this if called frequently
            campaign_tag = next((t for t in existing_tags if t.get("attributes", {}).get("name") == tag_name), None)
            
            if campaign_tag:
                tag_id_to_apply = campaign_tag.get("id")
            else:
                # Create the tag if it doesn't exist
                logger.info(f"Creating campaign tag: {tag_name}")
                new_tag = self.client.create_tag(tag_name)
                if new_tag and new_tag.get("id"):
                    tag_id_to_apply = new_tag["id"]
                    logger.info(f"Successfully created campaign tag {tag_name} with ID {tag_id_to_apply}")
                else:
                    logger.error(f"Failed to create or get ID for campaign tag: {tag_name}")
                    # Record metric?
                    return # Cannot apply tag
            
            # Apply the tag
            if tag_id_to_apply:
                logger.info(f"Applying campaign tag '{tag_name}' (ID: {tag_id_to_apply}) to company {company_id}")
                self.client.add_tag_to_record(record_id=company_id, tag_id=tag_id_to_apply)
                logger.debug(f"Successfully applied campaign tag {tag_id_to_apply} to {company_id}")
                
        except Exception as e:
            logger.exception(f"Failed to apply campaign tag '{tag_name}' to company {company_id}: {e}")
            # Record metric?
            # Don't block the overall sync for tag failure
    
    def get_podcast_from_crm(self, attio_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a podcast from Attio by ID and converts it to internal format.
        
        Args:
            attio_id: Attio company ID
            
        Returns:
            Podcast data dictionary or None if not found
        """
        try:
            company = self.client.get_company(attio_id)
            if company:
                return attio_company_to_podcast(company)
            return None
        except Exception as e:
            logger.error(f"Error retrieving podcast from Attio: {e}")
            return None
    
    def search_podcasts_in_crm(self, query: str) -> List[Dict[str, Any]]:
        """
        Searches for podcasts (companies) in Attio by name or other attributes.
        
        Args:
            query: Search query string
            
        Returns:
            List of matching podcast data dictionaries in internal format.
        """
        logger.info(f"Searching for podcasts in Attio CRM with query: '{query}'")
        matching_leads = []
        try:
            # Use the new search method from the client
            # We assume the results are in response['data'] which is a list of Attio company objects
            search_response = self.client.search_companies(query=query)
            
            # Check if 'data' key exists and is a list
            attio_companies = search_response.get('data')
            if isinstance(attio_companies, list):
                logger.info(f"Found {len(attio_companies)} potential matches in Attio.")
                for company_data in attio_companies:
                    try:
                        # Convert each Attio company back to our internal format
                        podcast_lead = attio_company_to_podcast(company_data)
                        matching_leads.append(podcast_lead)
                    except Exception as conversion_error:
                        logger.error(f"Error converting Attio company data to podcast lead: {company_data.get('id', 'N/A')}. Error: {conversion_error}")
                        # Optionally skip this record or handle differently
            else:
                logger.warning(f"Attio search response did not contain a list under the 'data' key. Response: {search_response}")
                
        except Exception as e:
            logger.exception(f"Error searching podcasts in Attio CRM: {e}")
            # Return empty list on error

        return matching_leads
    
    def delete_podcast_from_crm(self, attio_id: str) -> bool:
        """
        Deletes a podcast from Attio.
        
        Args:
            attio_id: Attio company ID
            
        Returns:
            True if successfully deleted, False otherwise
        """
        try:
            self.client.delete_company(attio_id)
            return True
        except Exception as e:
            logger.error(f"Error deleting podcast from Attio: {e}")
            return False 