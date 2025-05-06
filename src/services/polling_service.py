import logging
from typing import List, Dict, Any, Optional

# Import necessary components
from ..persistence.mongodb import _get_collection, LEADS_COLLECTION
from ..api.attio_client import AttioClient
from ..models.lead import PodcastLead # Assuming this model has attio_id and vetting_status
from ..utils.attio_converter import attio_tag_to_internal_status # To map Attio tags

logger = logging.getLogger(__name__)

class PollingService:
    """
    Service responsible for periodically polling Attio for status updates
    and syncing them back to the local database.
    """

    def __init__(self):
        """Initializes the PollingService with necessary clients/connections."""
        try:
            self.attio_client = AttioClient()
            self.leads_collection = _get_collection(LEADS_COLLECTION)
            logger.info("PollingService initialized successfully.")
        except Exception as e:
            logger.exception("Failed to initialize PollingService components.")
            # Depending on requirements, either raise or handle gracefully
            self.attio_client = None
            self.leads_collection = None
            raise # Re-raise for clarity that initialization failed

    def _get_local_leads_with_attio_id(self) -> List[Dict[str, Any]]:
        """Fetches local leads that have an associated Attio ID."""
        if not self.leads_collection:
            logger.error("Leads collection not available in PollingService.")
            return []
        
        try:
            # Find leads that have the 'attio_id' field present and non-empty
            cursor = self.leads_collection.find(
                {"attio_id": {"$exists": True, "$ne": None, "$ne": ""}}
            )
            return list(cursor)
        except Exception as e:
            logger.exception("Error fetching local leads with Attio ID.")
            return []

    def _determine_status_from_attio_company(self, attio_company_data: Dict[str, Any]) -> Optional[str]:
        """
        Determines the internal status based on tags present in Attio company data.
        
        Args:
            attio_company_data: The dictionary representing the company record from Attio API.
            
        Returns:
            The internal status string or None if no relevant status tag is found.
        """
        # Attio API structure assumption: Tags are associated with the record.
        # We might need to make another API call to get tags for a record, 
        # OR the tags might be included in the get_company response.
        # Assuming get_company includes associated tags (needs verification).
        
        # Placeholder: Extract tags from the response. Adjust based on actual structure.
        tags = attio_company_data.get("tags", []) # Example: [{id: "tag1", name: "Vetting: Approved"}, ...]
        
        if not isinstance(tags, list):
             logger.warning(f"Expected 'tags' to be a list in Attio data, got: {type(tags)}. Cannot determine status.")
             return None

        relevant_status = None
        for tag_info in tags:
            if isinstance(tag_info, dict):
                tag_name = tag_info.get("name") # Assuming tag structure {id: ..., name: ...}
                if tag_name:
                    internal_status = attio_tag_to_internal_status(tag_name)
                    if internal_status:
                        # Found a relevant status tag
                        if relevant_status:
                            logger.warning(f"Multiple status tags found for Attio record {attio_company_data.get('id')}. Using first one found: '{relevant_status}'. Found: {tag_name}")
                        else:
                            relevant_status = internal_status
                        # Optionally break here if only one status tag should ever apply
                        # break 
        
        if not relevant_status:
             logger.debug(f"No relevant status tag found for Attio record {attio_company_data.get('id')}.")
             
        return relevant_status


    def poll_and_sync_attio_status(self):
        """
        Polls Attio for updates on leads synced from the local system and updates
        the local status if discrepancies are found.
        """
        if not self.attio_client or not self.leads_collection:
            logger.error("PollingService not properly initialized. Skipping poll.")
            return

        logger.info("Starting Attio status polling cycle...")
        
        local_leads = self._get_local_leads_with_attio_id()
        if not local_leads:
            logger.info("No local leads with Attio IDs found to poll.")
            return
            
        logger.info(f"Found {len(local_leads)} local leads with Attio IDs to check.")
        
        updated_count = 0
        failed_count = 0

        for lead in local_leads:
            local_lead_id = lead.get("_id")
            attio_id = lead.get("attio_id")
            current_local_status = lead.get("vetting_status")

            if not attio_id: # Should not happen due to query, but double-check
                continue 
                
            logger.debug(f"Checking status for local lead {local_lead_id} (Attio ID: {attio_id}). Current local status: '{current_local_status}'")

            try:
                # Get the latest data from Attio for this company
                attio_company_data = self.attio_client.get_company(attio_id)
                
                if not attio_company_data:
                    logger.warning(f"Could not retrieve company data from Attio for ID: {attio_id}. Skipping sync for this lead.")
                    failed_count += 1
                    continue

                # Determine the status based on Attio data (e.g., tags)
                attio_status = self._determine_status_from_attio_company(attio_company_data)

                if attio_status is None:
                    logger.debug(f"Could not determine status from Attio data for {attio_id}. Skipping update.")
                    continue

                # Compare and update if different
                if attio_status != current_local_status:
                    logger.info(f"Status mismatch for Attio ID {attio_id}. Local: '{current_local_status}', Attio: '{attio_status}'. Updating local record.")
                    
                    update_result = self.leads_collection.update_one(
                        {"_id": local_lead_id},
                        {"$set": {"vetting_status": attio_status}}
                    )
                    
                    if update_result.modified_count == 1:
                        logger.info(f"Successfully updated local status for lead {local_lead_id}.")
                        updated_count += 1
                    else:
                        logger.error(f"Failed to update local status for lead {local_lead_id} despite mismatch detection.")
                        failed_count += 1
                else:
                     logger.debug(f"Status match for Attio ID {attio_id} ('{current_local_status}'). No update needed.")

            except Exception as e:
                logger.exception(f"Error processing lead with Attio ID {attio_id}: {e}")
                failed_count += 1

        logger.info(f"Attio status polling cycle finished. Leads updated: {updated_count}, Failed checks: {failed_count}") 