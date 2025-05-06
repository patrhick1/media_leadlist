import logging
from typing import Dict, Any, Optional

# TODO: Import necessary modules (e.g., database connection, models, CRM agent)
# from ..persistence.mongodb import get_db_connection
# from ..models.lead import PodcastLead
# from ..agents.crm_agent import CRMAgent # Might need AttioClient directly or conversion utils

# Import necessary modules
from ..persistence.mongodb import _get_collection, LEADS_COLLECTION
from ..models.lead import PodcastLead # Assuming PodcastLead has vetting_status and attio_id
# Import the new converter function
from ..utils.attio_converter import attio_tag_to_internal_status 

logger = logging.getLogger(__name__)

async def process_attio_update(record_id: str, event_data: Dict[str, Any]):
    """
    Processes an update event received from an Attio webhook.

    Args:
        record_id: The Attio ID of the record that was updated (e.g., company ID).
        event_data: The payload data from the Attio webhook event.
    """
    logger.info(f"Processing Attio update for record_id: {record_id}")
    logger.debug(f"Event data: {event_data}")

    try:
        # --- Get Database Collection ---
        leads_collection = _get_collection(LEADS_COLLECTION)

        # --- Determine New Status ---
        new_status = determine_new_status_from_event(event_data)

        if not new_status:
            logger.info(f"No relevant status change detected in event data for Attio ID: {record_id}. No update needed.")
            return

        # --- Find Local Lead --- 
        # Find the lead in our DB that corresponds to the Attio record_id
        # Note: Pymongo methods are synchronous, so we don't need await here unless using an async driver like Motor
        local_lead_data = leads_collection.find_one({"attio_id": record_id})
        
        if not local_lead_data:
            logger.warning(f"No local lead found with attio_id matching Attio record_id: {record_id}")
            return

        # --- Update Local Status if Changed ---
        current_status = local_lead_data.get('vetting_status')
        
        if current_status != new_status:
            logger.info(f"Updating status for lead {local_lead_data.get('_id', 'N/A')} (Attio ID: {record_id}) from '{current_status}' to '{new_status}'")
            # Update the lead in the database
            update_result = leads_collection.update_one(
                {"attio_id": record_id}, # Filter by attio_id to be sure
                {"$set": {"vetting_status": new_status}} # Set the new status
                # Add other fields to $set if the webhook provides more update info
            )
            
            if update_result.modified_count == 1:
                 logger.info(f"Successfully updated status for local lead corresponding to Attio ID: {record_id}")
            elif update_result.matched_count == 1 and update_result.modified_count == 0:
                 logger.warning(f"Found local lead for Attio ID {record_id}, but status was already '{new_status}'. No update made.")
            else:
                 # This case (matched_count=0) should theoretically be caught by the find_one check above
                 logger.error(f"Failed to find and update status for local lead corresponding to Attio ID: {record_id}")
        else:
             logger.info(f"Local lead status for Attio ID {record_id} is already '{new_status}'. No update needed.")

    except Exception as e:
        logger.exception(f"Error processing Attio webhook for record_id {record_id}: {e}")
        # Optionally, raise the exception or handle it for retry mechanisms

def determine_new_status_from_event(event_data: Dict[str, Any]) -> Optional[str]:
    """
    Determines the new internal status based on Attio event data.
    Currently assumes the status change comes via added tags.
    
    Args:
        event_data: The payload from the Attio webhook.
        
    Returns:
        The corresponding internal status string (e.g., "approved") or None.
    """
    new_internal_status = None
    
    # --- Check for Added Tags --- 
    # This structure is an assumption. Adjust based on actual Attio payload.
    tags_info = event_data.get('tags')
    if isinstance(tags_info, dict):
        added_tags = tags_info.get('added')
        if isinstance(added_tags, list):
            for tag_name in added_tags:
                # Find the *first* recognized status tag that was added
                internal_status = attio_tag_to_internal_status(tag_name)
                if internal_status:
                    logger.info(f"Detected relevant status tag added: '{tag_name}' -> internal status '{internal_status}'")
                    new_internal_status = internal_status
                    break # Process the first relevant tag found

    # --- TODO: Check for Changed Attributes --- 
    # Alternatively, the status might be a direct attribute change.
    # changed_attributes = event_data.get('changed_attributes')
    # if isinstance(changed_attributes, dict):
    #     if 'vetting_status' in changed_attributes:
    #         # Assuming Attio uses a direct attribute matching our internal status
    #         new_status_value = changed_attributes['vetting_status'][1] # Get the 'new' value
    #         # Validate if new_status_value is one of our known internal statuses
    #         # if is_valid_internal_status(new_status_value):
    #         #    new_internal_status = new_status_value
    #         pass

    if not new_internal_status:
        logger.debug("No relevant status change detected in added tags or attributes.")

    return new_internal_status 