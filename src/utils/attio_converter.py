import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

def podcast_to_attio_company(podcast_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts podcast data from our internal format to Attio company object format.
    
    Args:
        podcast_data: Dictionary containing podcast information
        
    Returns:
        Dictionary formatted for Attio company creation
    """
    # Create attribute mapping that matches Attio schema
    attio_attributes = {
        "company_name": podcast_data.get("podcast_name", ""),
        "description": podcast_data.get("description", ""),
        "website": podcast_data.get("website", ""),
        "apple_podcasts_url": podcast_data.get("apple_podcasts_url", ""),
        "spotify_url": podcast_data.get("spotify_url", ""),
        "rss_feed": podcast_data.get("rss_feed", ""),
        "email": podcast_data.get("email", ""),
        "category": podcast_data.get("category", ""),
        "subcategory": podcast_data.get("subcategory", ""),
        "rating": float(podcast_data.get("rating", 0.0)),
        "rating_count": int(podcast_data.get("rating_count", 0)),
        "review_count": int(podcast_data.get("review_count", 0))
    }
    
    # Add podcast analysis data if available
    if "analysis" in podcast_data:
        analysis = podcast_data["analysis"]
        attio_attributes.update({
            "audience_size": analysis.get("audience_size", ""),
            "episode_frequency": analysis.get("episode_frequency", ""),
            "avg_episode_length": analysis.get("avg_episode_length", ""),
            "content_rating": analysis.get("content_rating", ""),
            "content_type": analysis.get("content_type", ""),
            "monetization": analysis.get("monetization", "")
        })
    
    # Add host information if available
    if "hosts" in podcast_data and podcast_data["hosts"]:
        hosts = podcast_data["hosts"]
        if isinstance(hosts, list):
            attio_attributes["hosts"] = ", ".join([h.get("name", "") for h in hosts])
        else:
            attio_attributes["hosts"] = hosts
    
    # Add social media links if available
    if "social_media" in podcast_data:
        social = podcast_data["social_media"]
        attio_attributes.update({
            "twitter": social.get("twitter", ""),
            "instagram": social.get("instagram", ""),
            "facebook": social.get("facebook", ""),
            "linkedin": social.get("linkedin", ""),
            "youtube": social.get("youtube", "")
        })
    
    # Add vetting status data
    attio_attributes["vetting_status"] = podcast_data.get("vetting_status", "new")
    attio_attributes["vetting_score"] = float(podcast_data.get("vetting_score", 0.0))
    
    # Remove any None values
    return {k: v for k, v in attio_attributes.items() if v is not None}

def vetting_status_to_attio_tag(status: str) -> str:
    """
    Maps internal vetting status to an Attio tag name.
    
    Args:
        status: Internal vetting status string
        
    Returns:
        Formatted tag name for Attio
    """
    status_mapping = {
        "new": "Vetting: New",
        "in_progress": "Vetting: In Progress",
        "approved": "Vetting: Approved",
        "rejected": "Vetting: Rejected",
        "pending_review": "Vetting: Pending Review",
        "contacted": "Outreach: Contacted",
        "scheduled": "Outreach: Scheduled",
        "completed": "Outreach: Completed"
    }
    
    return status_mapping.get(status.lower(), f"Status: {status}")

# --- NEW: Inverse mapping from Attio tag back to internal status ---
def attio_tag_to_internal_status(tag_name: str) -> Optional[str]:
    """
    Maps an Attio tag name back to our internal status string.
    
    Args:
        tag_name: The full name of the tag from Attio (e.g., "Vetting: Approved")
        
    Returns:
        The corresponding internal status string (e.g., "approved") or None if not a recognized status tag.
    """
    # Reverse the mapping from vetting_status_to_attio_tag
    status_mapping_inverse = {
        "Vetting: New": "new",
        "Vetting: In Progress": "in_progress",
        "Vetting: Approved": "approved",
        "Vetting: Rejected": "rejected",
        "Vetting: Pending Review": "pending_review",
        "Outreach: Contacted": "contacted",
        "Outreach: Scheduled": "scheduled",
        "Outreach: Completed": "completed"
    }
    
    return status_mapping_inverse.get(tag_name)

def attio_company_to_podcast(attio_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts Attio company data back to our internal podcast format.
    
    Args:
        attio_data: Dictionary containing Attio company information
        
    Returns:
        Dictionary formatted for internal podcast representation
    """
    # Extract attributes from Attio response
    attributes = attio_data.get("attributes", {})
    
    # Build podcast data structure
    podcast_data = {
        "podcast_name": attributes.get("company_name", ""),
        "description": attributes.get("description", ""),
        "website": attributes.get("website", ""),
        "apple_podcasts_url": attributes.get("apple_podcasts_url", ""),
        "spotify_url": attributes.get("spotify_url", ""),
        "rss_feed": attributes.get("rss_feed", ""),
        "email": attributes.get("email", ""),
        "category": attributes.get("category", ""),
        "subcategory": attributes.get("subcategory", ""),
        "rating": attributes.get("rating", 0.0),
        "rating_count": attributes.get("rating_count", 0),
        "review_count": attributes.get("review_count", 0),
        "vetting_status": attributes.get("vetting_status", "new"),
        "vetting_score": attributes.get("vetting_score", 0.0),
        "attio_id": attio_data.get("id", "")
    }
    
    # Add analysis data
    podcast_data["analysis"] = {
        "audience_size": attributes.get("audience_size", ""),
        "episode_frequency": attributes.get("episode_frequency", ""),
        "avg_episode_length": attributes.get("avg_episode_length", ""),
        "content_rating": attributes.get("content_rating", ""),
        "content_type": attributes.get("content_type", ""),
        "monetization": attributes.get("monetization", "")
    }
    
    # Parse hosts
    if "hosts" in attributes and attributes["hosts"]:
        host_text = attributes["hosts"]
        if isinstance(host_text, str):
            hosts = [{"name": name.strip()} for name in host_text.split(",")]
            podcast_data["hosts"] = hosts
    
    # Add social media links
    podcast_data["social_media"] = {
        "twitter": attributes.get("twitter", ""),
        "instagram": attributes.get("instagram", ""),
        "facebook": attributes.get("facebook", ""),
        "linkedin": attributes.get("linkedin", ""),
        "youtube": attributes.get("youtube", "")
    }
    
    return podcast_data 