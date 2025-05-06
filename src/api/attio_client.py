import os
import logging
import requests
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

from .base_client import PodcastAPIClient
from .exceptions import APIClientError, AuthenticationError, APIRequestError

logger = logging.getLogger(__name__)

# Load API credentials from environment variables
ATTIO_API_KEY = os.getenv("ATTIO_API_KEY")
ATTIO_BASE_URL = os.getenv("ATTIO_BASE_URL", "https://api.attio.com/v2")
ATTIO_WORKSPACE_ID = os.getenv("ATTIO_WORKSPACE_ID")

class AttioClient(PodcastAPIClient):
    """
    API Client for Attio CRM, inheriting from PodcastAPIClient base class.
    
    This client handles all interactions with the Attio API including authentication,
    lead creation/management, campaign tagging, and status synchronization.
    """

    def __init__(self):
        """
        Initialize the Attio API client with credentials from environment variables.
        Raises AuthenticationError if required credentials are missing.
        """
        if not ATTIO_API_KEY:
            logger.error("ATTIO_API_KEY environment variable not set.")
            raise AuthenticationError("Attio API key not configured")
            
        if not ATTIO_WORKSPACE_ID:
            logger.error("ATTIO_WORKSPACE_ID environment variable not set.")
            raise AuthenticationError("Attio workspace ID not configured")
            
        super().__init__(api_key=ATTIO_API_KEY, base_url=ATTIO_BASE_URL)
        self.workspace_id = ATTIO_WORKSPACE_ID
        logger.info("AttioClient initialized successfully.")

    def _set_auth_header(self):
        """Sets the Authorization Bearer token header for Attio authentication."""
        if self.api_key:
            self.session.headers.update({
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            })
            logger.debug("Attio API authentication headers set.")

    def search_podcasts(self, query: str, **kwargs) -> Dict[str, Any]:
        """
        Implements the abstract method from PodcastAPIClient, but not applicable for Attio.
        Raises NotImplementedError as this CRM client doesn't search for podcasts.
        """
        raise NotImplementedError("AttioClient does not support podcast search operations")

    # --- Company Object Operations ---

    def get_companies(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        Retrieves a list of companies from Attio.
        
        Args:
            limit: Maximum number of companies to return.
            offset: Offset for pagination.
            
        Returns:
            Dictionary containing company records.
        """
        endpoint = f"workspaces/{self.workspace_id}/objects/company/records"
        params = {
            "limit": limit,
            "offset": offset
        }
        return self._request("GET", endpoint, params=params)

    def get_company(self, company_id: str) -> Dict[str, Any]:
        """
        Retrieve a specific company by ID.
        
        Args:
            company_id: The Attio ID of the company to retrieve.
            
        Returns:
            Dictionary containing company details.
        """
        endpoint = f"workspaces/{self.workspace_id}/objects/company/records/{company_id}"
        return self._request("GET", endpoint)

    def create_company(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new company record in Attio.
        
        Args:
            company_data: Dictionary containing company attributes according to Attio's API schema.
            
        Returns:
            Dictionary containing the created company record.
        """
        endpoint = f"workspaces/{self.workspace_id}/objects/company/records"
        return self._request("POST", endpoint, json={"attributes": company_data})

    def update_company(self, company_id: str, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing company record in Attio.
        
        Args:
            company_id: The Attio ID of the company to update.
            company_data: Dictionary containing company attributes to update.
            
        Returns:
            Dictionary containing the updated company record.
        """
        endpoint = f"workspaces/{self.workspace_id}/objects/company/records/{company_id}"
        return self._request("PATCH", endpoint, json={"attributes": company_data})

    def delete_company(self, company_id: str) -> Dict[str, Any]:
        """
        Delete a company record from Attio.
        
        Args:
            company_id: The Attio ID of the company to delete.
            
        Returns:
            Dictionary containing the deletion result.
        """
        endpoint = f"workspaces/{self.workspace_id}/objects/company/records/{company_id}"
        return self._request("DELETE", endpoint)

    # --- Tag/Campaign Operations ---

    def get_tags(self) -> Dict[str, Any]:
        """
        Retrieve all available tags in the Attio workspace.
        
        Returns:
            Dictionary containing tag information.
        """
        endpoint = f"workspaces/{self.workspace_id}/tags"
        return self._request("GET", endpoint)

    def create_tag(self, tag_name: str, color: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new tag in Attio, which can be used for campaign marking.
        
        Args:
            tag_name: Name of the tag to create.
            color: Optional color for the tag (hex code).
            
        Returns:
            Dictionary containing the created tag.
        """
        endpoint = f"workspaces/{self.workspace_id}/tags"
        tag_data = {"name": tag_name}
        if color:
            tag_data["color"] = color
        return self._request("POST", endpoint, json=tag_data)

    def add_tag_to_record(self, record_id: str, tag_id: str, object_type: str = "company") -> Dict[str, Any]:
        """
        Add a tag to a specific record (typically a company).
        
        Args:
            record_id: ID of the record to tag.
            tag_id: ID of the tag to apply.
            object_type: The object type (default: "company").
            
        Returns:
            Dictionary containing the operation result.
        """
        endpoint = f"workspaces/{self.workspace_id}/objects/{object_type}/records/{record_id}/tags/{tag_id}"
        return self._request("PUT", endpoint)

    def remove_tag_from_record(self, record_id: str, tag_id: str, object_type: str = "company") -> Dict[str, Any]:
        """
        Remove a tag from a specific record.
        
        Args:
            record_id: ID of the record.
            tag_id: ID of the tag to remove.
            object_type: The object type (default: "company").
            
        Returns:
            Dictionary containing the operation result.
        """
        endpoint = f"workspaces/{self.workspace_id}/objects/{object_type}/records/{record_id}/tags/{tag_id}"
        return self._request("DELETE", endpoint)

    # --- NEW: Search Operation --- #
    def search_companies(self, query: str, search_fields: List[str] = ["company_name", "description"], limit: int = 25) -> Dict[str, Any]:
        """
        Searches for company records in Attio based on a query string.
        
        Args:
            query: The search term.
            search_fields: List of Attio attribute slugs to search within.
            limit: Maximum number of results to return.
            
        Returns:
            Dictionary containing search results, typically a list of company records.
            Returns an empty dict if search fails or returns no results.
        """
        # Endpoint assumption: Attio might use a dedicated search endpoint.
        # Adjust if the actual endpoint is different (e.g., using query params on GET /records).
        endpoint = f"workspaces/{self.workspace_id}/objects/company/records/search"
        
        # Filter structure assumption: Attio likely has a specific filter syntax.
        # This example uses a simple OR condition with 'contains'.
        # Consult Attio API documentation for the correct filter structure.
        filters = {
            "operator": "or",
            "conditions": [
                {
                    "attribute": field,
                    "operator": "contains",
                    "value": query
                }
                for field in search_fields
            ]
        }
        
        payload = {
            "query": filters,
            "limit": limit
        }
        
        try:
            # Use the inherited _request method
            # Assume POST is the correct method for search with a filter body
            response = self._request("POST", endpoint, json=payload) 
            # The base _request handles errors, retries etc.
            # Assume results are in a list under a key like 'data' or similar
            return response if response else {"data": []} # Return empty list structure if no results
        except APIRequestError as e:
            # Handle specific client/server errors from the search request
            logger.error(f"Attio company search failed: {e}. Status: {e.status_code}")
            return {"data": []}
        except Exception as e:
            # Catch unexpected errors during search
            logger.exception(f"An unexpected error occurred during Attio company search: {e}")
            return {"data": []}

    # --- Batch Operations ---

    def batch_create_records(self, records: List[Dict[str, Any]], object_type: str = "company") -> Dict[str, Any]:
        """
        Create multiple records in a single batch operation.
        
        Args:
            records: List of record data dictionaries.
            object_type: The object type to create (default: "company").
            
        Returns:
            Dictionary containing batch operation results.
        """
        endpoint = f"workspaces/{self.workspace_id}/objects/{object_type}/records/batch"
        records_data = [{"attributes": record} for record in records]
        return self._request("POST", endpoint, json={"records": records_data})

    # --- Webhook Management ---

    def get_webhooks(self) -> Dict[str, Any]:
        """
        Get all configured webhooks for the workspace.
        
        Returns:
            Dictionary containing webhook information.
        """
        endpoint = f"workspaces/{self.workspace_id}/webhooks"
        return self._request("GET", endpoint)

    def create_webhook(self, target_url: str, events: List[str], secret: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new webhook for bidirectional sync.
        
        Args:
            target_url: URL where webhook events will be sent.
            events: List of event types to subscribe to.
            secret: Optional secret for webhook verification.
            
        Returns:
            Dictionary containing the created webhook.
        """
        endpoint = f"workspaces/{self.workspace_id}/webhooks"
        webhook_data = {
            "target_url": target_url,
            "events": events
        }
        if secret:
            webhook_data["secret"] = secret
        return self._request("POST", endpoint, json=webhook_data)

    def delete_webhook(self, webhook_id: str) -> Dict[str, Any]:
        """
        Delete an existing webhook.
        
        Args:
            webhook_id: ID of the webhook to delete.
            
        Returns:
            Dictionary containing the operation result.
        """
        endpoint = f"workspaces/{self.workspace_id}/webhooks/{webhook_id}"
        return self._request("DELETE", endpoint) 