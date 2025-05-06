from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from .campaign import CampaignConfiguration
from .lead import PodcastLead
from .vetting import VettingResult
from .podcast_profile import EnrichedPodcastProfile

class AgentState(BaseModel):
    """Defines the state passed between agents in the workflow."""
    current_step: str
    campaign_config: Optional[CampaignConfiguration] = None
    # Change leads to store dictionaries directly from search/unification
    leads: List[Dict[str, Any]] = Field(default_factory=list)
    # vetting_results: List[VettingResult] = Field(default_factory=list)
    enriched_profiles: List[EnrichedPodcastProfile] = Field(default_factory=list)
    # TODO: Add human review results if needed
    messages_history: List[Any] = Field(default_factory=list) # For LangGraph history
    execution_status: str = "pending" # e.g., pending, running, completed, failed
    error_message: Optional[str] = None
    
    # Optional fields to store intermediate results for debugging/analysis
    raw_listennotes_results: List[Dict[str, Any]] = Field(default_factory=list)
    raw_podscan_results: List[Dict[str, Any]] = Field(default_factory=list)
    search_results_csv_path: Optional[str] = None # Path to the unified CSV
    enriched_profiles_csv_path: Optional[str] = None # CSV path for enriched output
    vetting_results: List[Optional[VettingResult]] = Field(default_factory=list)
    # neighborhood_results: Optional[Dict[str, Any]] = None # Example for future

    class Config:
        validate_assignment = True
        extra = 'forbid' 