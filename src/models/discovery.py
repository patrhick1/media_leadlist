from pydantic import BaseModel, Field
from typing import Optional, List, Literal, Dict

# Import dependent models
from .guests import Guest, GuestAppearance
from .lead import PodcastLead

class DiscoveryResult(BaseModel):
    """Represents a podcast lead found via discovery methods, consolidating context and scores from potentially multiple sources."""

    # The actual podcast found
    podcast_lead: PodcastLead = Field(..., description="The podcast lead identified by the discovery process.")

    # How was this podcast found? (Can be multiple)
    discovery_methods: List[Literal["guest_shadowing", "neighborhood_mapping"]] = Field(default_factory=list, description="List of methods that discovered this podcast lead.")

    # Stores the score calculated by each method that found this podcast
    method_scores: Dict[Literal["guest_shadowing", "neighborhood_mapping"], float] = Field(default_factory=dict, description="Scores assigned by each discovery method.")

    # --- Consolidated Optional Context Fields ---

    # Context if found via Guest Shadowing (can be multiple guests/appearances)
    source_guests: List[Guest] = Field(default_factory=list, description="Guests whose appearances led to this discovery.")
    triggering_appearances: List[GuestAppearance] = Field(default_factory=list, description="Specific guest appearances that triggered this discovery.")

    # Context if found via Neighborhood Mapping (can be multiple sources/attributes)
    source_podcasts: List[PodcastLead] = Field(default_factory=list, description="Podcasts used as the source for neighborhood mapping.")
    matched_attributes: List[str] = Field(default_factory=list, description="Consolidated list of attributes shared between source and discovered podcasts (e.g., ['Category: Tech']).")

    # A potential unified score across all methods (to be developed)
    unified_relevance_score: Optional[float] = Field(None, description="A normalized score for ranking results across different methods.")

    class Config:
        validate_assignment = True
        # Allow extra fields if needed in the future?
        # extra = 'allow' 

class GuestShadowingFilterConfig(BaseModel):
    """Configuration for filtering results from guest shadowing."""
    min_method_score: Optional[float] = Field(None, description="Minimum score derived from guest appearance relevance (recency, prominence).")
    min_guest_popularity: Optional[float] = Field(None, description="Minimum popularity score of the source guest who led to this discovery.")
    # Add other potential guest-related filters here

class NeighborhoodFilterConfig(BaseModel):
    """Configuration for filtering results from neighborhood mapping."""
    min_method_score: Optional[float] = Field(None, description="Minimum similarity score to the source podcast.")
    # min_category_overlap: Optional[int] = Field(None, description="Minimum number of shared categories with the source podcast.") # Requires structured matched_attributes
    # require_same_network: Optional[bool] = Field(None, description="Whether to only include podcasts from the same network as the source.") # Requires network info
    # Add other potential similarity filters here 