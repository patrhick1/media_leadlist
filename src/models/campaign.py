from pydantic import BaseModel, Field
from typing import Optional, List, Literal
import uuid

class CampaignConfiguration(BaseModel):
    """Configuration for a specific lead generation campaign."""
    campaign_id: str = Field(..., description="Unique identifier for the campaign run.")
    search_type: Literal["topic", "related"] = Field("topic", description="Type of search to perform.")
    
    # --- Topic Search Specific --- #
    target_audience: Optional[str] = Field(None, description="Target audience description (required for topic search). Example: 'Software developers interested in AI'")
    key_messages: Optional[List[str]] = Field(default_factory=list, description="Key messages or angles for the guest/topic (used for topic search).")
    num_keywords_to_generate: Optional[int] = Field(10, ge=1, le=30, description="Number of keywords to generate for topic search.")
    max_results_per_keyword: Optional[int] = Field(50, ge=1, le=200, description="Maximum results per keyword per API for topic search.")

    # --- Related Search Specific --- #
    seed_rss_url: Optional[str] = Field(None, description="Seed RSS URL (required for related search).")
    max_depth: Optional[int] = Field(2, ge=1, le=3, description="Depth for related search.")
    max_total_results: Optional[int] = Field(50, ge=1, le=200, description="Maximum total unique results for related search.")

    # --- Vetting Criteria (Required for the full workflow) --- #
    ideal_podcast_description: str = Field(..., description="Description of the ideal podcast for the guest/client.")
    guest_bio: str = Field(..., description="Biography or background of the guest/client.")
    guest_talking_points: List[str] = Field(..., description="Key talking points or angles for the guest/client (list of strings).")

    # --- Optional Future Fields --- #
    tone_preferences: Optional[str] = Field(None, description="Preferred tone for communication or analysis.")
    # Add more fields as needed, like specific exclusion keywords, etc.

    class Config:
        validate_assignment = True
        # Potential future validation:
        # @validator('target_audience', always=True)
        # def check_topic_search_requirements(cls, v, values):
        #     if values.get('search_type') == 'topic' and not v:
        #         raise ValueError('target_audience is required for topic search')
        #     return v
        #
        # @validator('seed_rss_url', always=True)
        # def check_related_search_requirements(cls, v, values):
        #     if values.get('search_type') == 'related' and not v:
        #         raise ValueError('seed_rss_url is required for related search')
        #     return v
        extra = 'forbid' # Ensure no extra fields are allowed 