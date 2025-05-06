from pydantic import BaseModel, Field
from typing import Optional, List, Literal
import uuid

class CampaignConfiguration(BaseModel):
    campaign_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    target_audience: Optional[str] = None
    key_messages: Optional[List[str]] = None
    tone_preferences: Optional[str] = None
    additional_context: Optional[str] = None
    
    search_type: Literal["topic", "related"] = "topic"
    seed_rss_url: Optional[str] = None

    class Config:
        validate_assignment = True
        extra = 'forbid' # Ensure no extra fields are allowed 