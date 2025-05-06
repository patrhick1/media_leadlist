from pydantic import BaseModel, Field
from typing import Optional, List

class PodcastLead(BaseModel):
    podcast_id: str
    name: str
    description: str
    email: Optional[str] = None
    host_information: Optional[str] = None
    audience_demographics: Optional[str] = None
    contact_details: Optional[str] = None
    rss_url: Optional[str] = None
    relevance_score: Optional[float] = None
    categories: List[str] = Field(default_factory=list)
    network: Optional[str] = None
    tags: List[str] = Field(default_factory=list)

    class Config:
        validate_assignment = True
        extra = 'forbid' 