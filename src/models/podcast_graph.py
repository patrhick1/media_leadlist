from pydantic import BaseModel, Field
from typing import List, Dict

class RelatedPodcast(BaseModel):
    """Represents a podcast related to a target podcast, found via neighborhood analysis."""
    podcast_id: str = Field(..., description="The ID of the related podcast.")
    score: float = Field(..., description="Combined similarity score (0.0-1.0) based on weighted factors.")
    relationship_types: List[str] = Field(..., description="Types of relationships found (e.g., 'category', 'network', 'topic').")
    common_elements: Dict[str, List[str]] = Field(default_factory=dict, description="Specific elements shared (e.g., {'categories': ['Tech', 'AI']}).") 