from pydantic import BaseModel, Field
from typing import Dict, Optional

class VettingResult(BaseModel):
    """Represents the result of a podcast vetting assessment."""
    podcast_id: str
    composite_score: float = Field(..., description="Overall quality score (0-100)")
    quality_tier: str = Field(..., description="Assigned quality tier (e.g., A, B, C, D)")
    explanation: str = Field(..., description="Human-readable explanation of the assessment")
    metric_scores: Dict[str, float] = Field(..., description="Individual scores for each quality metric")
    error: Optional[str] = Field(None, description="Any error message if vetting failed")

    class Config:
        validate_assignment = True
        extra = 'forbid' 