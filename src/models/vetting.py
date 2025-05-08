from pydantic import BaseModel, Field
from typing import Dict, Optional, Literal
from datetime import datetime

class VettingResult(BaseModel):
    """Represents the result of a podcast vetting assessment."""
    podcast_id: str
    
    # Programmatic Consistency Checks
    programmatic_consistency_passed: bool = Field(..., description="Whether the podcast passed basic recency and frequency checks.")
    programmatic_consistency_reason: str = Field(..., description="Explanation for programmatic consistency result (e.g., 'Recent and frequent' or 'Last episode too old').")
    last_episode_date: Optional[datetime] = Field(None, description="Date of the last known episode.")
    days_since_last_episode: Optional[int] = Field(None, description="Days since the last episode was published.")
    average_frequency_days: Optional[float] = Field(None, description="Average number of days between episodes.")
    
    # LLM-based Match Vetting
    llm_match_score: Optional[float] = Field(None, ge=0, le=100, description="Score (0-100) from LLM assessing match with guest/ideal podcast criteria.")
    llm_match_explanation: Optional[str] = Field(None, description="LLM's explanation for the match score.")
    
    # Overall Assessment
    composite_score: float = Field(..., ge=0, le=100, description="Overall quality and match score (0-100).")
    quality_tier: Literal["A", "B", "C", "D", "Unvetted"] = Field(..., description="Assigned quality and match tier (e.g., A, B, C, D, or Unvetted if only partial).")
    final_explanation: str = Field(..., description="Comprehensive human-readable explanation of the overall assessment, combining programmatic and LLM insights.")
    
    # Retain metric_scores for detailed programmatic aspects if needed, or for other non-consistency programmatic checks in future
    metric_scores: Dict[str, float] = Field(default_factory=dict, description="Individual scores for various quality metrics, including programmatic consistency components.")
    error: Optional[str] = Field(None, description="Any error message if vetting failed for this specific podcast.")

    class Config:
        validate_assignment = True
        extra = 'forbid' 