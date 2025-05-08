from pydantic import BaseModel, Field
from typing import Optional

class LLMVettingOutput(BaseModel):
    match_score: Optional[float] = Field(None, ge=0, le=100, description="LLM's match score (0-100).")
    explanation: Optional[str] = Field(None, description="LLM's textual explanation for the score.")

    class Config:
        validate_assignment = True 