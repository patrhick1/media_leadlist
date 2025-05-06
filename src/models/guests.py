from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date

class Guest(BaseModel):
    """Represents a podcast guest entity."""
    guest_id: str = Field(..., description="Unique identifier for the guest.")
    name: str = Field(..., description="Primary name of the guest.")
    aliases: List[str] = Field(default_factory=list, description="Alternative names or known aliases.")
    popularity_score: Optional[float] = Field(None, description="Calculated score indicating guest popularity/reach.")
    # Potentially add fields like: primary_topic, social_media_links, etc.

    class Config:
        validate_assignment = True
        # extra = 'allow' # If we want to add more details later flexibility

class GuestAppearance(BaseModel):
    """Represents an instance of a guest appearing on a podcast episode."""
    appearance_id: str = Field(..., description="Unique identifier for this specific appearance.")
    guest_id: str = Field(..., description="ID of the guest who appeared.")
    podcast_id: str = Field(..., description="ID of the podcast where the appearance occurred.")
    episode_id: Optional[str] = Field(None, description="ID of the specific episode, if known.")
    episode_title: Optional[str] = Field(None, description="Title of the episode, if known.")
    appearance_date: Optional[date] = Field(None, description="Date of the episode publication or appearance.")
    prominence_score: Optional[float] = Field(None, description="Score indicating how prominently the guest featured (e.g., based on description mentions, duration). Placeholder for now.")
    episode_position: Optional[int] = Field(None, description="Position of the guest in the episode (e.g., 1st guest, 2nd guest), if known.")
    # Potentially add: episode_link, appearance_notes

    class Config:
        validate_assignment = True
        # extra = 'allow' 