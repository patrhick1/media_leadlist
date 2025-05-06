from pydantic import BaseModel, HttpUrl, Field
from typing import Optional, List
from urllib.parse import urlparse

class SocialUrl(BaseModel):
    """Structured representation of a social media URL discovered for a podcast/host."""
    platform: str  # e.g., "twitter", "linkedin"
    url: HttpUrl
    # Optional normalized hostname for quick access (e.g., "twitter.com")
    host: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        # Auto-populate host if missing
        if not self.host:
            parsed = urlparse(str(self.url))
            object.__setattr__(self, 'host', parsed.netloc.lower()) 

# --- NEW: Pydantic Model for Gemini Structured Output --- #
class GeminiPodcastEnrichment(BaseModel):
    """Pydantic model for structured data extraction using Gemini for podcast and host social profiles."""
    host_names: Optional[List[str]] = Field(None, description="List of host names for the podcast.")
    
    # Podcast social media URLs
    podcast_twitter_url: Optional[str] = Field(None, description="The official Twitter URL of the podcast.")
    podcast_linkedin_url: Optional[str] = Field(None, description="The official LinkedIn page URL of the podcast (less common).")
    podcast_instagram_url: Optional[str] = Field(None, description="The official Instagram URL of the podcast.")
    podcast_facebook_url: Optional[str] = Field(None, description="The official Facebook page URL of the podcast.")
    podcast_youtube_url: Optional[str] = Field(None, description="The official YouTube channel URL of the podcast.")
    podcast_tiktok_url: Optional[str] = Field(None, description="The official TikTok URL of the podcast.")
    
    # Host social media URLs (primary host if multiple)
    host_linkedin_url: Optional[str] = Field(None, description="The LinkedIn profile URL of the primary host.")
    host_twitter_url: Optional[str] = Field(None, description="The Twitter profile URL of the primary host.")
    # Add other host social platforms if needed, e.g., host_instagram_url

    class Config:
        extra = 'ignore' # Allow ignoring extra fields from LLM output if any
        validate_assignment = True # Ensure re-validation on attribute assignment if used 