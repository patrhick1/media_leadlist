from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid

# Import the base lead model if we want to reuse fields
# from .lead import PodcastLead # Keep commented if not directly inheriting

class EpisodeInfo(BaseModel):
    """Standardized structure for a single episode's metadata."""
    episode_id: Optional[str] = None
    title: Optional[str] = None
    published_date: Optional[datetime] = None
    summary: Optional[str] = None
    duration_seconds: Optional[int] = None # Standardized duration
    audio_url: Optional[HttpUrl] = None
    link: Optional[HttpUrl] = None

class SocialProfileInfo(BaseModel):
    """Standardized structure for social media profile data."""
    platform: str # e.g., 'linkedin', 'twitter'
    profile_url: Optional[HttpUrl] = None
    handle: Optional[str] = None
    followers: Optional[int] = None
    # Add other metrics like engagement rate, posting frequency if available
    average_engagement: Optional[float] = None # Example engagement metric

class EnrichedPodcastProfile(BaseModel):
    """A standardized model holding combined data from discovery and enrichment."""
    # --- Unique Identifier (Using API ID from search step) ---
    unified_profile_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Internal unique ID for this enriched profile")
    source_api: Optional[str] = None # e.g., 'listennotes', 'podscan', 'combined'
    api_id: Optional[str] = None # ID from the source API
    
    # --- Core Podcast Info --- #
    # Inherited/copied from the initial unified search result
    title: Optional[str] = None
    description: Optional[str] = None
    image_url: Optional[HttpUrl] = None
    website: Optional[HttpUrl] = None # Primary website (might be from RSS or API)
    language: Optional[str] = None
    podcast_spotify_id: Optional[str] = None
    itunes_id: Optional[int] = None
    total_episodes: Optional[int] = None
    last_posted_at: Optional[datetime] = None # From Podscan or RSS
    
    # --- Host Information --- #
    host_names: Optional[List[str]] = None # Discovered via Gemini

    # --- RSS Feed Enrichment --- #
    rss_feed_url: Optional[HttpUrl] = None
    # podcast_link: Optional[HttpUrl] = None # Redundant? Website field preferred?
    rss_owner_name: Optional[str] = None # From <itunes:owner> name
    rss_owner_email: Optional[str] = None # From <itunes:owner> email
    rss_explicit: Optional[bool] = None # From <itunes:explicit>
    rss_categories: Optional[List[str]] = None # Specific categories from RSS
    # episode_count: Optional[int] = None # Use total_episodes from API? 
    latest_episode_date: Optional[datetime] = None
    first_episode_date: Optional[datetime] = None
    average_duration_seconds: Optional[float] = None
    publishing_frequency_days: Optional[float] = None # Estimated days between episodes
    recent_episodes: Optional[List[EpisodeInfo]] = Field(None, max_items=10) 

    # --- Specific Social URLs (Discovered via Gemini/RSS/API) --- #
    # Podcast URLs
    podcast_twitter_url: Optional[HttpUrl] = None
    podcast_linkedin_url: Optional[HttpUrl] = None # Company page?
    podcast_instagram_url: Optional[HttpUrl] = None
    podcast_facebook_url: Optional[HttpUrl] = None
    podcast_youtube_url: Optional[HttpUrl] = None
    podcast_tiktok_url: Optional[HttpUrl] = None
    podcast_other_social_url: Optional[HttpUrl] = None # Catch-all
    # Host URLs (Assume single primary host for now, could expand later)
    host_twitter_url: Optional[HttpUrl] = None 
    host_linkedin_url: Optional[HttpUrl] = None
    # Add host Instagram etc. if needed

    # --- Contact Information --- #
    primary_email: Optional[str] = None # Best email found (owner > API email > other)

    # --- Metrics & Engagement (From APIs & Apify/SocialDiscovery) --- #
    listen_score: Optional[int] = None # From ListenNotes
    listen_score_global_rank: Optional[str] = None # From ListenNotes
    audience_size: Optional[int] = None # From Podscan reach
    itunes_rating_average: Optional[float] = None
    itunes_rating_count: Optional[int] = None
    spotify_rating_average: Optional[float] = None
    spotify_rating_count: Optional[int] = None
    # Specific follower counts (from Apify scraping)
    twitter_followers: Optional[int] = None 
    linkedin_connections: Optional[int] = None # Or followers depending on actor
    # --- NEW: Add fields for following count and verified status --- #
    twitter_following: Optional[int] = None 
    is_twitter_verified: Optional[bool] = None
    # --- END NEW --- #
    # Add other platforms (Instagram followers, etc.)
    instagram_followers: Optional[int] = None # New
    tiktok_followers: Optional[int] = None    # New
    facebook_likes: Optional[int] = None      # New (or facebook_followers)
    youtube_subscribers: Optional[int] = None # New
    # Engagement metrics
    twitter_average_engagement: Optional[float] = None # Example
    # Add other platforms...
    
    # Keep the detailed list from SocialProfileInfo if needed for source tracing
    social_profiles_details: Optional[List[SocialProfileInfo]] = None 

    # --- Vetting Info (Populated by VettingAgent) --- #
    # Removed relevance_score, audience_demographics, contact_details from core info
    # Add vetting-specific fields here if needed later, or let VettingAgent update its own state.
    # Example: quality_tier: Optional[str] = None

    # --- Metadata --- #
    last_enriched_timestamp: datetime = Field(default_factory=datetime.utcnow)
    data_sources: List[str] = Field(default_factory=list, description="List of sources used for enrichment (e.g., 'rss', 'gemini_host', 'apify_twitter')")

    class Config:
        validate_assignment = True
        # extra = 'ignore' # Ignore extra fields during parsing if needed 