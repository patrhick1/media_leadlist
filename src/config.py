"""Centralized configuration for the Podcast Vetting System.

This module reads environment variables (optionally from a .env file) using
Pydantic's `BaseSettings`.  Values can then be accessed via the `settings`
instance *or* the convenience constants exported at the bottom for
back-compatibility with existing code that performs `from ..config import
MONGO_URI, DB_NAME, CHECKPOINTS_COLLECTION`.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Optional

from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

# Load variables in the current working directory from a .env file *before*
# Pydantic reads the environment.  If the .env file is missing, this is a
# no-op.
# load_dotenv(find_dotenv()) # Explicitly find .env


class Settings(BaseSettings):
    """Application configuration pulled from environment variables."""

    # --- MongoDB ------------------------------------------------------------
    # MONGO_URI: str = Field(..., env="MONGO_DB_URI")
    # DB_NAME: str = Field("podcast_vetting_db", env="MONGO_DB_NAME")
    # CHECKPOINTS_COLLECTION: str = Field("checkpoints", env="CHECKPOINTS_COLLECTION")
    
    PGL_FRONTEND_PASSWORD: str = Field(default="DEFAULT_PGL_PASSWORD_SHOULD_BE_OVERRIDDEN_BY_ENV", env="PGL_FRONTEND_PASSWORD")
    # --- External API Keys --------------------------------------------------
    ATTIO_API_KEY: Optional[str] = None
    LISTENNOTES_API_KEY: Optional[str] = None
    PODSCAN_API_KEY: Optional[str] = None
    APIFY_API_KEY: Optional[str] = None

    # --- Neighborhood-mapping parameters ------------------------------------
    NEIGHBORHOOD_SIZE: int = Field(10, description="Default number of neighbors to return")
    NEIGHBORHOOD_CACHE_SIZE: int = Field(128, description="Size of internal TTL cache for neighborhood lookups")
    NEIGHBORHOOD_WEIGHT_CATEGORY: float = Field(0.4, description="Weight for category match in neighborhood similarity")
    NEIGHBORHOOD_WEIGHT_NETWORK: float = Field(0.2, description="Weight for network match in neighborhood similarity")
    NEIGHBORHOOD_WEIGHT_TOPIC: float = Field(0.4, description="Weight for topic match in neighborhood similarity")

    # --- Misc ---------------------------------------------------------------
    # Pydantic v2 settings configuration
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Ignore unknown env vars rather than error
    )

    # --- JWT Settings for Session Management ---
    JWT_SECRET_KEY: str = Field(default="super-secret-key-please-change-in-env", env="JWT_SECRET_KEY") # IMPORTANT: Override in .env or Replit Secrets
    JWT_ALGORITHM: str = Field(default="HS256", env="JWT_ALGORITHM")
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=30, env="JWT_ACCESS_TOKEN_EXPIRE_MINUTES")


def get_settings() -> Settings:  # pragma: no cover
    """Return a cached *singleton* Settings instance.

    Using `lru_cache()` ensures we only parse environment variables once per
    process, which is both efficient and guarantees that every part of the app
    sees identical configuration values.
    """
    # load_dotenv(find_dotenv()) # Explicitly load .env just before Settings instantiation
    return Settings()  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Removed Convenience exports (Use Dependency Injection instead)
# ---------------------------------------------------------------------------
# settings = get_settings() # Removed global instance
# Removed constant exports - get values from Settings instance via DI

__all__ = [
    "Settings",
    "get_settings",
    # Constants
    # "MONGO_URI",
    # "DB_NAME",
    # "CHECKPOINTS_COLLECTION",
    "ATTIO_API_KEY",
    "LISTENNOTES_API_KEY",
    "PODSCAN_API_KEY",
    "APIFY_API_KEY",
    "NEIGHBORHOOD_SIZE",
    "NEIGHBORHOOD_CACHE_SIZE",
    "NEIGHBORHOOD_WEIGHT_CATEGORY",
    "NEIGHBORHOOD_WEIGHT_NETWORK",
    "NEIGHBORHOOD_WEIGHT_TOPIC",
] 