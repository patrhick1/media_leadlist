# src/utils/csv_utils.py
import csv
import logging
from typing import List, Dict, Any, TYPE_CHECKING
import os # Import os for path handling
import json # Import json for handling social links list

# Static type checkers (e.g., Pylance) need to resolve the forward reference
if TYPE_CHECKING:
    from ..models.podcast_profile import EnrichedPodcastProfile  # pragma: no cover
    from ..models.vetting import VettingResult # Added VettingResult here

logger = logging.getLogger(__name__)

# Define the list of headers in the desired order for the CSV
# This ensures consistency even if dictionary keys change order
CSV_HEADERS = [
    "source_api", "api_id", "title", "description", "rss_url", "website", "email",
    "itunes_id", "latest_episode_id", "latest_pub_date_ms", "earliest_pub_date_ms",
    "total_episodes", "update_frequency_hours", "listen_score", "listen_score_global_rank",
    "podcast_spotify_id", "audience_size", "itunes_rating_average", "itunes_rating_count",
    "spotify_rating_average", "spotify_rating_count", "last_posted_at",
    "image_url",
    "instagram_url",
    "twitter_url",
    "linkedin_url",
    "tiktok_url",
    "youtube_url",
    "facebook_url",
    "other_social_url"
]

# --- New: Headers and writer for enriched profiles ---
ENRICHED_CSV_HEADERS = [
    # Core identification
    'unified_profile_id', 'source_api', 'api_id',
    'title', 'description', 'image_url', 'website', 'language',
    'rss_feed_url', 'total_episodes', 'first_episode_date', 'latest_episode_date',
    'average_duration_seconds', 'publishing_frequency_days',
    # Host and contact
    'host_names', 'rss_owner_name', 'rss_owner_email', 'primary_email',
    # Social URLs
    'podcast_twitter_url', 'podcast_linkedin_url', 'podcast_instagram_url',
    'podcast_facebook_url', 'podcast_youtube_url', 'podcast_tiktok_url',
    'podcast_other_social_url', 'host_twitter_url', 'host_linkedin_url',
    # Metrics
    'listen_score', 'listen_score_global_rank', 'audience_size',
    'itunes_rating_average', 'itunes_rating_count', 'spotify_rating_average',
    'spotify_rating_count', 'twitter_followers', 'linkedin_connections',
    # Metadata
    'data_sources', 'last_enriched_timestamp'
]

# --- New: Headers and writer for vetting results ---
VETTING_CSV_HEADERS = [
    'podcast_id', 'quality_tier', 'composite_score',
    'programmatic_consistency_passed', 'programmatic_consistency_reason',
    'llm_match_score', 'llm_match_explanation',
    'final_explanation', 'days_since_last_episode', 'average_frequency_days',
    'last_episode_date', 'error', 'metric_scores' # Added metric_scores
]

def write_dicts_to_csv(data: List[Dict[str, Any]], filename: str):
    """Writes a list of unified dictionaries to a CSV file with predefined headers."""
    if not data:
        logger.warning(f"No data provided to write to CSV file: {filename}")
        return

    # Ensure the directory exists
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
    except Exception as e:
        logger.error(f"Could not create directory for CSV {filename}: {e}")
        return
        
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Use the predefined CSV_HEADERS
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS, extrasaction='ignore') # ignore extra fields if any
            writer.writeheader()
            writer.writerows(data)
        logger.info(f"Successfully wrote {len(data)} records to {filename}")
    except IOError as e:
        logger.error(f"Error writing to CSV file {filename}: {e}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred while writing to CSV {filename}: {e}")

def _serialize_value(key: str, value: Any) -> Any:
    """Helper to convert complex values (lists, dicts, datetime, ms timestamps) to CSV-safe string."""
    from datetime import datetime as _dt, timezone as _tz 
    
    if value is None:
        return None
        
    # Check if key suggests it's a millisecond timestamp and value is numeric
    if key.endswith('_ms') and isinstance(value, (int, float)):
        try:
            # Convert milliseconds to seconds
            timestamp_sec = value / 1000.0
            # Create datetime object in UTC
            dt_object = _dt.fromtimestamp(timestamp_sec, _tz.utc)
            return dt_object.isoformat() # Return ISO format string
        except (ValueError, OSError, TypeError) as e:
            logger.warning(f"Could not convert millisecond timestamp for key '{key}' (value: {value}): {e}")
            return value # Return original value if conversion fails
            
    if isinstance(value, list):
        return '; '.join(str(v) for v in value)
    if isinstance(value, dict):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    if isinstance(value, _dt):
        return value.isoformat()
        
    return value

def write_enriched_profiles_to_csv(profiles: List['EnrichedPodcastProfile'], filename: str):
    """Writes all enriched profile fields to CSV.

    The header list is generated dynamically by:
    1. Taking the field order from `EnrichedPodcastProfile` model.
    2. Adding any extra keys that appear in the dumped rows.
    This guarantees the CSV will always include every enriched field.
    """

    if not profiles:
        logger.warning(f"No enriched profiles provided to write to CSV file: {filename}")
        return

    # Lazy import here to avoid circular dependency issues
    from ..models.podcast_profile import EnrichedPodcastProfile  # type: ignore

    # Collect rows and union of keys
    dict_rows: List[Dict[str, Any]] = []
    all_keys: set[str] = set()

    for profile in profiles:
        if not isinstance(profile, EnrichedPodcastProfile):
            logger.warning(f"Skipping non-EnrichedPodcastProfile object: {profile}")
            continue

        raw_row = profile.model_dump(mode='python', exclude_none=False)  # Keep field order

        # Serialize complex types for CSV
        serialized_row: Dict[str, Any] = {}
        for key, val in raw_row.items():
            serialized_row[key] = _serialize_value(key, val)

        dict_rows.append(serialized_row)
        all_keys.update(serialized_row.keys())

    if not dict_rows:
        logger.warning("No valid enriched profiles to write.")
        return

    # Build header list preserving model field order first
    model_field_order = list(EnrichedPodcastProfile.model_fields.keys())
    header_order: List[str] = [f for f in model_field_order if f in all_keys]
    # Append any extra keys discovered that are not in the model order
    header_order.extend([k for k in sorted(all_keys) if k not in header_order])

    # Ensure directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header_order, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(dict_rows)
        logger.info(f"Successfully wrote {len(dict_rows)} enriched profiles to {filename} with {len(header_order)} columns")
    except IOError as e:
        logger.error(f"Error writing enriched CSV {filename}: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error writing enriched CSV {filename}: {e}")

def write_vetting_results_to_csv(results: List['VettingResult'], filename: str):
    """Writes all vetting result fields to CSV.

    The header list is generated dynamically by:
    1. Taking the field order from `VettingResult` model.
    2. Adding any extra keys that appear in the dumped rows.
    This guarantees the CSV will always include every vetting field.
    """

    if not results:
        logger.warning(f"No vetting results provided to write to CSV file: {filename}")
        return

    # Lazy import here to avoid circular dependency issues if VettingResult is used elsewhere
    from ..models.vetting import VettingResult # type: ignore

    dict_rows: List[Dict[str, Any]] = []
    all_keys: set[str] = set()

    for result in results:
        if not isinstance(result, VettingResult):
            logger.warning(f"Skipping non-VettingResult object: {result}")
            continue

        # Dump the model, ensuring metric_scores (a dict) is handled by _serialize_value
        raw_row = result.model_dump(mode='python', exclude_none=False)

        serialized_row: Dict[str, Any] = {}
        for key, val in raw_row.items():
            serialized_row[key] = _serialize_value(key, val) # _serialize_value handles dicts via json.dumps
        
        dict_rows.append(serialized_row)
        all_keys.update(serialized_row.keys())

    if not dict_rows:
        logger.warning("No valid vetting results to write.")
        return

    # Build header list preserving model field order first
    model_field_order = list(VettingResult.model_fields.keys())
    header_order: List[str] = [f for f in model_field_order if f in all_keys]
    header_order.extend([k for k in sorted(all_keys) if k not in header_order])

    # Ensure directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header_order, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(dict_rows)
        logger.info(f"Successfully wrote {len(dict_rows)} vetting results to {filename} with {len(header_order)} columns")
    except IOError as e:
        logger.error(f"Error writing vetting CSV {filename}: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error writing vetting CSV {filename}: {e}")

# Example usage (optional)
# if __name__ == '__main__':
#     logging.basicConfig(level=logging.INFO)
#     dummy_data = [
#         {'source_api': 'listennotes', 'api_id': '123', 'title': 'Podcast A', 'email': 'a@example.com', 'listen_score': 80, 'extra_field': 'abc'},
#         {'source_api': 'podscan', 'api_id': '456', 'title': 'Podcast B', 'website': 'b.com', 'description': 'Desc B'}
#     ]
#     output_filename = "test_output.csv"
#     write_dicts_to_csv(dummy_data, output_filename)
#     print(f"Check {output_filename}") 