import json
import logging
import re
from urllib.parse import urlparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define path to the mapping configuration
MAPPING_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "attio_mapping.json"

def load_mapping_config() -> Dict[str, Any]:
    """Loads the Attio field mapping configuration from the JSON file."""
    try:
        with open(MAPPING_CONFIG_PATH, 'r') as f:
            config = json.load(f)
        logging.info(f"Successfully loaded Attio mapping config from {MAPPING_CONFIG_PATH}")
        return config
    except FileNotFoundError:
        logging.error(f"Attio mapping config file not found at {MAPPING_CONFIG_PATH}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from {MAPPING_CONFIG_PATH}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading the mapping config: {e}")
        raise

# --- Helper Functions ---

def is_valid_url(url_string: Any) -> bool:
    """Checks if a string is a valid HTTP/HTTPS URL."""
    if not isinstance(url_string, str):
        return False
    try:
        result = urlparse(url_string)
        # Check for http/https scheme and network location
        return result.scheme in ['http', 'https'] and bool(result.netloc)
    except ValueError:
        return False

def is_valid_email(email_string: Any) -> bool:
    """Checks if a string is a valid email address format."""
    if not isinstance(email_string, str):
        return False
    # Corrected regex (removed extra backslash before dot)
    email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return re.match(email_regex, email_string) is not None

def extract_domain(url_string: str) -> Optional[str]:
    """Extracts the network location (domain) from a URL string."""
    if not is_valid_url(url_string):
        return None
    try:
        return urlparse(url_string).netloc
    except ValueError:
        return None

def format_date_attio(date_string: Any) -> Optional[str]:
    """Formats a date string into YYYY-MM-DD format. Handles various input formats."""
    if not isinstance(date_string, str):
        # Attempt conversion if it's a datetime object
        if isinstance(date_string, datetime):
            try:
                return date_string.strftime('%Y-%m-%d')
            except Exception:
                return None
        return None

    # Add more expected input formats if necessary
    possible_formats = ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d-%b-%Y", "%Y-%m-%dT%H:%M:%S.%fZ"]
    for fmt in possible_formats:
        try:
            return datetime.strptime(date_string, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None # Return None if no format matches

def join_list(items: Any) -> Optional[str]:
    """Joins list items into a comma-separated string."""
    if not isinstance(items, list):
        return None
    return ", ".join(str(item) for item in items)

def extract_linkedin_company_url(social_profiles: Any) -> Optional[str]:
    """Extracts the LinkedIn Company URL from a list of social profiles."""
    if not isinstance(social_profiles, list):
        return None
    for profile in social_profiles:
        if isinstance(profile, dict) and profile.get('platform') == 'linkedin':
            url = profile.get('url')
            # Basic check for company URL pattern - refine if needed
            if url and 'linkedin.com/company/' in url and is_valid_url(url):
                return url
    return None

# --- LeadValidator Class ---

class LeadValidator:
    """
    Validates and transforms podcast lead data based on the Attio mapping configuration.
    """
    def __init__(self):
        self.mapping_config = load_mapping_config()
        self.mappings = {item['source_field']: item for item in self.mapping_config.get('mappings', [])}

    def validate_lead(self, lead_data: Dict[str, Any]) -> Tuple[bool, List[str], Optional[Dict[str, Any]]]:
        """
        Validates a single lead against the Attio mapping requirements.

        Args:
            lead_data: A dictionary representing the podcast lead data.

        Returns:
            A tuple containing:
                - bool: True if the lead is valid, False otherwise.
                - List[str]: A list of validation errors found.
                - Optional[Dict[str, Any]]: Processed data mapped to Attio fields if valid, else None.
        """
        errors = []
        processed_data_for_export: Dict[str, Any] = {}
        logging.info(f"Starting validation for lead: {lead_data.get('name', 'N/A')}")

        for source_field, mapping_info in self.mappings.items():
            attio_field = mapping_info.get('attio_field')
            required = mapping_info.get('required', False)
            field_type = mapping_info.get('type')
            notes = mapping_info.get('notes', '')

            value = lead_data.get(source_field)
            transformed_value = value # Start with original value
            field_errors = [] # Collect errors specific to this field processing

            # 1. Check required fields
            if required and (value is None or value == ""):
                # Use field_errors here, append to main errors later if processing stops
                field_errors.append(f"Required field '{source_field}' (maps to '{attio_field}') is missing.")
                # Don't continue here, let type checking handle None if needed, but mark error

            # Process even if required field is missing initially to catch type errors if value is None but shouldn't be
            if value is None and not required:
                processed_data_for_export[attio_field] = None
                continue # Skip further validation/transformation for missing optional fields

            # 2. Type Validation & Transformation
            valid_type = True
            try:
                # Basic Type Checks
                if field_type == 'text':
                    if not isinstance(transformed_value, str):
                        try:
                            transformed_value = str(transformed_value)
                        except Exception:
                            field_errors.append(f"Field '{source_field}' could not be converted to text: Got type {type(value).__name__}")
                            valid_type = False
                elif field_type == 'number':
                    # Check for NaN explicitly before attempting conversion
                    if isinstance(transformed_value, float) and transformed_value != transformed_value: # Check for NaN
                        field_errors.append(f"Field '{source_field}' (maps to '{attio_field}') cannot be NaN.")
                        valid_type = False
                    elif not isinstance(transformed_value, (int, float)):
                        try:
                            # Try converting. If it becomes NaN, catch it.
                            converted_float = float(transformed_value)
                            if converted_float != converted_float: # Check again after conversion
                                field_errors.append(f"Field '{source_field}' (maps to '{attio_field}') resulted in NaN after conversion: Got '{value}'")
                                valid_type = False
                            else:
                                transformed_value = converted_float
                        except (ValueError, TypeError):
                             field_errors.append(f"Field '{source_field}' (maps to '{attio_field}') must be a number: Got '{value}'")
                             valid_type = False
                elif field_type == 'url':
                    # Special handling: if notes mention extraction, the source might be complex (e.g., list)
                    # The actual URL validation happens *after* potential extraction
                    if "Extract LinkedIn URL" not in notes:
                         if not is_valid_url(transformed_value):
                            field_errors.append(f"Field '{source_field}' (maps to '{attio_field}') is not a valid URL: {value}")
                            valid_type = False
                elif field_type == 'email':
                    if not is_valid_email(transformed_value):
                        field_errors.append(f"Field '{source_field}' (maps to '{attio_field}') is not a valid email: {value}")
                        valid_type = False
                elif field_type == 'date':
                    formatted_date = format_date_attio(transformed_value)
                    if formatted_date is None:
                        field_errors.append(f"Field '{source_field}' (maps to '{attio_field}') has invalid date format: {value}")
                        valid_type = False
                    else:
                         transformed_value = formatted_date # Use the formatted date
                elif field_type == 'multi-select':
                     # Type check happens first
                     if not isinstance(transformed_value, list):
                          field_errors.append(f"Field '{source_field}' (maps to '{attio_field}') should be a list for multi-select: Got type {type(value).__name__}")
                          valid_type = False
                     # Transformation applied later based on notes

                # Apply transformations based on notes only if the initial type was potentially valid
                # or if the transformation itself handles complex types (like list joining)
                if valid_type or field_type == 'multi-select': # Allow multi-select transformation even if list validation passed
                    if "Extract domain" in notes:
                        # Expects a string input for domain extraction
                        if isinstance(value, str):
                            extracted = extract_domain(value)
                            if extracted is None and required:
                                field_errors.append(f"Could not extract domain from required field '{source_field}': {value}")
                            transformed_value = extracted
                        elif required:
                            field_errors.append(f"Cannot extract domain from non-string required field '{source_field}': type {type(value).__name__}")
                            transformed_value = None # Can't extract, not required
                        else:
                            transformed_value = None # Can't extract, not required
                    elif "Join list with ','" in notes:
                        # Expects list input here
                        if isinstance(value, list):
                            joined = join_list(value)
                            # join_list handles empty list correctly, no error needed if None
                            transformed_value = joined
                        else:
                             # Error already added if not a list and type is multi-select
                             # If type wasn't multi-select but notes mention join, it's ambiguous - maybe log?
                             pass
                    elif "Extract LinkedIn URL where platform='linkedin'" in notes:
                         # Expects list input
                         if isinstance(value, list):
                            linkedin_url = extract_linkedin_company_url(value)
                            # Now validate the *extracted* URL if found
                            if linkedin_url and not is_valid_url(linkedin_url):
                                field_errors.append(f"Extracted LinkedIn URL for '{source_field}' is invalid: {linkedin_url}")
                                transformed_value = None # Don't store invalid extracted URL
                            elif required and linkedin_url is None:
                                field_errors.append(f"Required LinkedIn company URL not found or invalid in '{source_field}'.")
                                transformed_value = None
                            else:
                                transformed_value = linkedin_url
                         elif required:
                             field_errors.append(f"Cannot extract LinkedIn URL from non-list required field '{source_field}': type {type(value).__name__}")
                             transformed_value = None
                         else:
                            transformed_value = None # Can't extract, not required

                # Store the potentially transformed value for export if no critical errors occurred for this field
                if not field_errors and attio_field:
                     processed_data_for_export[attio_field] = transformed_value
                elif field_errors and attio_field:
                    # Ensure field exists in output even if invalid, maybe as None or original?
                    # Let's store None to indicate failure for this field clearly
                    processed_data_for_export[attio_field] = None

            except Exception as e:
                logging.error(f"Unexpected error validating/transforming field '{source_field}' for lead '{lead_data.get('name', 'N/A')}': {e}", exc_info=True)
                field_errors.append(f"Internal error processing field '{source_field}': {e}")
                if attio_field:
                     processed_data_for_export[attio_field] = None # Mark as None on unexpected error

            # Add field-specific errors to the main error list
            errors.extend(field_errors)

        is_valid = not errors
        if is_valid:
            logging.info(f"Validation successful for lead: {lead_data.get('name', 'N/A')}")
            return is_valid, errors, processed_data_for_export
        else:
            logging.warning(f"Validation failed for lead: {lead_data.get('name', 'N/A')}. Errors: {errors}")
            return is_valid, errors, None # Return None for processed data if invalid

# Example Usage (Optional - for testing)
# if __name__ == "__main__":
#     validator = LeadValidator()
#     test_lead_valid = {
#         "name": "Valid Podcast",
#         "podcast_link": "https://example.com/podcast",
#         "description": "A great show.",
#         "image_url": "https://example.com/logo.png",
#         "categories": ["Tech", "Business"],
#         "language": "English",
#         "episode_count": 150,
#         "latest_episode_date": "2024-01-15",
#         "average_duration_seconds": 1800,
#         "social_profiles": [{"platform": "linkedin", "url": "https://linkedin.com/company/example"}],
#         "email": "contact@example.com",
#         "author": "Host Name"
#     }
#     test_lead_invalid = {
#         "name": "Invalid Podcast",
#         "podcast_link": None, # Missing required domain source
#         "email": "not-an-email"
#     }

#     is_valid, errors, processed_data = validator.validate_lead(test_lead_valid)
#     print(f"Valid Lead - Is Valid: {is_valid}, Errors: {errors}")
#     print(f"Processed Data: {processed_data}")

#     is_valid, errors, processed_data = validator.validate_lead(test_lead_invalid)
#     print(f"Invalid Lead - Is Valid: {is_valid}, Errors: {errors}")
#     print(f"Processed Data: {processed_data}") 