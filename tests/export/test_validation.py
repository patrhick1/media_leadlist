import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

# Import functions and class from the module to be tested
from src.export.validation import (
    is_valid_url,
    is_valid_email,
    extract_domain,
    format_date_attio,
    join_list,
    extract_linkedin_company_url,
    LeadValidator,
    load_mapping_config # Import to patch it
)

# Mock Attio mapping configuration
MOCK_ATTIO_CONFIG = {
  "description": "Mock Mapping",
  "version": 1,
  "target_object": "Company",
  "mappings": [
    {
      "source_field": "name",
      "attio_field": "Company Name",
      "required": True,
      "type": "text",
      "notes": "Direct mapping."
    },
    {
      "source_field": "podcast_link",
      "attio_field": "Domain",
      "required": True,
      "type": "text", # Storing domain as text
      "notes": "Requires transformation: Extract domain from podcast_link URL."
    },
     {
      "source_field": "image_url",
      "attio_field": "Logo URL",
      "required": False,
      "type": "url",
      "notes": "Mapping podcast image URL."
    },
    {
      "source_field": "categories",
      "attio_field": "Industry Tags",
      "required": False,
      "type": "multi-select", # Stored as text after join
      "notes": "Requires transformation: Join list with ','."
    },
     {
      "source_field": "latest_episode_date",
      "attio_field": "Last Activity Date",
      "required": False,
      "type": "date",
      "notes": "Requires transformation: Format date to Attio's requirement (YYYY-MM-DD)."
    },
    {
      "source_field": "episode_count",
      "attio_field": "Episode Count",
      "required": False,
      "type": "number",
      "notes": "Direct mapping."
    },
     {
      "source_field": "social_profiles",
      "attio_field": "LinkedIn Company URL",
      "required": False, # Making it optional for easier testing here
      "type": "url",
      "notes": "Requires transformation: Extract LinkedIn URL where platform='linkedin' and format matches 'linkedin.com/company/...'."
    },
     {
      "source_field": "email",
      "attio_field": "Contact Email",
      "required": False,
      "type": "email",
      "notes": "Direct mapping."
    },
    { # Example of required field check
      "source_field": "must_exist",
      "attio_field": "Must Exist Field",
      "required": True,
      "type": "text"
    }
  ]
}


class TestValidationHelpers(unittest.TestCase):

    def test_is_valid_url(self):
        self.assertTrue(is_valid_url("http://example.com"))
        self.assertTrue(is_valid_url("https://www.example.com/path?query=1"))
        self.assertFalse(is_valid_url("invalid-url"))
        self.assertFalse(is_valid_url("ftp://example.com")) # Assuming we only want http/https
        self.assertFalse(is_valid_url(None))
        self.assertFalse(is_valid_url(123))

    def test_is_valid_email(self):
        self.assertTrue(is_valid_email("test@example.com"))
        self.assertTrue(is_valid_email("test.name+alias@example.co.uk"))
        self.assertFalse(is_valid_email("invalid-email"))
        self.assertFalse(is_valid_email("test@example"))
        self.assertFalse(is_valid_email("test@.com"))
        self.assertFalse(is_valid_email(None))
        self.assertFalse(is_valid_email(123))

    def test_extract_domain(self):
        self.assertEqual(extract_domain("https://www.example.com/path"), "www.example.com")
        self.assertEqual(extract_domain("http://example.com?query=1"), "example.com")
        self.assertIsNone(extract_domain("invalid-url"))
        self.assertIsNone(extract_domain(""))

    def test_format_date_attio(self):
        self.assertEqual(format_date_attio("2024-01-15"), "2024-01-15")
        self.assertEqual(format_date_attio("2024/03/20"), "2024-03-20")
        self.assertEqual(format_date_attio("12/31/2023"), "2023-12-31")
        self.assertEqual(format_date_attio("15-Jan-2024"), "2024-01-15")
        self.assertEqual(format_date_attio(datetime(2023, 5, 1)), "2023-05-01")
        self.assertIsNone(format_date_attio("invalid-date"))
        self.assertIsNone(format_date_attio("2024-13-01")) # Invalid month
        self.assertIsNone(format_date_attio(None))
        self.assertIsNone(format_date_attio(12345))

    def test_join_list(self):
        self.assertEqual(join_list(["Tech", "Business"]), "Tech, Business")
        self.assertEqual(join_list([1, "Two", 3.0]), "1, Two, 3.0")
        self.assertEqual(join_list([]), "")
        self.assertIsNone(join_list("not a list"))
        self.assertIsNone(join_list(None))

    def test_extract_linkedin_company_url(self):
        profiles_ok = [
            {"platform": "twitter", "url": "https://twitter.com/example"},
            {"platform": "linkedin", "url": "https://linkedin.com/company/exampleinc"}
        ]
        profiles_no_linkedin = [
            {"platform": "twitter", "url": "https://twitter.com/example"}
        ]
        profiles_invalid_url = [
            {"platform": "linkedin", "url": "invalid-linkedin-url"}
        ]
        profiles_personal = [
             {"platform": "linkedin", "url": "https://linkedin.com/in/person"}
        ]
        self.assertEqual(extract_linkedin_company_url(profiles_ok), "https://linkedin.com/company/exampleinc")
        self.assertIsNone(extract_linkedin_company_url(profiles_no_linkedin))
        self.assertIsNone(extract_linkedin_company_url(profiles_invalid_url))
        self.assertIsNone(extract_linkedin_company_url(profiles_personal))
        self.assertIsNone(extract_linkedin_company_url("not a list"))
        self.assertIsNone(extract_linkedin_company_url(None))
        self.assertIsNone(extract_linkedin_company_url([]))


# Remove the class-level patch decorator
# @patch('src.export.validation.load_mapping_config', return_value=MOCK_ATTIO_CONFIG)
class TestLeadValidator(unittest.TestCase):

    # Patch within setUp
    def setUp(self):
        # Start the patcher
        self.patcher = patch('src.export.validation.load_mapping_config', return_value=MOCK_ATTIO_CONFIG)
        self.mock_load_config = self.patcher.start()
        # Ensure the test instance uses the patched version
        self.validator = LeadValidator()
        # Add cleanup to stop the patcher after each test
        self.addCleanup(self.patcher.stop)

    # Remove mock_load_config argument from test methods
    def test_valid_lead(self):
        lead_data = {
            "name": "Valid Podcast",
            "podcast_link": "https://example.com/podcast",
            "image_url": "https://example.com/logo.png",
            "categories": ["Tech", "News"],
            "latest_episode_date": "2024-01-01",
            "episode_count": 100,
            "social_profiles": [{"platform": "linkedin", "url": "https://linkedin.com/company/validco"}],
            "email": "valid@example.com",
            "must_exist": "I am here"
        }
        is_valid, errors, processed_data = self.validator.validate_lead(lead_data)
        self.assertTrue(is_valid, f"Errors found: {errors}") # Add message for clarity
        self.assertEqual(errors, [])
        self.assertIsNotNone(processed_data) # Check that data is returned on success
        self.mock_load_config.assert_called_once()

    def test_missing_required_field(self):
        lead_data = {
            "name": "Missing Required",
            "podcast_link": "https://example.com/podcast",
            # "must_exist" is missing
        }
        is_valid, errors, processed_data = self.validator.validate_lead(lead_data)
        self.assertFalse(is_valid)
        self.assertIn("Required field 'must_exist' (maps to 'Must Exist Field') is missing.", errors)
        self.assertIsNone(processed_data) # Check data is None on failure

    def test_invalid_url_type(self):
         lead_data = {
            "name": "Invalid URL",
            "podcast_link": "https://example.com/podcast",
            "image_url": "not-a-valid-url", # Invalid URL type
            "must_exist": "Present"
        }
         is_valid, errors, processed_data = self.validator.validate_lead(lead_data)
         self.assertFalse(is_valid)
         self.assertIn("Field 'image_url' (maps to 'Logo URL') is not a valid URL: not-a-valid-url", errors)
         self.assertIsNone(processed_data)

    def test_invalid_email_type(self):
        lead_data = {
            "name": "Invalid Email",
            "podcast_link": "https://example.com/podcast",
            "email": "invalid-email-format", # Invalid email type
            "must_exist": "Present"
        }
        is_valid, errors, processed_data = self.validator.validate_lead(lead_data)
        self.assertFalse(is_valid)
        self.assertIn("Field 'email' (maps to 'Contact Email') is not a valid email: invalid-email-format", errors)
        self.assertIsNone(processed_data)

    def test_invalid_number_type(self):
        lead_data = {
            "name": "Invalid Number",
            "podcast_link": "https://example.com/podcast",
            "episode_count": "not a number", # Invalid number type
            "must_exist": "Present"
        }
        is_valid, errors, processed_data = self.validator.validate_lead(lead_data)
        self.assertFalse(is_valid)
        self.assertIn("Field 'episode_count' (maps to 'Episode Count') must be a number: Got 'not a number'", errors)
        self.assertIsNone(processed_data)

    def test_invalid_date_type(self):
        lead_data = {
            "name": "Invalid Date",
            "podcast_link": "https://example.com/podcast",
            "latest_episode_date": "invalid-date-format", # Invalid date type
            "must_exist": "Present"
        }
        is_valid, errors, processed_data = self.validator.validate_lead(lead_data)
        self.assertFalse(is_valid)
        self.assertIn("Field 'latest_episode_date' (maps to 'Last Activity Date') has invalid date format: invalid-date-format", errors)
        self.assertIsNone(processed_data)

    def test_invalid_multi_select_type(self):
        lead_data = {
            "name": "Invalid MultiSelect",
            "podcast_link": "https://example.com/podcast",
            "categories": "not-a-list", # Invalid multi-select type
            "must_exist": "Present"
        }
        is_valid, errors, processed_data = self.validator.validate_lead(lead_data)
        self.assertFalse(is_valid)
        self.assertIn("Field 'categories' (maps to 'Industry Tags') should be a list for multi-select: Got type str", errors)
        self.assertIsNone(processed_data)

    def test_failed_domain_extraction_required(self):
         lead_data = {
            "name": "Bad Domain",
            "podcast_link": "invalid-url-for-domain", # Required field, invalid source for transform
            "must_exist": "Present"
        }
         is_valid, errors, processed_data = self.validator.validate_lead(lead_data)
         self.assertFalse(is_valid)
         # Because podcast_link -> Domain is required, *and* the source URL is invalid, the extraction fails
         self.assertIn("Could not extract domain from required field 'podcast_link': invalid-url-for-domain", errors)
         self.assertIsNone(processed_data)

    def test_multiple_errors(self):
         lead_data = {
            "name": "Multiple Issues",
            "podcast_link": "https://example.com/podcast",
            # must_exist is missing
            "email": "bad-email",
            "episode_count": "NaN" # This should now trigger a specific NaN error
         }
         is_valid, errors, processed_data = self.validator.validate_lead(lead_data)
         self.assertFalse(is_valid)
         # Check content of errors more carefully
         self.assertIn("Required field 'must_exist' (maps to 'Must Exist Field') is missing.", errors)
         self.assertIn("Field 'email' (maps to 'Contact Email') is not a valid email: bad-email", errors)
         # Expecting an error related to NaN conversion now
         self.assertTrue(any("NaN" in error and "episode_count" in error for error in errors),
                         f"Expected a NaN-related error for episode_count, got: {errors}")
         self.assertEqual(len(errors), 3, f"Expected 3 errors, got {len(errors)}: {errors}")
         self.assertIsNone(processed_data)


if __name__ == '__main__':
    unittest.main() 