import logging
from typing import List, Dict, Any

from .validation import LeadValidator
from .csv_exporter import CsvExporter
from .models import ExportLog

# Configure logging if not already configured by a higher level module
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

class ExportService:
    """
    Orchestrates the validation and CSV export process for podcast leads.
    """

    def __init__(self):
        # Initialize validator and load mapping config once
        self.validator = LeadValidator()
        # Initialize exporter, passing the config loaded by the validator
        self.exporter = CsvExporter(self.validator.mapping_config)
        log.info("ExportService initialized.")

    def process_and_export_leads(self, raw_leads: List[Dict[str, Any]], output_dir: str = "exports") -> ExportLog:
        """
        Validates a list of raw leads, exports the valid ones to CSV,
        and returns a log summarizing the operation.

        Args:
            raw_leads: A list of dictionaries, each representing a raw podcast lead.
            output_dir: The directory to save the CSV export file.

        Returns:
            An ExportLog object detailing the outcome of the export attempt.
        """
        total_processed = len(raw_leads)
        valid_leads_data = []
        all_validation_errors: Dict[str, List[str]] = {}
        log_status = "" # Will be set later
        output_file = None
        system_error_msg = None

        log.info(f"Starting export process for {total_processed} leads.")

        # 1. Validate leads and collect errors/processed data
        for i, lead in enumerate(raw_leads):
            lead_identifier = lead.get('name', f'Lead Index {i}') # Use name or index as identifier
            log.debug(f"Validating lead: {lead_identifier}")
            try:
                is_valid, errors, processed_data = self.validator.validate_lead(lead)
                if is_valid and processed_data:
                    valid_leads_data.append(processed_data)
                elif errors:
                    all_validation_errors[lead_identifier] = errors
            except Exception as e:
                log.error(f"Unexpected validation error for lead {lead_identifier}: {e}", exc_info=True)
                all_validation_errors[lead_identifier] = [f"Internal validation error: {e}"]

        exported_count = len(valid_leads_data)
        log.info(f"Validation complete. {exported_count} valid leads out of {total_processed}.")

        # 2. Export valid leads if any
        if exported_count > 0:
            try:
                log.info(f"Attempting to export {exported_count} valid leads to CSV.")
                output_file = self.exporter.export_to_file(valid_leads_data, output_dir=output_dir)
                log.info(f"CSV export successful. File saved to: {output_file}")
                # Determine status based on validation results
                if exported_count == total_processed:
                    log_status = "SUCCESS"
                else:
                    log_status = "PARTIAL_SUCCESS"
            except Exception as e:
                log.error(f"CSV export failed: {e}", exc_info=True)
                log_status = "SYSTEM_FAILURE" # Failure during export itself
                system_error_msg = f"CSV export failed: {e}"
                # Keep any validation errors collected earlier
        else:
            # No leads were valid to export
            log.info("No valid leads to export.")
            if total_processed > 0:
                log_status = "VALIDATION_FAILURE" # All leads failed validation
            else:
                log_status = "NO_LEADS_PROVIDED"

        # 3. Create and return the log
        export_log = ExportLog(
            status=log_status,
            total_leads_processed=total_processed,
            leads_exported_count=exported_count,
            output_file_path=output_file,
            validation_errors=all_validation_errors if all_validation_errors else None,
            system_error=system_error_msg
        )

        log.info(f"Export process finished with status: {log_status}")
        return export_log

# Example Usage (Illustrative)
# if __name__ == '__main__':
#     # Assume raw_leads is a list of dicts loaded from somewhere
#     sample_raw_leads = [
#         {
#          "name": "Valid Lead 1", "podcast_link": "https://good.com", "must_exist": "y",
#          "email": "good@example.com", "episode_count": 10
#         },
#         {
#          "name": "Invalid Lead 1", "podcast_link": "https://bad.com" # Missing must_exist
#         },
#         {
#          "name": "Invalid Lead 2", "podcast_link": "https://bad2.com", "must_exist": "y",
#          "email": "bad-email"
#         },
#          {
#          "name": "Valid Lead 2", "podcast_link": "https://good2.com", "must_exist": "y",
#          "latest_episode_date": "2024-03-15", "categories": ["Science"]
#         }
#     ]

#     export_service = ExportService()
#     result_log = export_service.process_and_export_leads(sample_raw_leads, output_dir="temp_export_logs")

#     print("\n--- Export Log --- ")
#     # Pydantic v2 uses model_dump_json
#     try:
#         print(result_log.model_dump_json(indent=2))
#     except AttributeError:
#         # Fallback for Pydantic v1
#         print(result_log.json(indent=2)) 