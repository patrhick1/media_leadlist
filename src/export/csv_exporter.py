import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CsvExporter:
    """
    Handles the generation of CSV files from processed podcast lead data.
    """

    def __init__(self, mapping_config: Dict[str, Any]):
        """Initializes the exporter with the Attio mapping config."""
        self.mapping_config = mapping_config
        # Extract Attio field names in the order they appear in the config for the header
        self.attio_field_order = [item.get('attio_field') for item in self.mapping_config.get('mappings', []) if item.get('attio_field')]
        logging.info(f"CSV Exporter initialized with header order: {self.attio_field_order}")

    def generate_csv_string(self, processed_leads: List[Dict[str, Any]]) -> str:
        """
        Generates a CSV formatted string from a list of processed lead data.

        Args:
            processed_leads: A list of dictionaries, where each dictionary
                             represents a lead with keys as Attio field names
                             and values as the processed data.

        Returns:
            A string containing the CSV data, including the header row.
        """
        if not processed_leads:
            logging.warning("No processed leads provided for CSV generation.")
            return "" # Return empty string or maybe just the header?

        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)

        # Write Header
        writer.writerow(self.attio_field_order)
        logging.info(f"Writing CSV header: {self.attio_field_order}")

        # Write Rows
        row_count = 0
        for lead_data in processed_leads:
            # Ensure row values are in the same order as the header
            row = [lead_data.get(field_name) for field_name in self.attio_field_order]
            writer.writerow(row)
            row_count += 1

        logging.info(f"Successfully generated CSV string with {row_count} data rows.")
        return output.getvalue()

    def export_to_file(self, processed_leads: List[Dict[str, Any]], output_dir: str = "exports") -> str:
        """
        Generates a CSV string and saves it to a file with a timestamped name.

        Args:
            processed_leads: A list of processed lead dictionaries.
            output_dir: The directory where the CSV file will be saved.
                          Defaults to "exports" relative to the project root.

        Returns:
            The absolute path to the generated CSV file.
        """
        csv_string = self.generate_csv_string(processed_leads)
        if not csv_string:
            logging.error("CSV string generation failed, cannot export to file.")
            raise ValueError("No CSV data generated.")

        # Ensure output directory exists
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Create timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"attio_export_{timestamp}.csv"
        file_path = output_path / filename

        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                f.write(csv_string)
            logging.info(f"Successfully exported {len(processed_leads)} leads to {file_path.resolve()}")
            return str(file_path.resolve())
        except IOError as e:
            logging.error(f"Failed to write CSV file to {file_path}: {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred during file export: {e}")
            raise

# Example Usage (Requires LeadValidator and load_mapping_config from validation.py)
# if __name__ == '__main__':
#     from validation import LeadValidator, load_mapping_config

#     # Sample valid lead data (modify as needed)
#     test_lead_1 = {
#         "name": "Valid Podcast 1", "podcast_link": "https://example1.com", "must_exist": "Yes",
#         "categories": ["Tech"], "latest_episode_date": "2024-01-10", "episode_count": 50
#     }
#     test_lead_2 = {
#         "name": "Valid Podcast 2", "podcast_link": "https://example2.org", "must_exist": "Sure",
#         "image_url": "https://example2.org/logo.png", "email": "contact@example2.org"
#     }
#     test_lead_invalid = {"name": "Invalid Podcast"} # Missing required fields

#     validator = LeadValidator()
#     mapping_conf = validator.mapping_config # Reuse loaded config

#     valid_processed_leads = []
#     leads_to_process = [test_lead_1, test_lead_2, test_lead_invalid]

#     for lead in leads_to_process:
#         is_valid, errors, processed_data = validator.validate_lead(lead)
#         if is_valid and processed_data:
#             valid_processed_leads.append(processed_data)
#         else:
#             print(f"Skipping invalid lead '{lead.get('name')}': {errors}")

#     if valid_processed_leads:
#         exporter = CsvExporter(mapping_conf)

#         # Generate string
#         csv_output_string = exporter.generate_csv_string(valid_processed_leads)
#         print("\n--- Generated CSV String ---")
#         print(csv_output_string)

#         # Export to file
#         try:
#             filepath = exporter.export_to_file(valid_processed_leads, output_dir="temp_exports")
#             print(f"\n--- Exported to File ---")
#             print(f"File saved to: {filepath}")
#         except Exception as e:
#             print(f"Error exporting to file: {e}")
#     else:
#         print("\nNo valid leads to export.") 