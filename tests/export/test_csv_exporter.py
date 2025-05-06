import unittest
import csv
import io
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

from src.export.csv_exporter import CsvExporter

# Use a similar mock config as in validation tests, focus on header fields
MOCK_ATTIO_CONFIG_FOR_EXPORT = {
  "mappings": [
    {"attio_field": "Company Name"},
    {"attio_field": "Domain"},
    {"attio_field": "Logo URL"},
    {"attio_field": "Industry Tags"},
    {"attio_field": "Last Activity Date"},
    {"attio_field": "Episode Count"},
    {"attio_field": "LinkedIn Company URL"},
    {"attio_field": "Contact Email"},
    {"attio_field": "Must Exist Field"} # Keep all potential headers
  ]
}

# Expected Header based on mock config order
EXPECTED_HEADER = "Company Name,Domain,Logo URL,Industry Tags,Last Activity Date,Episode Count,LinkedIn Company URL,Contact Email,Must Exist Field\r\n"

class TestCsvExporter(unittest.TestCase):

    def setUp(self):
        self.exporter = CsvExporter(MOCK_ATTIO_CONFIG_FOR_EXPORT)
        self.processed_lead_1 = {
            "Company Name": "Podcast One",
            "Domain": "example1.com",
            "Logo URL": "http://logo.co/1.png",
            "Industry Tags": "Tech, News",
            "Last Activity Date": "2024-01-10",
            "Episode Count": 50,
            "LinkedIn Company URL": None,
            "Contact Email": "one@example.com",
            "Must Exist Field": "Yes"
        }
        self.processed_lead_2 = {
            "Company Name": "Podcast Two",
            "Domain": "example2.org",
            "Logo URL": None,
            "Industry Tags": "Business",
            "Last Activity Date": "2023-12-20",
            "Episode Count": 150.5,
            "LinkedIn Company URL": "http://linkedin.com/company/p2",
            "Contact Email": "two@example.org",
            "Must Exist Field": "Indeed"
        }
        self.processed_leads = [self.processed_lead_1, self.processed_lead_2]

    def test_init_header_order(self):
        expected_order = [
            "Company Name", "Domain", "Logo URL", "Industry Tags",
            "Last Activity Date", "Episode Count", "LinkedIn Company URL",
            "Contact Email", "Must Exist Field"
        ]
        self.assertEqual(self.exporter.attio_field_order, expected_order)

    def test_generate_csv_string_empty(self):
        csv_string = self.exporter.generate_csv_string([])
        self.assertEqual(csv_string, "")

    def test_generate_csv_string_success(self):
        csv_string = self.exporter.generate_csv_string(self.processed_leads)

        # Verify header
        self.assertTrue(csv_string.startswith(EXPECTED_HEADER))

        # Verify content using csv reader
        reader = csv.reader(io.StringIO(csv_string))
        header = next(reader)
        self.assertEqual(header, self.exporter.attio_field_order)

        row1 = next(reader)
        self.assertEqual(row1, [
            "Podcast One", "example1.com", "http://logo.co/1.png",
            "Tech, News", "2024-01-10", "50", # Episode count likely stringified
            "", # None becomes empty string
            "one@example.com", "Yes"
        ])

        row2 = next(reader)
        self.assertEqual(row2, [
            "Podcast Two", "example2.org", "", # None becomes empty string
            "Business", "2023-12-20", "150.5", # Float stringified
            "http://linkedin.com/company/p2",
            "two@example.org", "Indeed"
        ])

    # Mock file system operations for file export test
    @patch('pathlib.Path.mkdir')
    @patch('builtins.open', new_callable=mock_open) # Mocks open()
    @patch('src.export.csv_exporter.datetime') # Mock datetime to control timestamp
    def test_export_to_file_success(self, mock_dt, mock_file_open, mock_mkdir):
        # Setup mock datetime
        mock_now = MagicMock()
        mock_now.strftime.return_value = "20240101_120000"
        mock_dt.now.return_value = mock_now

        output_dir = "test_exports_dir"
        expected_filename = "attio_export_20240101_120000.csv"
        expected_filepath = Path(output_dir) / expected_filename

        # Mock Path resolve to return a predictable absolute path string
        with patch.object(Path, 'resolve', return_value=Path(f"C:/abs/path/to/{output_dir}/{expected_filename}")):
            returned_path = self.exporter.export_to_file(self.processed_leads, output_dir=output_dir)

        # Assertions
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_file_open.assert_called_once_with(expected_filepath, 'w', newline='', encoding='utf-8')

        # Check if the correct CSV string was written
        handle = mock_file_open() # Get the file handle mock
        written_content = handle.write.call_args[0][0]
        self.assertTrue(written_content.startswith(EXPECTED_HEADER))
        self.assertIn("Podcast One", written_content)
        self.assertIn("Podcast Two", written_content)

        self.assertEqual(returned_path, str(Path(f"C:/abs/path/to/{output_dir}/{expected_filename}")))

    def test_export_to_file_no_data(self):
        with self.assertRaises(ValueError) as cm:
            self.exporter.export_to_file([])
        self.assertIn("No CSV data generated", str(cm.exception))

    def test_export_to_file_write_error(self):
         # Use patch as context managers instead
        with patch('src.export.csv_exporter.datetime') as mock_dt, \
             patch('builtins.open', new_callable=mock_open) as mock_file_open, \
             patch('pathlib.Path.mkdir') as mock_mkdir:

            # Setup mock datetime
            mock_now = MagicMock()
            mock_now.strftime.return_value = "20240101_120000"
            mock_dt.now.return_value = mock_now

            # Simulate an IOError during write
            mock_file_open.side_effect = IOError("Disk full")

            with self.assertRaises(IOError) as cm:
                self.exporter.export_to_file(self.processed_leads, output_dir="error_dir")

            self.assertIn("Disk full", str(cm.exception))
            mock_mkdir.assert_called_once()
            # Check open was called, even though write failed
            self.assertTrue(mock_file_open.called)

if __name__ == '__main__':
    unittest.main() 