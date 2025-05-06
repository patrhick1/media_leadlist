from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

class ExportLog(BaseModel):
    """Represents the log of a single CSV export attempt."""
    timestamp: datetime = Field(default_factory=datetime.now)
    status: str # e.g., "SUCCESS", "PARTIAL_SUCCESS", "VALIDATION_FAILURE", "SYSTEM_FAILURE"
    total_leads_processed: int
    leads_exported_count: int
    output_file_path: Optional[str] = None
    # Store validation errors keyed by lead identifier (e.g., name or a generated ID)
    validation_errors: Optional[Dict[str, List[str]]] = None
    system_error: Optional[str] = None # For file system or other unexpected errors

    class Config:
        # Allow population by field name, useful if creating from dict
        populate_by_name = True
        # Example for JSON serialization
        json_schema_extra = {
            "example": {
                "timestamp": "2024-04-16T15:00:00.123Z",
                "status": "PARTIAL_SUCCESS",
                "total_leads_processed": 10,
                "leads_exported_count": 8,
                "output_file_path": "C:/path/to/exports/attio_export_20240416_150000.csv",
                "validation_errors": {
                    "Podcast Lead 3": ["Required field 'podcast_link' is missing."],
                    "Podcast Lead 9": ["Field 'email' is not a valid email: bad-email@"]
                },
                "system_error": None
            }
        } 