from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
import uuid

class MetricRecord(BaseModel):
    """
    Represents a single metric event recorded by the system.
    """
    metric_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    event_name: str # e.g., "agent_step_start", "agent_step_end", "error", "lead_processed"
    campaign_id: Optional[str] = None # Link metric to a specific campaign if applicable
    agent_step: Optional[str] = None # e.g., "search", "vetting", "crm_sync"
    duration_ms: Optional[float] = None # For timed events
    count: Optional[int] = None # For counting occurrences
    metadata: Optional[Dict[str, Any]] = None # For extra context (e.g., error details, lead_id)

    class Config:
        # Example for MongoDB: Allow extra fields if needed, though usually explicit is better
        # extra = "allow"
        # Example: ensure metric_id is used as _id if needed, though usually handled separately
        # populate_by_name = True
        # alias_generator = lambda field_name: "_id" if field_name == "metric_id" else field_name
        pass 