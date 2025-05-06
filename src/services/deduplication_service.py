import logging
from typing import List, Set, Dict, Any, Optional
from collections import defaultdict

# Remove PodcastLead if no longer primary focus
# from ..models.lead import PodcastLead

logger = logging.getLogger(__name__)

class DeduplicationService:
    """Service to handle deduplication and merging of unified podcast data."""

    def deduplicate_and_merge(self, 
                              unified_records: List[Dict[str, Any]], 
                              key_field: str = "rss_url", 
                              priority_source: str = "listennotes") -> List[Dict[str, Any]]:
        """Deduplicates and merges a list of unified podcast dictionaries.

        Identifies duplicates based on the key_field (typically rss_url).
        Merges duplicates, prioritizing fields from the priority_source.

        Args:
            unified_records: List of unified podcast data dictionaries.
            key_field: The dictionary key to use for identifying duplicates (e.g., "rss_url").
            priority_source: The value in the 'source_api' field to prioritize 
                             when merging conflicting fields (e.g., "listennotes").

        Returns:
            A list of unique, potentially merged podcast data dictionaries.
        """
        if not unified_records:
            return []

        logger.info(f"Starting deduplication and merging for {len(unified_records)} records based on '{key_field}'...")
        
        # Group records by the key_field
        grouped_records = defaultdict(list)
        skipped_records = 0
        for record in unified_records:
            key = record.get(key_field)
            if key:
                grouped_records[key].append(record)
            else:
                 # Handle records missing the key field (e.g., log and skip)
                 logger.warning(f"Skipping record during deduplication due to missing key field '{key_field}': {record.get('api_id', 'N/A')}")
                 skipped_records += 1
                 
        final_list: List[Dict[str, Any]] = []
        merged_count = 0

        for key, group in grouped_records.items():
            if len(group) == 1:
                # No duplicate for this key, add directly
                final_list.append(group[0])
            else:
                # Duplicates found, perform merge
                merged_count += (len(group) - 1)
                logger.debug(f"Merging {len(group)} duplicates for key '{key}': {key}")
                
                # Find the priority record (if one exists)
                priority_record = next((r for r in group if r.get("source_api") == priority_source), None)
                
                # Start with the priority record, or the first record if no priority source found
                merged_record = (priority_record or group[0]).copy()
                
                # Iterate through the group again to merge data from others
                for record_to_merge in group:
                    if record_to_merge is merged_record: # Don't merge with self
                        continue
                        
                    for field, value in record_to_merge.items():
                        # If the field is missing or None in the merged record,
                        # OR if the value to merge is not None and the current value IS None,
                        # then update the merged record.
                        if field not in merged_record or merged_record[field] is None:
                           if value is not None: # Only merge non-None values into missing fields
                                merged_record[field] = value
                        # Optional: Add more sophisticated merge rules here if needed 
                        # (e.g., combining lists, preferring longer descriptions etc.)
                
                final_list.append(merged_record)

        logger.info(f"Deduplication complete. Merged {merged_count} duplicates. Skipped {skipped_records} records without key. Returning {len(final_list)} unique records.")
        return final_list

    # Future enhancement: Implement fuzzy matching and merging logic here
    # def deduplicate_fuzzy(self, leads: List[PodcastLead], threshold: float = 0.85) -> List[PodcastLead]:
    #     pass

# Example Usage (optional)
# if __name__ == '__main__':
#     logging.basicConfig(level=logging.INFO)
#     # Create some dummy leads
#     lead1 = PodcastLead(podcast_id="123", name="Podcast A", description="Desc A")
#     lead2 = PodcastLead(podcast_id="456", name="Podcast B", description="Desc B")
#     lead3 = PodcastLead(podcast_id="123", name="Podcast A Repeat", description="Desc A Repeat") # Duplicate ID
#     lead4 = PodcastLead(podcast_id="789", name="Podcast C", description="Desc C")
#     lead_list = [lead1, lead2, lead3, lead4]
#
#     deduplicator = DeduplicationService()
#     unique_list = deduplicator.deduplicate(lead_list)
#
#     print("\nOriginal List:")
#     for lead in lead_list:
#         print(f" - {lead.podcast_id}: {lead.name}")
#
#     print("\nUnique List:")
#     for lead in unique_list:
#         print(f" - {lead.podcast_id}: {lead.name}") 