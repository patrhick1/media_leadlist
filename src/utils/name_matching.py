import re
import unicodedata
from typing import List, Optional, Dict

from thefuzz import process, fuzz

# Assuming Guest model is defined elsewhere and imported if needed for type hints
from ..models.guests import Guest

DEFAULT_MATCH_THRESHOLD = 85 # Default score threshold for fuzzy matching

def normalize_name(name: str) -> str:
    """Normalize guest names for better matching.
    
    - Lowercase
    - Remove punctuation and extra whitespace
    - Handle simple titles/suffixes (can be expanded)
    - Normalize unicode characters
    """
    if not name:
        return ""
    
    titles_suffixes = ['dr', 'prof', 'mr', 'mrs', 'ms', 'jr', 'sr', 'phd', 'md', 'iii', 'iv']
    try:
        # Normalize unicode characters (e.g., accented letters) -> ascii
        normalized = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    except UnicodeDecodeError:
        normalized = name # Fallback if contains non-ascii representable chars

    cleaned = normalized.lower()
    # Remove specific punctuation, keep spaces
    cleaned = re.sub(r'[.,!?"-]', '', cleaned) 
    words = cleaned.split()
    # Remove titles/suffixes
    filtered_words = [word for word in words if word not in titles_suffixes]
    
    if not filtered_words:
        return ""
        
    return " ".join(filtered_words).strip()

def match_guest_by_name(query_name: str, guest_dict: Dict[str, Guest], threshold: int = DEFAULT_MATCH_THRESHOLD) -> Optional[str]:
    """Attempts to find the best matching guest ID from a dictionary of Guest objects.

    Args:
        query_name: The name to search for.
        guest_dict: A dictionary where keys are guest_ids and values are Guest objects.
        threshold: The minimum fuzzy match score (0-100) required.

    Returns:
        The guest_id of the best match above the threshold, or None if no good match found.
    """
    if not query_name or not guest_dict:
        return None

    normalized_query = normalize_name(query_name)
    if not normalized_query:
        return None

    choices = []
    for guest_id, guest in guest_dict.items():
        normalized_primary = normalize_name(guest.name)
        if normalized_primary:
            choices.append((normalized_primary, guest_id))
        for alias in guest.aliases:
            normalized_alias = normalize_name(alias)
            if normalized_alias:
                choices.append((normalized_alias, guest_id))

    if not choices:
        return None

    # Check for exact match first
    exact_matches = [choice for choice in choices if choice[0] == normalized_query]
    if exact_matches:
        return exact_matches[0][1]

    # Fuzzy match if no exact match
    best_match = process.extractOne(
        normalized_query,
        [choice[0] for choice in choices],
        scorer=fuzz.WRatio, 
        score_cutoff=threshold
    )

    if best_match:
        matched_string = best_match[0]
        # Find the first guest_id associated with the matched string
        for choice_str, guest_id in choices:
            if choice_str == matched_string:
                return guest_id
        return None # Should not happen if best_match is found
    else:
        return None # No match above threshold 