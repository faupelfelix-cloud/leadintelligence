"""
confidence_utils.py — Shared confidence scoring for the Lead Intelligence System.

Provides a single function to calculate a numeric confidence score (0-100) from
a data_confidence JSON dict. Used by all enrichment scripts and housekeeping.

Usage:
    from confidence_utils import calculate_confidence_score

    # From a dict (during enrichment)
    score = calculate_confidence_score({"funding": "high", "pipeline": "low"})
    # Returns: 40 (weakest link = "low" = 40)

    # From a JSON string (reading from Airtable)
    score = calculate_confidence_score('{"funding": "high", "pipeline": "medium"}')
    # Returns: 70
"""

import json
from typing import Union, Dict, Optional

# Confidence level → numeric score mapping
CONFIDENCE_SCORES = {
    'high': 100,
    'medium': 70,
    'low': 40,
    'unverified': 10,
    'unknown': 0,
    'failed': 0,
}


def calculate_confidence_score(data_confidence: Union[str, Dict, None]) -> int:
    """Convert a data_confidence value to a single numeric score (0-100).
    
    Uses the MINIMUM confidence across all fields — the chain is only
    as strong as the weakest link.
    
    Args:
        data_confidence: Either a JSON string, a dict, or None
        
    Returns:
        Integer 0-100. Returns 0 if input is None, empty, or unparseable.
    """
    if not data_confidence:
        return 0
    
    # Parse string to dict if needed
    if isinstance(data_confidence, str):
        try:
            data_confidence = json.loads(data_confidence)
        except (json.JSONDecodeError, ValueError):
            return 0
    
    if not isinstance(data_confidence, dict) or not data_confidence:
        return 0
    
    scores = []
    for field, level in data_confidence.items():
        score = CONFIDENCE_SCORES.get(str(level).lower().strip(), 0)
        scores.append(score)
    
    return min(scores) if scores else 0
