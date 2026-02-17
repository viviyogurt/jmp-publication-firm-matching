"""
Utilities for patent processing.

This module provides utility functions for:
- AI patent identification (Stage 1)
- Strategic classification (Stage 2)
- Fuzzy matching for firm linkage
- Data validation
"""

from .keyword_lists import (
    AI_KEYWORDS,
    AI_CPC_CODES,
    INFRASTRUCTURE_KEYWORDS,
    ALGORITHM_KEYWORDS,
    APPLICATION_KEYWORDS,
    CPC_TO_STRATEGIC_CATEGORY,
)

from .classification import (
    is_ai_by_cpc,
    is_ai_by_text,
    is_ai_patent,
    classify_strategic_category_text,
    classify_strategic_category_cpc,
    classify_strategic_category,
    is_software_patent,
)

__all__ = [
    # Keyword lists
    'AI_KEYWORDS',
    'AI_CPC_CODES',
    'INFRASTRUCTURE_KEYWORDS',
    'ALGORITHM_KEYWORDS',
    'APPLICATION_KEYWORDS',
    'CPC_TO_STRATEGIC_CATEGORY',

    # Classification functions
    'is_ai_by_cpc',
    'is_ai_by_text',
    'is_ai_patent',
    'classify_strategic_category_text',
    'classify_strategic_category_cpc',
    'classify_strategic_category',
    'is_software_patent',
]
