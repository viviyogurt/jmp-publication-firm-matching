"""
Classification utilities for AI patent identification and strategic classification.

Two-Stage Classification Approach:
- Stage 1: AI identification (Is this AI?)
- Stage 2: Strategic classification (What type of AI?)
"""

import re
from typing import Optional, List, Set
from .keyword_lists import (
    AI_KEYWORDS,
    AI_CPC_CODES,
    INFRASTRUCTURE_KEYWORDS,
    ALGORITHM_KEYWORDS,
    APPLICATION_KEYWORDS,
    CPC_TO_STRATEGIC_CATEGORY,
)


# =============================================================================
# STAGE 1: AI IDENTIFICATION FUNCTIONS
# =============================================================================

def is_ai_by_cpc(cpc_codes: List[str]) -> bool:
    """
    Check if patent is AI-related based on CPC codes.

    Args:
        cpc_codes: List of CPC classification codes

    Returns:
        True if any CPC code matches AI-related classes
    """
    if not cpc_codes:
        return False

    # Check if any CPC code starts with AI-related prefixes
    for cpc in cpc_codes:
        cpc_prefix = cpc[:4] if len(cpc) >= 4 else cpc
        if cpc_prefix in AI_CPC_CODES:
            return True

    return False


def is_ai_by_text(text: Optional[str]) -> bool:
    """
    Check if patent is AI-related based on text content.

    Args:
        text: Patent abstract or title text

    Returns:
        True if text contains AI-related keywords
    """
    if not text:
        return False

    text_lower = text.lower()

    # Check if any AI keyword appears in text
    for keyword in AI_KEYWORDS:
        if keyword in text_lower:
            return True

    return False


def is_ai_patent(
    abstract: Optional[str] = None,
    title: Optional[str] = None,
    cpc_codes: Optional[List[str]] = None,
    method: str = 'union'
) -> bool:
    """
    Determine if patent is AI-related using dual approach.

    Args:
        abstract: Patent abstract text
        title: Patent title text
        cpc_codes: List of CPC classification codes
        method: 'union' (CPC OR text), 'intersection' (CPC AND text),
                'cpc_only', or 'text_only'

    Returns:
        True if patent is AI-related according to specified method
    """
    # Combine title and abstract for text analysis
    text = ' '.join(filter(None, [title, abstract]))

    # Classify by each method
    ai_by_cpc = is_ai_by_cpc(cpc_codes) if cpc_codes else False
    ai_by_text = is_ai_by_text(text)

    # Apply combination method
    if method == 'union':
        return ai_by_cpc or ai_by_text
    elif method == 'intersection':
        return ai_by_cpc and ai_by_text
    elif method == 'cpc_only':
        return ai_by_cpc
    elif method == 'text_only':
        return ai_by_text
    else:
        raise ValueError(f"Unknown method: {method}")


# =============================================================================
# STAGE 2: STRATEGIC CLASSIFICATION FUNCTIONS
# =============================================================================

def classify_strategic_category_text(
    abstract: Optional[str] = None,
    title: Optional[str] = None
) -> str:
    """
    Classify AI patent into strategic category based on text.

    Categories (mutually exclusive, priority order):
    1. Infrastructure: Hardware, computing systems
    2. Algorithm: ML methods, model architectures
    3. Application: End-user products, business applications
    4. Unknown: None of the above

    Args:
        abstract: Patent abstract text
        title: Patent title text

    Returns:
        Strategic category: 'Infrastructure', 'Algorithm', 'Application', or 'Unknown'
    """
    # Combine title and abstract
    text = ' '.join(filter(None, [title, abstract]))

    if not text:
        return 'Unknown'

    text_lower = text.lower()

    # Check infrastructure first (mutually exclusive, priority order)
    if any(keyword in text_lower for keyword in INFRASTRUCTURE_KEYWORDS):
        return 'Infrastructure'

    # Then check algorithms
    elif any(keyword in text_lower for keyword in ALGORITHM_KEYWORDS):
        return 'Algorithm'

    # Then check applications
    elif any(keyword in text_lower for keyword in APPLICATION_KEYWORDS):
        return 'Application'

    # Default to unknown
    else:
        return 'Unknown'


def classify_strategic_category_cpc(cpc_codes: List[str]) -> str:
    """
    Classify AI patent into strategic category based on CPC codes.

    Args:
        cpc_codes: List of CPC classification codes

    Returns:
        Strategic category: 'Infrastructure', 'Algorithm', 'Application', or 'Unknown'
    """
    if not cpc_codes:
        return 'Unknown'

    # Map each CPC code to category
    categories = []
    for cpc in cpc_codes:
        cpc_prefix = cpc[:4] if len(cpc) >= 4 else cpc
        category = CPC_TO_STRATEGIC_CATEGORY.get(cpc_prefix, 'Unknown')
        categories.append(category)

    # Use majority vote
    if not categories:
        return 'Unknown'

    # Count occurrences
    category_counts = {}
    for cat in categories:
        category_counts[cat] = category_counts.get(cat, 0) + 1

    # Return most common category
    return max(category_counts, key=category_counts.get)


def classify_strategic_category(
    abstract: Optional[str] = None,
    title: Optional[str] = None,
    cpc_codes: Optional[List[str]] = None,
    method: str = 'text'
) -> str:
    """
    Classify AI patent into strategic category using specified method.

    Args:
        abstract: Patent abstract text
        title: Patent title text
        cpc_codes: List of CPC classification codes
        method: 'text' (default), 'cpc', or 'hybrid' (requires both)

    Returns:
        Strategic category: 'Infrastructure', 'Algorithm', 'Application', or 'Unknown'
    """
    if method == 'text':
        return classify_strategic_category_text(abstract, title)
    elif method == 'cpc':
        return classify_strategic_category_cpc(cpc_codes) if cpc_codes else 'Unknown'
    elif method == 'hybrid':
        # Require both text and CPC classification to agree
        text_category = classify_strategic_category_text(abstract, title)
        cpc_category = classify_strategic_category_cpc(cpc_codes) if cpc_codes else 'Unknown'

        # Return text category if CPC is unknown or they agree
        if cpc_category == 'Unknown':
            return text_category
        elif text_category == cpc_category:
            return text_category
        else:
            # Disagreement - prioritize text classification
            return text_category
    else:
        raise ValueError(f"Unknown method: {method}")


# =============================================================================
# SOFTWARE PATENT CLASSIFICATION (for Alice Corp DID)
# =============================================================================

def is_software_patent(
    abstract: Optional[str] = None,
    title: Optional[str] = None,
    cpc_codes: Optional[List[str]] = None
) -> bool:
    """
    Check if patent is a software patent (for Alice Corp DID analysis).

    Note: This is separate from AI classification.
    A patent can be both AI and software, or only one.

    Args:
        abstract: Patent abstract text
        title: Patent title text
        cpc_codes: List of CPC classification codes

    Returns:
        True if patent is software-related
    """
    # Combine title and abstract
    text = ' '.join(filter(None, [title, abstract]))

    if not text:
        return False

    text_lower = text.lower()

    # Check for software keywords
    software_keywords = [
        'software',
        'computer program',
        'business method',
        'e-commerce',
        'online platform',
    ]

    if any(keyword in text_lower for keyword in software_keywords):
        return True

    # Check CPC codes
    if cpc_codes:
        for cpc in cpc_codes:
            if cpc.startswith('G06F') or cpc.startswith('G06Q'):
                return True

    return False


# =============================================================================
# BATCH CLASSIFICATION FUNCTIONS
# =============================================================================

def batch_classify_ai_patents(df, text_col='patent_abstract', cpc_col='cpc_codes'):
    """
    Batch classify patents as AI or not AI.

    Args:
        df: DataFrame with patent data
        text_col: Column name for abstract text
        cpc_col: Column name for CPC codes list

    Returns:
        Series with boolean AI classification
    """
    return df.apply(
        lambda row: is_ai_patent(
            abstract=row.get(text_col),
            title=row.get('patent_title'),
            cpc_codes=row.get(cpc_col),
            method='union'
        ),
        axis=1
    )


def batch_classify_strategic_category(
    df,
    abstract_col='patent_abstract',
    title_col='patent_title',
    cpc_col='cpc_codes',
    method='text'
):
    """
    Batch classify AI patents into strategic categories.

    Args:
        df: DataFrame with AI patent data
        abstract_col: Column name for abstract text
        title_col: Column name for title text
        cpc_col: Column name for CPC codes list
        method: Classification method ('text', 'cpc', or 'hybrid')

    Returns:
        Series with strategic category classification
    """
    return df.apply(
        lambda row: classify_strategic_category(
            abstract=row.get(abstract_col),
            title=row.get(title_col),
            cpc_codes=row.get(cpc_col),
            method=method
        ),
        axis=1
    )
