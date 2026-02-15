"""
Improved Wikipedia Matching with Ambiguity Resolution

This script implements improved Wikipedia matching that addresses key ambiguity issues:
1. Requires minimum name length (5+ characters)
2. Requires substring match (Wikipedia name must be IN firm name)
3. Excludes generic words (group, systems, international, associates, technologies)
4. Uses Jaro-Winkler fuzzy similarity for validation
5. High confidence thresholds (0.95+ for similarity > 0.85, 0.98 for similarity > 0.95)

Target: ~500-800 high-quality matches (vs 8,585 noisy matches from naive approach)
"""

import polars as pl
from pathlib import Path
import logging
import re
from typing import Optional, List, Dict, Set
from rapidfuzz import fuzz

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_ENRICHED = DATA_INTERIM / "publication_institutions_enriched.parquet"
COMPUSTAT_FIRMS = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_wikipedia_improved.parquet"
PROGRESS_LOG = LOGS_DIR / "match_wikipedia_improved.log"

DATA_PROCESSED_LINK.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROGRESS_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Generic words to exclude from matching
GENERIC_WORDS = {
    'group', 'groups',
    'system', 'systems', 'systems',
    'international', 'international',
    'associate', 'associates', 'associates',
    'technology', 'technologies', 'technologies',
    'company', 'companies',
    'corporation', 'corporations',
    'incorporated', 'inc',
    'limited', 'ltd', 'llc',
    'holdings', 'holding',
    'solutions', 'solution',
    'services', 'service',
    'industries', 'industry',
    'worldwide', 'global',
    'enterprises', 'enterprise',
    'labs', 'laboratory', 'laboratories',
    'research', 'research',
    'company', 'co',
}

# Minimum name length threshold
MIN_NAME_LENGTH = 5

# Similarity thresholds for confidence scoring
SIMILARITY_THRESHOLD_HIGH = 0.95  # For 0.98 confidence
SIMILARITY_THRESHOLD_MED = 0.85   # For 0.95 confidence


# ============================================================================
# Name Extraction and Normalization
# ============================================================================

def extract_company_name_from_wikipedia(wikipedia_url: str) -> Optional[str]:
    """
    Extract company name from Wikipedia URL.

    Examples:
    - http://en.wikipedia.org/wiki/Google → "Google"
    - https://en.wikipedia.org/wiki/Google_DeepMind → "Google DeepMind"
    - http://en.wikipedia.org/wiki/IBM → "IBM"
    """
    if not wikipedia_url:
        return None

    try:
        # Remove protocol
        url = wikipedia_url.replace('http://', '').replace('https://', '')

        # Extract from en.wikipedia.org/wiki/URL_PATH
        if 'en.wikipedia.org/wiki/' in url:
            # Get the page title
            page_title = url.split('en.wikipedia.org/wiki/')[-1]

            # Remove any URL fragments (#)
            page_title = page_title.split('#')[0]

            # Replace underscores with spaces
            company_name = page_title.replace('_', ' ')

            # URL decode special characters (e.g., %20 -> space)
            company_name = company_name.replace('%20', ' ').replace('%26', '&').replace('%27', "'")

            return company_name.strip() if company_name.strip() else None

        return None
    except Exception as e:
        logger.debug(f"Error extracting name from Wikipedia URL {wikipedia_url}: {e}")
        return None


def normalize_name(name: str) -> str:
    """
    Normalize company name for comparison.

    Steps:
    1. Convert to lowercase
    2. Remove punctuation and special characters
    3. Remove extra whitespace
    4. Remove common suffixes
    """
    if not name:
        return ''

    # Convert to lowercase
    name = name.lower().strip()

    # Remove punctuation and special characters
    name = re.sub(r'[^\w\s&]', ' ', name)

    # Remove extra whitespace
    name = re.sub(r'\s+', ' ', name).strip()

    # Remove common suffixes (after main name)
    suffixes = [
        ' inc', ' ltd', ' corp', ' plc', ' gmbh', ' ag', ' co', ' corporation',
        ' company', ' industries', ' worldwide', ' group',
        ' holdings', ' limited', ' technologies', ' solutions', ' services',
        ' international', ' associates', ' laboratories', ' laboratory',
        ' incorporated', ' llc', ' inc'
    ]

    for suffix in sorted(suffixes, key=len, reverse=True):
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()

    return name


def is_generic_word(name: str) -> bool:
    """
    Check if a name is a generic word that should be excluded.

    Returns True if the name is in the generic words list or is too short.
    """
    if not name:
        return True

    name_normalized = name.lower().strip()

    # Check minimum length
    if len(name_normalized) < MIN_NAME_LENGTH:
        return True

    # Check if it's a generic word
    if name_normalized in GENERIC_WORDS:
        return True

    return False


def extract_meaningful_parts(name: str) -> List[str]:
    """
    Extract meaningful parts from a company name, excluding generic words.

    Example: "International Business Machines Corp" → ["business", "machines"]
    Example: "Google DeepMind" → ["google", "deepmind"]
    """
    if not name:
        return []

    # Normalize and split into words
    normalized = normalize_name(name)
    words = normalized.split()

    # Filter out generic words and short words
    meaningful = [
        w for w in words
        if w not in GENERIC_WORDS and len(w) >= MIN_NAME_LENGTH
    ]

    return meaningful


# ============================================================================
# Similarity and Matching Functions
# ============================================================================

def calculate_similarity(str1: str, str2: str) -> float:
    """
    Calculate string similarity using WRatio (Weighted Ratio).

    WRatio is a composite similarity metric that combines:
    - Simple ratio
    - Partial ratio
    - Token sort ratio
    - Token set ratio

    Returns a value between 0.0 and 1.0.
    """
    if not str1 or not str2:
        return 0.0

    try:
        # Normalize to lowercase for better comparison
        str1_norm = str1.lower()
        str2_norm = str2.lower()

        # Use WRatio for robust similarity measurement
        similarity = fuzz.WRatio(str1_norm, str2_norm)
        return similarity / 100.0  # RapidFuzz returns 0-100, normalize to 0-1
    except Exception as e:
        logger.debug(f"Error calculating similarity: {e}")
        return 0.0


def check_substring_match(wiki_name: str, firm_name: str) -> bool:
    """
    Check if Wikipedia name is a substring of firm name.

    Both names should be normalized before comparison.
    """
    if not wiki_name or not firm_name:
        return False

    wiki_normalized = normalize_name(wiki_name)
    firm_normalized = normalize_name(firm_name)

    return wiki_normalized in firm_normalized


def find_best_firm_match(wiki_name: str,
                         firms_df: pl.DataFrame,
                         firm_conm_col: str = 'conm_clean',
                         firm_conml_col: str = 'conml_clean') -> Optional[Dict]:
    """
    Find the best matching firm for a Wikipedia company name.

    Strategy:
    1. Check if wiki_name is substring of firm name (required)
    2. Calculate Jaro-Winkler similarity
    3. Assign confidence based on similarity thresholds
    4. Return best match or None if no high-quality match found

    Returns:
        Dictionary with GVKEY, confidence, similarity, etc. or None
    """
    if not wiki_name:
        return None

    wiki_normalized = normalize_name(wiki_name)

    # Filter for substring matches
    firm_matches = []

    for firm_row in firms_df.iter_rows(named=True):
        conm_clean = firm_row.get(firm_conm_col, '')
        conml_clean = firm_row.get(firm_conml_col, '')

        # Check substring match in conm_clean
        if conm_clean and wiki_normalized in conm_clean.lower():
            similarity = calculate_similarity(wiki_normalized, conm_clean.lower())
            firm_matches.append((firm_row, similarity, conm_clean))

        # Check substring match in conml_clean
        elif conml_clean and wiki_normalized in conml_clean.lower():
            similarity = calculate_similarity(wiki_normalized, conml_clean.lower())
            firm_matches.append((firm_row, similarity, conml_clean))

    # If no matches, return None
    if not firm_matches:
        return None

    # Sort by similarity (descending) and get best match
    firm_matches.sort(key=lambda x: x[1], reverse=True)
    best_firm_row, best_similarity, matched_name = firm_matches[0]

    # Apply similarity thresholds
    if best_similarity < SIMILARITY_THRESHOLD_MED:
        # Similarity too low, skip this match
        logger.debug(f"  Similarity {best_similarity:.4f} below threshold {SIMILARITY_THRESHOLD_MED}")
        return None

    # Assign confidence based on similarity
    if best_similarity >= SIMILARITY_THRESHOLD_HIGH:
        confidence = 0.98
    else:
        confidence = 0.95

    return {
        'GVKEY': best_firm_row['GVKEY'],
        'LPERMNO': best_firm_row.get('LPERMNO'),
        'firm_conm': best_firm_row['conm'],
        'matched_name': matched_name,
        'similarity': best_similarity,
        'confidence': confidence,
    }


def match_institution_via_wikipedia(inst_row: Dict,
                                    firms_df: pl.DataFrame,
                                    matched_ids: Set[str]) -> List[Dict]:
    """
    Match an institution to firms using improved Wikipedia matching.

    Steps:
    1. Extract company name from Wikipedia URL
    2. Validate name length (must be 5+ chars)
    3. Check for generic words (skip if too generic)
    4. Find best firm match via substring + similarity
    5. Return match if similarity > 0.85
    """
    matches = []

    institution_id = inst_row['institution_id']
    display_name = inst_row['display_name']
    wikipedia_url = inst_row.get('wikipedia_url')
    paper_count = inst_row.get('paper_count', 0)
    is_company = inst_row.get('is_company', 0)

    # Skip if already matched
    if institution_id in matched_ids:
        return matches

    # Must have Wikipedia URL
    if not wikipedia_url:
        return matches

    # Extract company name from Wikipedia URL
    wiki_company = extract_company_name_from_wikipedia(wikipedia_url)
    if not wiki_company:
        return matches

    # Validate minimum name length
    if len(wiki_company) < MIN_NAME_LENGTH:
        logger.debug(f"Skipping {wiki_company}: name too short ({len(wiki_company)} < {MIN_NAME_LENGTH})")
        return matches

    # Check if name is too generic
    if is_generic_word(wiki_company):
        logger.debug(f"Skipping {wiki_company}: generic word")
        return matches

    # Find best firm match
    best_match = find_best_firm_match(wiki_company, firms_df)

    if not best_match:
        # No high-quality match found
        return matches

    # Create match record
    matches.append({
        'GVKEY': best_match['GVKEY'],
        'LPERMNO': best_match['LPERMNO'],
        'firm_conm': best_match['firm_conm'],
        'institution_id': institution_id,
        'institution_display_name': display_name,
        'wikipedia_url': wikipedia_url,
        'wiki_company_name': wiki_company,
        'matched_firm_name': best_match['matched_name'],
        'similarity_score': round(best_match['similarity'], 4),
        'match_type': 'wikipedia_improved',
        'match_confidence': best_match['confidence'],
        'match_method': 'wikipedia_substring_similarity',
        'institution_is_company': is_company,
        'institution_paper_count': paper_count,
    })

    return matches


# ============================================================================
# Main Processing
# ============================================================================

def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("IMPROVED WIKIPEDIA MATCHING WITH AMBIGUITY RESOLUTION")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/6] Loading data...")

    if not INSTITUTIONS_ENRICHED.exists():
        raise FileNotFoundError(f"Institutions file not found: {INSTITUTIONS_ENRICHED}")
    if not COMPUSTAT_FIRMS.exists():
        raise FileNotFoundError(f"Firms file not found: {COMPUSTAT_FIRMS}")

    institutions_df = pl.read_parquet(INSTITUTIONS_ENRICHED)
    firms_df = pl.read_parquet(COMPUSTAT_FIRMS)

    logger.info(f"  Loaded {len(institutions_df):,} institutions")
    logger.info(f"  Loaded {len(firms_df):,} firms")

    # Step 2: Filter institutions with Wikipedia URLs
    logger.info("\n[2/6] Filtering institutions with Wikipedia URLs...")
    inst_with_wikipedia = institutions_df.filter(
        pl.col('wikipedia_url').is_not_null()
    )
    logger.info(f"  {len(inst_with_wikipedia):,} institutions have Wikipedia URLs")

    # Step 3: Load previously matched institution IDs
    logger.info("\n[3/6] Loading previously matched institution IDs...")
    matched_ids = set()

    stage1_file = DATA_PROCESSED_LINK / "publication_firm_matches_stage1.parquet"
    if stage1_file.exists():
        stage1_matches = pl.read_parquet(stage1_file)
        matched_ids.update(stage1_matches['institution_id'].to_list())
        logger.info(f"  Stage 1: {len(stage1_matches):,} matches")

    # Check other match files
    for match_file in [
        "publication_firm_matches_wikidata.parquet",
        "publication_firm_matches_parent_cascade.parquet",
        "publication_firm_matches_smart_urls.parquet",
        "publication_firm_matches_acronyms.parquet",
    ]:
        file_path = DATA_PROCESSED_LINK / match_file
        if file_path.exists():
            matches = pl.read_parquet(file_path)
            matched_ids.update(matches['institution_id'].to_list())
            logger.info(f"  {match_file}: {len(matches):,} matches")

    logger.info(f"  Total previously matched institutions: {len(matched_ids):,}")

    # Step 4: Filter institutions not yet matched
    logger.info("\n[4/6] Filtering unmatched institutions...")
    inst_unmatched = inst_with_wikipedia.filter(
        ~pl.col('institution_id').is_in(list(matched_ids))
    )
    logger.info(f"  {len(inst_unmatched):,} institutions with Wikipedia URLs not yet matched")

    # Step 5: Run matching
    logger.info("\n[5/6] Running improved Wikipedia matching...")
    logger.info(f"  Using WRatio similarity metric (case-insensitive)")
    logger.info(f"  Similarity thresholds:")
    logger.info(f"    - High confidence (0.98): similarity >= {SIMILARITY_THRESHOLD_HIGH}")
    logger.info(f"    - Medium confidence (0.95): similarity >= {SIMILARITY_THRESHOLD_MED}")
    logger.info(f"  Minimum name length: {MIN_NAME_LENGTH} characters")

    all_matches = []
    total = len(inst_unmatched)
    skipped_short = 0
    skipped_generic = 0
    skipped_no_match = 0
    skipped_low_similarity = 0

    for i, inst_row in enumerate(inst_unmatched.iter_rows(named=True)):
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} institutions ({len(all_matches)} matches so far)...")

        matches = match_institution_via_wikipedia(inst_row, firms_df, matched_ids)

        if matches:
            all_matches.extend(matches)
        else:
            # Track why we skipped
            wiki_url = inst_row.get('wikipedia_url')
            if wiki_url:
                wiki_name = extract_company_name_from_wikipedia(wiki_url)
                if wiki_name:
                    if len(wiki_name) < MIN_NAME_LENGTH:
                        skipped_short += 1
                    elif is_generic_word(wiki_name):
                        skipped_generic += 1
                    else:
                        # Name passed filters but no good match found
                        best_match = find_best_firm_match(wiki_name, firms_df)
                        if best_match is None:
                            skipped_no_match += 1
                        else:
                            skipped_low_similarity += 1

    logger.info(f"  Completed. Found {len(all_matches):,} total matches")
    logger.info(f"\n  Skipped statistics:")
    logger.info(f"    - Name too short: {skipped_short:,}")
    logger.info(f"    - Generic word: {skipped_generic:,}")
    logger.info(f"    - No substring match: {skipped_no_match:,}")
    logger.info(f"    - Low similarity: {skipped_low_similarity:,}")

    # Step 6: Save output
    logger.info("\n[6/6] Saving improved Wikipedia matches...")
    logger.info(f"Output: {OUTPUT_FILE}")

    if not all_matches:
        logger.warning("  No matches found!")
        matches_df = pl.DataFrame()
    else:
        matches_df = pl.DataFrame(all_matches)

        # Deduplicate: if same institution matched to multiple firms, keep highest similarity
        matches_df = (
            matches_df
            .sort(['institution_id', 'similarity_score'], descending=[False, True])
            .unique(subset=['institution_id', 'GVKEY'], keep='first')
        )

        logger.info(f"  After deduplication: {len(matches_df):,} unique institution-firm matches")
        matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
        logger.info(f"  Saved {len(matches_df):,} matches")

        # Summary statistics
        logger.info("\n" + "=" * 80)
        logger.info("IMPROVED WIKIPEDIA MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total matches: {len(matches_df):,}")
        logger.info(f"Unique institutions matched: {matches_df['institution_id'].n_unique():,}")
        logger.info(f"Unique firms matched: {matches_df['GVKEY'].n_unique():,}")

        # Coverage statistics
        coverage_pct = (len(matches_df) / len(inst_unmatched)) * 100
        logger.info(f"\nMatch rate: {coverage_pct:.2f}% of unmatched institutions with Wikipedia URLs")

        # Similarity distribution
        logger.info(f"\nSimilarity score statistics:")
        logger.info(f"  Mean: {matches_df['similarity_score'].mean():.4f}")
        logger.info(f"  Median: {matches_df['similarity_score'].median():.4f}")
        logger.info(f"  Min: {matches_df['similarity_score'].min():.4f}")
        logger.info(f"  Max: {matches_df['similarity_score'].max():.4f}")

        # Confidence distribution
        if 'match_confidence' in matches_df.columns:
            logger.info(f"\nConfidence distribution:")
            for conf, count in matches_df.group_by('match_confidence').agg(
                pl.len().alias('count')
            ).sort('count', descending=True).iter_rows(named=True):
                logger.info(f"  {conf['match_confidence']:.2f}: {conf['count']:,} matches")

        # Paper count statistics
        if 'institution_paper_count' in matches_df.columns:
            total_papers = matches_df['institution_paper_count'].sum()
            logger.info(f"\nTotal papers covered: {total_papers:,}")

        # Show example matches
        logger.info("\nExample high-confidence matches:")
        for i, row in enumerate(matches_df.head(20).iter_rows(named=True), 1):
            logger.info(f"  {i}. {row['institution_display_name'][:60]}")
            logger.info(f"     Wikipedia: {row['wiki_company_name'][:50]}")
            logger.info(f"     → {row['firm_conm'][:50]}")
            logger.info(f"     Similarity: {row['similarity_score']:.4f} | Confidence: {row['match_confidence']:.2f}")

    logger.info("\n" + "=" * 80)
    logger.info("IMPROVED WIKIPEDIA MATCHING COMPLETE")
    logger.info("=" * 80)

    return matches_df


if __name__ == "__main__":
    main()
