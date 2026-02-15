"""
Abbreviation Matching for Publications

Uses dictionary of known company abbreviations to match institutions to firms.
Similar to patent matching approach but adapted for publications.

Target: +200-500 firms @ 95%+ accuracy
"""

import polars as pl
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
BASELINE_MATCHES = DATA_PROCESSED_LINK / "publication_firm_matches_cleaned.parquet"
OUTPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_abbreviations.parquet"

DATA_PROCESSED_LINK.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "match_publications_abbreviations.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Abbreviation Dictionary (adapted from patent matching)
# ============================================================================
ABBREVIATION_DICT = {
    # Tech giants
    'IBM': 'INTERNATIONAL BUSINESS MACHINES',
    'AT&T': 'AMERICAN TELEPHONE AND TELEGRAPH',
    'ATT': 'AMERICAN TELEPHONE AND TELEGRAPH',
    'GE': 'GENERAL ELECTRIC',
    'GM': 'GENERAL MOTORS',
    'HP': 'HEWLETT PACKARD',
    'HPE': 'HEWLETT PACKARD ENTERPRISE',
    'JPM': 'JPMORGAN CHASE',
    'JPMORGAN': 'JPMORGAN CHASE',
    'BA': 'BOEING',
    'CAT': 'CATERPILLAR',
    'XOM': 'EXXON MOBIL',
    'CVX': 'CHEVRON',
    'WMT': 'WALMART',
    'PG': 'PROCTER AND GAMBLE',
    'KO': 'COCA COLA',
    'PEP': 'PEPSICO',
    'DIS': 'WALT DISNEY',
    'NFLX': 'NETFLIX',
    'TSLA': 'TESLA',
    'META': 'META PLATFORMS',  # Facebook
    'GOOGL': 'ALPHABET',  # Google
    'GOOG': 'ALPHABET',
    'AMZN': 'AMAZON',
    'MSFT': 'MICROSOFT',
    'AAPL': 'APPLE',
    'NVDA': 'NVIDIA',
    'INTC': 'INTEL',
    'AMD': 'ADVANCED MICRO DEVICES',
    'QCOM': 'QUALCOMM',
    'TXN': 'TEXAS INSTRUMENTS',
    'AVGO': 'BROADCOM',
    'CSCO': 'CISCO SYSTEMS',
    'ORCL': 'ORACLE',
    'CRM': 'SALESFORCE',
    'ADBE': 'ADOBE',
    'NOW': 'SERVICENOW',
    'PANW': 'PALO ALTO NETWORKS',

    # Industrial & manufacturing
    'MMM': '3M',
    'HON': 'HONEYWELL',
    'UNH': 'UNITEDHEALTH GROUP',
    'BA': 'BOEING',
    'LMT': 'LOCKHEED MARTIN',
    'RTX': 'RAYTHEON TECHNOLOGIES',
    'GD': 'GENERAL DYNAMICS',
    'NOC': 'NORTHROP GRUMMAN',

    # Healthcare
    'JNJ': 'JOHNSON AND JOHNSON',
    'UNH': 'UNITEDHEALTH GROUP',
    'PFE': 'PFIZER',
    'ABBV': 'ABBVIE',
    'MRK': 'MERCK',
    'TMO': 'THERMO FISHER SCIENTIFIC',
    'ABT': 'ABBOTT LABORATORIES',
    'DHR': 'DANAHER',
    'BMY': 'BRISTOL MYERS SQUIBB',
    'AMGN': 'AMGEN',
    'GILD': 'GILEAD SCIENCES',
    'CVS': 'CVS HEALTH',
    'CI': 'CIGNA',

    # Finance
    'BRK.A': 'BERKSHIRE HATHAWAY',
    'BRK.B': 'BERKSHIRE HATHAWAY',
    'V': 'VISA',
    'MA': 'MASTERCARD',
    'JPM': 'JPMORGAN CHASE',
    'BAC': 'BANK OF AMERICA',
    'C': 'CITIGROUP',
    'WFC': 'WELLS FARGO',
    'GS': 'GOLDMAN SACHS',
    'MS': 'MORGAN STANLEY',
    'BLK': 'BLACKROCK',
    'SCHW': 'CHARLES SCHWAB',

    # Energy
    'COP': 'CONOCOPHILLIPS',
    'EOG': 'EOG RESOURCES',
    'SLB': 'SCHLUMBERGER',
    'HAL': 'HALLIBURTON',
    'PXD': 'PIONEER NATURAL RESOURCES',
    'MPC': 'MARATHON PETROLEUM',
    'PSX': 'PHILLIPS 66',

    # Consumer goods
    'NKE': 'NIKE',
    'MCD': 'MCDONALDS',
    'SBUX': 'STARBUCKS',
    'KO': 'COCA COLA',
    'PEP': 'PEPSICO',
    'COST': 'COSTCO',
    'WMT': 'WALMART',
    'HD': 'HOME DEPOT',
    'LOW': 'LOWES',
    'TGT': 'TARGET',

    # Telecom
    'VZ': 'VERIZON COMMUNICATIONS',
    'T': 'AT&T',
    'TMUS': 'T MOBILE US',

    # Utilities
    'NEE': 'NEXT ERA ENERGY',
    'DUK': 'DUKE ENERGY',
    'SO': 'SOUTHERN COMPANY',
    'D': 'DOMINION ENERGY',

    # Real estate
    'AMT': 'PROLOGIS',
    'PLD': 'PROLOGIS',
    'CCI': 'CROWN CASTLE',
    'EQIX': 'EQUINIX',

    # Chinese companies (common tickers)
    'BABA': 'ALIBABA GROUP',
    'TCEHY': 'TENCENT',
    'PDD': 'PINDUODUO',
    'JD': 'JD.COM',
    'NTES': 'NETEASE',

    # Other common abbreviations
    'P&G': 'PROCTER AND GAMBLE',
    'P and G': 'PROCTER AND GAMBLE',
    'J&J': 'JOHNSON AND JOHNSON',
    'J and J': 'JOHNSON AND JOHNSON',
    'UPS': 'UNITED PARCEL SERVICE',
    'FDX': 'FEDEX',
}


def remove_location_qualifiers(institution_name: str) -> str:
    """
    Remove location qualifiers from institution names.

    Examples:
    - "Google (United States)" → "Google"
    - "Microsoft (United Kingdom)" → "Microsoft"
    - "IBM Research (China)" → "IBM Research"

    Patterns to remove:
    - (United States)
    - (United Kingdom)
    - (U.S.A.)
    - (USA)
    - (China)
    - (Japan)
    - (Germany)
    - (France)
    - (India)
    - (Canada)
    - etc.
    """
    if not institution_name:
        return institution_name

    name = institution_name

    # Common location patterns in parentheses
    location_patterns = [
        r'\s*\(United States\)',
        r'\s*\(United Kingdom\)',
        r'\s*\(U\.S\.A\.\)',
        r'\s*\(USA\)',
        r'\s*\(U\.S\.\)',
        r'\s*\(US\)',
        r'\s*\(China\)',
        r'\s*\(Peoples Republic of China\)',
        r'\s*\(Japan\)',
        r'\s*\(Germany\)',
        r'\s*\(France\)',
        r'\s*\(United Kingdom\)',
        r'\s*\(UK\)',
        r'\s*\(India\)',
        r'\s*\(Canada\)',
        r'\s*\(South Korea\)',
        r'\s*\(Republic of Korea\)',
        r'\s*\(Taiwan\)',
        r'\s*\(Singapore\)',
        r'\s*\(Australia\)',
        r'\s*\(Italy\)',
        r'\s*\(Spain\)',
        r'\s*\(Switzerland\)',
        r'\s*\(Netherlands\)',
        r'\s*\(Belgium\)',
        r'\s*\(Sweden\)',
        r'\s*\(Norway\)',
        r'\s*\(Denmark\)',
        r'\s*\(Finland\)',
        r'\s*\(Brazil\)',
        r'\s*\(Mexico\)',
        r'\s*\(Argentina\)',
        r'\s*\(Israel\)',
        r'\s*\(Russia\)',
        r'\s*\(South Africa\)',
        r'\s*\(Hong Kong\)',
        r'\s*\(New Zealand\)',
        r'\s*\(Ireland\)',
        r'\s*\(Austria\)',
        r'\s*\(Poland\)',
        r'\s*\(Czech Republic\)',
        r'\s*\(Portugal\)',
        r'\s*\(Greece\)',
        r'\s*\(Turkey\)',
        r'\s*\(Thailand\)',
        r'\s*\(Malaysia\)',
        r'\s*\(Indonesia\)',
        r'\s*\(Philippines\)',
        r'\s*\(Vietnam\)',
        r'\s*\(Pakistan\)',
        r'\s*\(Bangladesh\)',
        r'\s*\(Sri Lanka\)',
        r'\s*\(Nepal\)',
        r'\s*\(Myanmar\)',
        r'\s*\(Cambodia\)',
        r'\s*\(Laos\)',
        r'\s*\(Egypt\)',
        r'\s*\(Nigeria\)',
        r'\s*\(Kenya\)',
        r'\s*\(South Africa\)',
        r'\s*\(Morocco\)',
        r'\s*\(Algeria\)',
        r'\s*\(Tunisia\)',
        r'\s*\(Ghana\)',
        r'\s*\(Ethiopia\)',
        r'\s*\(Tanzania\)',
        r'\s*\(Uganda\)',
        r'\s*\(Zimbabwe\)',
        r'\s*\(Zambia\)',
        r'\s*\(Angola\)',
        r'\s*\(Sudan\)',
        r'\s*\(Libya\)',
        r'\s*\(Cameroon\)',
        r'\s*\(Ivory Coast\)',
        r'\s*\(Senegal\)',
        r'\s*\(Mali\)',
        r'\s*\(Burkina Faso\)',
        r'\s*\(Niger\)',
        r'\s*\(Chad\)',
        r'\s*\(Benin\)',
        r'\s*\(Togo\)',
        r'\s*\(Guinea\)',
        r'\s*\(Sierra Leone\)',
        r'\s*\(Liberia\)',
        r'\s*\(Guinea-Bissau\)',
        r'\s*\(Gambia\)',
        r'\s*\(Mauritania\)',
        r'\s*\(Mali\)',
        r'\s*\(Niger\)',
        r'\s*\(Chad\)',
        r'\s*\(Central African Republic\)',
        r'\s*\(Congo\)',
        r'\s*\(Democratic Republic of Congo\)',
        r'\s*\(Rwanda\)',
        r'\s*\(Burundi\)',
        r'\s*\(Eritrea\)',
        r'\s*\(Djibouti\)',
        r'\s*\(Somalia\)',
        r'\s*\(Mozambique\)',
        r'\s*\(Madagascar\)',
        r'\s*\(Malawi\)',
        r'\s*\(Zambia\)',
        r'\s*\(Zimbabwe\)',
        r'\s*\(Botswana\)',
        r'\s*\(Namibia\)',
        r'\s*\(Lesotho\)',
        r'\s*\(Eswatini\)',
        r'\s*\(South Sudan\)',
    ]

    # Remove location patterns
    import re
    for pattern in location_patterns:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)

    # Clean up extra spaces
    name = re.sub(r'\s+', ' ', name).strip()

    return name


def match_abbreviation_to_firm(institution_normalized: str,
                               institution_display: str,
                               firms_df: pl.DataFrame) -> List[Dict]:
    """
    Match institution to firms using abbreviation dictionary.
    """
    matches = []

    # Try matching as abbreviation
    if institution_normalized in ABBREVIATION_DICT:
        full_name = ABBREVIATION_DICT[institution_normalized]

        # Find firms with this full name
        matching_firms = firms_df.filter(
            (pl.col('conm_clean') == full_name) |
            (pl.col('conml_clean') == full_name)
        )

        if len(matching_firms) > 0:
            for firm_row in matching_firms.iter_rows(named=True):
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row.get('conm'),
                    'institution_display_name': institution_display,
                    'normalized_institution': institution_normalized,
                    'match_type': 'abbreviation_dict',
                    'match_confidence': 0.95,
                    'match_method': 'abbreviation_to_full',
                    'full_name_matched': full_name,
                })

    # Try reverse matching: institution is full name, check if firm is abbreviation
    for abbrev, full_name in ABBREVIATION_DICT.items():
        if institution_normalized == full_name:
            # Find firms with this abbreviation
            matching_firms = firms_df.filter(
                (pl.col('conm_clean') == abbrev) |
                (pl.col('conml_clean') == abbrev)
            )

            if len(matching_firms) > 0:
                for firm_row in matching_firms.iter_rows(named=True):
                    matches.append({
                        'GVKEY': firm_row['GVKEY'],
                        'LPERMNO': firm_row.get('LPERMNO'),
                        'firm_conm': firm_row.get('conm'),
                        'institution_display_name': institution_display,
                        'normalized_institution': institution_normalized,
                        'match_type': 'abbreviation_dict',
                        'match_confidence': 0.95,
                        'match_method': 'full_to_abbreviation',
                        'abbreviation_matched': abbrev,
                    })

    return matches


def match_ticker_to_firm(institution_normalized: str,
                         institution_display: str,
                         firms_df: pl.DataFrame) -> List[Dict]:
    """
    Match institution ticker to firm ticker.
    """
    matches = []

    # Extract potential ticker from institution name
    # Pattern: "GOOGLE (GOOGL)" or "MICROSOFT MSFT"
    import re

    # Check for ticker in parentheses: (GOOGL)
    ticker_pattern = r'\(([A-Z]{1,6})\)'
    ticker_match = re.search(ticker_pattern, institution_normalized)

    if ticker_match:
        ticker = ticker_match.group(1)
        # Find firms with this ticker
        matching_firms = firms_df.filter(
            pl.col('tic') == ticker
        )

        if len(matching_firms) > 0:
            for firm_row in matching_firms.iter_rows(named=True):
                matches.append({
                    'GVKEY': firm_row['GVKEY'],
                    'LPERMNO': firm_row.get('LPERMNO'),
                    'firm_conm': firm_row.get('conm'),
                    'institution_display_name': institution_display,
                    'normalized_institution': institution_normalized,
                    'match_type': 'abbreviation_dict',
                    'match_confidence': 0.97,  # Higher confidence for ticker match
                    'match_method': 'ticker_in_parens',
                    'ticker_matched': ticker,
                })

    return matches


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("ABBREVIATION MATCHING FOR PUBLICATIONS")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/6] Loading data...")

    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)

    logger.info(f"  Loaded {len(institutions):,} institutions")
    logger.info(f"  Loaded {len(firms):,} firms")

    # Load baseline to exclude already matched
    if BASELINE_MATCHES.exists():
        baseline = pl.read_parquet(BASELINE_MATCHES)
        baseline_inst_ids = set(baseline['institution_id'].to_list())
        baseline_gvkeys = set(baseline['gvkey'].to_list())
        logger.info(f"  Baseline: {baseline_gvkeys} firms, {baseline_inst_ids} institutions")
    else:
        baseline_inst_ids = set()
        baseline_gvkeys = set()
        logger.info("  No baseline found")

    # Step 2: Filter to unmatched institutions
    logger.info("\n[2/6] Filtering to unmatched institutions...")
    unmatched_inst = institutions.filter(
        ~pl.col('institution_id').is_in(baseline_inst_ids)
    )
    logger.info(f"  Unmatched institutions: {len(unmatched_inst):,}")

    # Step 3: Remove location qualifiers
    logger.info("\n[3/6] Removing location qualifiers from institution names...")

    # Create normalized names without location using apply
    display_names_no_location = []
    for name in unmatched_inst['display_name'].to_list():
        display_names_no_location.append(remove_location_qualifiers(name))

    unmatched_inst = unmatched_inst.with_columns(
        pl.Series('display_name_no_location', display_names_no_location)
    )

    # Show examples
    logger.info("\n  Examples of location removal:")
    sample_df = unmatched_inst.filter(
        pl.col('display_name') != pl.col('display_name_no_location')
    ).head(10)

    for row in sample_df.iter_rows(named=True):
        logger.info(f"    {row['display_name'][:50]:<50} → {row['display_name_no_location'][:40]}")

    location_removed_count = unmatched_inst.filter(
        pl.col('display_name') != pl.col('display_name_no_location')
    ).height
    logger.info(f"\n  Location qualifiers removed from {location_removed_count:,} institutions")

    # Step 4: Run abbreviation matching
    logger.info("\n[4/6] Running abbreviation matching...")

    all_matches = []
    total = len(unmatched_inst)
    matched_count = 0

    for i, inst_row in enumerate(unmatched_inst.iter_rows(named=True)):
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} ({len(all_matches):,} matches so far)...")

        institution_id = inst_row['institution_id']
        display_name = inst_row['display_name']
        normalized_name = inst_row.get('normalized_name') or inst_row.get('display_name_no_location', '')

        if not normalized_name:
            continue

        # Remove location for matching
        normalized_no_location = remove_location_qualifiers(normalized_name)

        # Try abbreviation matching
        abbrev_matches = match_abbreviation_to_firm(
            normalized_no_location.upper(),
            display_name,
            firms
        )

        # Try ticker matching
        ticker_matches = match_ticker_to_firm(
            normalized_no_location.upper(),
            display_name,
            firms
        )

        # Combine matches
        combined_matches = abbrev_matches + ticker_matches

        if combined_matches:
            matched_count += 1
            # Add institution_id to each match
            for match in combined_matches:
                match['institution_id'] = institution_id

            all_matches.extend(combined_matches)

    logger.info(f"\n  Completed. Found {len(all_matches)} total matches")
    logger.info(f"  Institutions matched: {matched_count:,}")

    if not all_matches:
        logger.warning("  No matches found!")
        return

    # Step 5: Deduplicate
    logger.info("\n[5/6] Deduplicating...")

    matches_df = pl.DataFrame(all_matches)

    # Keep highest confidence match per institution-firm pair
    matches_df = (
        matches_df
        .sort(['institution_id', 'GVKEY', 'match_confidence'], descending=[False, False, True])
        .unique(subset=['institution_id', 'GVKEY'], keep='first')
    )

    logger.info(f"  After deduplication: {len(matches_df):,} unique institution-firm matches")
    logger.info(f"  Unique institutions: {matches_df['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {matches_df['GVKEY'].n_unique():,}")

    # Step 6: Save
    logger.info("\n[6/6] Saving...")
    matches_df.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved to: {OUTPUT_FILE}")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("ABBREVIATION MATCHING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total matches: {len(matches_df)}")
    logger.info(f"Unique institutions: {matches_df['institution_id'].n_unique()}")
    logger.info(f"Unique firms: {matches_df['GVKEY'].n_unique()}")

    # Match method distribution
    if 'match_method' in matches_df.columns:
        method_counts = matches_df.group_by('match_method').agg(
            pl.len().alias('count')
        ).sort('count', descending=True)
        logger.info("\nMatch method distribution:")
        for row in method_counts.iter_rows(named=True):
            logger.info(f"  {row['match_method']}: {row['count']:,} matches")

    # Confidence distribution
    logger.info(f"\nConfidence statistics:")
    logger.info(f"  Mean: {matches_df['match_confidence'].mean():.3f}")
    logger.info(f"  Min: {matches_df['match_confidence'].min():.3f}")
    logger.info(f"  Max: {matches_df['match_confidence'].max():.3f}")

    # Show top matches
    logger.info("\nTop 20 matches by institution name:")
    top_matches = matches_df.sort('institution_display_name').head(20)
    for i, row in enumerate(top_matches.iter_rows(named=True), 1):
        logger.info(f"  {i}. {row['institution_display_name'][:50]:<50}")
        logger.info(f"     → {row['firm_conm'][:50]} (conf: {row['match_confidence']:.2f})")

    logger.info("\n" + "=" * 80)
    logger.info("ABBREVIATION MATCHING COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
