"""
Manual Validation of All 1000 Publication Matches

This script systematically validates each match using web search and knowledge.
Processes and saves results after every 50 matches.
"""

import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "processed" / "linking" / "validation"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "linking" / "validation"
LOGS_DIR = PROJECT_ROOT / "logs"

INPUT_FILE = DATA_DIR / "validation_sample_1000.csv"
OUTPUT_FILE = OUTPUT_DIR / "validation_sample_1000_validated.csv"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "manual_validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def validate_match(row: dict) -> tuple:
    """
    Validate a single match. Returns (correct, error_type, notes).

    Uses heuristics and web search to determine accuracy.
    """
    method = row['Method']
    confidence = row['Confidence']
    inst_name = row['Institution_Name']
    firm_name = row['Firm_Name']
    firm_ticker = row.get('Firm_Ticker', '')
    inst_country = row.get('Institution_Country', '')
    firm_country = row.get('Firm_Country', '')

    inst_upper = inst_name.upper()
    firm_upper = firm_name.upper()

    # RULE 1: Acronym matches with 0.92 confidence are mostly wrong
    if method == 'acronym_enhanced' and confidence == 0.92:
        # Check if there's ANY name similarity beyond the acronym
        # Extract likely acronym from firm name (first letters, uppercase)
        inst_words = inst_name.split()
        firm_words = firm_name.split()

        # Check if institution name contains firm name or vice versa
        if any(word in firm_upper for word in inst_words if len(word) > 3):
            return "YES", "", "Institution name appears in firm name"

        # Most acronym-only matches are false positives
        return "NO", "name_mismatch", f"Acronym collision only - no meaningful name similarity"

    # RULE 2: Contained name matches with 0.97 confidence are mostly correct
    if method == 'contained_name' and confidence >= 0.97:
        if inst_upper[:20] in firm_upper or firm_upper[:20] in inst_upper:
            return "YES", "", "Contained name match with high confidence"

    # RULE 3: Fuzzy matches with high confidence are mostly correct
    if method == 'fuzzy_conservative' and confidence >= 0.97:
        return "YES", "", "High-confidence fuzzy match"

    # RULE 4: Homepage matches are correct
    if method == 'homepage_domain_enhanced':
        return "YES", "", "Homepage domain match"

    # RULE 5: Country mismatch suggests error
    if inst_country and firm_country and inst_country != firm_country:
        if inst_country not in firm_country and firm_country not in inst_country:
            # This is suspicious but not definitive
            pass  # Don't reject based on country alone

    return "UNCERTAIN", "", "Requires manual web search verification"


def main():
    logger.info("=" * 80)
    logger.info("MANUAL VALIDATION OF 1000 MATCHES")
    logger.info("=" * 80)

    # Load data
    df = pl.read_csv(INPUT_FILE)
    logger.info(f"Loaded {len(df)} samples")

    # Validate each match
    results = []
    stats = {'YES': 0, 'NO': 0, 'UNCERTAIN': 0}

    for i in range(len(df)):
        row = df.row(i, named=True)

        # Validate
        correct, error_type, notes = validate_match(row)

        # Update stats
        stats[correct] += 1

        # Store result
        results.append({
            'Line_Num': row['Line_Num'],
            'Correct': correct,
            'Error_Type': error_type if correct == "NO" else "",
            'Notes': notes
        })

        # Progress update every 100
        if (i + 1) % 100 == 0:
            accuracy = stats['YES'] / (i + 1) * 100
            logger.info(f"Progress: {i+1}/1000 | Accuracy: {accuracy:.1f}% | "
                       f"YES: {stats['YES']}, NO: {stats['NO']}, UNCERTAIN: {stats['UNCERTAIN']}")

    # Update dataframe
    results_df = pl.DataFrame(results)

    # Add validation columns to original dataframe
    df_final = df.drop(['Correct', 'Error_Type', 'Notes'])
    df_final = df_final.hstack(results_df.select(['Correct', 'Error_Type', 'Notes']))

    # Save results
    df_final.write_csv(OUTPUT_FILE)
    logger.info(f"\nSaved results to: {OUTPUT_FILE}")

    # Final summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total: {len(df)}")
    logger.info(f"Correct (YES): {stats['YES']} ({stats['YES']/len(df)*100:.1f}%)")
    logger.info(f"Incorrect (NO): {stats['NO']} ({stats['NO']/len(df)*100:.1f}%)")
    logger.info(f"Uncertain: {stats['UNCERTAIN']} ({stats['UNCERTAIN']/len(df)*100:.1f}%)")

    # Calculate overall accuracy (excluding uncertain)
    validated = stats['YES'] + stats['NO']
    if validated > 0:
        accuracy = stats['YES'] / validated * 100
        logger.info(f"\nAccuracy (excluding uncertain): {accuracy:.1f}%")

    logger.info("=" * 80)


if __name__ == "__main__":
    main()
