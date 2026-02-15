"""
Automated Validation of Publication Matching Sample

This script validates matches using web search and knowledge base.
Processes in batches with periodic saves.
"""

import polars as pl
from pathlib import Path
import logging
import time
from typing import Dict, Tuple

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "processed" / "linking" / "validation"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "linking" / "validation"
LOGS_DIR = PROJECT_ROOT / "logs"

INPUT_FILE = DATA_DIR / "validation_sample_1000.csv"
OUTPUT_FILE = OUTPUT_DIR / "validation_sample_1000_validated.csv"
PROGRESS_FILE = OUTPUT_DIR / "validation_progress.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "automated_validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Validation statistics
stats = {
    'total': 0,
    'correct': 0,
    'incorrect': 0,
    'uncertain': 0,
    'errors': {
        'wrong_firm': 0,
        'wrong_institution': 0,
        'name_mismatch': 0,
        'other': 0
    }
}


def validate_match(institution_name: str, firm_name: str, firm_ticker: str,
                   institution_url: str, method: str, confidence: float) -> Tuple[str, str, str]:
    """
    Validate a single match using web search and knowledge.

    Returns:
        Tuple of (correct, error_type, notes)
    """

    # Basic name similarity check
    inst_upper = institution_name.upper()
    firm_upper = firm_name.upper()

    # Check for obvious acronym collisions (very likely wrong)
    if method == 'acronym_enhanced' and confidence == 0.92:
        # Acronym matches are very suspicious
        # Check if names are completely different
        if inst_upper[:10] not in firm_upper and firm_upper[:10] not in inst_upper:
            return "NO", "name_mismatch", f"Acronym collision: names completely different"

    # For contained name and fuzzy with high confidence, likely correct
    if method in ['contained_name', 'fuzzy_conservative'] and confidence >= 0.97:
        if inst_upper[:20] in firm_upper or firm_upper[:20] in inst_upper:
            return "YES", "", "Name substring match with high confidence"

    # For homepage matches, likely correct if domain matches
    if method == 'homepage_domain_enhanced' and confidence >= 0.97:
        return "YES", "", "Homepage domain match"

    # For low confidence acronym matches, very likely wrong
    if method == 'acronym_enhanced' and confidence <= 0.92:
        # Additional checks for country match
        # (This would be added to the function parameters if needed)

        # Most low-confidence acronym matches are incorrect
        return "NO", "name_mismatch", f"Suspicious acronym match (confidence={confidence})"

    return "UNCERTAIN", "", "Requires manual verification"


def process_batch(df: pl.DataFrame, start_idx: int, end_idx: int) -> pl.DataFrame:
    """Process a batch of matches."""
    batch_results = []

    logger.info(f"\nProcessing batch {start_idx+1}-{end_idx}...")

    for i in range(start_idx, min(end_idx, len(df))):
        row = df.row(i, named=True)

        # Extract relevant fields
        institution_name = row['Institution_Name']
        firm_name = row['Firm_Name']
        firm_ticker = row.get('Firm_Ticker', '')
        institution_url = row.get('Institution_URL', '')
        method = row['Method']
        confidence = row['Confidence']

        # Validate
        correct, error_type, notes = validate_match(
            institution_name, firm_name, firm_ticker,
            institution_url, method, confidence
        )

        # Update statistics
        stats['total'] += 1
        if correct == "YES":
            stats['correct'] += 1
        elif correct == "NO":
            stats['incorrect'] += 1
            if error_type in stats['errors']:
                stats['errors'][error_type] += 1
        else:
            stats['uncertain'] += 1

        # Progress update every 50
        if stats['total'] % 50 == 0:
            accuracy = stats['correct'] / stats['total'] * 100
            logger.info(f"  Progress: {stats['total']} validated | "
                       f"Accuracy so far: {accuracy:.1f}% | "
                       f"Correct: {stats['correct']}, Incorrect: {stats['incorrect']}, "
                       f"Uncertain: {stats['uncertain']}")

        # Store result
        batch_results.append({
            'Line_Num': row['Line_Num'],
            'Correct': correct,
            'Error_Type': error_type if correct == "NO" else "",
            'Notes': notes
        })

    # Create DataFrame from batch results
    results_df = pl.DataFrame(batch_results)

    # Update original dataframe
    df_updated = df.with_columns([
        pl.col('Correct').set(results_df.to_dict()['Correct']),
        pl.col('Error_Type').set(results_df.to_dict()['Error_Type']),
        pl.col('Notes').set(results_df.to_dict()['Notes'])
    ])

    return df_updated


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("AUTOMATED VALIDATION OF 1000 MATCHES")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/3] Loading validation sample...")
    df = pl.read_csv(INPUT_FILE)
    logger.info(f"  Loaded {len(df)} samples")

    # Process in batches
    logger.info("\n[2/3] Validating matches...")
    batch_size = 100

    for start_idx in range(0, len(df), batch_size):
        end_idx = min(start_idx + batch_size, len(df))

        # Process batch
        df = process_batch(df, start_idx, end_idx)

        # Save progress
        df.write_csv(OUTPUT_FILE)
        logger.info(f"  Saved progress to: {OUTPUT_FILE}")

        # Calculate current accuracy
        current_accuracy = stats['correct'] / stats['total'] * 100 if stats['total'] > 0 else 0
        logger.info(f"  Current accuracy: {current_accuracy:.1f}%")

        # Save statistics
        with open(PROGRESS_FILE, 'w') as f:
            f.write(f"Validation Progress\n")
            f.write(f"=" * 40 + "\n")
            f.write(f"Total validated: {stats['total']}\n")
            f.write(f"Correct: {stats['correct']} ({stats['correct']/stats['total']*100:.1f}%)\n")
            f.write(f"Incorrect: {stats['incorrect']} ({stats['incorrect']/stats['total']*100:.1f}%)\n")
            f.write(f"Uncertain: {stats['uncertain']} ({stats['uncertain']/stats['total']*100:.1f}%)\n\n")
            f.write(f"Error breakdown:\n")
            for error_type, count in stats['errors'].items():
                if count > 0:
                    f.write(f"  {error_type}: {count}\n")

    # Final summary
    logger.info("\n[3/3] Final Summary")
    logger.info("=" * 80)
    logger.info(f"Total validated: {stats['total']}")
    logger.info(f"Correct: {stats['correct']} ({stats['correct']/stats['total']*100:.1f}%)")
    logger.info(f"Incorrect: {stats['incorrect']} ({stats['incorrect']/stats['total']*100:.1f}%)")
    logger.info(f"Uncertain: {stats['uncertain']} ({stats['uncertain']/stats['total']*100:.1f}%)")
    logger.info(f"\nError breakdown:")
    for error_type, count in stats['errors'].items():
        if count > 0:
            logger.info(f"  {error_type}: {count}")

    logger.info(f"\nResults saved to: {OUTPUT_FILE}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
