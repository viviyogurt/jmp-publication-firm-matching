"""
Validation Script for Optimized Publication-Firm Matching

This script validates the accuracy of the optimized matching results:
1. Creates a random sample of 200 matches
2. Outputs for manual validation
3. Calculates accuracy statistics
4. Identifies potential false positives

Target: >95% accuracy
"""
import polars as pl
from pathlib import Path
import logging
import random

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "linking"

INPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_optimized.parquet"
VALIDATION_SAMPLE = OUTPUT_DIR / "validation_sample_200.csv"
VALIDATION_LOG = LOGS_DIR / "validation_optimized_matching.log"

LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(VALIDATION_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Main Validation Workflow
# ============================================================================

def main():
    """Main validation workflow."""

    logger.info("=" * 80)
    logger.info("VALIDATION: Optimized Publication-Firm Matching")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/5] Loading data...")
    df = pl.read_parquet(INPUT_FILE)
    logger.info(f"  Loaded {len(df):,} matches")
    logger.info(f"  Unique firms: {df['gvkey'].n_unique():,}")
    logger.info(f"  Unique institutions: {df['institution_id'].n_unique():,}")

    # Statistics
    logger.info("\n[2/5] Analyzing match distribution...")
    logger.info(f"\nConfidence Distribution:")
    for conf in sorted(df['confidence'].unique(), reverse=True):
        count = df.filter(pl.col('confidence') == conf).shape[0]
        pct = count / len(df) * 100
        logger.info(f"  {conf}: {count:,} ({pct:.1f}%)")

    logger.info(f"\nMatch Method Distribution:")
    method_counts = df.group_by('match_method').agg(
        pl.len().alias('count')
    ).sort('count', descending=True)

    for row in method_counts.iter_rows(named=True):
        pct = row['count'] / len(df) * 100
        logger.info(f"  {row['match_method']:30}: {row['count']:>6} ({pct:>5.1f}%)")

    # Create random sample
    logger.info("\n[3/5] Creating random sample for manual validation...")
    random_seed = 42
    sample_size = 200

    # Set seed for reproducibility
    random.seed(random_seed)

    # Get random sample of row indices
    all_indices = list(range(len(df)))
    sampled_indices = random.sample(all_indices, min(sample_size, len(all_indices)))

    # Create sample dataframe
    sample_df = df[sampled_indices].sort('confidence', descending=True)

    # Select relevant columns for validation
    validation_columns = [
        'institution_id',
        'display_name',
        'gvkey',
        'conm',
        'match_method',
        'confidence',
        'paper_count',
    ]

    validation_sample = sample_df.select(validation_columns)

    # Save sample
    logger.info(f"\n[4/5] Saving validation sample...")
    validation_sample.write_csv(VALIDATION_SAMPLE)
    logger.info(f"  Saved {len(validation_sample)} matches to {VALIDATION_SAMPLE}")

    # Validation checklist
    logger.info("\n[5/5] Creating validation checklist...")

    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION CHECKLIST")
    logger.info("=" * 80)

    logger.info("\nPlease validate the sample file: " + str(VALIDATION_SAMPLE))
    logger.info("\nFor each match, mark:")
    logger.info("  ✓ = Correct")
    logger.info("  ✗ = Incorrect (false positive)")
    logger.info("  ? = Unsure")

    logger.info("\nExpected accuracy: >95%")
    logger.info(f"Target correct: {int(sample_size * 0.95)}+ out of {sample_size}")

    logger.info("\nValidation steps:")
    logger.info("1. Open the CSV file in Excel/spreadsheet")
    logger.info("2. Add a column 'validation_status'")
    logger.info("3. Mark each row as ✓, ✗, or ?")
    logger.info("4. Count correct vs incorrect")
    logger.info("5. Report accuracy")

    # Sample first 20 for quick review
    logger.info("\n" + "=" * 80)
    logger.info("SAMPLE OF MATCHES (Top 20 by confidence)")
    logger.info("=" * 80)

    logger.info("\n{:5} {:50} {:40} {:10} {:8}".format(
        "", "Institution", "Firm", "Method", "Conf"
    ))
    logger.info("-" * 120)

    for i, row in enumerate(sample_df.head(20).iter_rows(named=True), 1):
        inst_name = row['display_name'][:48] if row['display_name'] else "N/A"
        firm_name = row['conm'][:38] if row['conm'] else "N/A"
        method = row['match_method'][:8] if row['match_method'] else "N/A"
        conf = f"{row['confidence']:.2f}" if row['confidence'] else "N/A"

        logger.info("{:>5} {:50} {:40} {:10} {:8}".format(
            f"{i}.", inst_name, firm_name, method, conf
        ))

    # Analysis by confidence level
    logger.info("\n" + "=" * 80)
    logger.info("EXPECTED ACCURACY BY CONFIDENCE LEVEL")
    logger.info("=" * 80)

    logger.info("\nConfidence 0.98:")
    conf_098 = sample_df.filter(pl.col('confidence') == 0.98)
    logger.info(f"  Count: {len(conf_098)}")
    logger.info(f"  Expected accuracy: 98-99%")
    logger.info(f"  Methods: Homepage exact, Alternative names")

    logger.info("\nConfidence 0.97:")
    conf_097 = sample_df.filter(pl.col('confidence') == 0.97)
    logger.info(f"  Count: {len(conf_097)}")
    logger.info(f"  Expected accuracy: 95-97%")
    logger.info(f"  Methods: Ticker acronyms")

    logger.info("\nConfidence 0.96-0.94:")
    conf_096_094 = sample_df.filter(
        (pl.col('confidence') >= 0.94) &
        (pl.col('confidence') < 0.97)
    )
    logger.info(f"  Count: {len(conf_096_094)}")
    logger.info(f"  Expected accuracy: 92-95%")
    logger.info(f"  Methods: Combination methods")

    # Potential issues to check
    logger.info("\n" + "=" * 80)
    logger.info("POTENTIAL ISSUES TO CHECK")
    logger.info("=" * 80)

    logger.info("\n1. Short institution names (<8 characters)")
    short_names = sample_df.filter(
        pl.col('display_name').str.len_chars() < 8
    )
    logger.info(f"   Found: {len(short_names)} matches")

    for row in short_names.iter_rows(named=True):
        logger.info(f"     - '{row['display_name']}' → {row['conm']}")

    logger.info("\n2. Generic terms in institution names")
    generic_terms = ['international', 'group', 'technologies', 'innovations',
                    'associates', 'solutions', 'services', 'scientific']

    for term in generic_terms:
        matches = sample_df.filter(
            pl.col('display_name').str.to_lowercase().str.contains(term)
        )
        if len(matches) > 0:
            logger.info(f"\n   Institutions with '{term}': {len(matches)}")
            for row in matches.head(5).iter_rows(named=True):
                logger.info(f"     - {row['display_name'][:50]} → {row['conm'][:40]}")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)

    logger.info(f"\nTotal matches: {len(df):,}")
    logger.info(f"Validation sample: {len(validation_sample)}")
    logger.info(f"Random seed: {random_seed}")
    logger.info(f"Target accuracy: >95%")
    logger.info(f"Target correct: {int(sample_size * 0.95)}+")

    logger.info("\nNext steps:")
    logger.info("1. Review validation sample: " + str(VALIDATION_SAMPLE))
    logger.info("2. Mark each match as correct (✓) or incorrect (✗)")
    logger.info("3. Calculate accuracy: correct / total")
    logger.info("4. If accuracy <95%, identify problematic patterns")

    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SETUP COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
