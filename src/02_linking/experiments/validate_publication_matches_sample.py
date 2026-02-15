"""
Publication Matching Validation - Sample 1000 Matches

This script creates a stratified sample of 1000 matches for manual validation.

Stratification:
1. Match type (acronym_enhanced, contained_name, fuzzy_conservative, homepage)
2. Confidence level (0.92, 0.97, 0.98+)
3. Multi-match status (single vs. multiple firms per institution)

Output: CSV file with all relevant information + columns for manual verification
"""

import polars as pl
from pathlib import Path
import logging

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
OUTPUT_DIR = DATA_PROCESSED_LINK / "validation"
LOGS_DIR = PROJECT_ROOT / "logs"

INPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_final.parquet"
COMPUSTAT_STANDARDIZED = PROJECT_ROOT / "data" / "interim" / "compustat_firms_standardized.parquet"
INSTITUTIONS_MASTER = PROJECT_ROOT / "data" / "interim" / "publication_institutions_master.parquet"

OUTPUT_FILE = OUTPUT_DIR / "validation_sample_1000.csv"
SEED = 42  # For reproducibility

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "validate_publication_matches_sample.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_and_enrich_data():
    """Load matches and enrich with institution and firm details."""
    logger.info("\n[1/5] Loading and enriching data...")

    # Load matches
    matches = pl.read_parquet(INPUT_FILE)
    logger.info(f"  Loaded {len(matches):,} matches")

    # Load firms for additional context
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    logger.info(f"  Loaded {len(firms):,} firms")

    # Load institutions for additional context
    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    logger.info(f"  Loaded {len(institutions):,} institutions")

    # Enrich matches with firm details
    matches = matches.join(
        firms.select(['GVKEY', 'tic', 'fic', 'busdesc', 'weburl']),
        on='GVKEY',
        how='left'
    )

    # Enrich matches with institution details
    matches = matches.join(
        institutions.select(['institution_id', 'homepage_url', 'country_code']),
        on='institution_id',
        how='left'
    )

    # Count how many firms each institution is matched to
    inst_match_counts = matches.group_by('institution_id').agg(
        pl.len().alias('num_firms_for_institution')
    )
    matches = matches.join(inst_match_counts, on='institution_id', how='left')

    # Count how many institutions each firm is matched to
    firm_match_counts = matches.group_by('GVKEY').agg(
        pl.len().alias('num_institutions_for_firm')
    )
    matches = matches.join(firm_match_counts, on='GVKEY', how='left')

    logger.info(f"  Enriched {len(matches):,} matches")

    return matches


def create_strata(matches: pl.DataFrame):
    """Create strata for stratified sampling."""
    logger.info("\n[2/5] Creating strata...")

    # Create confidence strata
    matches = matches.with_columns(
        pl.when(pl.col('match_confidence') >= 0.98)
        .then(pl.lit('0.98+'))
        .when(pl.col('match_confidence') >= 0.97)
        .then(pl.lit('0.97'))
        .when(pl.col('match_confidence') >= 0.95)
        .then(pl.lit('0.95-0.96'))
        .otherwise(pl.lit('0.92-0.94'))
        .alias('confidence_stratum')
    )

    # Create multi-match strata
    matches = matches.with_columns(
        pl.when(pl.col('num_firms_for_institution') > 1)
        .then(pl.lit('multi-match'))
        .otherwise(pl.lit('single-match'))
        .alias('multi_match_stratum')
    )

    # Create combined stratum key
    matches = matches.with_columns(
        pl.concat_str([
            pl.col('match_type'),
            pl.lit('_'),
            pl.col('confidence_stratum'),
            pl.lit('_'),
            pl.col('multi_match_stratum')
        ]).alias('stratum')
    )

    # Show stratum distribution
    stratum_counts = matches.group_by(['match_type', 'confidence_stratum', 'multi_match_stratum']).agg(
        pl.len().alias('count')
    ).sort('count', descending=True)

    logger.info("\n  Stratum distribution:")
    logger.info("  " + "-" * 80)
    for row in stratum_counts.iter_rows(named=True):
        logger.info(f"    {row['match_type']:<25} {row['confidence_stratum']:<10} {row['multi_match_stratum']:<15} {row['count']:>6,}")

    return matches


def stratified_sample(matches: pl.DataFrame, n_total: int = 1000):
    """Create stratified sample."""
    logger.info(f"\n[3/5] Creating stratified sample (n={n_total})...")

    # Calculate stratum sizes (proportional allocation)
    total = len(matches)
    stratum_sizes = matches.group_by('stratum').agg(
        pl.len().alias('count')
    ).with_columns(
        (pl.col('count') * n_total / total).round(0).cast(int).alias('sample_size')
    )

    # Ensure minimum of 1 per stratum
    stratum_sizes = stratum_sizes.with_columns(
        pl.max_horizontal(['sample_size', pl.lit(1)]).alias('sample_size')
    )

    # Adjust if total exceeds n_total
    total_allocated = stratum_sizes['sample_size'].sum()
    if total_allocated > n_total:
        logger.warning(f"  Total allocated ({total_allocated}) exceeds target ({n_total}), scaling down...")
        stratum_sizes = stratum_sizes.with_columns(
            (pl.col('sample_size') * n_total / total_allocated).round(0).cast(int).alias('sample_size')
        )

    # Sample from each stratum
    sampled_dfs = []
    for row in stratum_sizes.iter_rows(named=True):
        stratum = row['stratum']
        sample_size = row['sample_size']

        stratum_df = matches.filter(pl.col('stratum') == stratum)

        if len(stratum_df) <= sample_size:
            # Take all if stratum is smaller than sample size
            sampled = stratum_df
        else:
            # Random sample
            sampled = stratum_df.sample(n=sample_size, seed=SEED)

        sampled_dfs.append(sampled)

    # Combine samples
    sample = pl.concat(sampled_dfs)

    logger.info(f"  Sampled {len(sample)} matches from {len(stratum_sizes)} strata")

    # Show sample distribution
    sample_dist = sample.group_by(['match_type', 'confidence_stratum']).agg(
        pl.len().alias('sample_count')
    ).sort('match_type', 'confidence_stratum')

    logger.info("\n  Sample distribution:")
    logger.info("  " + "-" * 60)
    for row in sample_dist.iter_rows(named=True):
        logger.info(f"    {row['match_type']:<25} {row['confidence_stratum']:<10} {row['sample_count']:>6,}")

    return sample


def create_validation_format(sample: pl.DataFrame):
    """Create user-friendly validation format."""
    logger.info("\n[4/5] Creating validation format...")

    # Select and rename columns for clarity
    validation_df = sample.select([
        # Match info
        pl.col('match_type').alias('Method'),
        pl.col('match_confidence').alias('Confidence'),

        # Institution info
        pl.col('institution_id').alias('Institution_ID'),
        pl.col('institution_display_name').alias('Institution_Name'),
        pl.col('homepage_url').alias('Institution_URL'),
        pl.col('country_code').alias('Institution_Country'),

        # Firm info
        pl.col('GVKEY').alias('Firm_GVKEY'),
        pl.col('firm_conm').alias('Firm_Name'),
        pl.col('tic').alias('Firm_Ticker'),
        pl.col('fic').alias('Firm_Country'),
        pl.col('busdesc').alias('Firm_Business_Description'),
        pl.col('weburl').alias('Firm_URL'),

        # Quality flags
        pl.col('num_firms_for_institution').alias('Inst_Num_Firms'),
        pl.col('num_institutions_for_firm').alias('Firm_Num_Institutions'),
    ]).sort(['Method', 'Confidence'])

    # Add validation columns
    validation_df = validation_df.with_columns([
        pl.lit(None).cast(str).alias('Correct'),  # YES/NO/UNCERTAIN
        pl.lit(None).cast(str).alias('Error_Type'),  # If incorrect: wrong_firm, wrong_institution, name_mismatch, other
        pl.lit(None).cast(str).alias('Notes'),  # Free text for notes
        pl.lit(None).cast(int).alias('Line_Num'),  # Line number for easy reference
    ])

    # Add line numbers
    validation_df = validation_df.with_columns(
        pl.arange(1, len(validation_df) + 1).alias('Line_Num')
    )

    logger.info(f"  Created validation format with {len(validation_df)} rows")

    return validation_df


def save_sample(validation_df: pl.DataFrame):
    """Save validation sample to CSV."""
    logger.info("\n[5/5] Saving validation sample...")

    validation_df.write_csv(OUTPUT_FILE, separator=',')

    logger.info(f"  Saved to: {OUTPUT_FILE}")

    # Create instructions file
    instructions_file = OUTPUT_DIR / "VALIDATION_INSTRUCTIONS.txt"
    with open(instructions_file, 'w') as f:
        from datetime import datetime
        f.write("=" * 80 + "\n")
        f.write("PUBLICATION MATCHING VALIDATION INSTRUCTIONS\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Sample Size: {len(validation_df)} matches\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write("HOW TO VALIDATE:\n")
        f.write("-" * 40 + "\n\n")
        f.write("For each match (row):\n\n")
        f.write("1. RESEARCH the institution and firm:\n")
        f.write("   - Open institution URL in browser\n")
        f.write("   - Search for firm name + institution name together\n")
        f.write("   - Check Wikipedia, company websites, news articles\n\n")
        f.write("2. DETERMINE if match is correct:\n")
        f.write("   - Correct = institution is indeed this firm (or subsidiary/partner)\n")
        f.write("   - Incorrect = clear error (wrong firm, wrong institution)\n")
        f.write("   - Uncertain = not sure, needs more research\n\n")
        f.write("3. FILL IN columns:\n\n")
        f.write("   Correct (required):\n")
        f.write("   - YES: Match is correct\n")
        f.write("   - NO: Match is incorrect\n")
        f.write("   - UNCERTAIN: Not sure\n\n")
        f.write("   Error_Type (required if Correct=NO):\n")
        f.write("   - wrong_firm: Wrong firm entirely\n")
        f.write("   - wrong_institution: Institution matched to wrong firm\n")
        f.write("   - name_mismatch: Names look similar but are different entities\n")
        f.write("   - other: Describe in Notes\n\n")
        f.write("   Notes (optional):\n")
        f.write("   - Any additional context or uncertainty\n\n")

        f.write("QUALITY CHECKS:\n")
        f.write("-" * 40 + "\n")
        f.write(f"• Matches with Inst_Num_Firms > 1: {(validation_df['Inst_Num_Firms'] > 1).sum():,}\n")
        f.write(f"• Matches with Firm_Num_Institutions > 10: {(validation_df['Firm_Num_Institutions'] > 10).sum():,}\n")
        f.write(f"• Acronym matches: {(validation_df['Method'] == 'acronym_enhanced').sum():,}\n")
        f.write(f"• Contained name matches: {(validation_df['Method'] == 'contained_name').sum():,}\n")
        f.write(f"• Fuzzy matches: {(validation_df['Method'] == 'fuzzy_conservative').sum():,}\n\n")

        f.write("IMPORTANT: Pay special attention to:\n")
        f.write("1. Acronym matches with Confidence 0.92 (may be low quality)\n")
        f.write("2. Institutions matched to multiple firms (Inst_Num_Firms > 1)\n")
        f.write("3. Firms matched to >10 institutions (potential false positives)\n\n")

        f.write("AFTER VALIDATION:\n")
        f.write("-" * 40 + "\n")
        f.write("1. Save the CSV with your validations\n")
        f.write("2. Run: python src/02_linking/analyze_validation_results.py\n")
        f.write("3. Review accuracy report and error analysis\n\n")

        f.write("=" * 80 + "\n")

    logger.info(f"  Saved instructions to: {instructions_file}")

    # Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SAMPLE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total matches in sample: {len(validation_df)}")
    logger.info(f"\nBy method:")
    for method in validation_df['Method'].unique().to_list():
        count = (validation_df['Method'] == method).sum()
        logger.info(f"  {method}: {count:,} ({count/len(validation_df)*100:.1f}%)")

    logger.info(f"\nBy confidence:")
    for conf in ['0.92-0.94', '0.95-0.96', '0.97', '0.98+']:
        if conf == '0.98+':
            count = (validation_df['Confidence'] >= 0.98).sum()
        elif conf == '0.97':
            count = ((validation_df['Confidence'] >= 0.97) & (validation_df['Confidence'] < 0.98)).sum()
        elif conf == '0.95-0.96':
            count = ((validation_df['Confidence'] >= 0.95) & (validation_df['Confidence'] < 0.97)).sum()
        else:
            count = (validation_df['Confidence'] < 0.95).sum()
        logger.info(f"  {conf}: {count:,} ({count/len(validation_df)*100:.1f}%)")

    logger.info(f"\nQuality flags:")
    logger.info(f"  Multi-match institutions: {(validation_df['Inst_Num_Firms'] > 1).sum():,}")
    logger.info(f"  Firms with >10 institutions: {(validation_df['Firm_Num_Institutions'] > 10).sum():,}")

    logger.info("\n" + "=" * 80)


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("PUBLICATION MATCHING VALIDATION - SAMPLE 1000 MATCHES")
    logger.info("=" * 80)

    # Step 1: Load and enrich data
    matches = load_and_enrich_data()

    # Step 2: Create strata
    matches = create_strata(matches)

    # Step 3: Stratified sample
    sample = stratified_sample(matches, n_total=1000)

    # Step 4: Create validation format
    validation_df = create_validation_format(sample)

    # Step 5: Save sample
    save_sample(validation_df)

    logger.info("\nNEXT STEPS:")
    logger.info("1. Open the CSV file: {}".format(OUTPUT_FILE))
    logger.info("2. Follow instructions in: {}".format(OUTPUT_DIR / "VALIDATION_INSTRUCTIONS.txt"))
    logger.info("3. Validate each match (fill in Correct, Error_Type, Notes columns)")
    logger.info("4. Save the CSV and run analysis script")

    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SAMPLE CREATED")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
