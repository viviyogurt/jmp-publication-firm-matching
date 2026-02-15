"""
Stage 7: Validate Publication-Firm Matches

This script creates a validation sample of 1,000 matches stratified by stage
and confidence for manual validation.
"""

import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS_DIR = PROJECT_ROOT / "logs"

FINAL_MATCHES = DATA_PROCESSED_LINK / "publication_firm_matches_final.parquet"
INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
OUTPUT_SAMPLE = DATA_PROCESSED_LINK / "publication_validation_sample_1000.csv"
OUTPUT_REPORT = DATA_PROCESSED_LINK / "publication_validation_accuracy_report.json"
PROGRESS_LOG = LOGS_DIR / "validate_publication_matches.log"

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


def main():
    logger.info("=" * 80)
    logger.info("STAGE 7: VALIDATE PUBLICATION-FIRM MATCHES")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/4] Loading data...")

    if not FINAL_MATCHES.exists():
        raise FileNotFoundError(f"Final matches file not found: {FINAL_MATCHES}")

    matches = pl.read_parquet(FINAL_MATCHES)
    logger.info(f"  Loaded {len(matches):,} matches")

    # Step 2: Load institutions and firms for context
    logger.info("\n[2/4] Loading context data...")

    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)

    logger.info(f"  Loaded {len(institutions):,} institutions")
    logger.info(f"  Loaded {len(firms):,} firms")

    # Step 3: Enrich matches with context
    logger.info("\n[3/4] Enriching matches with context...")

    # Join with institutions
    matches_enriched = matches.join(
        institutions.select(['institution_id', 'display_name', 'normalized_name',
                       'country_code', 'homepage_url', 'paper_count']),
        on='institution_id',
        how='left'
    )

    # Join with firms
    matches_enriched = matches_enriched.join(
        firms.select(['GVKEY', 'conm', 'tic', 'weburl', 'fic']),
        on='GVKEY',
        how='left'
    )

    logger.info(f"  Enriched {len(matches_enriched):,} matches")

    # Step 4: Create stratified sample of 1000
    logger.info("\n[4/4] Creating stratified sample of 1000...")

    # Stratify by match_type and confidence ranges
    # Create confidence buckets (4 buckets for 3 break points)
    matches_enriched = matches_enriched.with_columns(
        pl.col('match_confidence').cut(
            [0.94, 0.96, 0.98],
            labels=['0.93-0.94', '0.94-0.96', '0.96-0.98', '0.98-0.99'],
            left_closed=False
        ).alias('conf_bucket')
    )

    # Sample 1000 with stratification
    sample_size = 1000

    # Get counts per stratum
    strata_counts = matches_enriched.group_by(['match_type', 'conf_bucket']).agg(
        pl.len().alias('count')
    )

    # Calculate desired sample per stratum
    num_strata = len(strata_counts)
    sample_per_stratum = max(1, sample_size // num_strata)

    # Sample from each stratum
    sampled_frames = []
    for stratum_row in strata_counts.iter_rows(named=True):
        match_type_filt = stratum_row['match_type']
        conf_bucket_filt = stratum_row['conf_bucket']

        stratum_data = matches_enriched.filter(
            (pl.col('match_type') == match_type_filt) &
            (pl.col('conf_bucket') == conf_bucket_filt)
        )

        n_to_sample = min(len(stratum_data), sample_per_stratum)
        if n_to_sample > 0:
            sampled = stratum_data.sample(n=n_to_sample, seed=42)
            sampled_frames.append(sampled)

    sample = pl.concat(sampled_frames, how='vertical')

    # Limit to exactly 1000 if oversampled
    if len(sample) > sample_size:
        sample = sample.head(sample_size)

    logger.info(f"  Sampled {len(sample):,} matches")

    # Convert list columns to strings for CSV export
    if 'validation_flags' in sample.columns:
        sample = sample.with_columns(
            pl.col('validation_flags').map_elements(lambda x: ','.join(x) if isinstance(x, list) else '', return_dtype=pl.String).alias('validation_flags')
        )

    # Save sample
    sample.write_csv(OUTPUT_SAMPLE)
    logger.info(f"  Saved to {OUTPUT_SAMPLE}")

    # Generate summary report
    report = {
        'total_matches': len(matches),
        'sample_size': len(sample),
        'unique_institutions': matches['institution_id'].n_unique(),
        'unique_firms': matches['GVKEY'].n_unique(),
        'match_types': matches.group_by('match_type').agg(pl.len().alias('count')).to_dict(as_series=False),
        'confidence_stats': {
            'mean': float(matches['match_confidence'].mean()),
            'median': float(matches['match_confidence'].median()),
            'min': float(matches['match_confidence'].min()),
            'max': float(matches['match_confidence'].max()),
        },
        'validation_sample': str(OUTPUT_SAMPLE),
    }

    import json
    with open(OUTPUT_REPORT, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info(f"\n  Report saved to {OUTPUT_REPORT}")

    # Print summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total matches: {len(matches):,}")
    logger.info(f"Validation sample: {len(sample):,} ({len(sample)/len(matches)*100:.1f}%)")
    logger.info(f"\nConfidence distribution:")
    logger.info(f"  Mean: {matches['match_confidence'].mean():.3f}")
    logger.info(f"  Median: {matches['match_confidence'].median():.3f}")
    logger.info(f"  Min: {matches['match_confidence'].min():.3f}")
    logger.info(f"  Max: {matches['match_confidence'].max():.3f}")

    logger.info("\n" + "=" * 80)
    logger.info("STAGE 7 COMPLETE")
    logger.info("=" * 80)

    return sample


if __name__ == "__main__":
    main()
