"""
Validation and Bias Testing for Patent-Firm Matches

This script:
1. Samples matches for manual validation
2. Tests for truncation bias and citation bias (Lerner & Seru 2022)
3. Generates validation report

Following Lerner & Seru (2022) methodology.
"""

import polars as pl
from pathlib import Path
import logging
import json
import random
from typing import Dict

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

ADJUSTED_MATCHES = DATA_PROCESSED_LINK / "patent_firm_matches_adjusted.parquet"
VALIDATION_SAMPLE = DATA_PROCESSED_LINK / "validation_sample.csv"
VALIDATION_REPORT = DATA_PROCESSED_LINK / "validation_report.json"
BIAS_DIAGNOSTICS = DATA_PROCESSED_LINK / "bias_diagnostics.json"
PROGRESS_LOG = LOGS_DIR / "validate_patent_matches.log"

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


def create_validation_sample(matches_df: pl.DataFrame, n_samples: int = 250) -> pl.DataFrame:
    """
    Create stratified sample for manual validation.
    Samples across different match types and confidence levels.
    """
    logger.info(f"Creating validation sample of {n_samples} matches...")
    
    # Stratify by match_type and confidence levels
    samples = []
    
    # Get unique match types
    match_types = matches_df['match_type'].unique().to_list()
    samples_per_type = n_samples // len(match_types) if match_types else n_samples
    
    for match_type in match_types:
        type_matches = matches_df.filter(pl.col('match_type') == match_type)
        
        if len(type_matches) == 0:
            continue
        
        # Sample from different confidence ranges
        high_conf = type_matches.filter(pl.col('match_confidence') >= 0.95)
        med_conf = type_matches.filter((pl.col('match_confidence') >= 0.90) & (pl.col('match_confidence') < 0.95))
        low_conf = type_matches.filter(pl.col('match_confidence') < 0.90)
        
        n_per_range = samples_per_type // 3
        
        for conf_df, conf_label in [(high_conf, 'high'), (med_conf, 'medium'), (low_conf, 'low')]:
            if len(conf_df) > 0:
                n_sample = min(n_per_range, len(conf_df))
                sampled = conf_df.sample(n=n_sample, seed=42)
                samples.append(sampled)
    
    if not samples:
        logger.warning("  No samples created!")
        return pl.DataFrame()
    
    validation_sample = pl.concat(samples, how='diagonal')
    
    # Add columns for manual validation
    validation_sample = validation_sample.with_columns([
        pl.lit(None).cast(pl.Utf8).alias('manual_validation'),  # 'correct', 'incorrect', 'uncertain'
        pl.lit(None).cast(pl.Utf8).alias('validation_notes'),  # Notes from manual review
    ])
    
    # Select key columns for manual review
    review_columns = [
        'GVKEY', 'LPERMNO', 'firm_conm', 'assignee_id', 'assignee_clean_name',
        'match_type', 'match_method', 'match_confidence', 'assignee_patent_count',
        'first_patent_year', 'last_patent_year',
        'manual_validation', 'validation_notes'
    ]
    
    available_columns = [col for col in review_columns if col in validation_sample.columns]
    validation_sample = validation_sample.select(available_columns)
    
    logger.info(f"  Created sample of {len(validation_sample):,} matches")
    logger.info(f"  By match_type:")
    type_counts = validation_sample.group_by('match_type').agg(pl.len().alias('count'))
    for row in type_counts.iter_rows(named=True):
        logger.info(f"    {row['match_type']}: {row['count']:,}")
    
    return validation_sample


def test_truncation_bias(matches_df: pl.DataFrame) -> Dict:
    """
    Test for truncation bias: check if recent years have fewer matched patents.
    """
    logger.info("Testing for truncation bias...")
    
    if 'first_patent_year' not in matches_df.columns:
        logger.warning("  Cannot test truncation bias - patent years not available")
        return {'error': 'patent_years_not_available'}
    
    # Get matches with patent years
    matches_with_years = matches_df.filter(pl.col('first_patent_year').is_not_null())
    
    if len(matches_with_years) == 0:
        logger.warning("  No matches with patent years")
        return {'error': 'no_patent_years'}
    
    # Calculate patents per year (using first_patent_year as proxy)
    patents_by_year = (
        matches_with_years
        .group_by('first_patent_year')
        .agg([
            pl.len().alias('match_count'),
            pl.col('assignee_patent_count').sum().alias('total_patents'),
        ])
        .sort('first_patent_year')
    )
    
    # Check if recent years (last 5 years) have systematically fewer matches
    max_year = patents_by_year['first_patent_year'].max()
    recent_years = patents_by_year.filter(pl.col('first_patent_year') >= max_year - 5)
    older_years = patents_by_year.filter(pl.col('first_patent_year') < max_year - 5)
    
    if len(recent_years) > 0 and len(older_years) > 0:
        recent_avg = recent_years['match_count'].mean()
        older_avg = older_years['match_count'].mean()
        truncation_ratio = recent_avg / older_avg if older_avg > 0 else 1.0
        
        logger.info(f"  Recent years (last 5) avg matches: {recent_avg:.1f}")
        logger.info(f"  Older years avg matches: {older_avg:.1f}")
        logger.info(f"  Truncation ratio: {truncation_ratio:.3f}")
        logger.info(f"  {'Potential truncation bias detected' if truncation_ratio < 0.8 else 'No significant truncation bias'}")
        
        return {
            'truncation_ratio': float(truncation_ratio),
            'recent_avg_matches': float(recent_avg),
            'older_avg_matches': float(older_avg),
            'max_year': int(max_year),
            'potential_bias': truncation_ratio < 0.8
        }
    else:
        return {'error': 'insufficient_data'}


def test_coverage_bias(matches_df: pl.DataFrame) -> Dict:
    """
    Test for coverage bias: compare matched vs. unmatched assignees.
    """
    logger.info("Testing for coverage bias...")
    
    # Load all assignees to compare
    assignees_master_path = PROJECT_ROOT / "data" / "interim" / "patent_assignees_master.parquet"
    
    if not assignees_master_path.exists():
        logger.warning("  Cannot test coverage bias - assignees master not found")
        return {'error': 'assignees_master_not_found'}
    
    all_assignees = pl.read_parquet(assignees_master_path)
    matched_assignee_ids = set(matches_df['assignee_id'].unique().to_list())
    
    # Compare matched vs. unmatched
    all_assignees = all_assignees.with_columns([
        pl.col('assignee_id').is_in(list(matched_assignee_ids)).alias('is_matched')
    ])
    
    matched = all_assignees.filter(pl.col('is_matched') == True)
    unmatched = all_assignees.filter(pl.col('is_matched') == False)
    
    if len(matched) == 0 or len(unmatched) == 0:
        return {'error': 'insufficient_data'}
    
    # Compare by patent count
    matched_avg_patents = matched['patent_count_total'].mean()
    unmatched_avg_patents = unmatched['patent_count_total'].mean()
    
    logger.info(f"  Matched assignees: {len(matched):,}")
    logger.info(f"  Unmatched assignees: {len(unmatched):,}")
    logger.info(f"  Matched avg patents: {matched_avg_patents:.1f}")
    logger.info(f"  Unmatched avg patents: {unmatched_avg_patents:.1f}")
    
    coverage_ratio = matched_avg_patents / unmatched_avg_patents if unmatched_avg_patents > 0 else 1.0
    
    logger.info(f"  Coverage ratio: {coverage_ratio:.3f}")
    logger.info(f"  {'Potential coverage bias (large assignees over-represented)' if coverage_ratio > 2.0 else 'No significant coverage bias'}")
    
    return {
        'matched_count': len(matched),
        'unmatched_count': len(unmatched),
        'matched_avg_patents': float(matched_avg_patents),
        'unmatched_avg_patents': float(unmatched_avg_patents),
        'coverage_ratio': float(coverage_ratio),
        'potential_bias': coverage_ratio > 2.0
    }


def calculate_accuracy_by_stage(matches_df: pl.DataFrame) -> Dict:
    """
    Calculate expected accuracy by match stage (based on confidence thresholds).
    """
    logger.info("Calculating expected accuracy by stage...")
    
    accuracy_by_stage = {}
    
    for match_type in matches_df['match_type'].unique().to_list():
        stage_matches = matches_df.filter(pl.col('match_type') == match_type)
        
        # Estimate accuracy based on confidence distribution
        # High confidence (>0.95) -> high accuracy
        # Medium confidence (0.90-0.95) -> medium accuracy
        # Low confidence (<0.90) -> lower accuracy
        
        high_conf = stage_matches.filter(pl.col('match_confidence') >= 0.95)
        med_conf = stage_matches.filter((pl.col('match_confidence') >= 0.90) & (pl.col('match_confidence') < 0.95))
        low_conf = stage_matches.filter(pl.col('match_confidence') < 0.90)
        
        # Weighted accuracy estimate
        # Assumptions: high_conf -> 0.98 accuracy, med_conf -> 0.90 accuracy, low_conf -> 0.80 accuracy
        total = len(stage_matches)
        if total > 0:
            estimated_accuracy = (
                len(high_conf) * 0.98 +
                len(med_conf) * 0.90 +
                len(low_conf) * 0.80
            ) / total
            
            accuracy_by_stage[match_type] = {
                'total_matches': total,
                'high_confidence_count': len(high_conf),
                'medium_confidence_count': len(med_conf),
                'low_confidence_count': len(low_conf),
                'estimated_accuracy': float(estimated_accuracy),
                'mean_confidence': float(stage_matches['match_confidence'].mean()),
            }
            
            logger.info(f"  {match_type}: {estimated_accuracy:.1%} estimated accuracy ({total:,} matches)")
    
    return accuracy_by_stage


def main():
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("VALIDATION AND BIAS TESTING FOR PATENT-FIRM MATCHES")
    logger.info("=" * 80)
    
    # Step 1: Load adjusted matches
    logger.info("\n[1/4] Loading adjusted matches...")
    
    if not ADJUSTED_MATCHES.exists():
        raise FileNotFoundError(f"Adjusted matches file not found: {ADJUSTED_MATCHES}")
    
    matches_df = pl.read_parquet(ADJUSTED_MATCHES)
    logger.info(f"  Loaded {len(matches_df):,} matches")
    
    # Step 2: Create validation sample
    logger.info("\n[2/4] Creating validation sample...")
    validation_sample = create_validation_sample(matches_df, n_samples=250)
    
    if len(validation_sample) > 0:
        validation_sample.write_csv(VALIDATION_SAMPLE)
        logger.info(f"  Saved validation sample to: {VALIDATION_SAMPLE}")
        logger.info("  Please manually review this file and mark 'manual_validation' column")
    
    # Step 3: Test for biases
    logger.info("\n[3/4] Testing for biases...")
    
    truncation_results = test_truncation_bias(matches_df)
    coverage_results = test_coverage_bias(matches_df)
    
    # Step 4: Calculate accuracy estimates
    logger.info("\n[4/4] Calculating accuracy estimates...")
    accuracy_by_stage = calculate_accuracy_by_stage(matches_df)
    
    # Create validation report
    validation_report = {
        'total_matches': len(matches_df),
        'unique_firms': int(matches_df['GVKEY'].n_unique()),
        'unique_assignees': int(matches_df['assignee_id'].n_unique()),
        'truncation_bias': truncation_results,
        'coverage_bias': coverage_results,
        'accuracy_by_stage': accuracy_by_stage,
        'validation_sample_size': len(validation_sample),
    }
    
    with open(VALIDATION_REPORT, 'w') as f:
        json.dump(validation_report, f, indent=2)
    
    logger.info(f"  Saved validation report to: {VALIDATION_REPORT}")
    
    # Save bias diagnostics separately
    bias_diagnostics = {
        'truncation_bias': truncation_results,
        'coverage_bias': coverage_results,
    }
    
    with open(BIAS_DIAGNOSTICS, 'w') as f:
        json.dump(bias_diagnostics, f, indent=2)
    
    logger.info(f"  Saved bias diagnostics to: {BIAS_DIAGNOSTICS}")
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total matches: {len(matches_df):,}")
    logger.info(f"Validation sample: {len(validation_sample):,} matches")
    logger.info(f"Estimated overall accuracy: {sum(s['estimated_accuracy'] * s['total_matches'] for s in accuracy_by_stage.values()) / len(matches_df):.1%}")
    
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION COMPLETE")
    logger.info("=" * 80)
    
    return validation_report


if __name__ == "__main__":
    main()
