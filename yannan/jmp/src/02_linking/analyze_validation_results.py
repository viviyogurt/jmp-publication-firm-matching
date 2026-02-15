"""
Analyze Validation Results

This script analyzes the manually validated sample to calculate actual accuracy rates.
"""

import polars as pl
from pathlib import Path
import logging
import json

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

VALIDATION_SAMPLE = DATA_PROCESSED_LINK / "validation_sample_1000_validated.csv"
OUTPUT_REPORT = DATA_PROCESSED_LINK / "validation_accuracy_report.json"
PROGRESS_LOG = LOGS_DIR / "analyze_validation_results.log"

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
    """Main processing pipeline."""
    logger.info("=" * 80)
    logger.info("ANALYZING VALIDATION RESULTS")
    logger.info("=" * 80)
    
    # Load validation sample
    logger.info("\n[1/3] Loading validation sample...")
    
    # Check for validated file first, then fall back to original
    validated_file = DATA_PROCESSED_LINK / "validation_sample_1000_validated.csv"
    original_file = DATA_PROCESSED_LINK / "validation_sample_1000.csv"
    
    if validated_file.exists():
        logger.info(f"  Found validated file: {validated_file}")
        sample = pl.read_csv(validated_file)
    elif VALIDATION_SAMPLE.exists():
        logger.info(f"  Using file: {VALIDATION_SAMPLE}")
        sample = pl.read_csv(VALIDATION_SAMPLE)
    else:
        raise FileNotFoundError(f"Validation sample file not found: {VALIDATION_SAMPLE}")
    logger.info(f"  Loaded {len(sample):,} validation samples")
    
    # Check if validation is complete
    validated = sample.filter(pl.col('is_correct').is_not_null() & (pl.col('is_correct') != ''))
    unvalidated = sample.filter(pl.col('is_correct').is_null() | (pl.col('is_correct') == ''))
    
    logger.info(f"  Validated: {len(validated):,}")
    logger.info(f"  Unvalidated: {len(unvalidated):,}")
    
    if len(validated) == 0:
        logger.warning("  No validation results found!")
        logger.info("  Please complete the validation in the CSV file first.")
        return
    
    # Calculate accuracy
    logger.info("\n[2/3] Calculating accuracy metrics...")
    
    # Count correct/incorrect/uncertain
    correct = validated.filter(pl.col('is_correct').str.to_uppercase() == 'YES')
    incorrect = validated.filter(pl.col('is_correct').str.to_uppercase() == 'NO')
    uncertain = validated.filter(pl.col('is_correct').str.to_uppercase() == 'UNCERTAIN')
    
    total_validated = len(validated)
    accuracy = len(correct) / total_validated if total_validated > 0 else 0.0
    
    logger.info(f"\nValidation Results:")
    logger.info(f"  Correct: {len(correct):,} ({len(correct)/total_validated*100:.1f}%)")
    logger.info(f"  Incorrect: {len(incorrect):,} ({len(incorrect)/total_validated*100:.1f}%)")
    logger.info(f"  Uncertain: {len(uncertain):,} ({len(uncertain)/total_validated*100:.1f}%)")
    logger.info(f"  Total validated: {total_validated:,}")
    logger.info(f"\n  ACCURACY: {accuracy:.1%}")
    
    # Accuracy by match type
    logger.info("\n[3/3] Accuracy by match type:")
    
    accuracy_by_type = {}
    for match_type in validated['match_type'].unique().to_list():
        type_data = validated.filter(pl.col('match_type') == match_type)
        type_correct = type_data.filter(pl.col('is_correct').str.to_uppercase() == 'YES')
        type_accuracy = len(type_correct) / len(type_data) if len(type_data) > 0 else 0.0
        
        accuracy_by_type[match_type] = {
            'total': len(type_data),
            'correct': len(type_correct),
            'incorrect': len(type_data.filter(pl.col('is_correct').str.to_uppercase() == 'NO')),
            'uncertain': len(type_data.filter(pl.col('is_correct').str.to_uppercase() == 'UNCERTAIN')),
            'accuracy': float(type_accuracy),
        }
        
        logger.info(f"  {match_type}:")
        logger.info(f"    Total: {len(type_data):,}")
        logger.info(f"    Correct: {len(type_correct):,}")
        logger.info(f"    Accuracy: {type_accuracy:.1%}")
    
    # Accuracy by confidence level
    logger.info("\nAccuracy by confidence level:")
    
    validated = validated.with_columns([
        pl.when(pl.col('match_confidence') >= 0.98)
        .then(pl.lit('Very High (≥0.98)'))
        .when(pl.col('match_confidence') >= 0.95)
        .then(pl.lit('High (0.95-0.98)'))
        .when(pl.col('match_confidence') >= 0.90)
        .then(pl.lit('Medium (0.90-0.95)'))
        .otherwise(pl.lit('Low (<0.90)'))
        .alias('confidence_level')
    ])
    
    accuracy_by_conf = {}
    for conf_level in validated['confidence_level'].unique().to_list():
        conf_data = validated.filter(pl.col('confidence_level') == conf_level)
        conf_correct = conf_data.filter(pl.col('is_correct').str.to_uppercase() == 'YES')
        conf_accuracy = len(conf_correct) / len(conf_data) if len(conf_data) > 0 else 0.0
        
        accuracy_by_conf[conf_level] = {
            'total': len(conf_data),
            'correct': len(conf_correct),
            'accuracy': float(conf_accuracy),
        }
        
        logger.info(f"  {conf_level}:")
        logger.info(f"    Total: {len(conf_data):,}")
        logger.info(f"    Correct: {len(conf_correct):,}")
        logger.info(f"    Accuracy: {conf_accuracy:.1%}")
    
    # Create report
    report = {
        'total_samples': len(sample),
        'validated_samples': total_validated,
        'unvalidated_samples': len(unvalidated),
        'overall_accuracy': float(accuracy),
        'correct_count': len(correct),
        'incorrect_count': len(incorrect),
        'uncertain_count': len(uncertain),
        'accuracy_by_type': accuracy_by_type,
        'accuracy_by_confidence': accuracy_by_conf,
    }
    
    # Save report
    with open(OUTPUT_REPORT, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"\nSaved accuracy report to: {OUTPUT_REPORT}")
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION ANALYSIS SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Overall Accuracy: {accuracy:.1%}")
    logger.info(f"Validated: {total_validated:,} / {len(sample):,} samples")
    logger.info(f"Correct: {len(correct):,}")
    logger.info(f"Incorrect: {len(incorrect):,}")
    logger.info(f"Uncertain: {len(uncertain):,}")
    
    logger.info("\n" + "=" * 80)
    logger.info("ANALYSIS COMPLETE")
    logger.info("=" * 80)
    
    return report


if __name__ == "__main__":
    main()
