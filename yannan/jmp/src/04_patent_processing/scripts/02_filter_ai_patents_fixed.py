#!/usr/bin/env python3
"""
Phase 2: Filter and Classify AI Patents (FIXED VERSION)

Key improvements:
1. Memory-efficient: CPC filter FIRST (95% reduction), use lazy loading
2. INTERSECTION approach: CPC AND text (not UNION)
3. Abstract-based classification: Not title-only
4. Refined keywords: Remove generic terms, add abbreviations
5. Multi-label classification: Allow multiple categories

Expected results:
- Memory: <8GB (not killed)
- AI patent rate: 5-10% (not 40%)
- Unknown rate: <20% (not 93%)
- Precision: >85%

Usage:
    python src/04_patent_processing/scripts/02_filter_ai_patents_fixed.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl
from tqdm import tqdm

# Import refined keywords
try:
    from utils.keyword_lists_refined import (
        AI_KEYWORDS_INTERSECTION,
        AI_CPC_CODES,
        INFRASTRUCTURE_KEYWORDS,
        ALGORITHM_KEYWORDS,
        APPLICATION_KEYWORDS,
        CONTEXTUAL_KEYWORDS,
        NEGATIVE_KEYWORDS,
        POSITIVE_CONTROL_COMPANIES,
    )
except ImportError:
    print("Warning: Could not import refined keywords, using defaults")
    AI_KEYWORDS_INTERSECTION = [
        'artificial intelligence', 'machine learning', 'neural network',
        'deep learning', 'computer vision', 'natural language processing',
    ]
    INFRASTRUCTURE_KEYWORDS = ['gpu', 'tpu', 'fpga', 'asic', 'ai accelerator']
    ALGORITHM_KEYWORDS = [
        'neural network', 'transformer', 'convolutional', 'recurrent',
        'reinforcement learning', 'nlp', 'bert', 'gpt', 'gan',
    ]
    APPLICATION_KEYWORDS = [
        'autonomous driving', 'chatbot', 'virtual assistant',
        'recommendation system', 'fraud detection',
    ]
    CONTEXTUAL_KEYWORDS = {}
    NEGATIVE_KEYWORDS = []

# Configuration
RAW_DIR = Path('/Data/patent/raw')
PROCESSED_DIR = Path('/Data/patent/processed')
PROCESSED_DIR.mkdir(exist_ok=True, parents=True)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def is_ai_by_text_intersection(abstract_text):
    """
    Check if patent is AI-related using high-precision keywords.

    NOTE: Used AFTER CPC filter, so we require explicit AI keywords.
    """
    if not abstract_text:
        return False

    text_lower = abstract_text.lower()

    # Check for explicit AI keywords
    for keyword in AI_KEYWORDS_INTERSECTION:
        if keyword in text_lower:
            return True

    return False


def check_contextual_keyword(text_lower, keyword, context_info):
    """
    Check if ambiguous keyword is used in valid context.

    Example: "dropout" in "neural network dropout" (valid) vs
             "dropout" in "voltage regulator dropout" (invalid)
    """
    if keyword not in text_lower:
        return False

    # Check if valid context present
    has_valid_context = any(ctx in text_lower for ctx in context_info['valid_context'])

    # Check if invalid context present
    has_invalid_context = any(ctx in text_lower for ctx in context_info['invalid_context'])

    # Valid if: has valid context AND no invalid context
    return has_valid_context and not has_invalid_context


def classify_strategic_multi_label(abstract_text):
    """
    Classify AI patent into strategic categories (multi-label).

    Returns dict with binary flags for each category.
    """
    if not abstract_text:
        return {
            'infrastructure': False,
            'algorithm': False,
            'application': False
        }

    text_lower = abstract_text.lower()

    # Check each category independently (multi-label)
    is_infra = any(kw in text_lower for kw in INFRASTRUCTURE_KEYWORDS)
    is_algo = any(kw in text_lower for kw in ALGORITHM_KEYWORDS)
    is_app = any(kw in text_lower for kw in APPLICATION_KEYWORDS)

    # Check contextual keywords
    for keyword, context_info in CONTEXTUAL_KEYWORDS.items():
        if check_contextual_keyword(text_lower, keyword, context_info):
            if context_info['category'] == 'infrastructure':
                is_infra = True
            elif context_info['category'] == 'algorithm':
                is_algo = True
            elif context_info['category'] == 'application':
                is_app = True

    return {
        'infrastructure': is_infra,
        'algorithm': is_algo,
        'application': is_app,
    }


# =============================================================================
# MAIN PROCESSING PIPELINE
# =============================================================================

def main():
    """Main processing function."""

    print("="*80)
    print("Phase 2: AI Patent Identification and Classification (FIXED)")
    print("="*80)
    print()

    # =========================================================================
    # STEP 1: Load CPC data and identify AI patents (reduces dataset 95%)
    # =========================================================================

    print("Step 1: Loading CPC data...")
    print("-" * 80)

    try:
        cpc = pl.read_csv(RAW_DIR / 'g_cpc_current.tsv', separator='\t')
        print(f"✅ Loaded CPC data: {cpc.height:,} records")
    except Exception as e:
        print(f"❌ Error loading CPC data: {e}")
        sys.exit(1)

    # Filter to AI-related CPC codes
    ai_cpc = cpc.filter(
        pl.col('cpc_subgroup').str.slice(0, 4).is_in(AI_CPC_CODES)
    )

    ai_cpc_ids = ai_cpc.select('patent_id').unique()
    print(f"✅ AI patents by CPC: {len(ai_cpc_ids):,}")

    # =========================================================================
    # STEP 2: Load patent metadata (ONLY CPC-matched patents - 95% reduction!)
    # =========================================================================

    print("\nStep 2: Loading patent metadata (CPC-matched only)...")
    print("-" * 80)

    try:
        # Use lazy loading - scan first, then filter, then collect
        patents_lazy = pl.scan_csv(
            RAW_DIR / 'g_patent.tsv',
            separator='\t',
            columns=['patent_id', 'patent_date', 'patent_type', 'patent_title']
        ).filter(
            pl.col('patent_id').is_in(ai_cpc_ids)  # FILTER EARLY!
        ).filter(
            pl.col('patent_date') >= '2010-01-01'
        ).filter(
            pl.col('patent_date') <= '2024-12-31'
        ).filter(
            pl.col('patent_type') == 'utility'
        )

        patents = patents_lazy.collect()
        print(f"✅ Loaded patents: {patents.height:,}")
    except Exception as e:
        print(f"❌ Error loading patents: {e}")
        sys.exit(1)

    # =========================================================================
    # STEP 3: Load abstracts (ONLY for CPC-matched patents)
    # =========================================================================

    print("\nStep 3: Loading patent abstracts (CPC-matched only)...")
    print("-" * 80)

    try:
        # Load abstracts only for CPC-matched patents
        abstracts_lazy = pl.scan_csv(
            RAW_DIR / 'g_patent_abstract.tsv',
            separator='\t',
            columns=['patent_id', 'patent_abstract']
        ).filter(
            pl.col('patent_id').is_in(patents['patent_id'])
        )

        abstracts = abstracts_lazy.collect()
        print(f"✅ Loaded abstracts: {abstracts.height:,}")
    except Exception as e:
        print(f"❌ Error loading abstracts: {e}")
        sys.exit(1)

    # =========================================================================
    # STEP 4: Merge and apply INTERSECTION classification (CPC AND text)
    # =========================================================================

    print("\nStep 4: Applying INTERSECTION classification (CPC AND text)...")
    print("-" * 80)

    # Merge patents with abstracts
    patents_with_abstracts = patents.join(abstracts, on='patent_id', how='left')
    print(f"✅ Patents with abstracts: {patents_with_abstracts.filter(pl.col('patent_abstract').is_not_null()).height:,}")

    # Apply text-based filter (INTERSECTION: CPC AND text both must match)
    print("Applying AI keyword filter (INTERSECTION approach)...")

    ai_patents = patents_with_abstracts.with_columns(
        pl.col('patent_abstract').map_elements(
            is_ai_by_text_intersection,
            return_dtype=bool
        ).alias('is_ai_text')
    ).filter(
        pl.col('is_ai_text') == True  # INTERSECTION: CPC AND text
    )

    print(f"✅ AI patents (CPC AND text): {ai_patents.height:,}")
    print(f"   AI patent rate: {ai_patents.height / patents.height:.1%}")

    # Check against literature benchmarks
    if ai_patents.height / patents.height > 0.15:
        print("⚠️  WARNING: AI rate >15% - still too high, may have false positives")
    elif ai_patents.height / patents.height < 0.02:
        print("⚠️  WARNING: AI rate <2% - may be too restrictive")
    else:
        print("✅ AI rate within expected range (5-10%)")

    # =========================================================================
    # STEP 5: Strategic classification with multi-label support
    # =========================================================================

    print("\nStep 5: Applying strategic classification (multi-label)...")
    print("-" * 80)

    # Classify each patent (multi-label)
    print("Classifying patents...")
    classification_results = ai_patents['patent_abstract'].map_elements(
        classify_strategic_multi_label,
        return_dtype=pl.Struct({
            'infrastructure': pl.Boolean,
            'algorithm': pl.Boolean,
            'application': pl.Boolean
        })
    )

    # Add classification columns
    ai_patents_classified = ai_patents.with_columns([
        classification_results.struct.field('infrastructure').alias('is_infrastructure'),
        classification_results.struct.field('algorithm').alias('is_algorithm'),
        classification_results.struct.field('application').alias('is_application'),
    ])

    # Add primary category (for backward compatibility)
    ai_patents_classified = ai_patents_classified.with_columns(
        pl.when(pl.col('is_infrastructure'))
        .then(pl.lit('Infrastructure'))
        .when(pl.col('is_algorithm'))
        .then(pl.lit('Algorithm'))
        .when(pl.col('is_application'))
        .then(pl.lit('Application'))
        .otherwise(pl.lit('Unknown'))
        .alias('ai_category_primary')
    )

    print("✅ Classification complete")

    # =========================================================================
    # STEP 6: Classification distribution report
    # =========================================================================

    print("\n" + "="*80)
    print("CLASSIFICATION RESULTS")
    print("="*80)

    # Primary category distribution
    category_dist = ai_patents_classified.groupby('ai_category_primary').agg([
        pl.count().alias('n_patents'),
        (pl.len() / ai_patents_classified.height * 100).alias('percentage')
    ]).sort('n_patents', descending=True)

    print("\nPrimary Category Distribution:")
    print(category_dist)

    # Multi-label overlap
    print("\nMulti-Label Overlaps:")
    n_infra_only = ai_patents_classified.filter(
        pl.col('is_infrastructure') & ~pl.col('is_algorithm') & ~pl.col('is_application')
    ).height
    n_algo_only = ai_patents_classified.filter(
        pl.col('is_algorithm') & ~pl.col('is_infrastructure') & ~pl.col('is_application')
    ).height
    n_app_only = ai_patents_classified.filter(
        pl.col('is_application') & ~pl.col('is_infrastructure') & ~pl.col('is_algorithm')
    ).height
    n_infra_algo = ai_patents_classified.filter(
        pl.col('is_infrastructure') & pl.col('is_algorithm') & ~pl.col('is_application')
    ).height
    n_algo_app = ai_patents_classified.filter(
        pl.col('is_algorithm') & pl.col('is_application') & ~pl.col('is_infrastructure')
    ).height
    n_all_three = ai_patents_classified.filter(
        pl.col('is_infrastructure') & pl.col('is_algorithm') & pl.col('is_application')
    ).height

    print(f"Infrastructure only: {n_infra_only:,} ({n_infra_only/ai_patents_classified.height*100:.1f}%)")
    print(f"Algorithm only: {n_algo_only:,} ({n_algo_only/ai_patents_classified.height*100:.1f}%)")
    print(f"Application only: {n_app_only:,} ({n_app_only/ai_patents_classified.height*100:.1f}%)")
    print(f"Infra + Algo: {n_infra_algo:,} ({n_infra_algo/ai_patents_classified.height*100:.1f}%)")
    print(f"Algo + App: {n_algo_app:,} ({n_algo_app/ai_patents_classified.height*100:.1f}%)")
    print(f"All three: {n_all_three:,} ({n_all_three/ai_patents_classified.height*100:.1f}%)")

    # =========================================================================
    # STEP 7: Sample validation
    # =========================================================================

    print("\n" + "="*80)
    print("SAMPLE VALIDATION (Review these manually)")
    print("="*80)

    # Sample 5 from each primary category
    for category in ['Infrastructure', 'Algorithm', 'Application', 'Unknown']:
        sample = ai_patents_classified.filter(
            pl.col('ai_category_primary') == category
        ).sample(5, seed=42)

        print(f"\n{category} Samples:")
        print("-" * 80)

        for row in sample.iter_rows(named=True):
            print(f"ID: {row['patent_id']}")
            print(f"Title: {row['patent_title'][:80]}...")
            abstract_preview = row['patent_abstract'][:150] if row['patent_abstract'] else "No abstract"
            print(f"Abstract: {abstract_preview}...")
            print(f"Multi-label: Infra={row['is_infrastructure']}, Algo={row['is_algorithm']}, App={row['is_application']}")
            print()

    # =========================================================================
    # STEP 8: Save results
    # =========================================================================

    print("="*80)
    print("SAVING RESULTS")
    print("="*80)

    # Save main dataset
    output_file = PROCESSED_DIR / 'ai_patents_2010_2024.parquet'
    ai_patents_classified.write_parquet(output_file, compression='snappy')
    print(f"✅ Saved {ai_patents_classified.height:,} AI patents")
    print(f"   Location: {output_file}")

    # Create validation set
    print("\nCreating validation set...")
    validation_samples = []

    for category in ['Infrastructure', 'Algorithm', 'Application', 'Unknown']:
        n_samples = 50 if category != 'Unknown' else 100

        sample = ai_patents_classified.filter(
            pl.col('ai_category_primary') == category
        ).select([
            'patent_id',
            'patent_title',
            'patent_abstract',
            'ai_category_primary',
            'is_infrastructure',
            'is_algorithm',
            'is_application',
        ]).sample(n_samples, seed=42)

        validation_samples.append(sample)

    validation_set = pl.concat(validation_samples)
    validation_file = PROCESSED_DIR / 'validation_set_for_manual_review.csv'
    validation_set.write_csv(validation_file)
    print(f"✅ Created validation set: {validation_set.height:,} patents")
    print(f"   Location: {validation_file}")

    # =========================================================================
    # FINAL SUMMARY
    # =========================================================================

    print("\n" + "="*80)
    print("✅ PROCESSING COMPLETE")
    print("="*80)

    print(f"\nResults summary:")
    print(f"  Total patents (2010-2024): {patents.height:,}")
    print(f"  AI patents (CPC AND text): {ai_patents_classified.height:,} ({ai_patents_classified.height/patents.height:.1%})")
    print(f"  Unknown rate: {category_dist.filter(pl.col('ai_category_primary') == 'Unknown')['percentage'].item():.1f}%")

    # Check if metrics are acceptable
    unknown_rate = category_dist.filter(pl.col('ai_category_primary') == 'Unknown')['percentage'].item()
    ai_rate = ai_patents_classified.height / patents.height

    print("\nQuality checks:")
    if unknown_rate < 20:
        print(f"  ✅ Unknown rate: {unknown_rate:.1f}% (<20% target)")
    else:
        print(f"  ⚠️  Unknown rate: {unknown_rate:.1f}% (target <20%)")

    if 0.05 <= ai_rate <= 0.15:
        print(f"  ✅ AI rate: {ai_rate:.1%} (5-15% target range)")
    else:
        print(f"  ⚠️  AI rate: {ai_rate:.1%} (target 5-15%)")

    print("\nNext steps:")
    print("  1. Review validation samples above")
    print("  2. Manually review validation_set_for_manual_review.csv")
    print("  3. Compute precision/recall metrics")
    print("  4. Update keyword lists if needed")
    print("  5. Continue to Phase 3: Match patents to firms")

    print("\n" + "="*80)

    return ai_patents_classified


if __name__ == '__main__':
    main()
