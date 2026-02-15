"""
Exploratory Analysis of Existing Publication Matching

Goal: Understand what we have and what we're missing before trying to improve.

Analysis:
1. Load existing homepage domain matches (baseline: 1,580 firms @ 97.6% accuracy)
2. Identify unmatched CRSP firms (opportunity for improvement)
3. Identify unmatched institutions (publications with no firm match)
4. Characterize what's being missed and why
5. Identify patterns to inform better matching strategies
"""

import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
DOCS_DIR = PROJECT_ROOT / "docs"
LOGS_DIR = PROJECT_ROOT / "logs"

# Baseline: existing cleaned dataset (1,574 firms @ 97.6% accuracy)
BASELINE_MATCHES = DATA_PROCESSED_LINK / "publication_firm_matches_cleaned.parquet"

# Data files
INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"

OUTPUT_DIR = DATA_PROCESSED_LINK / "exploration"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "explore_baseline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Run exploratory analysis."""
    logger.info("=" * 80)
    logger.info("EXPLORATORY ANALYSIS: BASELINE PUBLICATION MATCHING")
    logger.info("=" * 80)

    # Load baseline matches
    logger.info("\n[1/6] Loading baseline matches...")
    if not BASELINE_MATCHES.exists():
        # Try alternative location
        alt_path = DATA_PROCESSED_LINK / "publication_firm_matches_stage1.parquet"
        if alt_path.exists():
            baseline_matches = pl.read_parquet(alt_path)
            logger.info(f"  Loaded from: {alt_path}")
        else:
            raise FileNotFoundError(f"Baseline matches not found: {BASELINE_MATCHES}")
    else:
        baseline_matches = pl.read_parquet(BASELINE_MATCHES)
        logger.info(f"  Loaded from: {BASELINE_MATCHES}")

    logger.info(f"  Total matches: {len(baseline_matches):,}")
    logger.info(f"  Unique institutions: {baseline_matches['institution_id'].n_unique():,}")
    logger.info(f"  Unique firms: {baseline_matches['gvkey'].n_unique():,}")

    # Load all institutions and firms
    logger.info("\n[2/6] Loading institutions and firms...")

    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)

    logger.info(f"  Total institutions: {len(institutions):,}")
    logger.info(f"  Total firms: {len(firms):,}")

    # Identify matched and unmatched
    logger.info("\n[3/6] Identifying matched/unmatched entities...")

    matched_institution_ids = set(baseline_matches['institution_id'].to_list())
    matched_firm_gvkeys = set(baseline_matches['gvkey'].to_list())

    # Unmatched institutions
    unmatched_inst = institutions.filter(
        ~pl.col('institution_id').is_in(matched_institution_ids)
    )

    # Unmatched firms
    unmatched_firms = firms.filter(
        ~pl.col('GVKEY').is_in(matched_firm_gvkeys)
    )

    logger.info(f"\n  Matched institutions: {len(matched_institution_ids):,} ({len(matched_institution_ids)/len(institutions)*100:.1f}%)")
    logger.info(f"  Unmatched institutions: {len(unmatched_inst):,} ({len(unmatched_inst)/len(institutions)*100:.1f}%)")
    logger.info(f"  Matched firms: {len(matched_firm_gvkeys):,} ({len(matched_firm_gvkeys)/len(firms)*100:.1f}%)")
    logger.info(f"  Unmatched firms: {len(unmatched_firms):,} ({len(unmatched_firms)/len(firms)*100:.1f}%)")

    # Analyze unmatched institutions
    logger.info("\n[4/6] Analyzing unmatched institutions...")

    # By paper count
    logger.info("\n  Unmatched institutions by paper count:")
    unmatched_by_papers = unmatched_inst.sort('paper_count', descending=True)
    total_papers_unmatched = unmatched_inst['paper_count'].sum()
    logger.info(f"    Total papers in unmatched institutions: {total_papers_unmatched:,}")

    top_unmatched = unmatched_by_papers.head(20)
    for i, row in enumerate(top_unmatched.iter_rows(named=True), 1):
        logger.info(f"    {i}. {row['display_name'][:60]:<60} {row['paper_count']:>6,} papers")

    # By institution type
    if 'institution_type' in institutions.columns:
        logger.info("\n  Unmatched institutions by type:")
        inst_types = unmatched_inst.group_by('institution_type').agg([
            pl.len().alias('count'),
            pl.col('paper_count').sum().alias('total_papers')
        ]).sort('count', descending=True)

        for row in inst_types.iter_rows(named=True):
            logger.info(f"    {row['institution_type']:<30} {row['count']:>6,} institutions, {row['total_papers']:>8,} papers")

    # By company flag
    if 'is_company' in institutions.columns:
        logger.info("\n  Unmatched institutions that are companies:")
        unmatched_companies = unmatched_inst.filter(pl.col('is_company') == True)
        logger.info(f"    Count: {len(unmatched_companies):,}")
        logger.info(f"    Papers: {unmatched_companies['paper_count'].sum():,}")
        logger.info("\n    Top 20 unmatched companies by papers:")
        top_companies = unmatched_companies.sort('paper_count', descending=True).head(20)
        for i, row in enumerate(top_companies.iter_rows(named=True), 1):
            logger.info(f"      {i}. {row['display_name'][:60]:<60} {row['paper_count']:>6,} papers")

    # Analyze unmatched firms
    logger.info("\n[5/6] Analyzing unmatched firms...")

    # By industry (if available)
    if 'busdesc' in firms.columns:
        logger.info("\n  Sample of unmatched firms by business description:")
        unmatched_firms_sample = unmatched_firms.filter(pl.col('busdesc').is_not_null()).head(30)

        for i, row in enumerate(unmatched_firms_sample.iter_rows(named=True), 1):
            logger.info(f"    {i}. {row['conm'][:40]:<40} | {row['busdesc'][:60] if row['busdesc'] else 'N/A'}")

    # Large firms (market cap proxy)
    logger.info("\n  Unmatched firms by size (top 20 by name length - diversity indicator):")

    # Check which unmatched firms might be research-active
    # Look for firms with "Research", "Laboratories", "Institute", "Technology" in name
    research_keywords = ['RESEARCH', 'LABORATORIES', 'INSTITUTE', 'TECHNOLOGY',
                        'PHARMA', 'BIOTECH', 'SCIENCE', 'ENERGY', 'MATERIALS']

    research_firms = unmatched_firms.filter(
        pl.col('conm').str.contains('|'.join(research_keywords))
    )

    logger.info(f"\n  Unmatched research-oriented firms: {len(research_firms):,}")
    logger.info("\n    Top 30 by name:")
    for i, row in enumerate(research_firms.head(30).iter_rows(named=True), 1):
        logger.info(f"      {i}. {row['conm'][:60]:<60} | {row['tic'] if row.get('tic') else 'N/A':>6}")

    # Check for potential parent-subsidiary relationships
    # Firms that might appear in institution names as substrings
    logger.info("\n[6/6] Looking for missed contained name opportunities...")

    # For each unmatched firm, check if its name appears in any unmatched institution
    missed_opportunities = []

    # Sample first 1000 unmatched firms to check (for performance)
    sample_firms = unmatched_firms.head(1000)

    logger.info(f"  Checking {len(sample_firms):,} firms against {len(unmatched_inst):,} institutions...")

    for firm_row in sample_firms.iter_rows(named=True):
        gvkey = firm_row['GVKEY']  # Compustat uses uppercase
        firm_conm_clean = firm_row.get('conm_clean')

        if not firm_conm_clean:
            continue

        # Look for institutions containing this firm name
        matching_inst = unmatched_inst.filter(
            pl.col('display_name').str.contains(firm_conm_clean)
        )

        if len(matching_inst) > 0:
            for inst_row in matching_inst.iter_rows(named=True):
                missed_opportunities.append({
                    'GVKEY': gvkey,
                    'firm_conm': firm_row['conm'],
                    'firm_name_clean': firm_conm_clean,
                    'institution_id': inst_row['institution_id'],
                    'institution_display_name': inst_row['display_name'],
                    'institution_paper_count': inst_row['paper_count'],
                })

    if missed_opportunities:
        logger.info(f"\n  Found {len(missed_opportunities):,} potential contained-name matches!")

        missed_df = pl.DataFrame(missed_opportunities)

        logger.info("\n    Top 30 missed opportunities by paper count:")
        top_missed = missed_df.sort('institution_paper_count', descending=True).head(30)
        for i, row in enumerate(top_missed.iter_rows(named=True), 1):
            logger.info(f"      {i}. Institution: {row['institution_display_name'][:50]:<50}")
            logger.info(f"         Papers: {row['institution_paper_count']:>6,}")
            logger.info(f"         Firm: {row['firm_conm'][:50]}")

        # Save missed opportunities
        missed_file = OUTPUT_DIR / "missed_contained_name_opportunities.parquet"
        missed_df.write_parquet(missed_file)
        logger.info(f"\n    Saved {len(missed_df):,} missed opportunities to: {missed_file}")

    # Save exploration results
    logger.info("\n" + "=" * 80)
    logger.info("EXPLORATION COMPLETE")
    logger.info("=" * 80)

    # Summary
    logger.info(f"\nSUMMARY:")
    logger.info(f"  Baseline: {len(matched_firm_gvkeys):,} firms, {len(matched_institution_ids):,} institutions")
    logger.info(f"  Unmatched firms: {len(unmatched_firms):,} ({len(unmatched_firms)/len(firms)*100:.1f}% of all firms)")
    logger.info(f"  Unmatched institutions: {len(unmatched_inst):,} ({len(unmatched_inst)/len(institutions)*100:.1f}% of all institutions)")
    logger.info(f"  Papers in unmatched institutions: {total_papers_unmatched:,}")

    if len(research_firms) > 0:
        logger.info(f"\n  Research-oriented unmatched firms: {len(research_firms):,}")

    if missed_opportunities:
        logger.info(f"\n  Missed contained-name opportunities: {len(missed_opportunities):,}")

    logger.info("\n" + "=" * 80)


if __name__ == "__main__":
    main()
