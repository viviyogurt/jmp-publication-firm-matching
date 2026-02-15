"""
Explore Unmatched Institutions with High Paper Counts

Identify institutions we're missing that have significant publication activity.
Understand why they're not being matched and identify improvement opportunities.
"""

import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
DOCS_DIR = PROJECT_ROOT / "docs"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
MATCHED_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_with_location_removal.parquet"

LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "explore_unmatched.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Explore unmatched institutions."""
    logger.info("=" * 80)
    logger.info("EXPLORING UNMATCHED INSTITUTIONS WITH HIGH PAPER COUNTS")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/4] Loading data...")
    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    matched = pl.read_parquet(MATCHED_FILE)

    logger.info(f"  Loaded {len(institutions):,} institutions")
    logger.info(f"  Loaded {len(firms):,} firms")
    logger.info(f"  Loaded {len(matched):,} matched pairs")

    # Get matched institution IDs
    matched_ids = set(matched['institution_id'].to_list())
    logger.info(f"  Matched institutions: {len(matched_ids):,}")

    # Find unmatched institutions
    logger.info("\n[2/4] Finding unmatched institutions...")
    unmatched = institutions.filter(
        ~pl.col('institution_id').is_in(matched_ids)
    ).sort('paper_count', descending=True)

    logger.info(f"  Unmatched institutions: {len(unmatched):,}")
    logger.info(f"  Papers in unmatched institutions: {unmatched['paper_count'].sum():,}")

    # Analyze top unmatched
    logger.info("\n[3/4] Analyzing top 100 unmatched institutions...")

    top_100 = unmatched.head(100)

    # Group by type/pattern
    logger.info("\n  Top 50 unmatched institutions by paper count:")
    logger.info("  " + "-" * 78)

    for i, row in enumerate(top_100.head(50).iter_rows(named=True), 1):
        display_name = row['display_name']
        papers = row['paper_count']

        # Check for patterns
        issues = []

        # Check if it has location qualifier
        if '(' in display_name:
            issues.append("Has location qualifier")

        # Check if it's a company
        if row.get('is_company'):
            issues.append("Flagged as company")

        # Check type
        inst_type = row.get('institution_type', 'Unknown')

        # Check country
        country = row.get('country_code', 'Unknown')

        issue_str = f" [{', '.join(issues)}]" if issues else ""

        logger.info(f"  {i:2d}. {display_name[:60]:<60}")
        logger.info(f"      Papers: {papers:>7,} | Type: {inst_type:20} | Country: {country:15} |{issue_str}")

    # Analyze patterns
    logger.info("\n[4/4] Analyzing patterns...")

    # Count by institution type
    if 'institution_type' in unmatched.columns:
        logger.info("\n  Unmatched by institution type:")
        type_counts = unmatched.group_by('institution_type').agg([
            pl.len().alias('count'),
            pl.col('paper_count').sum().alias('total_papers')
        ]).sort('total_papers', descending=True)

        for row in type_counts.head(15).iter_rows(named=True):
            logger.info(f"    {row['institution_type']:<30} | {row['count']:>6,} institutions | {row['total_papers']:>10,} papers")

    # Check for companies that should be matched
    logger.info("\n  Checking unmatched institutions flagged as companies:")
    # Note: is_company column doesn't exist, skip this step
    logger.info("    (Skipped - is_company column not available)")

    # Check for potential firm matches
    logger.info("\n  Checking for potential firm name matches...")

    # Load firm names
    firm_names = firms['conm_clean'].to_list()

    # For each top unmatched, check if similar firm names exist
    logger.info("\n  Top 20 unmatched - searching for similar firm names:")

    from rapidfuzz import fuzz, process

    for i, row in enumerate(top_100.head(20).iter_rows(named=True), 1):
        inst_name = row['display_name']

        # Remove location for comparison
        import re
        inst_name_no_loc = re.sub(r'\s*\(.*?\)', '', inst_name).strip()

        # Clean for comparison
        inst_clean = re.sub(r'[^\w\s]', '', inst_name_no_loc).upper()
        inst_clean = re.sub(r'\s+', ' ', inst_clean).strip()

        # Find top 3 similar firm names
        if inst_clean:
            results = process.extract(
                inst_clean,
                firm_names,
                limit=3,
                scorer=fuzz.WRatio
            )

            logger.info(f"\n  {i}. {inst_name[:50]:<50} (Papers: {row['paper_count']:,})")

            best_score = results[0][1] if results else 0
            logger.info(f"     Best firm match score: {best_score:.1f}/100")

            if best_score >= 85:
                logger.info(f"     ⚠️  High similarity matches found:")
                for firm_name, score, _ in results:
                    if score >= 85:
                        # Get firm details
                        firm_details = firms.filter(
                            pl.col('conm_clean') == firm_name
                        )
                        if len(firm_details) > 0:
                            firm_row = firm_details.row(0, named=True)
                            logger.info(f"       → {firm_row['conm'][:50]:<50} (GVKEY: {firm_row['GVKEY']}, TIC: {firm_row.get('tic', 'N/A')})")
            elif best_score >= 70:
                logger.info(f"     ⚠️  Moderate similarity matches:")
                for firm_name, score, _ in results[:2]:
                    if score >= 70:
                        firm_details = firms.filter(
                            pl.col('conm_clean') == firm_name
                        )
                        if len(firm_details) > 0:
                            firm_row = firm_details.row(0, named=True)
                            logger.info(f"       → {firm_row['conm'][:50]:<50} (GVKEY: {firm_row['GVKEY']}, Score: {score:.1f})")
            else:
                logger.info(f"     ✗ No similar firm names found (best score: {best_score:.1f}/100)")

    # Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("UNMATCHED INSTITUTIONS SUMMARY")
    logger.info("=" * 80)

    total_papers_unmatched = unmatched['paper_count'].sum()
    total_papers_all = institutions['paper_count'].sum()
    total_papers_matched = matched_ids and institutions.filter(
        pl.col('institution_id').is_in(matched_ids)
    )['paper_count'].sum() or 0

    logger.info(f"\nTotal institutions: {len(institutions):,}")
    logger.info(f"Matched institutions: {len(matched_ids):,} ({len(matched_ids)/len(institutions)*100:.1f}%)")
    logger.info(f"Unmatched institutions: {len(unmatched):,} ({len(unmatched)/len(institutions)*100:.1f}%)")

    logger.info(f"\nTotal papers: {total_papers_all:,}")
    logger.info(f"Papers in matched institutions: {total_papers_matched:,} ({total_papers_matched/total_papers_all*100:.1f}%)")
    logger.info(f"Papers in unmatched institutions: {total_papers_unmatched:,} ({total_papers_unmatched/total_papers_all*100:.1f}%)")

    logger.info(f"\nTop 100 unmatched account for: {top_100['paper_count'].sum():,} papers")
    logger.info(f"Average papers per top 100 unmatched: {top_100['paper_count'].mean():.0f}")

    logger.info("\n" + "=" * 80)


if __name__ == "__main__":
    main()
