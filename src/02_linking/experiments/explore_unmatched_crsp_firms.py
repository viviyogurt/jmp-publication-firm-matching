"""
Explore CRSP Firms with High Paper Counts That Are Unmatched

Identify firms in Compustat that have publications but aren't matched,
understand why matching failed, and find improvement opportunities.
"""

import polars as pl
from pathlib import Path
import logging
from rapidfuzz import fuzz, process

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

INSTITUTIONS_MASTER = DATA_INTERIM / "publication_institutions_master.parquet"
COMPUSTAT_STANDARDIZED = DATA_INTERIM / "compustat_firms_standardized.parquet"
MATCHED_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_with_alternative_names.parquet"

LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "explore_unmatched_crsp_firms.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Explore unmatched CRSP firms with high paper counts."""
    logger.info("=" * 80)
    logger.info("EXPLORING UNMATCHED CRSP FIRMS WITH HIGH PAPER COUNTS")
    logger.info("=" * 80)

    # Load data
    logger.info("\n[1/6] Loading data...")
    institutions = pl.read_parquet(INSTITUTIONS_MASTER)
    firms = pl.read_parquet(COMPUSTAT_STANDARDIZED)
    matches = pl.read_parquet(MATCHED_FILE)

    logger.info(f"  Loaded {len(institutions):,} institutions")
    logger.info(f"  Loaded {len(firms):,} firms")
    logger.info(f"  Loaded {len(matches):,} matched pairs")

    # Get matched GVKEYs
    matched_gvkeys = set(matches['GVKEY'].to_list())
    logger.info(f"  Already matched firms: {len(matched_gvkeys):,}")

    # Identify unmatched firms that have publications
    logger.info("\n[2/6] Identifying unmatched firms with publications...")

    # Build institution name set for fuzzy matching
    inst_names = institutions['display_name'].to_list()

    unmatched_firms_with_papers = []

    for firm_row in firms.iter_rows(named=True):
        gvkey = firm_row['GVKEY']
        conm = firm_row['conm']
        conm_clean = firm_row['conm_clean']

        if gvkey in matched_gvkeys:
            continue

        # Try to find similar institution names
        if conm_clean:
            results = process.extract(
                conm_clean,
                inst_names,
                limit=3,
                scorer=fuzz.WRatio
            )

            if results and results[0][1] >= 80:  # 80% similarity threshold
                best_match_name = results[0][0]
                best_score = results[0][1]

                # Get the institution details
                inst_match = institutions.filter(
                    pl.col('display_name') == best_match_name
                )

                if len(inst_match) > 0:
                    inst_row = inst_match.row(0, named=True)

                    unmatched_firms_with_papers.append({
                        'GVKEY': gvkey,
                        'firm_conm': conm,
                        'firm_conm_clean': conm_clean,
                        'firm_tic': firm_row.get('tic', 'N/A'),
                        'firm_fic': firm_row.get('fic', 'N/A'),
                        'matched_institution': best_match_name,
                        'similarity_score': best_score,
                        'institution_id': inst_row['institution_id'],
                        'institution_papers': inst_row['paper_count'],
                        'institution_country': inst_row.get('country_code', 'N/A'),
                        'inst_alt_names': inst_row.get('alternative_names', []),
                    })

    logger.info(f"  Found {len(unmatched_firms_with_papers):,} unmatched firms with similar institutions")

    if len(unmatched_firms_with_papers) == 0:
        logger.warning("  No unmatched firms found!")
        return

    # Convert to dataframe
    unmatched_df = pl.DataFrame(unmatched_firms_with_papers)
    unmatched_df = unmatched_df.sort('institution_papers', descending=True)

    # Analyze by paper count
    logger.info("\n[3/6] Analyzing by paper count...")

    high_value = unmatched_df.filter(pl.col('institution_papers') >= 1000)
    medium_value = unmatched_df.filter(
        (pl.col('institution_papers') >= 500) &
        (pl.col('institution_papers') < 1000)
    )

    logger.info(f"  High-value (≥1,000 papers): {len(high_value):,} firms")
    logger.info(f"  Medium-value (500-999 papers): {len(medium_value):,} firms")

    # Show top 50 unmatched
    logger.info("\n[4/6] Top 50 unmatched firms by paper count...")
    logger.info("  " + "-" * 78)

    top_50 = unmatched_df.head(50)

    for i, row in enumerate(top_50.iter_rows(named=True), 1):
        logger.info(f"\n  {i:2d}. {row['firm_conm'][:60]}")
        logger.info(f"      GVKEY: {row['GVKEY']}, TIC: {row['firm_tic']}, Country: {row['firm_fic']}")
        logger.info(f"      Similar institution: {row['matched_institution'][:60]}")
        logger.info(f"      Papers: {row['institution_papers']:,}, Similarity: {row['similarity_score']:.1f}%")
        logger.info(f"      Inst country: {row['institution_country']}")

        # Show alternative names
        alt_names = row['inst_alt_names']
        if alt_names and len(alt_names) > 0:
            logger.info(f"      Alt names ({len(alt_names)}): {alt_names[:3]}")
        else:
            logger.info(f"      Alt names: NONE")

    # Analyze patterns
    logger.info("\n[5/6] Analyzing patterns...")

    # Count by similarity score range
    logger.info("\n  Distribution by similarity score:")
    ranges = [
        (95, 100, "95-100%"),
        (90, 94, "90-94%"),
        (85, 89, "85-89%"),
        (80, 84, "80-84%"),
    ]

    for min_score, max_score, label in ranges:
        count = unmatched_df.filter(
            (pl.col('similarity_score') >= min_score) &
            (pl.col('similarity_score') < max_score)
        ).height
        logger.info(f"    {label}: {count:,} firms")

    # Count by country
    logger.info("\n  Distribution by firm country (FIC):")
    country_counts = unmatched_df.group_by('firm_fic').agg(
        pl.len().alias('count'),
        pl.col('institution_papers').sum().alias('total_papers')
    ).sort('total_papers', descending=True).head(15)

    for row in country_counts.iter_rows(named=True):
        logger.info(f"    {row['firm_fic']}: {row['count']:,} firms, {row['total_papers']:,} papers")

    # Check why top matches failed
    logger.info("\n[6/6] Analyzing why top matches failed...")

    logger.info("\n  Top 20 - Detailed analysis:")
    for i, row in enumerate(top_50.head(20).iter_rows(named=True), 1):
        firm_name = row['firm_conm_clean']
        inst_name = row['matched_institution']

        # Detailed comparison
        logger.info(f"\n  {i}. {row['firm_conm'][:60]}")
        logger.info(f"     Firm (cleaned): {firm_name[:60]}")
        logger.info(f"     Institution: {inst_name[:60]}")

        # Check for common issues
        issues = []

        # Check if names are very different
        if row['similarity_score'] < 85:
            issues.append(f"Low similarity ({row['similarity_score']:.1f}%)")

        # Check country mismatch
        if row['firm_fic'] != row['institution_country'] and row['institution_country'] != 'N/A':
            issues.append(f"Country mismatch: firm={row['firm_fic']}, inst={row['institution_country']}")

        # Check if alternative names could help
        alt_names = row['inst_alt_names']
        if alt_names:
            # Check if any alt name matches better
            best_alt_score = 0
            best_alt_name = None
            for alt in alt_names:
                score = fuzz.WRatio(firm_name, alt)
                if score > best_alt_score:
                    best_alt_score = score
                    best_alt_name = alt

            if best_alt_score > row['similarity_score']:
                issues.append(f"Better alt name match: '{best_alt_name}' ({best_alt_score:.1f}%)")
            else:
                issues.append("Alt names don't help")
        else:
            issues.append("No alternative names")

        # Check for special characters
        if any(c in firm_name for c in ['&', '-', '.', ',']):
            issues.append("Firm name has special characters")

        for issue in issues:
            logger.info(f"     ⚠️  {issue}")

    # Summary statistics
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)

    total_papers_unmatched = unmatched_df['institution_papers'].sum()
    top_100_papers = unmatched_df.head(100)['institution_papers'].sum()

    logger.info(f"\nUnmatched firms with similar institutions: {len(unmatched_df):,}")
    logger.info(f"Total papers in these unmatched institutions: {total_papers_unmatched:,}")
    logger.info(f"Papers in top 100 unmatched: {top_100_papers:,}")
    logger.info(f"Average similarity score: {unmatched_df['similarity_score'].mean():.1f}%")

    # Potential for improvement
    very_high_similarity = unmatched_df.filter(pl.col('similarity_score') >= 95)
    logger.info(f"\nFirms with ≥95% similarity (potential matches): {len(very_high_similarity):,}")
    logger.info(f"  Papers in these: {very_high_similarity['institution_papers'].sum():,}")

    logger.info("\n" + "=" * 80)


if __name__ == "__main__":
    main()
