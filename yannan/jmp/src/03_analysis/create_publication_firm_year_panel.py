"""
Stage 6: Create Firm-Year Panel with Publications

This script aggregates papers to the firm-year level for analysis.

Process:
1. Load publication_firm_matches_final.parquet
2. Load ai_papers_firm_affiliated.parquet
3. Join papers with matches via institution_id
4. Aggregate by GVKEY and publication_year
5. Calculate paper counts and citation metrics
6. Output firm-year panel
"""

import polars as pl
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
DATA_PROCESSED_PUB = PROJECT_ROOT / "data" / "processed" / "publication"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_ANALYSIS = PROJECT_ROOT / "data" / "processed" / "analysis"
LOGS_DIR = PROJECT_ROOT / "logs"

FINAL_MATCHES = DATA_PROCESSED_LINK / "publication_firm_matches_final.parquet"
PAPERS_FILE = DATA_PROCESSED_PUB / "ai_papers_firm_affiliated.parquet"
OUTPUT_FILE = DATA_ANALYSIS / "firm_year_panel_with_publications.parquet"
PROGRESS_LOG = LOGS_DIR / "create_publication_firm_year_panel.log"

DATA_PROCESSED_PUB.mkdir(parents=True, exist_ok=True)
DATA_ANALYSIS.mkdir(parents=True, exist_ok=True)
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
    logger.info("STAGE 6: CREATE FIRM-YEAR PANEL WITH PUBLICATIONS")
    logger.info("=" * 80)

    # Step 1: Load data
    logger.info("\n[1/5] Loading data...")

    if not FINAL_MATCHES.exists():
        raise FileNotFoundError(f"Final matches file not found: {FINAL_MATCHES}")
    if not PAPERS_FILE.exists():
        raise FileNotFoundError(f"Papers file not found: {PAPERS_FILE}")

    matches = pl.read_parquet(FINAL_MATCHES)
    papers = pl.read_parquet(PAPERS_FILE)

    logger.info(f"  Loaded {len(matches):,} institution-firm matches")
    logger.info(f"  Loaded {len(papers):,} papers")

    # Step 2: Explode affiliations and join with matches
    logger.info("\n[2/5] Exploding affiliations and joining with matches...")

    # Check if papers have affiliation columns
    logger.info(f"  Paper columns: {papers.columns}")

    # Explode author_affiliation_ids and author_affiliations
    # Note: These are nested lists (list of affiliations per author)
    # Need to explode twice to get individual affiliations
    if 'author_affiliation_ids' in papers.columns:
        # First explode: papers -> authors
        papers = papers.explode('author_affiliation_ids')
        # Second explode: authors -> individual affiliations
        papers = papers.explode('author_affiliation_ids')
        join_col = 'author_affiliation_ids'
    elif 'author_affiliations' in papers.columns:
        papers = papers.explode('author_affiliations')
        papers = papers.explode('author_affiliations')
        join_col = 'author_affiliations'
    else:
        logger.error("  No affiliation column found!")
        return None

    # Join with matches
    papers_with_firms = papers.join(matches, left_on=join_col, right_on='institution_id', how='inner')
    logger.info(f"  After join: {len(papers_with_firms):,} paper-firm pairs")

    # Step 3: Filter to valid years
    logger.info("\n[3/5] Filtering to valid years...")
    if 'publication_year' in papers_with_firms.columns:
        papers_with_firms = papers_with_firms.filter(
            (pl.col('publication_year').is_between(1970, 2024)) &
            (pl.col('publication_year').is_not_null())
        )
        logger.info(f"  Papers in valid year range: {len(papers_with_firms):,}")
    else:
        logger.warning("  publication_year column not found!")

    # Step 4: Aggregate to firm-year level
    logger.info("\n[4/5] Aggregating to firm-year level...")

    # Group by GVKEY and publication_year
    agg_exprs = [
        pl.len().alias('total_papers'),
    ]

    # Add citation metrics if available
    if 'cited_by_count' in papers_with_firms.columns:
        agg_exprs.extend([
            pl.col('cited_by_count').sum().alias('total_citations'),
            pl.col('cited_by_count').mean().alias('mean_citations_per_paper'),
        ])

    firm_year_panel = papers_with_firms.group_by(['GVKEY', 'publication_year']).agg(
        *agg_exprs
    )

    logger.info(f"  Created {len(firm_year_panel):,} firm-year observations")

    # Step 5: Save output
    logger.info("\n[5/5] Saving firm-year panel...")
    firm_year_panel.write_parquet(OUTPUT_FILE, compression='snappy')
    logger.info(f"  Saved {len(firm_year_panel):,} observations to {OUTPUT_FILE}")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("FIRM-YEAR PANEL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total observations (firm-year pairs): {len(firm_year_panel):,}")
    logger.info(f"Unique firms: {firm_year_panel['GVKEY'].n_unique():,}")
    logger.info(f"Year range: {firm_year_panel['publication_year'].min():,} - {firm_year_panel['publication_year'].max():,}")

    if 'total_papers' in firm_year_panel.columns:
        total_papers = firm_year_panel['total_papers'].sum()
        logger.info(f"\nTotal papers: {total_papers:,}")
        logger.info(f"Papers per year stats:")
        year_stats = firm_year_panel.group_by('publication_year').agg(
            pl.col('total_papers').sum().alias('papers')
        ).sort('publication_year')
        for row in year_stats.head(10).iter_rows(named=True):
            logger.info(f"  {row['publication_year']}: {row['papers']:,} papers")

    if 'total_citations' in firm_year_panel.columns:
        total_citations = firm_year_panel['total_citations'].sum()
        logger.info(f"\nTotal citations: {total_citations:,}")

    logger.info("\n" + "=" * 80)
    logger.info("STAGE 6 COMPLETE")
    logger.info("=" * 80)

    return firm_year_panel


if __name__ == "__main__":
    main()
