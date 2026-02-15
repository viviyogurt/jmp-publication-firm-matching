"""
Analyze Merged ArXiv Dataset

Analyzes the merged ArXiv dataset to understand coverage:
1. Total number of papers
2. Timeframes (date range)
3. Papers per year
4. Missing values per column
5. Number of authors per paper
6. Category classification and counts

Date: 2025
"""

import polars as pl
from pathlib import Path
import logging
from collections import Counter
import re

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
OUTPUT_DIR = PROJECT_ROOT / "output" / "tables"

INPUT_FILE = DATA_PROCESSED / "merged_arxiv_complete.parquet"
OUTPUT_CSV = OUTPUT_DIR / "merged_arxiv_analysis.csv"
OUTPUT_YEARLY = OUTPUT_DIR / "merged_arxiv_yearly_stats.csv"
OUTPUT_CATEGORIES = OUTPUT_DIR / "merged_arxiv_category_stats.csv"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / "analyze_merged_arxiv.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# Helper Functions
# ============================================================================

def count_authors(authors_str):
    """Count number of authors from authors string."""
    if not authors_str or authors_str is None:
        return 0
    
    authors_str = str(authors_str)
    
    # Common separators: semicolon, comma, " and ", " & "
    # Count by splitting on common patterns
    # ArXiv format often uses semicolons or " and "
    
    # Try semicolon first (most common in ArXiv)
    if ';' in authors_str:
        return len([a.strip() for a in authors_str.split(';') if a.strip()])
    
    # Try " and " or " & "
    if ' and ' in authors_str or ' & ' in authors_str:
        # Count occurrences
        count = authors_str.count(' and ') + authors_str.count(' & ')
        return count + 1  # +1 for the last author
    
    # Try comma (less reliable, but try)
    if ',' in authors_str:
        # Be careful - commas might be in names
        # Rough estimate: count commas and add 1
        return authors_str.count(',') + 1
    
    # Single author
    return 1 if authors_str.strip() else 0


def extract_categories(categories_str):
    """Extract individual categories from categories string."""
    if not categories_str or categories_str is None:
        return []
    
    categories_str = str(categories_str)
    
    # Categories are typically space-separated, e.g., "math.CO math.PR" or "hep-th"
    # Split by space
    categories = [c.strip() for c in categories_str.split() if c.strip()]
    
    return categories


def extract_primary_category(categories_str):
    """Extract primary category (first category)."""
    categories = extract_categories(categories_str)
    return categories[0] if categories else None


def get_category_class(category):
    """Classify category into broad class."""
    if not category:
        return "Unknown"
    
    category = str(category).lower()
    
    # ArXiv category classification
    if category.startswith('math.'):
        return "Mathematics"
    elif category.startswith('cs.'):
        return "Computer Science"
    elif category.startswith('physics.'):
        return "Physics"
    elif category.startswith('astro-ph'):
        return "Astrophysics"
    elif category.startswith('cond-mat'):
        return "Condensed Matter"
    elif category.startswith('hep-'):
        return "High Energy Physics"
    elif category.startswith('quant-ph'):
        return "Quantum Physics"
    elif category.startswith('q-bio'):
        return "Quantitative Biology"
    elif category.startswith('q-fin'):
        return "Quantitative Finance"
    elif category.startswith('stat.'):
        return "Statistics"
    elif category.startswith('econ.'):
        return "Economics"
    elif category.startswith('eess.'):
        return "Electrical Engineering"
    else:
        return "Other"


# ============================================================================
# Main Analysis
# ============================================================================

def analyze_merged_arxiv():
    """
    Analyze the merged ArXiv dataset.
    """
    logger.info("=" * 80)
    logger.info("ANALYZING MERGED ARXIV DATASET")
    logger.info("=" * 80)
    logger.info("")
    
    # Step 1: Load dataset
    logger.info("Step 1: Loading merged dataset...")
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        return
    
    # Load schema first to check columns
    schema = pl.read_parquet(INPUT_FILE, n_rows=0).schema
    logger.info(f"Columns: {list(schema.keys())}")
    
    # Use lazy evaluation for efficiency
    df_lazy = pl.scan_parquet(INPUT_FILE)
    
    # Get basic count
    total_papers = df_lazy.select(pl.len()).collect().item()
    logger.info(f"Total papers: {total_papers:,}")
    logger.info("")
    
    # Step 2: Timeframes
    logger.info("Step 2: Analyzing timeframes...")
    
    min_date = None
    max_date = None
    has_date_count = 0
    
    if "published" in schema:
        date_stats = (
            df_lazy
            .select([
                pl.col("published").min().alias("min_date"),
                pl.col("published").max().alias("max_date"),
                pl.col("published").is_not_null().sum().alias("has_date_count")
            ])
            .collect()
        )
        
        min_date = date_stats["min_date"][0]
        max_date = date_stats["max_date"][0]
        has_date_count = date_stats["has_date_count"][0]
        
        logger.info(f"Date range: {min_date} to {max_date}")
        logger.info(f"Papers with publication date: {has_date_count:,} ({has_date_count/total_papers*100:.2f}%)")
        logger.info("")
    
    # Step 3: Papers per year
    logger.info("Step 3: Calculating papers per year...")
    
    yearly_stats = None
    
    if "published" in schema:
        yearly_stats = (
            df_lazy
            .filter(pl.col("published").is_not_null())
            .with_columns([
                pl.col("published").dt.year().alias("year")
            ])
            .group_by("year")
            .agg([
                pl.len().alias("paper_count")
            ])
            .sort("year")
            .collect()
        )
        
        logger.info(f"Years covered: {yearly_stats['year'].min()} to {yearly_stats['year'].max()}")
        logger.info(f"Total years: {len(yearly_stats)}")
        
        # Save yearly stats
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        yearly_stats.write_csv(OUTPUT_YEARLY)
        logger.info(f"Yearly statistics saved to: {OUTPUT_YEARLY}")
        logger.info("")
    
    # Step 4: Missing values per column
    logger.info("Step 4: Analyzing missing values...")
    
    missing_stats = (
        df_lazy
        .select([
            (pl.col(col).is_null().sum() / pl.len() * 100).alias(f"{col}_missing_pct")
            for col in schema.keys()
        ])
        .collect()
    )
    
    missing_dict = {}
    for col in schema.keys():
        pct = missing_stats[f"{col}_missing_pct"][0]
        missing_dict[col] = pct
        if pct > 0:
            logger.info(f"  {col}: {pct:.2f}% missing")
    
    logger.info("")
    
    # Step 5: Authors per paper
    logger.info("Step 5: Analyzing authors per paper...")
    
    author_counts = None
    author_stats_dict = None
    
    if "authors" in schema:
        # Count authors for each paper
        # Need to collect first, then process
        authors_df = df_lazy.select("authors").collect()
        
        author_counts = authors_df.with_columns([
            pl.col("authors")
            .map_batches(
                lambda s: pl.Series([count_authors(x) for x in s]),
                return_dtype=pl.Int64
            )
            .alias("num_authors")
        ])
        
        # Calculate statistics directly
        mean_authors = author_counts.select(pl.col("num_authors").mean()).item()
        median_authors = author_counts.select(pl.col("num_authors").median()).item()
        min_authors = author_counts.select(pl.col("num_authors").min()).item()
        max_authors = author_counts.select(pl.col("num_authors").max()).item()
        
        logger.info("Author statistics:")
        logger.info(f"  Mean: {mean_authors:.2f}")
        logger.info(f"  Median: {median_authors:.2f}")
        logger.info(f"  Min: {min_authors}")
        logger.info(f"  Max: {max_authors}")
        
        # Store for summary
        author_stats_dict = {
            'mean': mean_authors,
            'median': median_authors,
            'min': min_authors,
            'max': max_authors
        }
        
        # Distribution
        author_dist = (
            author_counts
            .group_by("num_authors")
            .agg(pl.len().alias("count"))
            .sort("num_authors")
            .head(20)  # Top 20
        )
        logger.info("\nTop 20 author count distribution:")
        for row in author_dist.iter_rows(named=True):
            logger.info(f"  {row['num_authors']} authors: {row['count']:,} papers")
        
        logger.info("")
    
    # Step 6: Category classification
    logger.info("Step 6: Analyzing categories...")
    
    category_data = None
    
    if "categories" in schema or "primary_category" in schema:
        # Use primary_category if available, otherwise extract from categories
        category_col = "primary_category" if "primary_category" in schema else "categories"
        
        # Extract primary category - collect first then process
        cat_df = df_lazy.select(category_col).collect()
        
        category_data = cat_df.with_columns([
            pl.col(category_col)
            .map_batches(
                lambda s: pl.Series([extract_primary_category(x) for x in s]),
                return_dtype=pl.Utf8
            )
            .alias("primary_cat")
        ])
        
        # Count by primary category
        category_counts = (
            category_data
            .filter(pl.col("primary_cat").is_not_null())
            .group_by("primary_cat")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(50)  # Top 50 categories
        )
        
        logger.info(f"Total unique primary categories: {category_data['primary_cat'].n_unique()}")
        logger.info("\nTop 50 primary categories:")
        for row in category_counts.iter_rows(named=True):
            logger.info(f"  {row['primary_cat']}: {row['count']:,} papers")
        
        # Classify by broad category class
        category_class_data = (
            category_data
            .with_columns([
                pl.col("primary_cat")
                .map_batches(
                    lambda s: pl.Series([get_category_class(x) for x in s]),
                    return_dtype=pl.Utf8
                )
                .alias("category_class")
            ])
        )
        
        class_counts = (
            category_class_data
            .filter(pl.col("category_class").is_not_null())
            .group_by("category_class")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )
        
        logger.info("\nPapers by broad category class:")
        for row in class_counts.iter_rows(named=True):
            pct = row['count'] / total_papers * 100
            logger.info(f"  {row['category_class']}: {row['count']:,} papers ({pct:.2f}%)")
        
        # Save category stats
        category_counts.write_csv(OUTPUT_CATEGORIES)
        logger.info(f"\nCategory statistics saved to: {OUTPUT_CATEGORIES}")
        logger.info("")
    
    # Step 7: Create summary report
    logger.info("Step 7: Creating summary report...")
    
    summary_data = {
        "metric": [
            "Total Papers",
            "Date Range Start",
            "Date Range End",
            "Papers with Publication Date",
            "Publication Date Coverage (%)",
            "Total Unique Categories",
            "Papers with Authors",
            "Mean Authors per Paper",
            "Median Authors per Paper",
        ],
        "value": [
            f"{total_papers:,}",
            str(min_date) if "published" in schema else "N/A",
            str(max_date) if "published" in schema else "N/A",
            f"{has_date_count:,}" if "published" in schema else "N/A",
            f"{has_date_count/total_papers*100:.2f}%" if "published" in schema else "N/A",
            f"{category_data['primary_cat'].n_unique()}" if ("categories" in schema or "primary_category" in schema) else "N/A",
            f"{author_counts.filter(pl.col('num_authors') > 0).height:,}" if "authors" in schema else "N/A",
            f"{author_stats_dict['mean']:.2f}" if ("authors" in schema and author_stats_dict) else "N/A",
            f"{author_stats_dict['median']:.2f}" if ("authors" in schema and author_stats_dict) else "N/A",
        ]
    }
    
    summary_df = pl.DataFrame(summary_data)
    summary_df.write_csv(OUTPUT_CSV)
    logger.info(f"Summary report saved to: {OUTPUT_CSV}")
    logger.info("")
    
    # Step 8: Print final summary
    logger.info("=" * 80)
    logger.info("ANALYSIS SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total papers: {total_papers:,}")
    if "published" in schema and min_date is not None:
        logger.info(f"Date range: {min_date} to {max_date}")
        if yearly_stats is not None:
            logger.info(f"Years covered: {yearly_stats['year'].min()} to {yearly_stats['year'].max()}")
    if "authors" in schema and author_stats_dict is not None:
        logger.info(f"Mean authors per paper: {author_stats_dict['mean']:.2f}")
    if ("categories" in schema or "primary_category" in schema) and category_data is not None:
        logger.info(f"Unique categories: {category_data['primary_cat'].n_unique()}")
    logger.info("")
    logger.info("=" * 80)
    logger.info("ANALYSIS COMPLETE")
    logger.info("=" * 80)


def main():
    """Main entry point."""
    try:
        analyze_merged_arxiv()
    except Exception as e:
        logger.error(f"Error during analysis: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
