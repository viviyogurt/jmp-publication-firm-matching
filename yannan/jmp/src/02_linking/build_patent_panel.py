"""
Build Assignee-Year Panel of AI Innovation from USPTO PatentsView Data.

This script processes raw USPTO patent data to create an assignee-year panel
measuring AI innovation intensity using the AI model predictions dataset.

"""

import polars as pl
import re
import zipfile
import io
from pathlib import Path


# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "patents"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"

# Input files
AI_PREDICTIONS_FILE = DATA_RAW / "ai_model_predictions.csv"
PATENT_FILE = DATA_RAW / "g_patent.tsv.zip"
ASSIGNEE_FILE = DATA_RAW / "g_assignee_disambiguated.tsv.zip"

# Output file
OUTPUT_FILE = DATA_INTERIM / "patents_panel.parquet"

# AI score threshold
AI_SCORE_THRESHOLD = 0.5


# ============================================================================
# Helper Functions
# ============================================================================
def read_zipped_tsv(zip_path: Path, **kwargs) -> pl.LazyFrame:
    """
    Read a zipped TSV file into a Polars LazyFrame.
    Extracts the first file from the zip archive and reads it as TSV.
    """
    with zipfile.ZipFile(zip_path, 'r') as zf:
        # Get the first file in the archive
        file_name = zf.namelist()[0]
        with zf.open(file_name) as f:
            content = f.read()
    
    # Read from bytes using polars
    df = pl.read_csv(
        io.BytesIO(content),
        separator="\t",
        quote_char='"',
        infer_schema_length=10000,
        **kwargs
    )
    return df.lazy()


def clean_organization_name(name: str | None) -> str | None:
    """
    Standardize organization names:
    - Convert to uppercase
    - Remove common suffixes (INC, LTD, CORP, LLC, CO, etc.)
    - Remove punctuation
    - Strip extra whitespace
    """
    if name is None or name == "":
        return None
    
    # Convert to uppercase
    name = name.upper()
    
    # Remove common corporate suffixes (order matters - longer patterns first)
    suffixes_pattern = r'\b(INCORPORATED|CORPORATION|COMPANY|LIMITED|L\.?L\.?C\.?|INC\.?|LTD\.?|CORP\.?|CO\.?|PLC\.?|S\.?A\.?|A\.?G\.?|GMBH|N\.?V\.?|B\.?V\.?)\b'
    name = re.sub(suffixes_pattern, '', name)
    
    # Remove punctuation (keep alphanumeric and spaces)
    name = re.sub(r'[^\w\s]', '', name)
    
    # Collapse multiple spaces and strip
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name if name else None


def main():
    """Main processing pipeline."""
    print("=" * 70)
    print("Building Assignee-Year Panel of AI Innovation")
    print("=" * 70)
    
    # Ensure output directory exists
    DATA_INTERIM.mkdir(parents=True, exist_ok=True)
    
    # -------------------------------------------------------------------------
    # Step 1: Load and filter AI patents
    # -------------------------------------------------------------------------
    print("\n[1/6] Loading AI patent predictions...")
    
    ai_patents = (
        pl.scan_csv(
            AI_PREDICTIONS_FILE,
            schema_overrides={"doc_id": pl.Utf8}  # Patent IDs can be non-numeric (e.g., RE28671)
        )
        .select([
            pl.col("doc_id").alias("patent_id"),
            pl.col("predict50_any_ai").alias("ai_flag"),
            # Compute composite AI score as max across all categories
            pl.max_horizontal([
                "ai_score_ml", "ai_score_evo", "ai_score_nlp", 
                "ai_score_speech", "ai_score_vision", "ai_score_planning",
                "ai_score_kr", "ai_score_hardware"
            ]).alias("ai_score")
        ])
        .filter(
            (pl.col("ai_flag") == 1) | (pl.col("ai_score") > AI_SCORE_THRESHOLD)
        )
    )
    
    # Collect count for logging
    ai_count = ai_patents.select(pl.len()).collect().item()
    print(f"    Found {ai_count:,} AI patents (flag=1 or score > {AI_SCORE_THRESHOLD})")
    
    # -------------------------------------------------------------------------
    # Step 2: Load patent dates and extract year
    # -------------------------------------------------------------------------
    print("\n[2/6] Loading patent grant dates...")
    
    patents = (
        read_zipped_tsv(PATENT_FILE, schema_overrides={"patent_id": pl.Utf8})
        .select([
            pl.col("patent_id"),
            pl.col("patent_date").str.slice(0, 4).cast(pl.Int32).alias("year")
        ])
    )
    
    # -------------------------------------------------------------------------
    # Step 3: Join AI patents with dates
    # -------------------------------------------------------------------------
    print("\n[3/6] Joining AI patents with grant dates...")
    
    ai_with_dates = ai_patents.join(
        patents,
        on="patent_id",
        how="inner"
    )
    
    # -------------------------------------------------------------------------
    # Step 4: Load assignee information
    # -------------------------------------------------------------------------
    print("\n[4/6] Loading assignee information...")
    
    assignees = (
        read_zipped_tsv(ASSIGNEE_FILE, schema_overrides={"patent_id": pl.Utf8})
        .select([
            pl.col("patent_id"),
            pl.col("assignee_id"),
            pl.col("disambig_assignee_organization").alias("organization")
        ])
        # Filter to only corporate assignees (non-null organization)
        .filter(pl.col("organization").is_not_null())
        .filter(pl.col("organization") != "")
    )
    
    # -------------------------------------------------------------------------
    # Step 5: Join with assignees and clean names
    # -------------------------------------------------------------------------
    print("\n[5/6] Joining with assignees and cleaning organization names...")
    
    ai_with_assignees = ai_with_dates.join(
        assignees,
        on="patent_id",
        how="inner"
    )
    
    # Collect to apply Python UDF for name cleaning
    df = ai_with_assignees.collect()
    print(f"    Patents with assignees: {len(df):,}")
    
    # Clean organization names
    df = df.with_columns(
        pl.col("organization")
        .map_elements(clean_organization_name, return_dtype=pl.Utf8)
        .alias("clean_name")
    )
    
    # Filter out null clean names
    df = df.filter(pl.col("clean_name").is_not_null())
    print(f"    After name cleaning: {len(df):,}")
    
    # -------------------------------------------------------------------------
    # Step 6: Aggregate to assignee-year panel
    # -------------------------------------------------------------------------
    print("\n[6/6] Aggregating to assignee-year panel...")
    
    panel = (
        df.group_by(["clean_name", "year"])
        .agg([
            pl.col("patent_id").n_unique().alias("total_ai_patents"),
            pl.col("ai_score").mean().alias("avg_ai_score"),
            pl.col("assignee_id").first().alias("assignee_id")  # Keep one ID for reference
        ])
        .sort(["clean_name", "year"])
    )
    
    print(f"    Final panel: {len(panel):,} assignee-year observations")
    print(f"    Unique assignees: {panel['clean_name'].n_unique():,}")
    print(f"    Year range: {panel['year'].min()} - {panel['year'].max()}")
    
    # -------------------------------------------------------------------------
    # Save output
    # -------------------------------------------------------------------------
    print(f"\nSaving to {OUTPUT_FILE}...")
    panel.write_parquet(OUTPUT_FILE)
    
    print("\n" + "=" * 70)
    print("Panel construction complete!")
    print("=" * 70)
    
    # Print sample
    print("\nSample of output:")
    print(panel.head(10))
    
    return panel


if __name__ == "__main__":
    main()

