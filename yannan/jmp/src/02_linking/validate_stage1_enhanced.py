"""
Validate Stage 1 Enhanced Publication-Firm Matches

This script performs comprehensive validation of Stage 1 enhanced matches,
including confidence distribution, match type breakdown, duplicate detection,
sample validation, coverage analysis, and high-frequency firm identification.

Input: data/processed/linking/publication_firm_matches_stage1_enhanced.parquet
Output: logs/stage1_enhanced_validation.txt
"""

import polars as pl
from pathlib import Path
import logging
from datetime import datetime

# Paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_LINK = PROJECT_ROOT / "data" / "processed" / "linking"
LOGS_DIR = PROJECT_ROOT / "logs"

# Files
INPUT_FILE = DATA_PROCESSED_LINK / "publication_firm_matches_stage1_enhanced.parquet"
OUTPUT_REPORT = LOGS_DIR / "stage1_enhanced_validation.txt"

# Constants
TOTAL_INSTITUTIONS = 115138  # Total number of institutions in the dataset

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "validate_stage1_enhanced.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_data():
    """Load the Stage 1 enhanced matches data."""
    logger.info(f"Loading data from {INPUT_FILE}")

    if not INPUT_FILE.exists():
        # Fallback to stage1 if enhanced doesn't exist
        fallback_file = DATA_PROCESSED_LINK / "publication_firm_matches_stage1.parquet"
        if fallback_file.exists():
            logger.warning(f"Stage 1 enhanced file not found, using {fallback_file}")
            input_file = fallback_file
        else:
            raise FileNotFoundError(f"Neither Stage 1 enhanced nor basic Stage 1 file found")
    else:
        input_file = INPUT_FILE

    df = pl.read_parquet(input_file)
    logger.info(f"Loaded {len(df):,} matches from {len(df['institution_id'].unique()):,} unique institutions")
    return df


def analyze_confidence_distribution(df):
    """Analyze the distribution of match confidence scores."""
    logger.info("\n" + "=" * 80)
    logger.info("1. CONFIDENCE DISTRIBUTION")
    logger.info("=" * 80)

    confidence_stats = df.select(
        pl.col("match_confidence").mean().alias("mean"),
        pl.col("match_confidence").median().alias("median"),
        pl.col("match_confidence").std().alias("std"),
        pl.col("match_confidence").min().alias("min"),
        pl.col("match_confidence").max().alias("max"),
    )

    output_lines = []
    output_lines.append("\n1. CONFIDENCE DISTRIBUTION")
    output_lines.append("=" * 80)

    for stat in ["mean", "median", "std", "min", "max"]:
        value = confidence_stats[stat][0]
        output_lines.append(f"{stat.capitalize():>10}: {value:.4f}")

    # Confidence bins
    output_lines.append("\nConfidence Bins:")
    bins = [
        (1.0, 1.0, "Perfect (1.0)"),
        (0.95, 0.999, "Very High (0.95-0.999)"),
        (0.90, 0.949, "High (0.90-0.949)"),
        (0.80, 0.899, "Medium-High (0.80-0.899)"),
        (0.0, 0.799, "Low (<0.80)")
    ]

    for min_val, max_val, label in bins:
        if min_val == max_val:
            count = df.filter(pl.col("match_confidence") == min_val).shape[0]
        else:
            count = df.filter(
                (pl.col("match_confidence") >= min_val) &
                (pl.col("match_confidence") < max_val)
            ).shape[0]
        pct = count / len(df) * 100
        output_lines.append(f"  {label:>30}: {count:>8,} ({pct:>5.2f}%)")

    return "\n".join(output_lines)


def analyze_match_type_breakdown(df):
    """Analyze matches by type and method."""
    logger.info("\n" + "=" * 80)
    logger.info("2. MATCH TYPE BREAKDOWN")
    logger.info("=" * 80)

    output_lines = []
    output_lines.append("\n2. MATCH TYPE BREAKDOWN")
    output_lines.append("=" * 80)

    # Match type distribution
    output_lines.append("\nBy Match Type:")
    type_counts = df.group_by("match_type").agg(
        pl.len().alias("count"),
        pl.col("institution_paper_count").sum().alias("total_papers")
    ).sort("count", descending=True)

    for row in type_counts.iter_rows(named=True):
        pct = row["count"] / len(df) * 100
        output_lines.append(
            f"  {row['match_type']:>20}: {row['count']:>8,} ({pct:>5.2f}%) - "
            f"{row['total_papers']:>8,} papers"
        )

    # Match method distribution
    output_lines.append("\nBy Match Method:")
    method_counts = df.group_by("match_method").agg(
        pl.len().alias("count")
    ).sort("count", descending=True)

    for row in method_counts.iter_rows(named=True):
        pct = row["count"] / len(df) * 100
        output_lines.append(f"  {row['match_method']:>40}: {row['count']:>8,} ({pct:>5.2f}%)")

    # Cross-tabulation
    output_lines.append("\nCross-Tabulation (Type x Method):")
    crosstab = df.group_by(["match_type", "match_method"]).agg(
        pl.len().alias("count")
    ).sort(["match_type", "count"], descending=[False, True])

    current_type = None
    for row in crosstab.iter_rows(named=True):
        if row["match_type"] != current_type:
            output_lines.append(f"\n  {row['match_type']}:")
            current_type = row["match_type"]
        output_lines.append(f"    {row['match_method']:>38}: {row['count']:>8,}")

    return "\n".join(output_lines)


def detect_duplicates(df):
    """Check for institutions matched to multiple firms (potential false positives)."""
    logger.info("\n" + "=" * 80)
    logger.info("3. DUPLICATE DETECTION")
    logger.info("=" * 80)

    output_lines = []
    output_lines.append("\n3. DUPLICATE DETECTION (Institutions matched to multiple firms)")
    output_lines.append("=" * 80)

    # Count firms per institution
    inst_firm_counts = df.group_by("institution_id").agg(
        pl.len().alias("num_firms"),
        pl.col("GVKEY").alias("gvkeys"),
        pl.col("firm_conm").alias("firm_names"),
        pl.col("institution_display_name").alias("inst_name"),
        pl.col("institution_paper_count").sum().alias("total_papers")
    )

    multi_firm = inst_firm_counts.filter(pl.col("num_firms") > 1).sort("num_firms", descending=True)

    output_lines.append(f"\nTotal institutions with multiple firm matches: {len(multi_firm):,}")
    output_lines.append(f"Percentage of matched institutions: {len(multi_firm) / inst_firm_counts.shape[0] * 100:.2f}%")

    if len(multi_firm) > 0:
        output_lines.append(f"\nTop 20 institutions with most firm matches:")
        output_lines.append("-" * 80)

        for i, row in enumerate(multi_firm.head(20).iter_rows(named=True), 1):
            output_lines.append(f"\n{i}. Institution: {row['inst_name']}")
            output_lines.append(f"   ID: {row['institution_id']}")
            output_lines.append(f"   Firms matched: {row['num_firms']}")
            output_lines.append(f"   Total papers: {row['total_papers']:,}")

            # Show matched firms
            firm_matches = df.filter(pl.col("institution_id") == row['institution_id']).select(
                "GVKEY", "firm_conm", "match_method", "match_confidence"
            ).sort("match_confidence", descending=True)

            for firm_row in firm_matches.iter_rows(named=True):
                output_lines.append(
                    f"     - {firm_row['GVKEY']} | {firm_row['firm_conm']:40s} | "
                    f"{firm_row['match_method']:30s} | {firm_row['match_confidence']:.3f}"
                )

    return "\n".join(output_lines)


def show_sample_matches(df):
    """Show random sample of matches for manual review."""
    logger.info("\n" + "=" * 80)
    logger.info("4. SAMPLE MATCHES FOR MANUAL REVIEW")
    logger.info("=" * 80)

    output_lines = []
    output_lines.append("\n4. SAMPLE MATCHES FOR MANUAL REVIEW (20 random examples)")
    output_lines.append("=" * 80)

    # Set random seed for reproducibility
    sample = df.sample(20, seed=42).sort("match_confidence", descending=True)

    for i, row in enumerate(sample.iter_rows(named=True), 1):
        output_lines.append(f"\n{'=' * 80}")
        output_lines.append(f"Sample {i}:")
        output_lines.append(f"  Institution ID:    {row['institution_id']}")
        output_lines.append(f"  Institution Name:  {row['institution_display_name']}")

        if "institution_clean_name" in row:
            output_lines.append(f"  Clean Name:        {row['institution_clean_name']}")

        output_lines.append(f"  Institution Papers:{row['institution_paper_count']:,}")
        output_lines.append(f"  Matched Firm:")
        output_lines.append(f"    GVKEY:           {row['GVKEY']}")
        output_lines.append(f"    LPERMNO:         {row['LPERMNO']}")
        output_lines.append(f"    Name:            {row['firm_conm']}")
        output_lines.append(f"  Match Details:")
        output_lines.append(f"    Type:            {row['match_type']}")
        output_lines.append(f"    Method:          {row['match_method']}")
        output_lines.append(f"    Confidence:      {row['match_confidence']:.4f}")

        # Additional fields if present
        if "similarity_score" in row and row["similarity_score"] is not None:
            output_lines.append(f"    Similarity:      {row['similarity_score']:.4f}")

        if "validation_flags" in row and row["validation_flags"]:
            output_lines.append(f"    Validation Flags: {row['validation_flags']}")

        if "matched_acronym" in row and row["matched_acronym"] is not None:
            output_lines.append(f"    Matched Acronym: {row['matched_acronym']}")

        if "matched_ticker" in row and row["matched_ticker"] is not None:
            output_lines.append(f"    Matched Ticker:  {row['matched_ticker']}")

        if "matched_domain" in row and row["matched_domain"] is not None:
            output_lines.append(f"    Matched Domain:  {row['matched_domain']}")

    return "\n".join(output_lines)


def analyze_coverage(df):
    """Analyze coverage of total institutions."""
    logger.info("\n" + "=" * 80)
    logger.info("5. COVERAGE ANALYSIS")
    logger.info("=" * 80)

    output_lines = []
    output_lines.append("\n5. COVERAGE ANALYSIS")
    output_lines.append("=" * 80)

    unique_institutions = df["institution_id"].n_unique()
    coverage_pct = unique_institutions / TOTAL_INSTITUTIONS * 100

    output_lines.append(f"\nTotal institutions in dataset: {TOTAL_INSTITUTIONS:,}")
    output_lines.append(f"Institutions with matches: {unique_institutions:,}")
    output_lines.append(f"Coverage: {coverage_pct:.2f}%")
    output_lines.append(f"Unmatched institutions: {TOTAL_INSTITUTIONS - unique_institutions:,}")

    # Paper coverage
    total_papers_matched = df["institution_paper_count"].sum()
    output_lines.append(f"\nTotal papers covered by matches: {total_papers_matched:,}")

    # Average papers per matched institution
    avg_papers = df.group_by("institution_id").agg(
        pl.col("institution_paper_count").sum()
    )["institution_paper_count"].mean()
    output_lines.append(f"Average papers per matched institution: {avg_papers:.1f}")

    return "\n".join(output_lines)


def identify_high_frequency_firms(df):
    """Identify top firms by number of matched institutions."""
    logger.info("\n" + "=" * 80)
    logger.info("6. HIGH-FREQUENCY FIRMS")
    logger.info("=" * 80)

    output_lines = []
    output_lines.append("\n6. TOP 20 FIRMS BY NUMBER OF MATCHED INSTITUTIONS")
    output_lines.append("=" * 80)
    output_lines.append("\n(Potential aggregators like 'Alphabet Inc' or conglomerates)")

    firm_stats = df.group_by(["GVKEY", "firm_conm"]).agg(
        pl.len().alias("num_institutions"),
        pl.col("institution_paper_count").sum().alias("total_papers"),
        pl.col("match_method").alias("methods")
    ).sort("num_institutions", descending=True)

    output_lines.append("\n{:>10} {:>10} {:>12} {:>40} {:>40}".format(
        "Rank", "GVKEY", "Institutions", "Firm Name", "Methods Used"
    ))
    output_lines.append("-" * 130)

    for i, row in enumerate(firm_stats.head(20).iter_rows(named=True), 1):
        # Get unique methods
        methods = list(set(row["methods"]))
        methods_str = ", ".join(methods[:3])  # Show first 3 methods
        if len(methods) > 3:
            methods_str += f" (+{len(methods) - 3} more)"

        gvkey = row['GVKEY'] if row['GVKEY'] is not None else 'N/A'
        output_lines.append(f"{i:>10} {gvkey:>10} {row['num_institutions']:>12,} "
                           f"{row['firm_conm']:>40} {methods_str:>40}")

        # Show top 3 institutions for this firm
        if i <= 5:  # Only show for top 5 firms to save space
            firm_insts = df.filter(pl.col("GVKEY") == row["GVKEY"]).sort(
                "institution_paper_count", descending=True
            ).head(3)

            output_lines.append(f"  Top institutions:")
            for inst_row in firm_insts.iter_rows(named=True):
                output_lines.append(
                    f"    - {inst_row['institution_display_name'][:60]:60s} | "
                    f"{inst_row['institution_paper_count']:>6,} papers | "
                    f"{inst_row['match_method']}"
                )
            output_lines.append("")

    return "\n".join(output_lines)


def generate_quality_insights(df):
    """Generate actionable insights based on validation."""
    logger.info("\n" + "=" * 80)
    logger.info("7. QUALITY INSIGHTS AND RECOMMENDATIONS")
    logger.info("=" * 80)

    output_lines = []
    output_lines.append("\n7. QUALITY INSIGHTS AND RECOMMENDATIONS")
    output_lines.append("=" * 80)

    insights = []

    # Check confidence distribution
    low_confidence = df.filter(pl.col("match_confidence") < 0.8).shape[0]
    low_conf_pct = low_confidence / len(df) * 100
    if low_conf_pct > 10:
        insights.append(
            f"⚠️  {low_conf_pct:.1f}% of matches have low confidence (<0.8). "
            f"Consider reviewing these {low_confidence:,} matches manually or applying stricter filters."
        )

    # Check for multi-firm institutions
    multi_firm_count = df.group_by("institution_id").agg(
        pl.len().alias("count")
    ).filter(pl.col("count") > 1).shape[0]
    if multi_firm_count > 100:
        insights.append(
            f"⚠️  {multi_firm_count:,} institutions are matched to multiple firms. "
            f"This may indicate ambiguous matches or subsidiaries. "
            f"Consider implementing a disambiguation strategy."
        )

    # Check coverage
    coverage = df["institution_id"].n_unique() / TOTAL_INSTITUTIONS * 100
    if coverage < 20:
        insights.append(
            f"ℹ️  Current coverage is {coverage:.1f}% of institutions. "
            f"Consider running Stage 2 and Stage 3 matching to improve coverage."
        )

    # Check for aggregator firms
    top_firm_count = df.group_by(["GVKEY", "firm_conm"]).agg(
        pl.len().alias("count")
    ).sort("count", descending=True)["count"][0]
    if top_firm_count > 50:
        top_firm_name = df.group_by(["GVKEY", "firm_conm"]).agg(
            pl.len().alias("count")
        ).sort("count", descending=True)["firm_conm"][0]
        insights.append(
            f"ℹ️  Firm '{top_firm_name}' is matched to {top_firm_count} institutions. "
            f"This may be a conglomerate or aggregator. Consider if this represents true "
            f"corporate structure or if additional entity resolution is needed."
        )

    # Check method diversity
    methods = df["match_method"].unique()
    insights.append(
        f"✓ Matching uses {len(methods)} different methods: {', '.join(sorted(methods))}. "
        f"This diversity helps improve coverage through multiple signal sources."
    )

    # Paper concentration
    top_papers_pct = (
        df.sort("institution_paper_count", descending=True)
        .head(100)["institution_paper_count"]
        .sum() / df["institution_paper_count"].sum() * 100
    )
    insights.append(
        f"ℹ️  Top 100 institutions by paper count account for {top_papers_pct:.1f}% of all papers. "
        f"Focus validation efforts on these high-impact institutions."
    )

    if insights:
        for insight in insights:
            output_lines.append(f"\n{insight}")
    else:
        output_lines.append("\nNo significant quality issues detected.")

    return "\n".join(output_lines)


def main():
    """Main validation workflow."""
    logger.info("=" * 80)
    logger.info("STAGE 1 ENHANCED MATCH VALIDATION")
    logger.info("=" * 80)
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    df = load_data()

    # Run all validation checks
    report_sections = []

    report_sections.append("=" * 80)
    report_sections.append("STAGE 1 ENHANCED MATCH VALIDATION REPORT")
    report_sections.append("=" * 80)
    report_sections.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_sections.append(f"Input File: {INPUT_FILE}")
    report_sections.append(f"Total Matches: {len(df):,}")
    report_sections.append(f"Unique Institutions: {df['institution_id'].n_unique():,}")
    report_sections.append(f"Unique Firms: {df['GVKEY'].n_unique():,}")

    # Run validation checks
    report_sections.append(analyze_confidence_distribution(df))
    report_sections.append(analyze_match_type_breakdown(df))
    report_sections.append(detect_duplicates(df))
    report_sections.append(show_sample_matches(df))
    report_sections.append(analyze_coverage(df))
    report_sections.append(identify_high_frequency_firms(df))
    report_sections.append(generate_quality_insights(df))

    # Combine and save report
    full_report = "\n".join(report_sections)

    OUTPUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_REPORT, "w") as f:
        f.write(full_report)

    logger.info(f"\n{'=' * 80}")
    logger.info(f"Validation complete! Report saved to: {OUTPUT_REPORT}")
    logger.info(f"{'=' * 80}")

    # Print summary to console
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Total matches: {len(df):,}")
    print(f"Unique institutions: {df['institution_id'].n_unique():,}")
    print(f"Unique firms: {df['GVKEY'].n_unique():,}")
    print(f"Coverage: {df['institution_id'].n_unique() / TOTAL_INSTITUTIONS * 100:.2f}%")
    print(f"Average confidence: {df['match_confidence'].mean():.3f}")
    print(f"\nFull report saved to: {OUTPUT_REPORT}")
    print("=" * 80)

    return df


if __name__ == "__main__":
    main()
