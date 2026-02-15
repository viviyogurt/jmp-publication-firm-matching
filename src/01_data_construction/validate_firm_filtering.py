"""
Validate Firm Affiliation Filtering

This script:
1. Samples 1000 random papers from the original AI papers dataset
2. Manually verifies firm affiliation status for each paper
3. Compares with the filtered results to calculate:
   - False Positive Rate (papers incorrectly marked as firm-affiliated)
   - False Negative Rate (papers incorrectly NOT marked as firm-affiliated)
   - Accuracy
   - Coverage
"""

import polars as pl
import json
import gzip
from pathlib import Path
from typing import Dict, Set, Optional, Tuple
import random
from collections import defaultdict

# ============================================================================
# Configuration
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "publication"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed" / "publication"
DATA_ANALYSIS = PROJECT_ROOT / "data" / "processed" / "analysis"

INSTITUTIONS_FILE = DATA_RAW / "institutions_all.jsonl.gz"
AI_PAPERS_FILE = DATA_PROCESSED / "ai_papers_condensed.parquet"
FIRM_PAPERS_FILE = DATA_PROCESSED / "ai_papers_firm_affiliated.parquet"
OUTPUT_FILE = DATA_ANALYSIS / "firm_filtering_validation_results.md"

SAMPLE_SIZE = 1000
RANDOM_SEED = 42

# ============================================================================
# Load Institutions Database
# ============================================================================

def normalize_institution_id(inst_id: str) -> Optional[str]:
    """Normalize institution ID to extract just the ID part."""
    if not inst_id or not isinstance(inst_id, str):
        return None
    
    inst_id = inst_id.strip()
    if inst_id.startswith('https://openalex.org/I'):
        return inst_id.split('/')[-1]
    if inst_id.startswith('I') and len(inst_id) > 1:
        return inst_id
    return None

def load_institutions_lookup(file_path: Path) -> Dict[str, str]:
    """Load institutions and create lookup table for institution types."""
    print("Loading institutions database...")
    lookup: Dict[str, str] = {}
    
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            try:
                inst = json.loads(line.strip())
                inst_id = inst.get('id', '')
                inst_type = inst.get('type') or 'unknown'
                
                normalized_id = normalize_institution_id(inst_id)
                if normalized_id:
                    lookup[normalized_id] = inst_type
            except:
                continue
    
    print(f"Loaded {len(lookup):,} institutions")
    return lookup

# ============================================================================
# Verify Firm Affiliation
# ============================================================================

def verify_firm_affiliation(
    author_affiliation_ids: list,
    institutions_lookup: Dict[str, str]
) -> Tuple[bool, int, int, list]:
    """
    Manually verify if a paper has firm affiliation.
    
    Returns:
        (has_firm, firm_count, total_institutions, firm_ids)
    """
    all_institution_ids: Set[str] = set()
    firm_ids = []
    
    for author_insts in author_affiliation_ids:
        if not isinstance(author_insts, list):
            continue
        
        for inst_id in author_insts:
            normalized_id = normalize_institution_id(inst_id)
            if normalized_id:
                all_institution_ids.add(normalized_id)
    
    if not all_institution_ids:
        return (False, 0, 0, [])
    
    firm_count = 0
    for inst_id in all_institution_ids:
        inst_type = institutions_lookup.get(inst_id, 'unknown')
        if inst_type == 'company':
            firm_count += 1
            firm_ids.append(inst_id)
    
    total_institutions = len(all_institution_ids)
    has_firm = firm_count > 0
    
    return (has_firm, firm_count, total_institutions, firm_ids)

# ============================================================================
# Validation
# ============================================================================

def validate_firm_filtering():
    """Main validation function."""
    print("=" * 80)
    print("FIRM AFFILIATION FILTERING VALIDATION")
    print("=" * 80)
    
    # Step 1: Load institutions
    print("\n[Step 1/5] Loading institutions database...")
    institutions_lookup = load_institutions_lookup(INSTITUTIONS_FILE)
    
    # Step 2: Load datasets
    print("\n[Step 2/5] Loading datasets...")
    print(f"  Loading original AI papers from: {AI_PAPERS_FILE}")
    all_papers = pl.read_parquet(AI_PAPERS_FILE)
    print(f"  Loaded {len(all_papers):,} papers")
    
    print(f"  Loading firm-affiliated papers from: {FIRM_PAPERS_FILE}")
    firm_papers = pl.read_parquet(FIRM_PAPERS_FILE)
    print(f"  Loaded {len(firm_papers):,} firm-affiliated papers")
    
    # Create set of firm-affiliated paper IDs for quick lookup
    firm_paper_ids = set(firm_papers['openalex_id'].to_list())
    print(f"  Created lookup set with {len(firm_paper_ids):,} firm-affiliated paper IDs")
    
    # Step 3: Sample random papers
    print(f"\n[Step 3/5] Sampling {SAMPLE_SIZE} random papers...")
    random.seed(RANDOM_SEED)
    total_papers = len(all_papers)
    sample_indices = random.sample(range(total_papers), min(SAMPLE_SIZE, total_papers))
    sampled_papers = all_papers[sample_indices]
    print(f"  Sampled {len(sampled_papers):,} papers")
    
    # Step 4: Verify each sampled paper
    print("\n[Step 4/5] Verifying firm affiliation for sampled papers...")
    results = []
    
    for i, row in enumerate(sampled_papers.iter_rows(named=True), 1):
        if i % 100 == 0:
            print(f"  Verified {i}/{len(sampled_papers)} papers...")
        
        paper_id = row['openalex_id']
        author_affiliation_ids = row.get('author_affiliation_ids', [])
        
        # Manual verification
        has_firm, firm_count, total_inst, firm_ids = verify_firm_affiliation(
            author_affiliation_ids, institutions_lookup
        )
        
        # Check if paper is in filtered dataset
        in_filtered = paper_id in firm_paper_ids
        
        # Determine classification
        if has_firm and in_filtered:
            classification = 'TP'  # True Positive
        elif has_firm and not in_filtered:
            classification = 'FN'  # False Negative
        elif not has_firm and in_filtered:
            classification = 'FP'  # False Positive
        else:
            classification = 'TN'  # True Negative
        
        results.append({
            'paper_id': paper_id,
            'title': row.get('title', '')[:100],  # Truncate for display
            'actual_has_firm': has_firm,
            'predicted_has_firm': in_filtered,
            'classification': classification,
            'firm_count': firm_count,
            'total_institutions': total_inst,
            'firm_ratio': firm_count / total_inst if total_inst > 0 else 0.0,
        })
    
    # Step 5: Calculate metrics
    print("\n[Step 5/5] Calculating validation metrics...")
    
    results_df = pl.DataFrame(results)
    
    # Count classifications
    tp = len([r for r in results if r['classification'] == 'TP'])
    fp = len([r for r in results if r['classification'] == 'FP'])
    fn = len([r for r in results if r['classification'] == 'FN'])
    tn = len([r for r in results if r['classification'] == 'TN'])
    
    total = len(results)
    
    # Calculate metrics
    accuracy = (tp + tn) / total if total > 0 else 0.0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    false_positive_rate = fp / (fp + tn) if (fp + tn) > 0 else 0.0
    false_negative_rate = fn / (fn + tp) if (fn + tp) > 0 else 0.0
    
    # Coverage: percentage of actual firm papers that were identified
    actual_firm_papers = tp + fn
    coverage = tp / actual_firm_papers if actual_firm_papers > 0 else 0.0
    
    # Print results
    print("\n" + "=" * 80)
    print("VALIDATION RESULTS")
    print("=" * 80)
    print(f"\nSample Size: {total:,} papers")
    print(f"\nConfusion Matrix:")
    print(f"  True Positives (TP):  {tp:4d} - Correctly identified as firm-affiliated")
    print(f"  False Positives (FP): {fp:4d} - Incorrectly identified as firm-affiliated")
    print(f"  False Negatives (FN): {fn:4d} - Missed firm-affiliated papers")
    print(f"  True Negatives (TN):  {tn:4d} - Correctly identified as NOT firm-affiliated")
    
    print(f"\nMetrics:")
    print(f"  Accuracy:           {accuracy:.4f} ({accuracy*100:.2f}%)")
    print(f"  Precision:          {precision:.4f} ({precision*100:.2f}%)")
    print(f"  Recall:             {recall:.4f} ({recall*100:.2f}%)")
    print(f"  F1 Score:           {f1_score:.4f}")
    print(f"  False Positive Rate: {false_positive_rate:.4f} ({false_positive_rate*100:.2f}%)")
    print(f"  False Negative Rate: {false_negative_rate:.4f} ({false_negative_rate*100:.2f}%)")
    print(f"  Coverage:           {coverage:.4f} ({coverage*100:.2f}%)")
    
    # Analyze errors
    print(f"\nError Analysis:")
    if fp > 0:
        fp_papers = [r for r in results if r['classification'] == 'FP']
        avg_firm_ratio_fp = sum(r['firm_ratio'] for r in fp_papers) / len(fp_papers)
        print(f"  False Positives: {len(fp_papers)} papers")
        print(f"    Average firm_ratio: {avg_firm_ratio_fp:.4f}")
        print(f"    Sample FP papers:")
        for r in fp_papers[:5]:
            print(f"      - {r['title'][:60]}... (firm_ratio: {r['firm_ratio']:.4f})")
    
    if fn > 0:
        fn_papers = [r for r in results if r['classification'] == 'FN']
        avg_firm_ratio_fn = sum(r['firm_ratio'] for r in fn_papers) / len(fn_papers) if fn_papers else 0
        print(f"\n  False Negatives: {len(fn_papers)} papers")
        print(f"    Average firm_ratio: {avg_firm_ratio_fn:.4f}")
        print(f"    Sample FN papers:")
        for r in fn_papers[:5]:
            print(f"      - {r['title'][:60]}... (firm_count: {r['firm_count']}, total_inst: {r['total_institutions']})")
    
    # Save detailed results
    print(f"\nSaving detailed results to: {OUTPUT_FILE}")
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    with open(OUTPUT_FILE, 'w') as f:
        f.write("# Firm Affiliation Filtering Validation Results\n\n")
        f.write(f"**Validation Date:** {Path(__file__).stat().st_mtime}\n")
        f.write(f"**Sample Size:** {total:,} papers\n")
        f.write(f"**Random Seed:** {RANDOM_SEED}\n\n")
        
        f.write("## Confusion Matrix\n\n")
        f.write(f"- **True Positives (TP):** {tp} - Correctly identified as firm-affiliated\n")
        f.write(f"- **False Positives (FP):** {fp} - Incorrectly identified as firm-affiliated\n")
        f.write(f"- **False Negatives (FN):** {fn} - Missed firm-affiliated papers\n")
        f.write(f"- **True Negatives (TN):** {tn} - Correctly identified as NOT firm-affiliated\n\n")
        
        f.write("## Metrics\n\n")
        f.write(f"- **Accuracy:** {accuracy:.4f} ({accuracy*100:.2f}%)\n")
        f.write(f"- **Precision:** {precision:.4f} ({precision*100:.2f}%)\n")
        f.write(f"- **Recall:** {recall:.4f} ({recall*100:.2f}%)\n")
        f.write(f"- **F1 Score:** {f1_score:.4f}\n")
        f.write(f"- **False Positive Rate:** {false_positive_rate:.4f} ({false_positive_rate*100:.2f}%)\n")
        f.write(f"- **False Negative Rate:** {false_negative_rate:.4f} ({false_negative_rate*100:.2f}%)\n")
        f.write(f"- **Coverage:** {coverage:.4f} ({coverage*100:.2f}%)\n\n")
        
        if fp > 0:
            f.write("## False Positives (Sample)\n\n")
            for r in fp_papers[:10]:
                f.write(f"- **{r['title'][:80]}**\n")
                f.write(f"  - Paper ID: {r['paper_id']}\n")
                f.write(f"  - Firm Ratio: {r['firm_ratio']:.4f}\n\n")
        
        if fn > 0:
            f.write("## False Negatives (Sample)\n\n")
            for r in fn_papers[:10]:
                f.write(f"- **{r['title'][:80]}**\n")
                f.write(f"  - Paper ID: {r['paper_id']}\n")
                f.write(f"  - Firm Count: {r['firm_count']}, Total Institutions: {r['total_institutions']}\n\n")
    
    print("Validation completed!")
    print("=" * 80)
    
    return {
        'tp': tp, 'fp': fp, 'fn': fn, 'tn': tn,
        'accuracy': accuracy, 'precision': precision, 'recall': recall,
        'f1_score': f1_score, 'false_positive_rate': false_positive_rate,
        'false_negative_rate': false_negative_rate, 'coverage': coverage
    }

# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    validate_firm_filtering()
