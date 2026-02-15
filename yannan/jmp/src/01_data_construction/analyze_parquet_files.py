import pandas as pd
import os
from pathlib import Path
import numpy as np
from datetime import datetime
import json

def get_file_size(filepath):
    """Get file size in MB"""
    size_bytes = os.path.getsize(filepath)
    return size_bytes / (1024 * 1024)  # Convert to MB

def detect_date_columns(df):
    """Detect potential date columns and extract date range"""
    date_range = None
    date_col = None
    
    # Common date column names
    date_keywords = ['date', 'time', 'created', 'updated', 'published', 'year', 'month', 'day']
    
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in date_keywords):
            try:
                # Try to convert to datetime
                if df[col].dtype == 'object':
                    dates = pd.to_datetime(df[col], errors='coerce')
                else:
                    dates = pd.to_datetime(df[col], errors='coerce')
                
                valid_dates = dates.dropna()
                if len(valid_dates) > 0:
                    min_date = valid_dates.min()
                    max_date = valid_dates.max()
                    date_range = f"{min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"
                    date_col = col
                    break
            except:
                continue
    
    # If no date column found, try to find numeric year columns
    if date_range is None:
        for col in df.columns:
            col_lower = col.lower()
            if 'year' in col_lower and pd.api.types.is_numeric_dtype(df[col]):
                valid_years = df[col].dropna()
                if len(valid_years) > 0:
                    min_year = int(valid_years.min())
                    max_year = int(valid_years.max())
                    date_range = f"{min_year} to {max_year}"
                    date_col = col
                    break
    
    return date_range if date_range else "N/A", date_col

def analyze_column(col_data, col_name):
    """Analyze a single column and return statistics"""
    result = {}
    
    # Missing value percentage
    missing_pct = (col_data.isna().sum() / len(col_data)) * 100
    result['missing_pct'] = round(missing_pct, 2)
    
    # Remove missing values for analysis
    non_missing = col_data.dropna()
    
    if len(non_missing) == 0:
        result['data_type'] = 'all_missing'
        result['range_or_examples'] = 'N/A'
        return result
    
    # Check if numeric
    if pd.api.types.is_numeric_dtype(col_data):
        result['data_type'] = 'numerical'
        if len(non_missing) > 0:
            min_val = non_missing.min()
            max_val = non_missing.max()
            if pd.api.types.is_integer_dtype(col_data):
                result['range_or_examples'] = f"[{int(min_val)}, {int(max_val)}]"
            else:
                result['range_or_examples'] = f"[{min_val:.4f}, {max_val:.4f}]"
        else:
            result['range_or_examples'] = 'N/A'
    else:
        result['data_type'] = 'non-numerical'
        # Get example values (up to 5 unique values)
        # Handle columns with unhashable types (like numpy arrays, lists)
        try:
        unique_vals = non_missing.unique()
        n_examples = min(5, len(unique_vals))
        examples = unique_vals[:n_examples]
        except (TypeError, ValueError) as e:
            # If unique() fails (e.g., unhashable types), sample first few values
            n_examples = min(5, len(non_missing))
            examples = non_missing.head(n_examples).values
            unique_vals = None
        
        # Convert to string representation, truncate if too long
        example_strs = []
        for ex in examples:
            try:
                # Handle numpy arrays and lists
                if isinstance(ex, (np.ndarray, list)):
                    ex_str = str(ex)[:50]
                else:
            ex_str = str(ex)
            if len(ex_str) > 50:
                ex_str = ex_str[:47] + "..."
            example_strs.append(ex_str)
            except Exception:
                example_strs.append("(unable to convert)")
        
        result['range_or_examples'] = " | ".join(example_strs)
        if unique_vals is not None and len(unique_vals) > n_examples:
            result['range_or_examples'] += f" ... ({len(unique_vals)} unique values)"
        elif unique_vals is None:
            result['range_or_examples'] += f" ... (contains unhashable types, showing {n_examples} examples)"
    
    return result

def analyze_parquet_file(filepath):
    """Analyze a single parquet file and return results"""
    print(f"Analyzing: {filepath}")
    
    results = []
    filename = os.path.basename(filepath)
    
    try:
        # Get file size first to determine if it's large
        file_size_mb = get_file_size(filepath)
        is_large_file = file_size_mb > 1000  # Files larger than 1GB
        
        if is_large_file:
            print(f"  Large file detected ({file_size_mb:.1f} MB) - this may take a while...")
            print(f"  Reading parquet file...")
        
        # Read parquet file
        df = pd.read_parquet(filepath)
        
        if is_large_file:
            print(f"  File loaded: {len(df)} rows, {len(df.columns)} columns")
            print(f"  Analyzing columns...")
        
        # Detect date range
        date_range, date_col = detect_date_columns(df)
        
        # Analyze each column
        total_cols = len(df.columns)
        for idx, col in enumerate(df.columns, 1):
            if is_large_file and idx % 5 == 0:
                print(f"  Progress: {idx}/{total_cols} columns analyzed...")
            
            col_analysis = analyze_column(df[col], col)
            
            result_row = {
                'file_name': filename,
                'date_range': date_range,
                'file_size_mb': round(file_size_mb, 2),
                'column_name': col,
                'data_type': col_analysis['data_type'],
                'missing_pct': col_analysis['missing_pct'],
                'range_or_examples': col_analysis['range_or_examples']
            }
            results.append(result_row)
        
        print(f"  Completed: {len(df.columns)} columns analyzed")
        
    except Exception as e:
        print(f"  Error analyzing {filename}: {str(e)}")
        # Still add a row with error information
        results.append({
            'file_name': filename,
            'date_range': 'ERROR',
            'file_size_mb': get_file_size(filepath) if os.path.exists(filepath) else 0,
            'column_name': 'ERROR',
            'data_type': 'ERROR',
            'missing_pct': 0,
            'range_or_examples': str(e)
        })
    
    return results

def main():
    # Maximum file size to analyze (in MB) - skip files larger than this
    MAX_FILE_SIZE_MB = 50000  # Skip files larger than 50GB
    
    # Collect all parquet files to analyze
    parquet_files = []
    skipped_files = []
    
    # 1. All parquet files under /home/kurtluo/yannan/jmp/data/raw/publication
    raw_dir = Path("/home/kurtluo/yannan/jmp/data/raw/publication")
    if raw_dir.exists():
        for filepath in raw_dir.glob("*.parquet"):
            file_size_mb = get_file_size(str(filepath))
            if file_size_mb > MAX_FILE_SIZE_MB:
                skipped_files.append((filepath.name, file_size_mb))
                print(f"Skipping large file: {filepath.name} ({file_size_mb:.1f} MB)")
            else:
                parquet_files.append(filepath)
    
    # 2. Three specific processed files
    processed_files = [
        "/home/kurtluo/yannan/jmp/data/processed/publication/arxiv_affiliations_from_authors.parquet",
        "/home/kurtluo/yannan/jmp/data/processed/publication/arxiv_paper_institutions.parquet",
        "/home/kurtluo/yannan/jmp/data/processed/publication/paper_institutions.parquet"
    ]
    
    for filepath in processed_files:
        if os.path.exists(filepath):
            file_size_mb = get_file_size(filepath)
            if file_size_mb > MAX_FILE_SIZE_MB:
                skipped_files.append((os.path.basename(filepath), file_size_mb))
                print(f"Skipping large file: {os.path.basename(filepath)} ({file_size_mb:.1f} MB)")
            else:
            parquet_files.append(Path(filepath))
        else:
            print(f"Warning: File not found: {filepath}")
    
    # 3. Example file
    example_file = "/home/kurtluo/yannan/jmp/data/processed/publication/arxiv_flattened_batches/batch_00654_flatten.parquet"
    if os.path.exists(example_file):
        file_size_mb = get_file_size(example_file)
        if file_size_mb > MAX_FILE_SIZE_MB:
            skipped_files.append((os.path.basename(example_file), file_size_mb))
            print(f"Skipping large file: {os.path.basename(example_file)} ({file_size_mb:.1f} MB)")
        else:
        parquet_files.append(Path(example_file))
    else:
        print(f"Warning: File not found: {example_file}")
    
    print(f"\nFound {len(parquet_files)} parquet files to analyze")
    if skipped_files:
        print(f"Skipped {len(skipped_files)} large files (> {MAX_FILE_SIZE_MB/1024:.1f} GB):")
        for filename, size_mb in skipped_files:
            print(f"  - {filename}: {size_mb/1024:.1f} GB")
    print()
    
    # Analyze all files
    all_results = []
    for filepath in parquet_files:
        results = analyze_parquet_file(str(filepath))
        all_results.extend(results)
    
    # Create DataFrame and save to CSV
    df_results = pd.DataFrame(all_results)
    output_csv = "/home/kurtluo/yannan/jmp/parquet_analysis_results.csv"
    df_results.to_csv(output_csv, index=False)
    print(f"\nResults saved to: {output_csv}")
    
    # Perform intersection/union analysis
    print("\nPerforming intersection/union analysis...")
    
    # Group by file and get column sets
    file_column_map = {}
    for _, row in df_results.iterrows():
        filename = row['file_name']
        col_name = row['column_name']
        if filename not in file_column_map:
            file_column_map[filename] = set()
        file_column_map[filename].add(col_name)
    
    # Calculate intersection and union
    all_columns = set()
    for cols in file_column_map.values():
        all_columns.update(cols)
    
    # Find common columns (intersection)
    if len(file_column_map) > 0:
        common_columns = set.intersection(*file_column_map.values())
    else:
        common_columns = set()
    
    # Find unique columns per file
    unique_columns_per_file = {}
    for filename, cols in file_column_map.items():
        unique_columns_per_file[filename] = cols - common_columns
    
    # Create analysis summary
    analysis_summary = {
        'total_files': len(file_column_map),
        'total_unique_columns': len(all_columns),
        'common_columns_count': len(common_columns),
        'common_columns': sorted(list(common_columns)),
        'files': {}
    }
    
    for filename, cols in file_column_map.items():
        analysis_summary['files'][filename] = {
            'total_columns': len(cols),
            'unique_columns_count': len(unique_columns_per_file[filename]),
            'unique_columns': sorted(list(unique_columns_per_file[filename])),
            'all_columns': sorted(list(cols))
        }
    
    # Save analysis summary
    summary_json = "/home/kurtluo/yannan/jmp/parquet_intersection_analysis.json"
    with open(summary_json, 'w') as f:
        json.dump(analysis_summary, f, indent=2)
    print(f"Intersection analysis saved to: {summary_json}")
    
    # Create a readable summary CSV
    summary_rows = []
    
    # Summary row for common columns
    summary_rows.append({
        'file_name': 'ALL_FILES_COMMON',
        'column_name': ', '.join(sorted(common_columns)) if common_columns else 'N/A',
        'note': f'Common columns across all {len(file_column_map)} files ({len(common_columns)} columns)'
    })
    
    # Summary rows for each file
    for filename, cols in file_column_map.items():
        summary_rows.append({
            'file_name': filename,
            'column_name': ', '.join(sorted(cols)),
            'note': f'Total: {len(cols)} columns, Unique: {len(unique_columns_per_file[filename])} columns'
        })
    
    df_summary = pd.DataFrame(summary_rows)
    summary_csv = "/home/kurtluo/yannan/jmp/parquet_intersection_summary.csv"
    df_summary.to_csv(summary_csv, index=False)
    print(f"Summary saved to: {summary_csv}")
    
    # Print summary to console
    print("\n" + "="*80)
    print("INTERSECTION/UNION ANALYSIS SUMMARY")
    print("="*80)
    print(f"Total files analyzed: {len(file_column_map)}")
    print(f"Total unique columns across all files: {len(all_columns)}")
    print(f"Common columns (intersection): {len(common_columns)}")
    if common_columns:
        print(f"  Common columns: {', '.join(sorted(common_columns))}")
    print("\nPer-file breakdown:")
    for filename, cols in file_column_map.items():
        print(f"\n  {filename}:")
        print(f"    Total columns: {len(cols)}")
        print(f"    Unique columns: {len(unique_columns_per_file[filename])}")
        if unique_columns_per_file[filename]:
            print(f"    Unique column names: {', '.join(sorted(unique_columns_per_file[filename]))}")
    print("="*80)

if __name__ == "__main__":
    main()
