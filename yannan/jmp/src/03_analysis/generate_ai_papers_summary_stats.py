#!/usr/bin/env python3
"""
Generate comprehensive summary statistics for AI papers dataset.
Output formatted as LaTeX tables following AER (American Economic Review) style requirements:
- No abbreviations
- Elegant and clear formatting
- Clear illustration of each column/row in table notes
"""

import polars as pl
import pyarrow.parquet as pq
from pathlib import Path
import json
import re
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Any
import argparse
from datetime import datetime
import numpy as np

# Configuration
OUTPUT_DIR = Path(__file__).parent.parent.parent / "output" / "tables"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LATEX_OUTPUT_DIR = Path(__file__).parent.parent.parent / "output" / "latex"
LATEX_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def process_files_in_chunks(data_dir: Path, sample_size: int = None, chunk_size: int = 20):
    """
    Process batch files in chunks to avoid memory issues.
    Yields DataFrames in chunks.
    
    Args:
        data_dir: Directory containing batch files
        sample_size: If provided, only process this many files (for testing)
        chunk_size: Number of files to process per chunk
    
    Yields:
        Polars DataFrame chunks
    """
    batch_files = sorted(data_dir.glob("*_flatten.parquet"))
    
    if sample_size:
        batch_files = batch_files[:sample_size]
        print(f"Processing sample of {len(batch_files)} files...")
    else:
        print(f"Processing all {len(batch_files)} batch files in chunks of {chunk_size}...")
    
    total_chunks = (len(batch_files) + chunk_size - 1) // chunk_size
    
    for chunk_idx in range(0, len(batch_files), chunk_size):
        chunk_files = batch_files[chunk_idx:chunk_idx + chunk_size]
        chunk_num = (chunk_idx // chunk_size) + 1
        
        print(f"\nProcessing chunk {chunk_num}/{total_chunks} ({len(chunk_files)} files)...")
        
        dfs = []
        for i, batch_file in enumerate(chunk_files):
            try:
                df = pl.read_parquet(batch_file)
                dfs.append(df)
            except Exception as e:
                print(f"  Warning: Failed to load {batch_file.name}: {e}")
                continue
        
        if not dfs:
            print(f"  Warning: No valid data in chunk {chunk_num}")
            continue
        
        # Combine chunk
        try:
            chunk_df = pl.concat(dfs, how='diagonal')
            print(f"  Chunk {chunk_num}: {len(chunk_df):,} rows, {len(chunk_df.columns)} columns")
            yield chunk_df
        except Exception as e:
            print(f"  Warning: Failed to combine chunk {chunk_num}: {e}")
            # Try fallback
            try:
                all_columns = set()
                for df in dfs:
                    all_columns.update(df.columns)
                all_columns = sorted(list(all_columns))
                
                aligned_dfs = []
                for df in dfs:
                    missing_cols = set(all_columns) - set(df.columns)
                    if missing_cols:
                        for col in missing_cols:
                            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
                    aligned_dfs.append(df.select(all_columns))
                
                chunk_df = pl.concat(aligned_dfs)
                print(f"  Chunk {chunk_num}: {len(chunk_df):,} rows, {len(chunk_df.columns)} columns (fallback)")
                yield chunk_df
            except Exception as e2:
                print(f"  Error: Failed to process chunk {chunk_num}: {e2}")
                continue

def extract_country_from_authorships(df: pl.DataFrame) -> pl.DataFrame:
    """Extract country information from authorships columns."""
    country_cols = [col for col in df.columns if 'country' in col.lower() and 'code' in col.lower()]
    
    if not country_cols:
        print("  Warning: No country_code columns found")
        return df
    
    # Collect all country codes
    countries = []
    for col in country_cols:
        countries.extend(df.select(pl.col(col).drop_nulls()).to_series().to_list())
    
    return pl.DataFrame({
        'country_codes': countries
    }).filter(pl.col('country_codes').is_not_null())

def extract_institutions_from_authorships(df: pl.DataFrame) -> pl.DataFrame:
    """Extract institution information from authorships columns."""
    institution_cols = [col for col in df.columns 
                       if 'institution' in col.lower() 
                       and 'display_name' in col.lower()
                       and 'authorships' in col.lower()]
    
    if not institution_cols:
        print("  Warning: No institution display_name columns found")
        return pl.DataFrame({'institution_names': []})
    
    # Collect all institution names
    institutions = []
    for col in institution_cols:
        institutions.extend(df.select(pl.col(col).drop_nulls()).to_series().to_list())
    
    return pl.DataFrame({
        'institution_names': institutions
    }).filter(pl.col('institution_names').is_not_null())

def extract_concepts(df: pl.DataFrame) -> pl.DataFrame:
    """Extract concept information."""
    concept_cols = [col for col in df.columns 
                   if 'concept' in col.lower() 
                   and 'display_name' in col.lower()
                   and not col.startswith('primary_topic')]
    
    if not concept_cols:
        print("  Warning: No concept display_name columns found")
        return pl.DataFrame({'concept_names': []})
    
    concepts = []
    for col in concept_cols:
        concepts.extend(df.select(pl.col(col).drop_nulls()).to_series().to_list())
    
    return pl.DataFrame({
        'concept_names': concepts
    }).filter(pl.col('concept_names').is_not_null())

def extract_keywords(df: pl.DataFrame) -> pl.DataFrame:
    """Extract keyword information."""
    keyword_cols = [col for col in df.columns 
                  if 'keyword' in col.lower() 
                  and 'display_name' in col.lower()]
    
    if not keyword_cols:
        print("  Warning: No keyword display_name columns found")
        return pl.DataFrame({'keyword_names': []})
    
    keywords = []
    for col in keyword_cols:
        keywords.extend(df.select(pl.col(col).drop_nulls()).to_series().to_list())
    
    return pl.DataFrame({
        'keyword_names': keywords
    }).filter(pl.col('keyword_names').is_not_null())

def extract_topics(df: pl.DataFrame) -> pl.DataFrame:
    """Extract topic information."""
    topic_cols = [col for col in df.columns 
                if 'topic' in col.lower() 
                and 'display_name' in col.lower()
                and not col.startswith('primary_topic')]
    
    if not topic_cols:
        print("  Warning: No topic display_name columns found")
        return pl.DataFrame({'topic_names': []})
    
    topics = []
    for col in topic_cols:
        topics.extend(df.select(pl.col(col).drop_nulls()).to_series().to_list())
    
    return pl.DataFrame({
        'topic_names': topics
    }).filter(pl.col('topic_names').is_not_null())

def accumulate_author_stats(chunk_df: pl.DataFrame, accumulated: Dict[str, Any] = None) -> Dict[str, Any]:
    """Accumulate author statistics from a chunk."""
    if accumulated is None:
        accumulated = {
            'all_counts': [],
            'total_papers': 0
        }
    
    if 'authors_count' in chunk_df.columns:
        counts = chunk_df.select(
            pl.col('authors_count').cast(pl.Int64, strict=False).alias('authors_count')
        ).drop_nulls().to_series().to_list()
        accumulated['all_counts'].extend(counts)
        accumulated['total_papers'] += len(counts)
    
    return accumulated

def finalize_author_stats(accumulated: Dict[str, Any]) -> Dict[str, Any]:
    """Finalize author statistics from accumulated data."""
    if not accumulated or not accumulated.get('all_counts'):
        return {}
    
    import numpy as np
    counts = np.array(accumulated['all_counts'])
    
    stats = {
        'total_papers': len(counts),
        'mean': float(np.mean(counts)),
        'median': float(np.median(counts)),
        'std': float(np.std(counts)),
        'min': int(np.min(counts)),
        'max': int(np.max(counts)),
        'percentiles': {
            'p25': float(np.percentile(counts, 25)),
            'p75': float(np.percentile(counts, 75)),
            'p90': float(np.percentile(counts, 90)),
            'p95': float(np.percentile(counts, 95)),
            'p99': float(np.percentile(counts, 99))
        },
        'distribution': {}
    }
    
    # Distribution
    from collections import Counter
    dist = Counter(counts)
    stats['distribution'] = {int(k): int(v) for k, v in dist.items()}
    
    return stats

def generate_author_distribution_stats(df: pl.DataFrame) -> Dict[str, Any]:
    """Generate author number distribution statistics."""
    print("\nGenerating author distribution statistics...")
    
    if 'authors_count' not in df.columns:
        print("  Warning: authors_count column not found")
        return {}
    
    # Cast to numeric if needed
    author_counts = df.select(
        pl.col('authors_count').cast(pl.Int64, strict=False).alias('authors_count')
    ).drop_nulls().to_series()
    
    if len(author_counts) == 0:
        print("  Warning: No author count data available")
        return {}
    
    mean_val = author_counts.mean()
    median_val = author_counts.median()
    std_val = author_counts.std()
    min_val = author_counts.min()
    max_val = author_counts.max()
    
    stats = {
        'total_papers': len(author_counts),
        'mean': float(mean_val) if mean_val is not None else None,
        'median': float(median_val) if median_val is not None else None,
        'std': float(std_val) if std_val is not None else None,
        'min': int(min_val) if min_val is not None else None,
        'max': int(max_val) if max_val is not None else None,
        'percentiles': {},
        'distribution': {}
    }
    
    # Calculate percentiles
    for p in [0.25, 0.75, 0.90, 0.95, 0.99]:
        p_val = author_counts.quantile(p)
        p_key = f'p{int(p*100)}'
        stats['percentiles'][p_key] = float(p_val) if p_val is not None else None
    
    # Distribution by author count
    dist = author_counts.value_counts().sort('authors_count')
    for row in dist.iter_rows(named=True):
        stats['distribution'][int(row['authors_count'])] = int(row['count'])
    
    return stats

def accumulate_country_stats(chunk_df: pl.DataFrame, accumulated: Dict[str, Any] = None) -> Dict[str, Any]:
    """Accumulate country statistics from a chunk."""
    if accumulated is None:
        accumulated = {'countries': []}
    
    country_df = extract_country_from_authorships(chunk_df)
    if len(country_df) > 0:
        accumulated['countries'].extend(country_df['country_codes'].to_list())
    
    return accumulated

def finalize_country_stats(accumulated: Dict[str, Any]) -> Dict[str, Any]:
    """Finalize country statistics from accumulated data."""
    if not accumulated or not accumulated.get('countries'):
        return {}
    
    country_df = pl.DataFrame({'country_codes': accumulated['countries']})
    country_counts = country_df['country_codes'].value_counts()
    
    stats = {
        'total_affiliations': len(country_df),
        'unique_countries': len(country_counts),
        'top_countries': []
    }
    
    top_countries = country_counts.head(20)
    for row in top_countries.iter_rows(named=True):
        stats['top_countries'].append({
            'country_code': row['country_codes'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(country_df) * 100)
        })
    
    return stats

def generate_country_distribution_stats(df: pl.DataFrame) -> Dict[str, Any]:
    """Generate country distribution statistics."""
    print("\nGenerating country distribution statistics...")
    
    country_df = extract_country_from_authorships(df)
    
    if len(country_df) == 0:
        print("  Warning: No country data found")
        return {}
    
    country_counts = country_df['country_codes'].value_counts()
    
    stats = {
        'total_affiliations': len(country_df),
        'unique_countries': len(country_counts),
        'top_countries': []
    }
    
    # Top 20 countries
    top_countries = country_counts.head(20)
    for row in top_countries.iter_rows(named=True):
        stats['top_countries'].append({
            'country_code': row['country_codes'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(country_df) * 100)
        })
    
    return stats

def accumulate_citation_stats(chunk_df: pl.DataFrame, accumulated: Dict[str, Any] = None) -> Dict[str, Any]:
    """Accumulate citation statistics from a chunk."""
    if accumulated is None:
        accumulated = {
            'citations': [],
            'references': [],
            'total_papers_cit': 0,
            'total_papers_ref': 0
        }
    
    if 'cited_by_count' in chunk_df.columns:
        citations = chunk_df.select(
            pl.col('cited_by_count').cast(pl.Int64, strict=False).alias('cited_by_count')
        ).drop_nulls().to_series().to_list()
        accumulated['citations'].extend(citations)
        accumulated['total_papers_cit'] += len(citations)
    
    if 'referenced_works_count' in chunk_df.columns:
        references = chunk_df.select(
            pl.col('referenced_works_count').cast(pl.Int64, strict=False).alias('referenced_works_count')
        ).drop_nulls().to_series().to_list()
        accumulated['references'].extend(references)
        accumulated['total_papers_ref'] += len(references)
    
    return accumulated

def finalize_citation_stats(accumulated: Dict[str, Any]) -> Dict[str, Any]:
    """Finalize citation statistics from accumulated data."""
    stats = {}
    
    if accumulated.get('citations'):
        import numpy as np
        citations = np.array(accumulated['citations'])
        stats['citations'] = {
            'total_papers': len(citations),
            'papers_with_citations': int(np.sum(citations > 0)),
            'mean': float(np.mean(citations)),
            'median': float(np.median(citations)),
            'std': float(np.std(citations)),
            'max': int(np.max(citations)),
            'percentiles': {
                'p25': float(np.percentile(citations, 25)),
                'p75': float(np.percentile(citations, 75)),
                'p90': float(np.percentile(citations, 90)),
                'p95': float(np.percentile(citations, 95)),
                'p99': float(np.percentile(citations, 99))
            }
        }
    
    if accumulated.get('references'):
        import numpy as np
        references = np.array(accumulated['references'])
        stats['references'] = {
            'total_papers': len(references),
            'papers_with_references': int(np.sum(references > 0)),
            'mean': float(np.mean(references)),
            'median': float(np.median(references)),
            'std': float(np.std(references)),
            'max': int(np.max(references)),
            'percentiles': {
                'p25': float(np.percentile(references, 25)),
                'p75': float(np.percentile(references, 75)),
                'p90': float(np.percentile(references, 90)),
                'p95': float(np.percentile(references, 95)),
                'p99': float(np.percentile(references, 99))
            }
        }
    
    return stats

def generate_citation_stats(df: pl.DataFrame) -> Dict[str, Any]:
    """Generate citation and reference distribution statistics."""
    print("\nGenerating citation and reference statistics...")
    
    stats = {}
    
    # Citation statistics
    if 'cited_by_count' in df.columns:
        citations = df.select(
            pl.col('cited_by_count').cast(pl.Int64, strict=False).alias('cited_by_count')
        ).drop_nulls().to_series()
        if len(citations) > 0:
            mean_cit = citations.mean()
            median_cit = citations.median()
            std_cit = citations.std()
            max_cit = citations.max()
            stats['citations'] = {
                'total_papers': len(citations),
                'papers_with_citations': int((citations > 0).sum()),
                'mean': float(mean_cit) if mean_cit is not None else None,
                'median': float(median_cit) if median_cit is not None else None,
                'std': float(std_cit) if std_cit is not None else None,
                'max': int(max_cit) if max_cit is not None else None,
                'percentiles': {}
            }
            for p in [0.25, 0.75, 0.90, 0.95, 0.99]:
                p_val = citations.quantile(p)
                p_key = f'p{int(p*100)}'
                stats['citations']['percentiles'][p_key] = float(p_val) if p_val is not None else None
        else:
            print("  Warning: No citation data available")
    else:
        print("  Warning: cited_by_count column not found")
    
    # Reference statistics
    if 'referenced_works_count' in df.columns:
        references = df.select(
            pl.col('referenced_works_count').cast(pl.Int64, strict=False).alias('referenced_works_count')
        ).drop_nulls().to_series()
        if len(references) > 0:
            mean_ref = references.mean()
            median_ref = references.median()
            std_ref = references.std()
            max_ref = references.max()
            stats['references'] = {
                'total_papers': len(references),
                'papers_with_references': int((references > 0).sum()),
                'mean': float(mean_ref) if mean_ref is not None else None,
                'median': float(median_ref) if median_ref is not None else None,
                'std': float(std_ref) if std_ref is not None else None,
                'max': int(max_ref) if max_ref is not None else None,
                'percentiles': {}
            }
            for p in [0.25, 0.75, 0.90, 0.95, 0.99]:
                p_val = references.quantile(p)
                p_key = f'p{int(p*100)}'
                stats['references']['percentiles'][p_key] = float(p_val) if p_val is not None else None
        else:
            print("  Warning: No reference data available")
    else:
        print("  Warning: referenced_works_count column not found")
    
    return stats

def accumulate_institution_stats(chunk_df: pl.DataFrame, accumulated: Dict[str, Any] = None) -> Dict[str, Any]:
    """Accumulate institution statistics from a chunk."""
    if accumulated is None:
        accumulated = {'institutions': []}
    
    institution_df = extract_institutions_from_authorships(chunk_df)
    if len(institution_df) > 0:
        accumulated['institutions'].extend(institution_df['institution_names'].to_list())
    
    return accumulated

def finalize_institution_stats(accumulated: Dict[str, Any]) -> Dict[str, Any]:
    """Finalize institution statistics from accumulated data."""
    if not accumulated or not accumulated.get('institutions'):
        return {}
    
    institution_df = pl.DataFrame({'institution_names': accumulated['institutions']})
    institution_counts = institution_df['institution_names'].value_counts()
    
    stats = {
        'total_affiliations': len(institution_df),
        'unique_institutions': len(institution_counts),
        'top_institutions': []
    }
    
    top_institutions = institution_counts.head(30)
    for row in top_institutions.iter_rows(named=True):
        stats['top_institutions'].append({
            'institution_name': row['institution_names'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(institution_df) * 100)
        })
    
    return stats

def generate_institution_distribution_stats(df: pl.DataFrame) -> Dict[str, Any]:
    """Generate author institution distribution statistics."""
    print("\nGenerating institution distribution statistics...")
    
    institution_df = extract_institutions_from_authorships(df)
    
    if len(institution_df) == 0:
        print("  Warning: No institution data found")
        return {}
    
    institution_counts = institution_df['institution_names'].value_counts()
    
    stats = {
        'total_affiliations': len(institution_df),
        'unique_institutions': len(institution_counts),
        'top_institutions': []
    }
    
    # Top 30 institutions
    top_institutions = institution_counts.head(30)
    for row in top_institutions.iter_rows(named=True):
        stats['top_institutions'].append({
            'institution_name': row['institution_names'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(institution_df) * 100)
        })
    
    return stats

def accumulate_concept_stats(chunk_df: pl.DataFrame, accumulated: Dict[str, Any] = None) -> Dict[str, Any]:
    """Accumulate concept statistics from a chunk."""
    if accumulated is None:
        accumulated = {'concepts': []}
    
    concept_df = extract_concepts(chunk_df)
    if len(concept_df) > 0:
        accumulated['concepts'].extend(concept_df['concept_names'].to_list())
    
    return accumulated

def finalize_concept_stats(accumulated: Dict[str, Any]) -> Dict[str, Any]:
    """Finalize concept statistics from accumulated data."""
    if not accumulated or not accumulated.get('concepts'):
        return {}
    
    concept_df = pl.DataFrame({'concept_names': accumulated['concepts']})
    concept_counts = concept_df['concept_names'].value_counts()
    
    stats = {
        'total_concept_assignments': len(concept_df),
        'unique_concepts': len(concept_counts),
        'top_concepts': []
    }
    
    top_concepts = concept_counts.head(30)
    for row in top_concepts.iter_rows(named=True):
        stats['top_concepts'].append({
            'concept_name': row['concept_names'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(concept_df) * 100)
        })
    
    return stats

def generate_concept_distribution_stats(df: pl.DataFrame) -> Dict[str, Any]:
    """Generate concepts distribution statistics."""
    print("\nGenerating concept distribution statistics...")
    
    concept_df = extract_concepts(df)
    
    if len(concept_df) == 0:
        print("  Warning: No concept data found")
        return {}
    
    concept_counts = concept_df['concept_names'].value_counts()
    
    stats = {
        'total_concept_assignments': len(concept_df),
        'unique_concepts': len(concept_counts),
        'top_concepts': []
    }
    
    # Top 30 concepts
    top_concepts = concept_counts.head(30)
    for row in top_concepts.iter_rows(named=True):
        stats['top_concepts'].append({
            'concept_name': row['concept_names'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(concept_df) * 100)
        })
    
    return stats

def accumulate_keyword_stats(chunk_df: pl.DataFrame, accumulated: Dict[str, Any] = None) -> Dict[str, Any]:
    """Accumulate keyword statistics from a chunk."""
    if accumulated is None:
        accumulated = {'keywords': []}
    
    keyword_df = extract_keywords(chunk_df)
    if len(keyword_df) > 0:
        accumulated['keywords'].extend(keyword_df['keyword_names'].to_list())
    
    return accumulated

def finalize_keyword_stats(accumulated: Dict[str, Any]) -> Dict[str, Any]:
    """Finalize keyword statistics from accumulated data."""
    if not accumulated or not accumulated.get('keywords'):
        return {}
    
    keyword_df = pl.DataFrame({'keyword_names': accumulated['keywords']})
    keyword_counts = keyword_df['keyword_names'].value_counts()
    
    stats = {
        'total_keyword_assignments': len(keyword_df),
        'unique_keywords': len(keyword_counts),
        'top_keywords': []
    }
    
    top_keywords = keyword_counts.head(30)
    for row in top_keywords.iter_rows(named=True):
        stats['top_keywords'].append({
            'keyword_name': row['keyword_names'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(keyword_df) * 100)
        })
    
    return stats

def generate_keyword_distribution_stats(df: pl.DataFrame) -> Dict[str, Any]:
    """Generate keyword distribution statistics."""
    print("\nGenerating keyword distribution statistics...")
    
    keyword_df = extract_keywords(df)
    
    if len(keyword_df) == 0:
        print("  Warning: No keyword data found")
        return {}
    
    keyword_counts = keyword_df['keyword_names'].value_counts()
    
    stats = {
        'total_keyword_assignments': len(keyword_df),
        'unique_keywords': len(keyword_counts),
        'top_keywords': []
    }
    
    # Top 30 keywords
    top_keywords = keyword_counts.head(30)
    for row in top_keywords.iter_rows(named=True):
        stats['top_keywords'].append({
            'keyword_name': row['keyword_names'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(keyword_df) * 100)
        })
    
    return stats

def accumulate_topic_stats(chunk_df: pl.DataFrame, accumulated: Dict[str, Any] = None) -> Dict[str, Any]:
    """Accumulate topic statistics from a chunk."""
    if accumulated is None:
        accumulated = {'topics': []}
    
    topic_df = extract_topics(chunk_df)
    if len(topic_df) > 0:
        accumulated['topics'].extend(topic_df['topic_names'].to_list())
    
    return accumulated

def finalize_topic_stats(accumulated: Dict[str, Any]) -> Dict[str, Any]:
    """Finalize topic statistics from accumulated data."""
    if not accumulated or not accumulated.get('topics'):
        return {}
    
    topic_df = pl.DataFrame({'topic_names': accumulated['topics']})
    topic_counts = topic_df['topic_names'].value_counts()
    
    stats = {
        'total_topic_assignments': len(topic_df),
        'unique_topics': len(topic_counts),
        'top_topics': []
    }
    
    top_topics = topic_counts.head(30)
    for row in top_topics.iter_rows(named=True):
        stats['top_topics'].append({
            'topic_name': row['topic_names'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(topic_df) * 100)
        })
    
    return stats

def generate_topic_distribution_stats(df: pl.DataFrame) -> Dict[str, Any]:
    """Generate topic distribution statistics."""
    print("\nGenerating topic distribution statistics...")
    
    topic_df = extract_topics(df)
    
    if len(topic_df) == 0:
        print("  Warning: No topic data found")
        return {}
    
    topic_counts = topic_df['topic_names'].value_counts()
    
    stats = {
        'total_topic_assignments': len(topic_df),
        'unique_topics': len(topic_counts),
        'top_topics': []
    }
    
    # Top 30 topics
    top_topics = topic_counts.head(30)
    for row in top_topics.iter_rows(named=True):
        stats['top_topics'].append({
            'topic_name': row['topic_names'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(topic_df) * 100)
        })
    
    return stats

def extract_sdgs(df: pl.DataFrame) -> pl.DataFrame:
    """Extract Sustainable Development Goal information."""
    sdg_cols = [col for col in df.columns 
               if 'sustainable_development_goals' in col.lower() 
               and 'display_name' in col.lower()]
    
    if not sdg_cols:
        print("  Warning: No SDG display_name columns found")
        return pl.DataFrame({'sdg_names': []})
    
    sdgs = []
    for col in sdg_cols:
        sdgs.extend(df.select(pl.col(col).drop_nulls()).to_series().to_list())
    
    return pl.DataFrame({
        'sdg_names': sdgs
    }).filter(pl.col('sdg_names').is_not_null())

def accumulate_sdg_stats(chunk_df: pl.DataFrame, accumulated: Dict[str, Any] = None) -> Dict[str, Any]:
    """Accumulate SDG statistics from a chunk."""
    if accumulated is None:
        accumulated = {'sdgs': []}
    
    sdg_df = extract_sdgs(chunk_df)
    if len(sdg_df) > 0:
        accumulated['sdgs'].extend(sdg_df['sdg_names'].to_list())
    
    return accumulated

def finalize_sdg_stats(accumulated: Dict[str, Any]) -> Dict[str, Any]:
    """Finalize SDG statistics from accumulated data."""
    if not accumulated or not accumulated.get('sdgs'):
        return {'available': False}
    
    sdg_df = pl.DataFrame({'sdg_names': accumulated['sdgs']})
    sdg_counts = sdg_df['sdg_names'].value_counts()
    
    stats = {
        'available': True,
        'total_sdg_assignments': len(sdg_df),
        'unique_sdgs': len(sdg_counts),
        'top_sdgs': []
    }
    
    top_sdgs = sdg_counts.head(20)
    for row in top_sdgs.iter_rows(named=True):
        stats['top_sdgs'].append({
            'sdg_name': row['sdg_names'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(sdg_df) * 100)
        })
    
    return stats

def generate_sdg_distribution_stats(df: pl.DataFrame) -> Dict[str, Any]:
    """Generate Sustainable Development Goal (SDG) distribution statistics."""
    print("\nGenerating SDG distribution statistics...")
    
    sdg_df = extract_sdgs(df)
    
    if len(sdg_df) == 0:
        print("  Info: No SDG data found")
        return {'available': False}
    
    sdg_counts = sdg_df['sdg_names'].value_counts()
    
    stats = {
        'available': True,
        'total_sdg_assignments': len(sdg_df),
        'unique_sdgs': len(sdg_counts),
        'top_sdgs': []
    }
    
    # Top 20 SDGs
    top_sdgs = sdg_counts.head(20)
    for row in top_sdgs.iter_rows(named=True):
        stats['top_sdgs'].append({
            'sdg_name': row['sdg_names'],
            'count': int(row['count']),
            'percentage': float(row['count'] / len(sdg_df) * 100)
        })
    
    return stats

def format_number(num: float, decimals: int = 2) -> str:
    """Format number for LaTeX table."""
    if num is None:
        return "---"
    if decimals == 0:
        return f"{int(num):,}"
    return f"{num:,.{decimals}f}"

def generate_latex_table_author_distribution(stats: Dict[str, Any]) -> str:
    """Generate LaTeX table for author distribution."""
    if not stats:
        return ""
    
    latex = """\\begin{table}[htbp]
\\centering
\\caption{Distribution of Number of Authors per Paper}
\\label{tab:author_distribution}
\\begin{tabular}{lrr}
\\toprule
Statistic & Value & Papers \\\\
\\midrule
"""
    
    latex += f"Total Papers & {format_number(stats['total_papers'], 0)} & --- \\\\\n"
    latex += f"Mean & {format_number(stats['mean'], 2)} & --- \\\\\n"
    latex += f"Median & {format_number(stats['median'], 2)} & --- \\\\\n"
    latex += f"Standard Deviation & {format_number(stats['std'], 2)} & --- \\\\\n"
    latex += f"Minimum & {format_number(stats['min'], 0)} & --- \\\\\n"
    latex += f"Maximum & {format_number(stats['max'], 0)} & --- \\\\\n"
    latex += "\\midrule\n"
    latex += f"25th Percentile & {format_number(stats['percentiles']['p25'], 2)} & --- \\\\\n"
    latex += f"75th Percentile & {format_number(stats['percentiles']['p75'], 2)} & --- \\\\\n"
    latex += f"90th Percentile & {format_number(stats['percentiles']['p90'], 2)} & --- \\\\\n"
    latex += f"95th Percentile & {format_number(stats['percentiles']['p95'], 2)} & --- \\\\\n"
    latex += f"99th Percentile & {format_number(stats['percentiles']['p99'], 2)} & --- \\\\\n"
    
    latex += """\\bottomrule
\\end{tabular}
\\begin{minipage}{\\textwidth}
\\footnotesize
\\textit{Notes:} This table presents the distribution of the number of authors per paper in the AI papers dataset. 
The statistics are computed across all papers in the dataset. 
Mean, median, and standard deviation are reported in the second column. 
Percentiles indicate the author count thresholds below which the specified percentage of papers fall.
\\end{minipage}
\\end{table}
"""
    
    return latex

def generate_latex_table_country_distribution(stats: Dict[str, Any]) -> str:
    """Generate LaTeX table for country distribution."""
    if not stats or 'top_countries' not in stats:
        return ""
    
    latex = """\\begin{table}[htbp]
\\centering
\\caption{Distribution of Author Affiliations by Country}
\\label{tab:country_distribution}
\\begin{tabular}{lrr}
\\toprule
Country Code & Number of Affiliations & Percentage \\\\
\\midrule
"""
    
    for country in stats['top_countries'][:15]:  # Top 15
        latex += f"{country['country_code']} & {format_number(country['count'], 0)} & {format_number(country['percentage'], 2)}\\% \\\\\n"
    
    latex += """\\bottomrule
\\end{tabular}
\\begin{minipage}{\\textwidth}
\\footnotesize
\\textit{Notes:} This table presents the distribution of author affiliations by country code in the AI papers dataset. 
The table shows the top 15 countries by number of author affiliations. 
Each row represents a country code (ISO 3166-1 alpha-2 standard) and reports the total number of author affiliations 
associated with that country and the corresponding percentage of all affiliations in the dataset.
\\end{minipage}
\\end{table}
"""
    
    return latex

def generate_latex_table_citation_stats(stats: Dict[str, Any]) -> str:
    """Generate LaTeX table for citation and reference statistics."""
    if not stats:
        return ""
    
    latex = """\\begin{table}[htbp]
\\centering
\\caption{Distribution of Citations and References}
\\label{tab:citation_distribution}
\\begin{tabular}{lrr}
\\toprule
Statistic & Citations & References \\\\
\\midrule
"""
    
    if 'citations' in stats:
        cit = stats['citations']
        latex += f"Total Papers & {format_number(cit['total_papers'], 0)} & "
        if 'references' in stats:
            ref = stats['references']
            latex += f"{format_number(ref['total_papers'], 0)} \\\\\n"
        else:
            latex += "--- \\\\\n"
        
        latex += f"Papers with Positive Count & {format_number(cit['papers_with_citations'], 0)} & "
        if 'references' in stats:
            latex += f"{format_number(ref['papers_with_references'], 0)} \\\\\n"
        else:
            latex += "--- \\\\\n"
        
        latex += f"Mean & {format_number(cit['mean'], 2)} & "
        if 'references' in stats:
            latex += f"{format_number(ref['mean'], 2)} \\\\\n"
        else:
            latex += "--- \\\\\n"
        
        latex += f"Median & {format_number(cit['median'], 2)} & "
        if 'references' in stats:
            latex += f"{format_number(ref['median'], 2)} \\\\\n"
        else:
            latex += "--- \\\\\n"
        
        latex += f"Standard Deviation & {format_number(cit['std'], 2)} & "
        if 'references' in stats:
            latex += f"{format_number(ref['std'], 2)} \\\\\n"
        else:
            latex += "--- \\\\\n"
        
        latex += f"Maximum & {format_number(cit['max'], 0)} & "
        if 'references' in stats:
            latex += f"{format_number(ref['max'], 0)} \\\\\n"
        else:
            latex += "--- \\\\\n"
        
        latex += "\\midrule\n"
        latex += f"25th Percentile & {format_number(cit['percentiles']['p25'], 2)} & "
        if 'references' in stats:
            latex += f"{format_number(ref['percentiles']['p25'], 2)} \\\\\n"
        else:
            latex += "--- \\\\\n"
        
        latex += f"75th Percentile & {format_number(cit['percentiles']['p75'], 2)} & "
        if 'references' in stats:
            latex += f"{format_number(ref['percentiles']['p75'], 2)} \\\\\n"
        else:
            latex += "--- \\\\\n"
        
        latex += f"90th Percentile & {format_number(cit['percentiles']['p90'], 2)} & "
        if 'references' in stats:
            latex += f"{format_number(ref['percentiles']['p90'], 2)} \\\\\n"
        else:
            latex += "--- \\\\\n"
        
        latex += f"95th Percentile & {format_number(cit['percentiles']['p95'], 2)} & "
        if 'references' in stats:
            latex += f"{format_number(ref['percentiles']['p95'], 2)} \\\\\n"
        else:
            latex += "--- \\\\\n"
        
        latex += f"99th Percentile & {format_number(cit['percentiles']['p99'], 2)} & "
        if 'references' in stats:
            latex += f"{format_number(ref['percentiles']['p99'], 2)} \\\\\n"
        else:
            latex += "--- \\\\\n"
    else:
        latex += "No citation data available & --- & --- \\\\\n"
    
    latex += """\\bottomrule
\\end{tabular}
\\begin{minipage}{\\textwidth}
\\footnotesize
\\textit{Notes:} This table presents the distribution of citations received and references made by papers in the AI papers dataset. 
Citations refer to the number of times a paper has been cited by other papers. 
References refer to the number of papers cited by each paper in the dataset. 
The statistics are computed across all papers with available data. 
Percentiles indicate the citation or reference count thresholds below which the specified percentage of papers fall.
\\end{minipage}
\\end{table}
"""
    
    return latex

def generate_latex_table_institution_distribution(stats: Dict[str, Any]) -> str:
    """Generate LaTeX table for institution distribution."""
    if not stats or 'top_institutions' not in stats:
        return ""
    
    latex = """\\begin{table}[htbp]
\\centering
\\caption{Distribution of Author Affiliations by Institution}
\\label{tab:institution_distribution}
\\begin{tabular}{lrr}
\\toprule
Institution Name & Number of Affiliations & Percentage \\\\
\\midrule
"""
    
    for inst in stats['top_institutions'][:20]:  # Top 20
        # Escape LaTeX special characters in institution names
        inst_name = inst['institution_name'].replace('&', '\\&').replace('_', '\\_')
        if len(inst_name) > 60:
            inst_name = inst_name[:57] + "..."
        latex += f"{inst_name} & {format_number(inst['count'], 0)} & {format_number(inst['percentage'], 2)}\\% \\\\\n"
    
    latex += """\\bottomrule
\\end{tabular}
\\begin{minipage}{\\textwidth}
\\footnotesize
\\textit{Notes:} This table presents the distribution of author affiliations by institution in the AI papers dataset. 
The table shows the top 20 institutions by number of author affiliations. 
Each row represents an institution and reports the total number of author affiliations associated with that institution 
and the corresponding percentage of all affiliations in the dataset. 
Institution names are truncated if they exceed 60 characters.
\\end{minipage}
\\end{table}
"""
    
    return latex

def generate_latex_table_concept_distribution(stats: Dict[str, Any]) -> str:
    """Generate LaTeX table for concept distribution."""
    if not stats or 'top_concepts' not in stats:
        return ""
    
    latex = """\\begin{table}[htbp]
\\centering
\\caption{Distribution of Research Concepts}
\\label{tab:concept_distribution}
\\begin{tabular}{lrr}
\\toprule
Concept Name & Number of Assignments & Percentage \\\\
\\midrule
"""
    
    for concept in stats['top_concepts'][:20]:  # Top 20
        concept_name = concept['concept_name'].replace('&', '\\&').replace('_', '\\_')
        if len(concept_name) > 50:
            concept_name = concept_name[:47] + "..."
        latex += f"{concept_name} & {format_number(concept['count'], 0)} & {format_number(concept['percentage'], 2)}\\% \\\\\n"
    
    latex += """\\bottomrule
\\end{tabular}
\\begin{minipage}{\\textwidth}
\\footnotesize
\\textit{Notes:} This table presents the distribution of research concepts assigned to papers in the AI papers dataset. 
The table shows the top 20 concepts by number of assignments. 
Each row represents a concept and reports the total number of times that concept is assigned to papers 
and the corresponding percentage of all concept assignments in the dataset. 
Concept names are truncated if they exceed 50 characters.
\\end{minipage}
\\end{table}
"""
    
    return latex

def generate_latex_table_keyword_distribution(stats: Dict[str, Any]) -> str:
    """Generate LaTeX table for keyword distribution."""
    if not stats or 'top_keywords' not in stats:
        return ""
    
    latex = """\\begin{table}[htbp]
\\centering
\\caption{Distribution of Keywords}
\\label{tab:keyword_distribution}
\\begin{tabular}{lrr}
\\toprule
Keyword & Number of Assignments & Percentage \\\\
\\midrule
"""
    
    for keyword in stats['top_keywords'][:20]:  # Top 20
        keyword_name = keyword['keyword_name'].replace('&', '\\&').replace('_', '\\_')
        if len(keyword_name) > 50:
            keyword_name = keyword_name[:47] + "..."
        latex += f"{keyword_name} & {format_number(keyword['count'], 0)} & {format_number(keyword['percentage'], 2)}\\% \\\\\n"
    
    latex += """\\bottomrule
\\end{tabular}
\\begin{minipage}{\\textwidth}
\\footnotesize
\\textit{Notes:} This table presents the distribution of keywords assigned to papers in the AI papers dataset. 
The table shows the top 20 keywords by number of assignments. 
Each row represents a keyword and reports the total number of times that keyword is assigned to papers 
and the corresponding percentage of all keyword assignments in the dataset. 
Keyword names are truncated if they exceed 50 characters.
\\end{minipage}
\\end{table}
"""
    
    return latex

def generate_latex_table_sdg_distribution(stats: Dict[str, Any]) -> str:
    """Generate LaTeX table for SDG distribution."""
    if not stats or not stats.get('available', False) or 'top_sdgs' not in stats:
        return ""
    
    latex = """\\begin{table}[htbp]
\\centering
\\caption{Distribution of Sustainable Development Goals}
\\label{tab:sdg_distribution}
\\begin{tabular}{lrr}
\\toprule
Sustainable Development Goal & Number of Assignments & Percentage \\\\
\\midrule
"""
    
    for sdg in stats['top_sdgs'][:17]:  # Top 17 (all SDGs)
        sdg_name = sdg['sdg_name'].replace('&', '\\&').replace('_', '\\_')
        if len(sdg_name) > 60:
            sdg_name = sdg_name[:57] + "..."
        latex += f"{sdg_name} & {format_number(sdg['count'], 0)} & {format_number(sdg['percentage'], 2)}\\% \\\\\n"
    
    latex += """\\bottomrule
\\end{tabular}
\\begin{minipage}{\\textwidth}
\\footnotesize
\\textit{Notes:} This table presents the distribution of Sustainable Development Goals (SDGs) assigned to papers in the AI papers dataset. 
The table shows all SDGs by number of assignments. 
Each row represents an SDG and reports the total number of times that SDG is assigned to papers 
and the corresponding percentage of all SDG assignments in the dataset. 
SDG names are truncated if they exceed 60 characters.
\\end{minipage}
\\end{table}
"""
    
    return latex

def generate_latex_table_topic_distribution(stats: Dict[str, Any]) -> str:
    """Generate LaTeX table for topic distribution."""
    if not stats or 'top_topics' not in stats:
        return ""
    
    latex = """\\begin{table}[htbp]
\\centering
\\caption{Distribution of Research Topics}
\\label{tab:topic_distribution}
\\begin{tabular}{lrr}
\\toprule
Topic Name & Number of Assignments & Percentage \\\\
\\midrule
"""
    
    for topic in stats['top_topics'][:20]:  # Top 20
        topic_name = topic['topic_name'].replace('&', '\\&').replace('_', '\\_')
        if len(topic_name) > 50:
            topic_name = topic_name[:47] + "..."
        latex += f"{topic_name} & {format_number(topic['count'], 0)} & {format_number(topic['percentage'], 2)}\\% \\\\\n"
    
    latex += """\\bottomrule
\\end{tabular}
\\begin{minipage}{\\textwidth}
\\footnotesize
\\textit{Notes:} This table presents the distribution of research topics assigned to papers in the AI papers dataset. 
The table shows the top 20 topics by number of assignments. 
Each row represents a topic and reports the total number of times that topic is assigned to papers 
and the corresponding percentage of all topic assignments in the dataset. 
Topic names are truncated if they exceed 50 characters.
\\end{minipage}
\\end{table}
"""
    
    return latex

def generate_latex_report(all_stats: Dict[str, Any], output_file: Path):
    """Generate complete LaTeX report document."""
    print("\nGenerating LaTeX report...")
    
    latex_content = """\\documentclass[12pt]{article}
\\usepackage[utf8]{inputenc}
\\usepackage{booktabs}
\\usepackage{multirow}
\\usepackage{graphicx}
\\usepackage{geometry}
\\geometry{a4paper, margin=1in}
\\usepackage{longtable}
\\usepackage{array}

\\title{Summary Statistics of Artificial Intelligence Papers Dataset}
\\author{Generated Report}
\\date{\\today}

\\begin{document}

\\maketitle

\\section{Introduction}

This report presents comprehensive summary statistics for the Artificial Intelligence (AI) papers dataset. 
The statistics are designed to provide a thorough overview of the dataset characteristics, including 
author distributions, geographic patterns, citation metrics, institutional affiliations, and research 
classifications. All tables follow the formatting requirements of top-tier economics journals, with 
clear column and row labels and comprehensive notes.

\\section{Summary Statistics}

"""
    
    # Add all tables
    if 'author_distribution' in all_stats:
        latex_content += generate_latex_table_author_distribution(all_stats['author_distribution'])
        latex_content += "\n\n"
    
    if 'country_distribution' in all_stats:
        latex_content += generate_latex_table_country_distribution(all_stats['country_distribution'])
        latex_content += "\n\n"
    
    if 'citation_stats' in all_stats:
        latex_content += generate_latex_table_citation_stats(all_stats['citation_stats'])
        latex_content += "\n\n"
    
    if 'institution_distribution' in all_stats:
        latex_content += generate_latex_table_institution_distribution(all_stats['institution_distribution'])
        latex_content += "\n\n"
    
    if 'concept_distribution' in all_stats:
        latex_content += generate_latex_table_concept_distribution(all_stats['concept_distribution'])
        latex_content += "\n\n"
    
    if 'keyword_distribution' in all_stats:
        latex_content += generate_latex_table_keyword_distribution(all_stats['keyword_distribution'])
        latex_content += "\n\n"
    
    if 'topic_distribution' in all_stats:
        latex_content += generate_latex_table_topic_distribution(all_stats['topic_distribution'])
        latex_content += "\n\n"
    
    if 'sdg_distribution' in all_stats and all_stats['sdg_distribution'].get('available', False):
        latex_content += generate_latex_table_sdg_distribution(all_stats['sdg_distribution'])
        latex_content += "\n\n"
    
    latex_content += """\\section{Conclusion}

This report provides a comprehensive overview of the AI papers dataset through detailed summary statistics. 
The tables presented above cover key dimensions of the dataset including author characteristics, 
geographic distribution, citation patterns, institutional affiliations, and research classifications. 
These statistics serve as a foundation for understanding the structure and composition of the dataset 
for subsequent research analysis.

\\end{document}
"""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(latex_content)
    
    print(f"LaTeX report saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Generate summary statistics for AI papers dataset')
    parser.add_argument('--data-dir', type=str, 
                       default='data/raw/publication/ai_papers_batches',
                       help='Directory containing flattened parquet batch files')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of batch files to process (for testing)')
    parser.add_argument('--output-dir', type=str, default=None,
                       help='Output directory for statistics files')
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        raise ValueError(f"Data directory does not exist: {data_dir}")
    
    if args.output_dir:
        global OUTPUT_DIR, LATEX_OUTPUT_DIR
        OUTPUT_DIR = Path(args.output_dir) / "tables"
        LATEX_OUTPUT_DIR = Path(args.output_dir) / "latex"
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        LATEX_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("AI Papers Dataset Summary Statistics Generator")
    print("=" * 80)
    
    # Process data in chunks to avoid OOM
    print("\nProcessing data in chunks...")
    
    # Initialize accumulators
    author_acc = None
    country_acc = None
    citation_acc = None
    institution_acc = None
    concept_acc = None
    keyword_acc = None
    topic_acc = None
    sdg_acc = None
    
    chunk_size = 10  # Process 10 files at a time to avoid OOM
    total_chunks = 0
    
    # Process chunks
    for chunk_df in process_files_in_chunks(data_dir, args.sample_size, chunk_size):
        total_chunks += 1
        
        # Accumulate statistics from this chunk
        print(f"  Accumulating statistics from chunk {total_chunks}...")
        author_acc = accumulate_author_stats(chunk_df, author_acc)
        country_acc = accumulate_country_stats(chunk_df, country_acc)
        citation_acc = accumulate_citation_stats(chunk_df, citation_acc)
        institution_acc = accumulate_institution_stats(chunk_df, institution_acc)
        concept_acc = accumulate_concept_stats(chunk_df, concept_acc)
        keyword_acc = accumulate_keyword_stats(chunk_df, keyword_acc)
        topic_acc = accumulate_topic_stats(chunk_df, topic_acc)
        sdg_acc = accumulate_sdg_stats(chunk_df, sdg_acc)
        
        # Free memory
        del chunk_df
        import gc
        gc.collect()
    
    print(f"\nProcessed {total_chunks} chunks. Finalizing statistics...")
    
    # Finalize all statistics
    all_stats = {}
    all_stats['author_distribution'] = finalize_author_stats(author_acc)
    all_stats['country_distribution'] = finalize_country_stats(country_acc)
    all_stats['citation_stats'] = finalize_citation_stats(citation_acc)
    all_stats['institution_distribution'] = finalize_institution_stats(institution_acc)
    all_stats['concept_distribution'] = finalize_concept_stats(concept_acc)
    all_stats['keyword_distribution'] = finalize_keyword_stats(keyword_acc)
    all_stats['topic_distribution'] = finalize_topic_stats(topic_acc)
    all_stats['sdg_distribution'] = finalize_sdg_stats(sdg_acc)
    
    # Save statistics as JSON
    stats_file = OUTPUT_DIR / "ai_papers_summary_statistics.json"
    print(f"\nSaving statistics to: {stats_file}")
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(all_stats, f, indent=2, default=str)
    
    # Generate LaTeX tables
    print("\nGenerating LaTeX tables...")
    latex_tables_file = LATEX_OUTPUT_DIR / "ai_papers_summary_tables.tex"
    with open(latex_tables_file, 'w', encoding='utf-8') as f:
        f.write("% LaTeX tables for AI papers summary statistics\n")
        f.write("% Generated: " + datetime.now().isoformat() + "\n\n")
        
        if 'author_distribution' in all_stats:
            f.write(generate_latex_table_author_distribution(all_stats['author_distribution']))
            f.write("\n\n")
        
        if 'country_distribution' in all_stats:
            f.write(generate_latex_table_country_distribution(all_stats['country_distribution']))
            f.write("\n\n")
        
        if 'citation_stats' in all_stats:
            f.write(generate_latex_table_citation_stats(all_stats['citation_stats']))
            f.write("\n\n")
        
        if 'institution_distribution' in all_stats:
            f.write(generate_latex_table_institution_distribution(all_stats['institution_distribution']))
            f.write("\n\n")
        
        if 'concept_distribution' in all_stats:
            f.write(generate_latex_table_concept_distribution(all_stats['concept_distribution']))
            f.write("\n\n")
        
        if 'keyword_distribution' in all_stats:
            f.write(generate_latex_table_keyword_distribution(all_stats['keyword_distribution']))
            f.write("\n\n")
        
        if 'topic_distribution' in all_stats:
            f.write(generate_latex_table_topic_distribution(all_stats['topic_distribution']))
            f.write("\n\n")
        
        if 'sdg_distribution' in all_stats and all_stats['sdg_distribution'].get('available', False):
            f.write(generate_latex_table_sdg_distribution(all_stats['sdg_distribution']))
            f.write("\n\n")
    
    print(f"LaTeX tables saved to: {latex_tables_file}")
    
    # Generate complete LaTeX report
    latex_report_file = LATEX_OUTPUT_DIR / "ai_papers_summary_report.tex"
    generate_latex_report(all_stats, latex_report_file)
    
    print("\n" + "=" * 80)
    print("Summary statistics generation complete!")
    print("=" * 80)
    print(f"\nOutput files:")
    print(f"  - Statistics JSON: {stats_file}")
    print(f"  - LaTeX tables: {latex_tables_file}")
    print(f"  - LaTeX report: {latex_report_file}")

if __name__ == "__main__":
    main()
