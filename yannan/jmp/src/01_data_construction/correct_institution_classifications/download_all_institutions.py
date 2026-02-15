"""
Download All Institutions from OpenAlex API
============================================

Downloads the complete OpenAlex institutions database and saves to a gzipped file.
This provides comprehensive coverage for firm classification.

Author: JMP Research Team
Date: 2025-02-09
"""

import json
import gzip
import requests
import time
from pathlib import Path

# Configuration
OUTPUT_FILE = Path("/home/kurtluo/yannan/jmp/data/raw/publication/institutions_all.gz")
API_BASE = "https://api.openalex.org"
PER_PAGE = 200  # Maximum per page
EMAIL = "your_email@example.com"  # OpenAlex requests polite pool access


def download_all_institutions():
    """
    Download all institutions from OpenAlex API using cursor-based pagination.
    Includes resume capability and better error handling.
    """
    print("="*70)
    print("DOWNLOADING ALL OPENALEX INSTITUTIONS (Cursor-based pagination)")
    print("="*70)

    # First, get total count
    print(f"\nGetting total institution count...")
    url = f"{API_BASE}/institutions?per_page=1&mailto={EMAIL}"
    response = requests.get(url, timeout=10)
    meta = response.json().get('meta', {})
    total_count = meta.get('count', 0)

    print(f"Total institutions: {total_count:,}")
    print(f"Records per page: {PER_PAGE:,}")
    print(f"Using cursor-based pagination (no page limit)")

    # Check if we can resume from existing file
    cursor = None
    institutions_count = 0

    if OUTPUT_FILE.exists():
        try:
            # Get last institution ID to resume from
            with gzip.open(OUTPUT_FILE, 'rt') as f:
                lines = list(f)
            if lines:
                institutions_count = len(lines)
                print(f"\nResuming from {institutions_count:,} institutions")
        except:
            print(f"\nStarting fresh (existing file incomplete)")
            OUTPUT_FILE.unlink()
            institutions_count = 0
    else:
        print(f"\nStarting fresh download")

    start_time = time.time()
    consecutive_errors = 0
    max_errors = 10
    batch_num = 0

    mode = 'at' if institutions_count > 0 else 'wt'
    with gzip.open(OUTPUT_FILE, mode) as f:
        while True:
            retries = 0
            max_retries = 5

            # Build URL with cursor
            if cursor:
                url = f"{API_BASE}/institutions?per_page={PER_PAGE}&cursor={cursor}&mailto={EMAIL}"
            else:
                url = f"{API_BASE}/institutions?per_page={PER_PAGE}&mailto={EMAIL}"

            while retries < max_retries:
                try:
                    response = requests.get(url, timeout=30)

                    if response.status_code != 200:
                        print(f"  ⚠️  Batch {batch_num+1}: HTTP {response.status_code} (retry {retries+1}/{max_retries})")
                        retries += 1
                        time.sleep(2 ** retries)
                        continue

                    data = response.json()
                    institutions = data.get('results', [])

                    if not institutions:
                        print(f"\n✅ No more institutions - download complete!")
                        consecutive_errors = 0
                        break

                    # Write each institution as a line (JSONL format)
                    for inst in institutions:
                        f.write(json.dumps(inst) + '\n')
                        institutions_count += 1

                    f.flush()  # Ensure data is written immediately

                    # Get next cursor
                    meta = data.get('meta', {})
                    cursor = meta.get('next_cursor')

                    if not cursor:
                        print(f"\n✅ No more pages - download complete!")
                        consecutive_errors = 0
                        break

                    # Reset error counter on success
                    consecutive_errors = 0
                    batch_num += 1

                    # Progress update
                    elapsed = time.time() - start_time
                    rate = institutions_count / elapsed if elapsed > 0 else 0
                    pct_complete = (institutions_count / total_count) * 100

                    if batch_num % 5 == 0:
                        print(f"  Batch {batch_num} ({pct_complete:.1f}%) - "
                              f"{institutions_count:,} institutions - "
                              f"Rate: {rate:.0f}/sec")

                    break  # Success, move to next batch

                except requests.exceptions.Timeout:
                    print(f"  ⚠️  Batch {batch_num+1}: Timeout (retry {retries+1}/{max_retries})")
                    retries += 1
                    time.sleep(2 ** retries)
                except requests.exceptions.ConnectionError:
                    print(f"  ⚠️  Batch {batch_num+1}: Connection error (retry {retries+1}/{max_retries})")
                    retries += 1
                    time.sleep(2 ** retries)
                except Exception as e:
                    print(f"  ⚠️  Batch {batch_num+1}: {type(e).__name__}: {e} (retry {retries+1}/{max_retries})")
                    retries += 1
                    time.sleep(2 ** retries)

            # Check if we exhausted retries
            if retries >= max_retries:
                consecutive_errors += 1
                print(f"  ❌ Batch {batch_num+1}: Failed after {max_retries} retries (consecutive errors: {consecutive_errors}/{max_errors})")

                if consecutive_errors >= max_errors:
                    print(f"\n❌ Too many consecutive errors. Stopping.")
                    print(f"   Downloaded {institutions_count:,} institutions so far.")
                    return institutions_count
            else:
                # Exit if no more institutions
                if not institutions or not cursor:
                    break

            # Rate limiting
            time.sleep(0.5)

    elapsed = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"DOWNLOAD COMPLETE!")
    print(f"{'='*70}")
    print(f"Total institutions downloaded: {institutions_count:,}")
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
    print(f"Average rate: {institutions_count/elapsed:.0f} institutions/second")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"File size: {OUTPUT_FILE.stat().st_size / (1024*1024):.1f} MB")
    print(f"{'='*70}")

    return institutions_count


if __name__ == "__main__":
    download_all_institutions()
