"""
Download All Institutions from OpenAlex Using Filtered Batches
===============================================================

Since OpenAlex has a 50-page (10,000 result) limit per query,
we use country-based filtering to download all institutions in batches.

Author: JMP Research Team
Date: 2025-02-09
"""

import json
import gzip
import requests
import time
from pathlib import Path

OUTPUT_FILE = Path("/home/kurtluo/yannan/jmp/data/raw/publication/institutions_all.gz")
API_BASE = "https://api.openalex.org"
PER_PAGE = 200
EMAIL = "your_email@example.com"

# List of countries to filter by (top countries by research output)
COUNTRIES = [
    'us', 'gb', 'cn', 'de', 'jp', 'fr', 'ca', 'in', 'au', 'it',
    'es', 'kr', 'br', 'nl', 'ru', 'ch', 'se', 'tw', 'be', 'pl',
    'mx', 'at', 'tr', 'dk', 'no', 'sg', 'il', 'fi', 'gr', 'pt',
    'ar', 'cz', 'hu', 'cl', 'ie', 'ua', 'za', 'hk', 'my', 'th'
]


def download_institutions_by_country():
    """Download institutions by country to work around pagination limits."""

    print("="*70)
    print("DOWNLOADING INSTITUTIONS BY COUNTRY")
    print("="*70)

    institutions_count = 0
    start_time = time.time()

    # Check for resume
    mode = 'wt'
    if OUTPUT_FILE.exists():
        try:
            with gzip.open(OUTPUT_FILE, 'rt') as f:
                lines = sum(1 for _ in f)
            print(f"\n⚠️  Existing file has {lines:,} institutions")
            print("    Delete file to restart: rm data/raw/publication/institutions_all.gz")
            return
        except:
            OUTPUT_FILE.unlink()

    with gzip.open(OUTPUT_FILE, mode) as f:
        for country_idx, country in enumerate(COUNTRIES):
            print(f"\n[{country_idx+1}/{len(COUNTRIES)}] Downloading institutions from {country.upper()}...")

            cursor = None
            page = 0

            while True:
                # Build URL with country filter and cursor
                if cursor:
                    url = f"{API_BASE}/institutions?filter=country_code:{country}&per_page={PER_PAGE}&cursor={cursor}&mailto={EMAIL}"
                else:
                    url = f"{API_BASE}/institutions?filter=country_code:{country}&per_page={PER_PAGE}&mailto={EMAIL}"

                try:
                    response = requests.get(url, timeout=30)

                    if response.status_code != 200:
                        print(f"  ⚠️  Page {page}: HTTP {response.status_code}")
                        break

                    data = response.json()
                    institutions = data.get('results', [])

                    if not institutions:
                        break

                    # Write institutions
                    for inst in institutions:
                        f.write(json.dumps(inst) + '\n')
                        institutions_count += 1

                    f.flush()

                    # Check for more pages
                    meta = data.get('meta', {})
                    cursor = meta.get('next_cursor')

                    if not cursor:
                        break

                    page += 1

                    if page % 2 == 0:
                        elapsed = time.time() - start_time
                        rate = institutions_count / elapsed
                        print(f"  Page {page}: {institutions_count:,} total institutions ({rate:.0f}/sec)")

                    time.sleep(0.3)  # Rate limiting

                except Exception as e:
                    print(f"  ⚠️  Error: {e}")
                    time.sleep(2)
                    continue

            print(f"  ✅ Complete: {country.upper()} done")

    elapsed = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"DOWNLOAD COMPLETE!")
    print(f"{'='*70}")
    print(f"Total institutions: {institutions_count:,}")
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
    print(f"File size: {OUTPUT_FILE.stat().st_size / (1024*1024):.1f} MB")


if __name__ == "__main__":
    download_institutions_by_country()
