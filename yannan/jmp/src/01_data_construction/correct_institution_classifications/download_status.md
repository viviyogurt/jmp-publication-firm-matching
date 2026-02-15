# OpenAlex Institutions Download Status

## What's Happening

The script is downloading the complete OpenAlex institutions database (120,658 institutions) from the API.

**Download script**: `download_all_institutions.py`
**Output file**: `data/raw/publication/institutions_all.gz`

## Progress Monitoring

Run this command to check progress:
```bash
bash src/01_data_construction/correct_institution_classifications/monitor_download.sh
```

Or manually:
```bash
ls -lh data/raw/publication/institutions_all.gz
zcat data/raw/publication/institutions_all.gz 2>/dev/null | wc -l
```

## Expected Timeline

- **Total institutions**: 120,658
- **Download rate**: ~60-80 institutions/second (with rate limiting)
- **Estimated time**: 25-30 minutes
- **Final file size**: ~250-300 MB compressed

## Once Download Complete

The complete institutions database will allow **instant classification** of all papers without any API calls.

Run this to process the dataset:
```bash
python src/01_data_construction/correct_institution_classifications/classify_fast.py process 10000
```

Or for the full dataset:
```bash
python src/01_data_construction/correct_institution_classifications/classify_fast.py process
```

## Why This Matters

The previous local database only had 1,054 institutions and didn't match any affiliations in the AI papers. The complete database with 120K+ institutions will have comprehensive coverage of all institutions in the dataset.

This means:
- ✅ Fast, instant lookups (no API calls needed)
- ✅ Complete coverage of all institutions
- ✅ Accurate firm/university/government classifications
- ✅ Can process all 17M papers in hours instead of weeks
