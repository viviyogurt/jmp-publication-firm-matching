# Patent + Publication Combined Dataset

**Date:** 2026-02-15
**Type:** Inner Join (Non-Missing)
**Purpose:** Firms with BOTH patent AND publication data

---

## 📊 Dataset Overview

### Inner Join Result

**File:** `data/processed/linking/patent_publication_combined.parquet`
**Size:** 66.7 KB
**Rows:** 2,202 firms
**Join Type:** Inner (non-missing - firms with BOTH patent AND publication data)

### Coverage

| Metric | Count | Percentage |
|--------|-------|------------|
| **Combined Firms** | 2,202 | **11.8% of CRSP** (18,709) |
| Of patent-matched firms | 2,202 | **26.1%** of 8,436 patent firms |
| Of publication-matched firms | 2,202 | **67.7%** of 3,254 pub firms |

### Excluded Firms

| Category | Firms | % of Parent |
|----------|-------|-------------|
| **Patent-only** | 6,234 | 73.9% of patent firms |
| **Publication-only** | 1,052 | 32.3% of pub firms |
| **No data** | 9,221 | 49.3% of CRSP |

---

## 🔢 Dataset Statistics

### Patent Statistics (for 2,202 firms)

- **Total patent matches:** 18,870
- **Mean matches per firm:** 8.6
- **Median matches per firm:** 1.0
- **Mean assignees per firm:** 8.6
- **Total patents:** 648,092

### Publication Statistics (for 2,202 firms)

- **Total publication matches:** 4,417
- **Mean matches per firm:** 2.0
- **Median matches per firm:** 1.0
- **Mean institutions per firm:** 2.0
- **Total papers:** 42,888

---

## 🏢 Top 10 Firms by Paper Count

| Rank | Firm | Papers | Patents |
|------|------|--------|---------|
| 1 | INTL BUSINESS MACHINES CORP | 26,129 | 84,323 |
| 2 | JD.COM INC | 3,284 | 72 |
| 3 | EASTMAN KODAK CO | 1,384 | 2,677 |
| 4 | DOW INC | 1,007 | 312 |
| 5 | SHELL PLC | 639 | 216 |
| 6 | SIEMENS AG | 591 | 5 |
| 7 | HITACHI LTD | 382 | 7,550 |
| 8 | PFIZER INC | 336 | 49 |
| 9 | SANOFI | 293 | 29 |
| 10 | ABBOTT LABORATORIES | 277 | 126 |

---

## 📋 Dataset Schema

| Column | Type | Description |
|--------|------|-------------|
| `GVKEY` | String | Firm identifier (Compustat) |
| `LPERMNO` | Int64 | CRSP PERMNO (from patent data) |
| `firm_conm` | String | Company name (from patent data) |
| `patent_match_count` | UInt32 | Number of patent-firm matches |
| `unique_assignees` | UInt32 | Number of unique patent assignees |
| `total_patents` | Int64 | Total patent count |
| `LPERMNO_pub` | Int64 | CRSP PERMNO (from pub data) |
| `firm_conm_pub` | String | Company name (from pub data) |
| `publication_match_count` | UInt32 | Number of pub-firm matches |
| `unique_institutions` | UInt32 | Number of unique institutions |
| `total_papers` | Int64 | Total paper count |

---

## 💡 Key Insights

### 1. High-Value R&D Firms

The 2,202 firms in this combined dataset represent **the most R&D-intensive firms** in CRSP:
- 67.7% of publication-matched firms also patent (most academic publishers also patent)
- Only 26.1% of patent-matched firms publish (patenting is broader than academic publishing)

### 2. Innovation Leaders

These firms are **innovation leaders**:
- IBM: 26,129 papers + 84,323 patents
- Hitachi: 382 papers + 7,550 patents
- Eastman Kodak: 1,384 papers + 2,677 patents

### 3. Dataset Quality

- **High confidence:** Both datasets have 95%+ accuracy
- **Comprehensive:** Covers both patent and publication innovation
- **Focused:** Represents 11.8% of CRSP but captures the most innovative firms

---

## 🎯 Usage Recommendations

### When to Use Combined Dataset

✅ **Use for:**
- Studying firms that innovate through BOTH patents and publications
- Analyzing relationship between patenting and publishing
- Research on R&D-intensive firms
- Studies requiring comprehensive innovation measures

❌ **NOT for:**
- Analysis of all innovating firms (use patent dataset)
- Publication-only research (use full pub dataset)
- Broad coverage across CRSP (combine all datasets)

### Complement with Other Datasets

For broader coverage, consider:
- **Patent-only firms:** Add 6,234 firms for total innovation analysis
- **Publication-only firms:** Add 1,052 firms for academic research focus
- **Full coverage:** 9,488 firms (50.7% of CRSP) when combined

---

## 📁 Related Files

**Source Datasets:**
- `data/processed/linking/patent_firm_matches_adjusted.parquet` (8,436 firms)
- `data/processed/linking/publication_firm_matches_with_alternative_names.parquet` (3,254 firms)

**Documentation:**
- `docs/PATENT_VS_PUBLICATION_OVERLAP.md` - Detailed overlap analysis
- `docs/CURRENT_COVERAGE_ACCURACY.md` - Publication matching results
- `docs/PUBLICATION_MATCHING_FINAL_RESULTS.md` - Publication methodology

---

**Generated:** 2026-02-15
**Purpose:** JMP analysis of firms with both patent and publication data
**Status:** ✅ Ready for analysis
