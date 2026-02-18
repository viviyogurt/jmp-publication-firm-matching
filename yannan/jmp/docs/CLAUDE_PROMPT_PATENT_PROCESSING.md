# Claude Code Prompt - Remote Server Patent Processing

**Copy and paste this entire prompt to Claude Code on your remote server**

---

I need your help implementing a comprehensive patent data processing pipeline for my JMP research project on **AI knowledge disclosure strategies**. The data is already downloaded, and I have detailed documentation. Here's what you need to know:

## 🎯 Research Overview

**Core Question:** When do firms choose to publish vs. patent AI innovations?

**Key Concepts:**
- **Translation Efficiency:** How well firms convert scientific publications into patentable inventions
- **Strategic Choice:** Firms decide between {Publish_Only, Patent_Only, Both, Neither}
- **Two-Stage Classification:**
  - Stage 1: Identify which patents are AI-related (technical identification)
  - Stage 2: Classify AI patents into strategic layers (Infrastructure/Algorithm/Application)

**Research Framework:**
- Aligns with KPSS (Kogan, Papanikolaou, Seru, Stoffman 2017) data processing methodology
- Adapts Babina et al. (2024) AI patent identification approach
- YOUR analysis (not KPSS replication): Translation efficiency, strategic choice, complementarity

## 📂 Data Structure

**Location on this server:**
```
/Data/
├── patent/
│   ├── raw/                    # PatentsView data (already downloaded)
│   │   ├── g_patent.tsv              # ~2 GB - Patent metadata
│   │   ├── g_patent_abstract.tsv     # ~1.6 GB - Patent abstracts (CRITICAL!)
│   │   ├── g_cpc_current.tsv         # ~1 GB - CPC classifications
│   │   ├── g_assignee_disambiguated.tsv  # ~200 MB - Assignee names (cleaned)
│   │   └── g_patent_assignee.tsv     # ~500 MB - Patent → assignee links
│   ├── processed/               # To create: Processed datasets
│   └── outputs/                 # To create: Analysis results
├── openscience/                 # OpenAlex publication data (already exists)
└── financials/                  # CRSP-CCM financial data (already exists)
```

**Existing processed data:**
- Firm-publication matched data from OpenAlex
- CRSP-CCM financial data with GVKEYs
- Firm-year panels already created

## 🔄 Two-Stage Classification Methodology

### Stage 1: AI Identification (Is this AI?)

**Union Approach (CPC OR Text):**

**1. CPC-based identification:**
- AI CPC codes: `G06N` (AI computing), `G06Q` (business AI), `G10L` (speech), `H04N` (computer vision)
- Patent matches if ANY CPC code starts with these prefixes

**2. Text-based identification:**
```python
AI_KEYWORDS = [
    'artificial intelligence', 'machine learning', 'neural network', 'deep learning',
    'natural language processing', 'computer vision', 'reinforcement learning',
    'convolutional neural', 'recurrent neural', 'transformer model',
    'generative adversarial', 'large language model', 'BERT', 'GPT', 'LLM',
    'diffusion model'
]
```
- Patent abstract/title contains ANY of these keywords (case-insensitive)

**3. Union:** Patent is AI if (CPC match) OR (text match)

**Output:** Binary flag `is_ai` (True/False)

### Stage 2: Strategic Classification (What type of AI?)

**Three mutually exclusive categories:**

**1. Infrastructure/Hardware:**
```python
INFRA_KEYWORDS = [
    'hardware', 'chip', 'processor', 'gpu', 'tpu', 'accelerator',
    'computing system', 'cloud platform', 'server', 'architecture',
    'integrated circuit', 'semiconductor', 'fpga', 'asic'
]
```
- Disclosure incentive: **Patent-heavy** (hardware easier to protect)

**2. Algorithms/Models:**
```python
ALGO_KEYWORDS = [
    'neural network', 'transformer', 'convolutional', 'recurrent',
    'training algorithm', 'optimization', 'loss function', 'attention',
    'backpropagation', 'gradient descent', 'model architecture',
    'embedding', 'representation learning', 'generative model'
]
```
- Disclosure incentive: **Publish-heavy** (algorithms establish priority)

**3. Applications/Software:**
```python
APP_KEYWORDS = [
    'recommendation system', 'chatbot', 'virtual assistant',
    'autonomous driving', 'medical diagnosis', 'fraud detection',
    'image recognition', 'speech recognition', 'natural language understanding',
    'predictive analytics', 'decision support system'
]
```
- Disclosure incentive: **Hybrid** (both patent and publish)

**Classification Logic (Priority Order):**
1. Check Infrastructure first → if match, classify as Infrastructure
2. Else check Algorithm → if match, classify as Algorithm
3. Else check Application → if match, classify as Application
4. Else → Unknown

**Output:** Categorical variable `ai_category` (Infrastructure/Algorithm/Application/Unknown)

## 📋 Implementation Requirements

### Phase 1: Data Verification (Already Done - Skip or Re-run)

Script: `src/04_patent_processing/scripts/01_verify_data.py`
- Check files exist and readable
- Count records
- Create directory structure

### Phase 2: Load and Filter AI Patents (CRITICAL!)

**Input:** `/Data/patent/raw/`

**Steps:**
1. Load `g_patent.tsv` - filter to patents granted 2010-2024
   ```python
   import polars as pl
   patents = pl.read_csv('/Data/patent/raw/g_patent.tsv', separator='\t')
   patents = patents.filter(
       (pl.col('patent_date') >= '2010-01-01') &
       (pl.col('patent_date') <= '2024-12-31')
   )
   ```

2. Load `g_patent_abstract.tsv` - **CRITICAL for text analysis!**
   ```python
   abstracts = pl.read_csv('/Data/patent/raw/g_patent_abstract.tsv', separator='\t')
   patents = patents.join(abstracts, on='patent_id', how='left')
   ```

3. Load `g_cpc_current.tsv` - for CPC-based classification
   ```python
   cpc = pl.read_csv('/Data/patent/raw/g_cpc_current.tsv', separator='\t')
   # Aggregate CPC codes per patent
   patent_cpc = cpc.groupby('patent_id').agg([
       pl.col('cpc_subgroup').list().alias('cpc_codes')
   ])
   patents = patents.join(patent_cpc, on='patent_id', how='left')
   ```

4. **Apply Stage 1 Classification (Is AI?):**
   ```python
   # CPC-based
   ai_cpc_patterns = ['G06N', 'G06Q', 'G10L', 'H04N']
   patents_with_cpc = patents.with_columns([
       pl.col('cpc_codes').map_elements(
           lambda codes: any(code[:4] in ai_cpc_patterns for code in codes if code),
           return_dtype=bool
       ).alias('is_ai_cpc')
   ])

   # Text-based
   ai_keywords_str = '|'.join(AI_KEYWORDS)
   patents_with_text = patents_with_cpc.with_columns([
       pl.col('patent_abstract').str.to_lower().str.contains(ai_keywords_str).alias('is_ai_text')
   ])

   # Union
   patents_final = patents_with_text.with_columns([
       (pl.col('is_ai_cpc') | pl.col('is_ai_text')).alias('is_ai')
   ])
   ```

5. **Apply Stage 2 Classification (Strategic Category):**
   ```python
   # Define classification function
   def classify_strategic(abstract_text):
       if not abstract_text:
           return 'Unknown'
       text_lower = abstract_text.lower()

       if any(kw in text_lower for kw in INFRA_KEYWORDS):
           return 'Infrastructure'
       elif any(kw in text_lower for kw in ALGO_KEYWORDS):
           return 'Algorithm'
       elif any(kw in text_lower for kw in APP_KEYWORDS):
           return 'Application'
       else:
           return 'Unknown'

   # Apply to AI patents only
   ai_patents = patents_final.filter(pl.col('is_ai') == True)
   ai_patents_classified = ai_patents.with_columns([
       pl.col('patent_abstract').map_elements(classify_strategic, return_dtype=str).alias('ai_category')
   ])
   ```

6. **Save results:**
   ```python
   ai_patents_classified.write_parquet('/Data/patent/processed/ai_patents_2010_2024.parquet')
   ```

**Expected Output:**
- ~30,000-60,000 AI patents (2010-2024)
- Classification distribution:
  - Infrastructure: 10-20%
  - Algorithm: 40-50%
  - Application: 20-30%
  - Unknown: 10-20%

### Phase 3: Match Patents to Firms

**Input:** AI patents from Phase 2

**Steps:**
1. Load `g_patent_assignee.tsv` and `g_assignee_disambiguated.tsv`
2. Filter to organizations only (assignee_type in [2, 3, 7])
3. Match assignee names to Compustat GVKEYs (fuzzy matching with threshold=85)
4. Merge with AI patents

**Expected Output:** ~15,000-40,000 AI patents matched to ~1,500-3,000 firms

**File:** `/Data/patent/processed/firm_patent_panel.parquet`

### Phase 4: Create Firm-Year Panel

**Input:** Matched patents from Phase 3

**Steps:**
1. Group by GVKEY and grant year
2. Aggregate: patent counts, abstract lists, grant dates
3. Merge with Compustat financials (market cap, R&D, assets)
4. Create panel with variables:
   - `gvkey`: Firm identifier
   - `year`: Fiscal year
   - `ai_patent_count`: Number of AI patents granted
   - `ai_patent_abstracts`: List of patent abstracts (for text analysis)
   - `ai_category_counts`: Patent counts by strategic category
   - `market_cap`: Firm market value
   - `rd_intensity`: R&D / Sales
   - `total_assets`: Firm size

**Expected Output:**
- 5,000-15,000 firm-year observations
- 1,500-3,000 unique firms
- Year range: 2010-2024

**File:** `/Data/patent/processed/firm_year_panel.parquet`

### Phase 5: Integration with Existing Data

**Already have on this server:**
- `/Data/openscience/` - OpenAlex publication data
- `/Data/financials/` - CRSP-CCM data
- Firm-publication matched datasets
- Firm-year panels with publications

**Merge strategy:**
```python
# Load existing publication panel
pub_panel = pl.read_parquet('/path/to/existing/firm_year_panel_with_pubs.parquet')

# Load patent panel
patent_panel = pl.read_parquet('/Data/patent/processed/firm_year_panel.parquet')

# Merge on GVKEY and year
combined_panel = pub_panel.join(
    patent_panel,
    on=['gvkey', 'year'],
    how='outer'  # Keep firms with only pubs or only patents
)

# Save
combined_panel.write_parquet('/Data/patent/outputs/firm_year_combined_pubs_patents.parquet')
```

## 🎯 Key Success Criteria

**Phase 2 (AI Patent ID & Classification):**
1. ✅ 30,000-60,000 AI patents identified
2. ✅ >90% have abstracts available
3. ✅ Stage 1: Both CPC and text methods used (union)
4. ✅ Stage 2: Strategic classification completed
5. ✅ Sample verification: Check 50 random AI patents - >90% should actually be AI
6. ✅ Classification accuracy: Check 20 patents from each category - >80% accuracy

**Phase 3 (Firm Matching):**
1. ✅ 1,500-3,000 firms matched
2. ✅ Top 1,000 assignees by patent count covered
3. ✅ GVKEYs validated against Compustat

**Phase 4 (Panel Creation):**
1. ✅ Firm-year panel: 5,000-15,000 observations
2. ✅ Complete coverage: 2010-2024
3. ✅ Patent abstracts available for text similarity analysis
4. ✅ Financial variables merged successfully

## 📊 Expected Final Datasets

**For your research analysis:**

1. **AI Patents with Classification:**
   - `/Data/patent/processed/ai_patents_2010_2024.parquet`
   - Columns: patent_id, patent_date, patent_title, patent_abstract, is_ai, ai_category

2. **Firm-Patent Panel:**
   - `/Data/patent/processed/firm_patent_panel.parquet`
   - Columns: patent_id, gvkey, patent_date, ai_category

3. **Firm-Year Panel (Main Analysis Dataset):**
   - `/Data/patent/processed/firm_year_panel.parquet`
   - Columns: gvkey, year, ai_patent_count, ai_patent_abstracts, market_cap, rd_intensity, ai_category_counts

4. **Combined Panel (Pubs + Patents):**
   - `/Data/patent/outputs/firm_year_combined_pubs_patents.parquet`
   - Ready for translation efficiency analysis and strategic choice modeling

## ⚙️ Technical Requirements

**Use these libraries:**
- `polars` for DataFrame operations (faster than pandas)
- `pyarrow` for Parquet support
- `rapidfuzz` for fuzzy matching
- `tqdm` for progress bars

**Memory management:**
- Use Polars lazy frames for large files
- Process in yearly chunks if needed
- Save intermediate results frequently

**Example lazy loading:**
```python
import polars as pl

# Lazy load
lf = pl.scan_csv('/Data/patent/raw/g_patent.tsv', separator='\t')

# Filter then collect
patents = lf.filter(
    pl.col('patent_date') >= '2010-01-01'
).collect()
```

## 📝 Documentation Available

All documentation is already in this repo at:
- `docs/patents/PATENT_PROCESSING_PLAN.md` - Comprehensive implementation plan
- `docs/research_plan_idea1_enhanced_v2.md` - Core research methodology
- `docs/IDENTIFICATION_UPDATE.md` - Causal identification strategies
- `src/04_patent_processing/README.md` - Implementation details
- `src/04_patent_processing/utils/classification.py` - Classification functions
- `src/04_patent_processing/utils/keyword_lists.py` - All keyword lists

## 🚀 Next Steps - What I Need You To Do

**Start here:**

1. **Verify the data exists:**
   ```bash
   ls -lh /Data/patent/raw/
   wc -l /Data/patent/raw/*.tsv
   ```

2. **Check existing documentation:**
   ```bash
   cat docs/patents/PATENT_PROCESSING_PLAN.md
   cat src/04_patent_processing/utils/keyword_lists.py
   ```

3. **Implement Phase 2** (AI patent identification and classification):
   - Create script based on the implementation plan
   - Use two-stage classification approach described above
   - Save to `/Data/patent/processed/ai_patents_2010_2024.parquet`

4. **Validate results:**
   - Print classification distribution
   - Sample 20 patents from each category for manual verification
   - Ensure >90% AI identification accuracy

5. **Continue with Phases 3-5** sequentially

**Important notes:**
- Time period: 2010-2024 (NOT 1977-2014 like KPSS)
- Patent type: Utility only (exclude design, plant)
- Grant date: Use patent_date, NOT application_date
- Focus: AI-specific analysis, NOT general innovation

**Data integration:**
- Merge with existing publication data at `/Data/openscience/`
- Merge with existing financial data at `/Data/financials/`
- Create combined firm-year panel for your research

**Expected challenges:**
- Large file sizes → Use Polars lazy evaluation
- Memory constraints → Process in chunks
- Fuzzy matching quality → May need manual verification for top firms

**If you encounter issues:**
1. Check the documentation in `docs/patents/`
2. Verify data files are complete and readable
3. Use sampling for development before full processing
4. Save intermediate results frequently

Please start by implementing Phase 2 and let me know the results! Focus on correct implementation of the two-stage classification approach.

---

**End of prompt**

Let me know if you need clarification on any part of the implementation!
