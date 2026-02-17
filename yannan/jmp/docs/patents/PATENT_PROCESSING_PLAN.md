# PatentsView Data Processing Plan - KPSS Methodology Adapted for AI Patents

## Overview

This plan processes **manually downloaded PatentsView data** to support the JMP research project on **AI knowledge disclosure strategies**. The research examines when firms choose to publish vs. patent AI innovations, requiring patent data that aligns with **KPSS (Kogan, Papanikolaou, Seru, Stoffman 2017)** methodology adapted for **AI-specific analysis**.

## Research Questions

1. How do AI patents affect firm value? (KPSS event study approach)
2. What is the translation efficiency between AI publications and AI patents? (Text similarity)
3. When do firms choose to publish vs. patent AI innovations? (Strategic choice)
4. How does AI innovation contribute to firm growth? (Babina et al. 2024 extension)

## Current State

- ✅ Financial data collected (CRSP-CCM)
- ✅ Publication data collected (OpenAlex)
- ✅ **Patent data downloaded manually** (saved to `/Data/patent/raw/`)
- ⏳ **Needs processing** following KPSS methodology

## Two-Stage Classification Approach

### Stage 1: Technical Identification (Is this AI?)

**Goal:** Identify ALL AI patents for main analysis

**Methods:**
1. **CPC-based identification** (KPSS technology classes):
   - G06N: AI-specific computing arrangements
   - G06Q: Data processing for business (AI applications)
   - G10L: Speech recognition/analysis
   - H04N: Image processing/communication (computer vision)

2. **Text-based identification** (Babina et al. 2024 approach):
   - AI keyword list: artificial intelligence, machine learning, neural network, deep learning, natural language processing, computer vision, reinforcement learning, convolutional neural, recurrent neural, transformer model, generative adversarial, large language model, BERT, GPT, LLM, diffusion model

3. **Union approach**: Patent matches EITHER CPC OR text criteria (broad coverage)

**Output:** Binary flag: Is this patent AI-related? (Yes/No)

### Stage 2: Strategic Classification (What type of AI?)

**Goal:** Classify AI innovations into strategic layers for discrete choice modeling

**Strategic Categories:**

| Category | Description | Example Patents | Disclosure Incentives |
|----------|-------------|-----------------|----------------------|
| **Infrastructure/Hardware** | Computing hardware, cloud systems, chips, specialized AI hardware | GPU architectures, tensor processing units, AI accelerators, cloud AI platforms | **Patent-heavy**: Hardware easier to protect, harder to reverse-engineer |
| **Algorithms/Models** | ML architectures, training methods, optimization techniques, model designs | Transformer architectures, neural network layers, training algorithms, loss functions | **Publish-heavy**: Algorithms establish priority, harder to patent, publication signals quality |
| **Applications/Software** | End-user AI products, business applications, AI-powered services | AI recommendation systems, chatbot applications, AI diagnostics, autonomous driving features | **Hybrid strategy**: Both patent (product features) and publish (technical depth) |

**Research Hypothesis:** Different AI types have different disclosure incentives
- Infrastructure → Patent preference (strong IP protection needed)
- Algorithms → Publication preference (priority signaling, hard to patent)
- Applications → Hybrid strategy (both product protection and technical signaling)

**Classification Method (Stage 2):**

**Rule-Based Keywords:**
```python
# Infrastructure keywords
infra_keywords = [
    'hardware', 'chip', 'processor', 'gpu', 'tpu', 'accelerator',
    'computing system', 'cloud platform', 'server', 'architecture',
    'integrated circuit', 'semiconductor', 'fpga', 'asic'
]

# Algorithm keywords
algo_keywords = [
    'neural network', 'transformer', 'convolutional', 'recurrent',
    'training algorithm', 'optimization', 'loss function', 'attention',
    'backpropagation', 'gradient descent', 'model architecture',
    'embedding', 'representation learning', 'generative model'
]

# Application keywords
app_keywords = [
    'recommendation system', 'chatbot', 'virtual assistant',
    'autonomous driving', 'medical diagnosis', 'fraud detection',
    'image recognition', 'speech recognition', 'natural language understanding',
    'predictive analytics', 'decision support system', 'ai application'
]

# Classification logic (mutually exclusive)
# 1. Check infrastructure first (hardware-specific terms)
# 2. If not infrastructure, check algorithms (model-specific terms)
# 3. If neither, classify as application
```

## Data Sources

### Downloaded PatentsView Data

**Location:** `/Data/patent/raw/`

**Required Files:**

| File | PatentsView ID | Size | Description |
|------|----------------|------|-------------|
| **g_patent.tsv** | 796 | ~2 GB | Patent metadata (ID, date, type, title) |
| **g_patent_abstract.tsv** | 795 | ~1.6 GB | Patent abstracts (text) |
| **g_cpc_current.tsv** | 794 | ~1 GB | CPC classifications |
| **g_assignee_disambiguated.tsv** | 792 | ~200 MB | Assignee names (cleaned) |
| **g_patent_assignee.tsv** | 797 | ~500 MB | Patent → assignee links |

### Existing Data to Integrate

**Location:** `/Data/financials/` and `/Data/openscience/`

| Data | Source | Format | Use |
|------|--------|--------|-----|
| **CRSP-CCM** | WRDS | CSV | Stock returns, firm fundamentals (GVKEY) |
| **OpenAlex** | OpenAlex | JSON/CSV | AI publications, abstracts, institution IDs |

## Implementation Phases

### Phase 1: Verify Downloaded Data

**Directory Structure:**
```
/Data/patent/
├── raw/                          # Manually downloaded (already done)
│   ├── g_patent.tsv
│   ├── g_patent_abstract.tsv
│   ├── g_cpc_current.tsv
│   ├── g_assignee_disambiguated.tsv
│   └── g_patent_assignee.tsv
├── processed/                    # To create
│   ├── ai_patents_2010_2024.parquet
│   ├── patent_event_dates.parquet
│   ├── firm_patent_panel.parquet
│   ├── firm_year_panel.parquet
│   └── software_patents_2010_2024.parquet
└── outputs/
    ├── event_study_data.csv
    ├── firm_year_innovation.csv
    ├── translation_metrics.csv
    └── strategic_classification_summary.csv
```

### Phase 2: Load and Filter Patents (2010-2024)

**Key Steps:**
1. Load patent metadata and filter to 2010-2024
2. Load and merge patent abstracts (CRITICAL for text analysis)
3. Identify AI patents using dual approach (CPC + text)
4. Apply Stage 2 strategic classification
5. Filter to utility patents only
6. Save processed AI patents with classification

**Expected Output:**
- ~30,000-60,000 AI utility patents (2010-2024)
- Classification distribution:
  - Infrastructure: 10-20%
  - Algorithm: 40-50%
  - Application: 20-30%
  - Unknown: 10-20%

### Phase 3: Match Patents to Firms

**Key Steps:**
1. Load assignee data (disambiguated)
2. Load Compustat firm data (CRSP-CCM)
3. Fuzzy matching (assignee → Compustat)
4. Create patent → firm mapping
5. Merge with AI patents

**Expected Output:** ~15,000-40,000 AI patents matched to ~1,500-3,000 firms

### Phase 4: Create Firm-Patent Panel

**KPSS Data Processing Approach:**
- Sample selection: Public firms only (Compustat-CRSP merged)
- Patent type: Utility patents only (exclude design, plant patents)
- Grant date: Use patent grant date (not application date)
- Firm linkage: Match assignees to GVKEYs
- Time period: 2010-2024

**Expected Output:**
- 5,000-15,000 firm-year observations
- 1,500-3,000 unique firms
- Year range: 2010-2024

### Phase 5: Translation Efficiency Analysis

**Goal:** Compute semantic similarity between AI patent abstracts and AI publication abstracts

**Method:**
1. Load publication data (OpenAlex)
2. Match publications to firms
3. Compute text similarity (SciBERT/PatBERT embeddings)
4. Calculate translation efficiency metrics

### Phase 6: Innovation Index Construction

**KPSS Innovation Index Formula:**
```
Innovation_t = Σ_i (w_i,t × CAR_i,t)
where w_i,t = MarketCap_i,t / TotalMarketCap_t
```

### Phase 7: Validation and Quality Checks

**Validation Steps:**
1. Sample validation (AI patent identification)
2. Strategic classification validation
3. Temporal distribution check
4. Firm matching quality check
5. Event study validation (KPSS benchmarks)
6. Cross-validation with publication data
7. Data integrity checks

## Technical Requirements

**Hardware:**
- **RAM:** 32-64 GB recommended
- **Storage:** 30 GB free space
- **GPU:** Optional for embedding computation

**Software (Python):**
```bash
polars>=0.20.0           # DataFrame operations
pyarrow>=12.0.0          # Parquet support
rapidfuzz>=3.0.0         # Fuzzy string matching
sentence-transformers>=2.2.0  # SciBERT/PatBERT embeddings
torch>=2.0.0             # PyTorch
scikit-learn>=1.3.0      # Cosine similarity
```

## Success Criteria

### Phase 2 (AI Patent Identification & Strategic Classification):
1. ✅ 30,000-60,000 AI patents identified (2010-2024)
2. ✅ >90% have abstracts available
3. ✅ Stage 1: Both CPC and text methods used
4. ✅ Stage 2: Strategic classification completed
5. ✅ Sample verification confirms >90% true positive rate
6. ✅ Strategic classification accuracy >80%

### Phase 3 (Firm Matching):
1. ✅ 1,500-3,000 firms matched to AI patents
2. ✅ Fuzzy matching accuracy >85%
3. ✅ Top 1,000 firms by patent count covered
4. ✅ GVKEYs validated against Compustat

### Phase 4 (Panel Creation):
1. ✅ Firm-year panel has 5,000-15,000 observations
2. ✅ Year range: 2010-2024
3. ✅ First patent identification working
4. ✅ Event dates extracted for KPSS event study

### Integration:
1. ✅ Patent data merges with CRSP-CCM (financials)
2. ✅ Patent data merges with OpenAlex (publications)
3. ✅ Datasets ready for:
   - Translation efficiency analysis
   - Strategic choice modeling
   - Panel regressions

## References

**KPSS (2017):** Kogan, Papanikolaou, Seru, Stoffman - "Technological Innovation, Resource Allocation, and Growth" (QJE 2017)

**Babina et al. (2024):** "Artificial Intelligence, Firm Growth, and Product Innovation" (JFE 2024)

**Your Research Proposal:** "Strategic Knowledge Disclosure in AI Innovation"
