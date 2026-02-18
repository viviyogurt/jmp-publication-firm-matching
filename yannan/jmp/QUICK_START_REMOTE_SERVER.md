# Quick Start Guide - Remote Server Workflow

## Step-by-Step Instructions

### 🚀 Step 1: Pull Updates (First Prompt)

**Copy and paste this simple prompt to Claude Code on your remote server:**

---

```
Please pull the latest updates from GitHub:

Repository: https://github.com/viviyogurt/jmp-publication-firm-matching.git
Branch: lightweight-master
Working directory: (your actual project path)

Commands to run:
1. cd /path/to/jmp-project/yannan/jmp
2. git pull origin lightweight-master
3. ls -la src/04_patent_processing/
4. ls -la docs/patents/
5. cat docs/CLAUDE_PROMPT_PATENT_PROCESSING.md

Tell me what new files and documentation are now available.
```

---

### 🎯 Step 2: Start Patent Processing (Second Prompt)

**After Claude pulls the updates, paste this:**

```
I want you to implement the patent processing pipeline as described in docs/CLAUDE_PROMPT_PATENT_PROCESSING.md

Please start by:
1. Reading the documentation in docs/CLAUDE_PROMPT_PATENT_PROCESSING.md
2. Checking if PatentsView data exists at /Data/patent/raw/
3. Implementing Phase 2: Two-stage AI patent classification
4. Creating the first processed dataset

Use the utility functions in src/04_patent_processing/utils/classification.py
Use the keywords in src/04_patent_processing/utils/keyword_lists.py

Follow the two-stage approach:
- Stage 1: AI identification (CPC + text union)
- Stage 2: Strategic classification (Infrastructure/Algorithm/Application)

Save results to /Data/patent/processed/ai_patents_2010_2024.parquet
```

---

## 📋 Alternative: All-in-One Prompt

**Or just paste this single prompt to do everything:**

```
I need you to process PatentsView data for AI innovation research. Please:

1. Pull latest changes from GitHub:
   git pull origin lightweight-master

2. Read the patent processing documentation:
   cat docs/CLAUDE_PROMPT_PATENT_PROCESSING.md

3. Verify PatentsView data exists:
   ls -lh /Data/patent/raw/

4. Implement Phase 2 of the patent processing pipeline:
   - Load g_patent.tsv and g_patent_abstract.tsv (filter 2010-2024)
   - Load g_cpc_current.tsv
   - Apply Stage 1: AI identification (CPC OR text union)
   - Apply Stage 2: Strategic classification (Infrastructure/Algorithm/Application)
   - Save to /Data/patent/processed/ai_patents_2010_2024.parquet

5. Report:
   - Number of AI patents found
   - Classification distribution (Infrastructure/Algorithm/Application/Unknown)
   - Sample 5 patents from each category

Use the functions and keywords from:
- src/04_patent_processing/utils/classification.py
- src/04_patent_processing/utils/keyword_lists.py

Expected: 30,000-60,000 AI patents with strategic classification.
```

---

## ✅ What to Expect

After you paste the prompt, Claude Code should:

1. ✅ Pull updates from GitHub
2. ✅ Read all the documentation
3. ✅ Verify PatentsView data exists
4. ✅ Implement two-stage classification
5. ✅ Create processed dataset
6. ✅ Report classification distribution

**Typical output:**
```
Total patents 2010-2024: 5,234,567
AI patents identified: 45,678
Classification distribution:
- Infrastructure: 5,234 (11.5%)
- Algorithm: 20,123 (44.1%)
- Application: 12,456 (27.3%)
- Unknown: 7,865 (17.2%)
Saved to: /Data/patent/processed/ai_patents_2010_2024.parquet
```

---

## 📍 File Locations Reference

**After pulling from GitHub, these files will be available:**

```
jmp-project/yannan/jmp/
├── docs/
│   ├── CLAUDE_PROMPT_PATENT_PROCESSING.md  ← Main instruction prompt
│   ├── patents/
│   │   └── PATENT_PROCESSING_PLAN.md        # Detailed background
│   ├── research_plan_idea1_enhanced_v2.md   # Research methodology
│   └── IDENTIFICATION_UPDATE.md             # Causal identification
│
└── src/04_patent_processing/
    ├── README.md                              # Quick start guide
    ├── requirements.txt                       # Python dependencies
    ├── utils/
    │   ├── classification.py                  # Classification functions
    │   └── keyword_lists.py                   # All keywords
    └── scripts/
        └── 01_verify_data.py                  # Data verification
```

---

## 🎯 Key Points

1. **First prompt:** Pull from GitHub + review files
2. **Second prompt:** Implement patent processing
3. **OR use all-in-one prompt** to do everything at once

The prompts are self-contained - Claude will know exactly what to do!

---

## 💡 Tips

**If Claude asks for clarification:**
- Tell it the actual project path on the server
- Confirm PatentsView data location
- Specify if you want to process all phases or just Phase 2

**If Claude encounters errors:**
- Check PatentsView data files exist and are readable
- Verify Polars is installed (`pip install polars`)
- Check available memory (use lazy loading if needed)

**To continue after Phase 2:**
```
Continue with Phase 3: Match patents to firms
Use g_patent_assignee.tsv and g_assignee_disambiguated.tsv
Fuzzy match assignee names to Compustat GVKEYs
```

---

That's it! Just copy-paste and Claude will handle the rest. 🚀
