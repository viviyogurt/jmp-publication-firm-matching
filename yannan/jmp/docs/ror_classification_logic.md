# ROR-Based Classification Logic - Visual Guide

## High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    PAPER WITH AFFILIATIONS                      │
│  Author: ["John Doe"]                                            │
│  Affiliations: [                                                 │
│    "IBM Research",                                               │
│    "Massachusetts Institute of Technology",                     │
│    "Google DeepMind"                                             │
│  ]                                                               │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 1: GET OPENALEX INSTITUTION IDs               │
│                                                                  │
│  Input:  Affiliation names                                      │
│  Output: OpenAlex IDs (from your dataset)                       │
│                                                                  │
│  "IBM Research"          → https://openalex.org/I4210123456    │
│  "MIT"                   → https://openalex.org/I130769515      │
│  "Google DeepMind"       → https://openalex.org/I4210178901    │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 2: GET ROR IDS FROM OPENALEX                  │
│                                                                  │
│  Query: https://api.openalex.org/institutions/{id}              │
│                                                                  │
│  OpenAlex ID           → ROR ID                                 │
│  ─────────────────────────────────                                │
│  I4210123456            → https://ror.org/05jq7q044 (IBM)       │
│  I130769515             → https://ror.org/05bwd1x58 (MIT)       │
│  I4210178901            → https://ror.org/03m2h9g62 (DeepMind)  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 3: QUERY ROR API FOR ORGANIZATION TYPE        │
│                                                                  │
│  Query: https://api.ror.org/organizations/{ror_id}              │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ ROR Response for https://ror.org/05bwd1x58             │    │
│  │ {                                                      │    │
│  │   "name": "Massachusetts Institute of Technology",     │    │
│  │   "types": ["Education"],  ← KEY FIELD!                │    │
│  │   "country": {"country_name": "United States"}         │    │
│  │ }                                                      │    │
│  └────────────────────────────────────────────────────────┘    │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 4: CLASSIFY BY ORGANIZATION TYPE               │
│                                                                  │
│  ┌──────────────────┐    ┌─────────────────────────────────┐   │
│  │ Organization Type│ →  │ Classification Rule            │   │
│  └──────────────────┘    └─────────────────────────────────┘   │
│                                                                  │
│  1. "Company"       →  ✅ FIRM                                 │
│     Examples: IBM, Google, Microsoft, Pfizer                    │
│                                                                  │
│  2. "Education"     →  ❌ NON_FIRM (University)                │
│     Examples: MIT, Stanford, Beijing University                 │
│                                                                  │
│  3. "Government"    →  ❌ NON_FIRM (Government Lab)            │
│     Examples: NIH, NASA, Max Planck, CNRS                       │
│                                                                  │
│  4. "Nonprofit"     →  ❌ NON_FIRM                             │
│     Examples: Gates Foundation, NGOs                            │
│                                                                  │
│  5. "Archive"       →  ❌ NON_FIRM                             │
│     Examples: Data archives, libraries                          │
│                                                                  │
│  6. "Facility"      →  ⚠️  REVIEW (Case-by-case)               │
│     → If "IBM Research Lab"     → ✅ FIRM                       │
│     → If "National Lab"          → ❌ NON_FIRM                 │
│     → If unclear                  → ⚠️  REVIEW                  │
│                                                                  │
│  7. "Other"         →  ⚠️  REVIEW                              │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 5: CALCULATE PAPER-LEVEL STATISTICS           │
│                                                                  │
│  For the example paper:                                          │
│  ─────────────────────────────────                               │
│  Affiliations:                                                   │
│    • IBM Research           → Company   → FIRM                  │
│    • MIT                    → Education → NON_FIRM              │
│    • Google DeepMind        → Company   → FIRM                  │
│                                                                  │
│  Results:                                                        │
│    • has_firm_affiliation:     TRUE                             │
│    • firm_count:               2                                │
│    • non_firm_count:           1                                │
│    • firm_affiliation_ratio:   2/3 = 0.67                       │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 6: FILTER PAPERS FOR DATASET                  │
│                                                                  │
│  Include paper in firm-only dataset if:                          │
│                                                                  │
│  Option A (Strict):     has_firm_affiliation == TRUE            │
│                         AND firm_affiliation_ratio >= 0.5       │
│                                                                  │
│  Option B (Conservative): has_firm_affiliation == TRUE          │
│                             AND firm_count > 0                  │
│                                                                  │
│  Option C (Pure Firm):    has_firm_affiliation == TRUE          │
│                         AND non_firm_count == 0                 │
│                         AND firm_affiliation_ratio == 1.0       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Decision Tree for Individual Organizations

```
                        ┌─────────────┐
                        │  START      │
                        │  With       │
                        │  Affiliation│
                        └──────┬──────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Has OpenAlex ID?     │
                    └──────┬───────────────┘
                           │
              ┌────────────┴────────────┐
              │ YES                     │ NO
              ▼                         ▼
     ┌────────────────┐       ┌──────────────────┐
     │ Get ROR ID     │       │ Name-based       │
     │ from OpenAlex  │       │ Classification   │
     │                │       │ (Fallback)        │
     └────────┬───────┘       └────────┬─────────┘
              │                         │
              ▼                         ▼
     ┌────────────────┐       ┌──────────────────┐
     │ Query ROR API  │       │ Check known uni  │
     │ for types      │       │ lists & keywords │
     └────────┬───────┘       └────────┬─────────┘
              │                         │
              ▼                         │
     ┌────────────────┐                  │
     │ What is the    │                  │
     │ ROR type?      │                  │
     └────────┬───────┘                  │
              │                          │
      ┌───────┼────────┐                 │
      │       │        │                 │
      ▼       ▼        ▼                 │
   Company  Education  Other             │
      │       │        │                 │
      ▼       ▼        ▼                 │
    ┌────┐ ┌────┐ ┌────────┐             │
    │FIRM│ │UNI │ │REVIEW  │             │
    └────┘ └────┘ └────────┘             │
      │      │      │                    │
      └──────┴──────┴────────────────────┘
                   │
                   ▼
         ┌─────────────────┐
         │ RETURN RESULT   │
         │                 │
         │ FIRM            │
         │ NON_FIRM_UNI    │
         │ NON_FIRM_GOV    │
         │ REVIEW_NEEDED   │
         └─────────────────┘
```

---

## Key Differences: Old vs New Classification

### OLD (BROKEN) - Keyword Matching
```
IF "Institute" in name:
    → Mark as FIRM  ❌ WRONG

IF "Technology" in name:
    → Mark as FIRM  ❌ WRONG

Examples of failures:
• "Massachusetts Institute of Technology" → FIRM (should be UNIVERSITY)
• "Georgia Institute of Technology" → FIRM (should be UNIVERSITY)
• "Max Planck Institute" → FIRM (should be GOVERNMENT)
• "National Institute of Health" → FIRM (should be GOVERNMENT)
```

### NEW (CORRECT) - ROR Type Classification
```
Query ROR API → Get organization type

IF type == "Company":
    → Mark as FIRM  ✅ CORRECT

IF type == "Education":
    → Mark as NON_FIRM  ✅ CORRECT

IF type == "Government":
    → Mark as NON_FIRM  ✅ CORRECT

Examples:
• MIT               → ROR type: Education → NON_FIRM ✅
• Georgia Tech      → ROR type: Education → NON_FIRM ✅
• Max Planck        → ROR type: Facility  → NON_FIRM ✅
• IBM               → ROR type: Company  → FIRM    ✅
• Google            → ROR type: Company  → FIRM    ✅
```

---

## Handling Edge Cases

### Edge Case 1: Research Labs with Ambiguous Names

```
"Santa Fe Institute"
├─ ROR Type: Nonprofit
└─ Classification: NON_FIRM ✅

"Institute for Advanced Study"
├─ ROR Type: Nonprofit
└─ Classification: NON_FIRM ✅

"Broad Institute"
├─ ROR Type: Facility (affiliated with MIT & Harvard)
├─ Additional check: Is it a company lab?
└─ Classification: NON_FIRM (university-affiliated) ✅
```

### Edge Case 2: Company Research Labs

```
"IBM Research - Almaden"
├─ ROR Type: Facility
├─ Parent: IBM (Company)
└─ Classification: FIRM ✅

"Google DeepMind"
├─ ROR Type: Company (subsidiary of Alphabet)
└─ Classification: FIRM ✅

"Microsoft Research"
├─ ROR Type: Facility
├─ Parent: Microsoft (Company)
└─ Classification: FIRM ✅
```

### Edge Case 3: University-Industry Partnerships

```
Paper with affiliations:
• "MIT"              → Education → NON_FIRM
• "IBM Research"     → Company   → FIRM
• "Stanford"         → Education → NON_FIRM

Result:
• has_firm_affiliation: TRUE
• firm_count: 1
• non_firm_count: 2
• firm_affiliation_ratio: 0.33

Decision: INCLUDE in dataset (has industry collaboration)
```

---

## Implementation Checklist

- [ ] Install required packages: `requests`, `pyarrow`, `pandas`
- [ ] Get ROR API access (free, no key required)
- [ ] Test classification on sample of known institutions
- [ ] Build university exclusion list (if needed)
- [ ] Build government lab exclusion list (if needed)
- [ ] Run on sample dataset (1000-10000 papers)
- [ ] Validate results manually
- [ ] Process full dataset
- [ ] Create final firm-only dataset with proper filtering

---

## API Examples

### Get ROR data for IBM:
```bash
curl "https://api.ror.org/organizations/05jq7q044"
```

### Get OpenAlex institution data:
```bash
curl "https://api.openalex.org/institutions/I4210109390"
```

### Get OpenAlex institutions with filter:
```bash
curl "https://api.openalex.org/institutions?filter=country_code:US,type:company"
```

---

**Last Updated:** 2025-02-08
**Status:** Ready for implementation
**Next Step:** Test on sample dataset
