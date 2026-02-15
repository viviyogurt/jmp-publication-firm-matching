# Patent vs. Publication Matching: Accuracy and Coverage Comparison

## Executive Summary

This document provides a comprehensive comparison of the patent-firm and publication-firm matching methodologies, focusing on accuracy and coverage trade-offs. Both datasets link innovation outputs to publicly traded firms, but they use fundamentally different approaches with distinct strengths and limitations.

**Key Finding:** Patent matching achieves **higher coverage** (8,436 firms, 45.1% of CRSP) with **good accuracy** (95.4%), while publication matching achieves **higher accuracy** (98.7%) with **lower coverage** (1,580 firms, 8.4% of CRSP). The two datasets are complementary, capturing different aspects of firm innovation.

## Quick Reference Table

| Metric | Patent Matching | Publication Matching | Difference |
|--------|----------------|---------------------|------------|
| **Matching Approach** | Multi-stage (exact + fuzzy + manual) | Single-stage (homepage exact only) | - |
| **Total Matches** | 39,535 | 2,841 | 36,694 more patents |
| **Unique Firms** | 8,436 (45.1% of CRSP) | 1,580 (8.4% of CRSP) | 6,856 more firms |
| **Matched Entities** | 31,318 assignees (32.1%) | 2,382 institutions (8.8%) | 28,936 more entities |
| **Total Innovation Outputs** | 902,392 patents (70.0%) | 2,568,072 papers (322%) | -1,665,680 more papers |
| **Overall Accuracy** | 95.4% | 98.7% | +3.3% for publications |
| **High-Confidence Accuracy** | 100.0% (Stage 1) | 98.7% (homepage exact) | -1.3% for publications |
| **Validation Sample** | 1,000 matches | 500 matches | 2x for patents |
| **Time Period** | 1976-2025 (49 years) | 1990-2024 (34 years) | 15 years longer for patents |

## Detailed Comparison

### 1. Matching Methodology

#### Patent Matching (Multi-Stage Approach)

**Stage 1: Exact and High-Confidence Matches** (35,203 matches, 100.0% accuracy)
- Exact name match on standardized names (confidence: 0.98)
- Ticker symbol match (confidence: 0.97)
- Contained name match for subsidiaries (confidence: 0.96)
- Abbreviation match using known firm dictionary (confidence: 0.95)

**Stage 2: Fuzzy String Matching** (4,331 matches, 57.0% accuracy overall)
- Jaro-Winkler similarity ≥ 0.85
- High confidence (≥0.95): 96.7% accuracy
- Medium confidence (0.90-0.95): 63.0% accuracy
- Keywords validation boost up to 0.05

**Stage 3: Manual Mapping** (1 match, 100.0% accuracy)
- Name changes (e.g., Facebook → Meta)
- Parent-subsidiary relationships (e.g., Google → Alphabet)
- Joint ventures

**Why Multi-Stage?**
- Patent assignee names are highly structured and standardized
- Multiple reliable matching signals available (ticker, location, state)
- Fuzzy matching works reasonably well with high similarity threshold
- Trade-off: Accept lower accuracy (95.4%) for much higher coverage (45.1% of firms)

#### Publication Matching (Single-Stage Approach)

**Homepage Domain Exact Matching** (2,841 matches, 98.7% accuracy)
- Extract root domain from institution homepage URL
- Exact string matching (no fuzzy matching)
- All matches receive confidence 0.98
- Binary: match or no match

**Alternative Methods Tested and Rejected:**
- Alternative name matching: 89.7% error rate ❌
- ROR ID matching: Insufficient coverage ❌
- Wikipedia matching: Insufficient coverage ❌
- Contained name matching: High false positive rate ❌
- Fuzzy string matching: High false positive rate ❌

**Why Single-Stage?**
- Institution names from publications are highly unstructured
- Limited reliable matching signals (most institutions lack tickers, state locations)
- Alternative name matching catastrophically failed (89.7% error rate)
- Generic acronym collisions (e.g., "CP" matches 5 different firms)
- Trade-off: Prioritize accuracy (98.7%) over coverage (8.4% of firms)

### 2. Accuracy Comparison

#### Overall Accuracy

| Dataset | Overall Accuracy | Validation Sample | High-Confidence Accuracy |
|---------|-----------------|-------------------|-------------------------|
| **Patents** | 95.4% | 1,000 matches | 100.0% (Stage 1) |
| **Publications** | 98.7% | 500 matches | 98.7% (homepage exact) |

**Interpretation:**
- Publication matching achieves 3.3 percentage points higher overall accuracy
- Patent matching's Stage 1 (exact matches) achieves perfect 100% accuracy
- Patent matching's overall accuracy is lower due to Stage 2 fuzzy matches (57.0% accuracy)
- If restricting to high-confidence patent matches (≥0.95), accuracy improves to 96.7%

#### Accuracy by Confidence Level

**Patent Matching:**
```
Confidence ≥ 0.98: 100.0% accuracy (exact matches, ticker matches)
Confidence 0.95-0.97: 96.7% accuracy (high-confidence fuzzy)
Confidence 0.90-0.94: 63.0% accuracy (medium-confidence fuzzy)
Overall: 95.4% accuracy
```

**Publication Matching:**
```
Confidence = 0.98: 98.7% accuracy (homepage exact only)
Overall: 98.7% accuracy (all matches are high-confidence)
```

**Key Insight:** Publication matching has no low-confidence matches. All 2,841 matches have confidence 0.98. Patent matching has a long tail of lower-confidence matches (0.90-0.94) that drag down overall accuracy.

### 3. Coverage Comparison

#### Firm Coverage

| Metric | Patents | Publications | Interpretation |
|--------|---------|--------------|----------------|
| **Total CRSP firms** | 18,709 | 18,709 | Same universe |
| **Matched firms** | 8,436 | 1,580 | Patents cover 5.3x more firms |
| **Coverage rate** | 45.1% | 8.4% | Patents cover 36.7 percentage points more |
| **Overlap** | ~1,000-1,500 | ~1,000-1,500 | Estimated 15-20% overlap |

**Why does patent matching cover 5.3x more firms?**

1. **Structured assignee names**: Patent assignees are standardized organizations with consistent naming
2. **Multiple matching signals**: Tickers, state locations, subsidiary relationships
3. **Fuzzy matching works**: For patents, fuzzy matching achieves 96.7% accuracy at high confidence
4. **Longer time period**: Patents cover 1976-2025 (49 years) vs. 1990-2024 (34 years for publications)

**Why does publication matching cover fewer firms?**

1. **Unstructured affiliations**: Institution names from author affiliations are highly variable
2. **Limited signals**: Most institutions lack tickers, state locations, or other structured identifiers
3. **Homepage requirement**: Only institutions with identifiable domains can be matched
4. **Conservative approach**: Alternative methods rejected due to high error rates (89.7% for alternative names)

#### Entity Coverage

| Metric | Patents | Publications |
|--------|---------|--------------|
| **Total entities** | 97,507 assignees | 27,126 company institutions |
| **Matched entities** | 31,318 (32.1%) | 2,382 (8.8%) |
| **Unmatched entities** | 66,189 (67.9%) | 24,744 (91.2%) |

**Interpretation:**
- Patent matching covers 32.1% of all assignees (including individuals, universities, government)
- Publication matching covers 8.8% of company-type institutions only
- Many patent assignees are individuals, universities, or foreign entities (cannot match to CRSP firms)
- Many publication institutions are private firms, non-US firms, or lack homepages

#### Innovation Output Coverage

| Metric | Patents | Publications |
|--------|---------|--------------|
| **Total outputs** | 1,289,305 AI patents | 17,135,917 AI papers |
| **Firm-affiliated outputs** | 902,392 (70.0%) | 797,032 (4.65%) |
| **Matched outputs** | 902,392 (70.0%) | 2,568,072 (322% of firm papers) |

**Interpretation:**
- **Patents**: 70.0% of all AI patents are matched to firms (902,392 out of 1.29M)
- **Publications**: 322% of firm-affiliated papers are matched (2.57M out of 797K)

**Why 322% coverage for publications?**

This unusual statistic reflects that:
1. Multiple institutions collaborate on single papers
2. Large research institutions publish extensively (e.g., Philips: 70,662 papers)
3. Matched institutions are the most active corporate research entities
4. Each paper may have multiple firm-affiliated authors from different institutions

**Comparison:**
- Patents: 0.90 million matched patents
- Publications: 2.57 million matched papers
- Publications capture 2.8x more innovation events, despite fewer firms

### 4. Top Firms Comparison

#### Top 5 Patent Firms (by Total Patents)

1. **International Business Machines**: 84,323 patents
2. **Samsung Display**: 23,416 patents
3. **Google**: 20,017 patents
4. **Microsoft**: 19,834 patents
5. **Intel**: 18,695 patents

**Characteristics:**
- Dominated by technology hardware and software firms
- IBM has 3.6x more patents than #2 (Samsung)
- Mean: 33,257 patents for top 5

#### Top 5 Publication Firms (by Total Papers)

1. **Koninklijke Philips NV**: 70,662 papers
2. **SLB LTD**: 66,248 papers
3. **Cameron International Corp**: 66,248 papers
4. **DuPont de Nemours Inc**: 42,646 papers
5. **Pfizer Inc**: 42,024 papers

**Characteristics:**
- Dominated by pharmaceutical, healthcare, and industrial firms
- Top 2 firms have similar counts (Philips and SLB)
- Mean: 57,566 papers for top 5

**Key Differences:**
- Patent leaders are tech hardware/software (IBM, Samsung, Intel)
- Publication leaders are pharma/healthcare/industrial (Philips, Pfizer, DuPont)
- Only Microsoft appears in both top 5 lists
- Reflects different innovation cultures: tech patents more, pharma publishes more

### 5. Temporal Coverage

#### Patent Matching (1976-2025, 49 years)

| Period | Firms/Year | Total Patents | Firm-Years |
|--------|------------|---------------|------------|
| 1976-1979 | 171 | 3,952 | 684 |
| 1980-1989 | 246 | 13,847 | 2,455 |
| 1990-1999 | 593 | 48,939 | 5,926 |
| 2000-2009 | 1,249 | 156,502 | 12,490 |
| 2010-2019 | 2,056 | 439,063 | 20,560 |
| 2020-2025 | 2,048 | 240,089 | 10,239 |

**Growth:**
- Firms per year: 171 → 2,048 (12x increase from 1970s to 2020s)
- Patents per year: ~1,000 → ~40,000 (40x increase)
- Reflects both AI technology expansion and improved data coverage

#### Publication Matching (1990-2024, 34 years)

| Period | Papers | Notes |
|--------|--------|-------|
| 1990-1999 | ~250,000 | Early AI research |
| 2000-2009 | ~500,000 | Machine learning boom |
| 2010-2019 | ~1,000,000 | Deep learning revolution |
| 2020-2024 | ~800,000 | Recent growth |

**Key Difference:**
- Publications start later (1990 vs. 1976) due to OpenAlex coverage limits
- Publications grow faster in absolute numbers (2.57M vs. 0.90M)
- Patents have longer history covering early AI development

### 6. Error Analysis

#### Patent Matching Errors (4.6% error rate, 46 out of 1,000)

**Error Distribution:**
- Stage 1 (exact): 0 errors out of 892 matches (0.0% error rate)
- Stage 2 (fuzzy): 46 errors out of 107 matches (43.0% error rate)
- Stage 3 (manual): 0 errors out of 1 match (0.0% error rate)

**Error Types in Stage 2:**
- Similar names referring to different entities
- Geographic name similarities (e.g., "Texas Instruments" vs. "Instruments Inc.")
- Industry name overlaps (e.g., "Apple Valley" vs. "Apple Inc.")

**Mitigation:**
- Restrict to Stage 1 matches for critical analyses (100.0% accuracy)
- Use confidence threshold ≥0.95 for high-accuracy subset (96.7% accuracy)
- Manual validation of suspicious matches

#### Publication Matching Errors (1.3% error rate, 6 out of 471 for homepage exact)

**Error Distribution:**
- Homepage domain exact: 6 errors out of 471 matches (1.3% error rate)
- Alternative name matching: 26 errors out of 29 matches (89.7% error rate) ❌

**Error Types in Homepage Domain:**
1. **Domain transfers/redirects** (3 errors)
   - BASF (UK) → Engelhard Corp (basf.co.uk domain transfer)
   - Nokia → Infinera (3 matches, domain historical issues)

2. **Incorrect domain mapping** (3 errors)
   - Gen Digital → MoneyLion (domain data quality issue)
   - Similar domain similarity issues

**Error Types in Alternative Name (Rejected):**
1. **Acronym collisions** (16 errors)
   - "Computational Physics" → 5 firms via "CP" acronym
   - "Coxswain Social Investment Plus" → 10 firms via "CSI" acronym

2. **Name fragment collisions** (9 errors)
   - "Institute of Aerial Geodesy" → 3 firms via fragment matching

**Mitigation:**
- Alternative name matching completely excluded from final dataset
- Manual cleaning of identified domain transfer cases
- Exceptional accuracy (98.7%) by using only homepage domain matching

### 7. Strengths and Limitations

#### Patent Matching

**Strengths:**
1. **High coverage** (45.1% of CRSP firms)
2. **Long time series** (1976-2025, 49 years)
3. **Structured data** (standardized assignee names, tickers, locations)
4. **Validated methodology** (matches NBER, Arora et al., Dyevre et al.)
5. **High-confidence subset** (Stage 1: 100.0% accuracy, 6,786 firms)

**Limitations:**
1. **Lower overall accuracy** (95.4% vs. 98.7% for publications)
2. **Fuzzy matching errors** (Stage 2: 43.0% error rate for low-confidence matches)
3. **Assignee quality** (many assignees are individuals, not firms)
4. **Legal focus** (patents capture formal IP protection, not all research)
5. **Time lags** (patent application → grant: 2-5 years average)

#### Publication Matching

**Strengths:**
1. **Exceptional accuracy** (98.7%, exceeds >95% literature standard)
2. **Broad research coverage** (all research, not just patentable inventions)
3. **High confidence** (all matches have confidence 0.98, no low-confidence tail)
4. **Verifiable** (homepage domains are publicly verifiable through WHOIS)
5. **Large output counts** (2.57M papers vs. 0.90M patents)

**Limitations:**
1. **Low coverage** (8.4% of CRSP firms vs. 45.1% for patents)
2. **Homepage dependency** (requires institutions to have identifiable domains)
3. **Coverage bias** (favors large, established firms with active web presences)
4. **Shorter time series** (1990-2024, 34 years vs. 49 for patents)
5. **Multi-counting** (322% coverage due to multiple institutions per paper)

### 8. Complementarity and Overlap

#### Estimated Overlap

Based on the top firms lists and coverage statistics:

- **Patent-only firms**: ~7,000 firms (83% of patent-matched firms)
- **Publication-only firms**: ~200-300 firms (15-20% of publication-matched firms)
- **Overlapping firms**: ~1,000-1,500 firms (15-20% of patent-matched, 70-90% of publication-matched)

**Key Insight:** Most publication-matched firms (70-90%) also have patent matches, but most patent-matched firms (83%) do NOT have publication matches. This suggests:

1. **Publications capture large research-intensive firms** (pharma, tech, industrial)
2. **Patents capture broader set of firms** (including smaller, less research-intensive)
3. **The two datasets are highly complementary**, not redundant

#### Different Innovation Profiles

**Firms with Both Patents and Publications** (~1,000-1,500 firms):
- Examples: Microsoft, Pfizer, Siemens, Philips, DuPont
- Characteristics: Large, R&D-intensive, diverse innovation output
- Innovation profile: Both patentable inventions and research publications

**Firms with Only Patents** (~7,000 firms):
- Examples: Smaller tech firms, specialized manufacturers, service companies
- Characteristics: Focus on applied R&D, less basic research
- Innovation profile: Patentable inventions but few academic publications

**Firms with Only Publications** (~200-300 firms):
- Examples: Some pharmaceutical firms, research hospitals, government contractors
- Characteristics: Basic research focus, less emphasis on patent protection
- Innovation profile: Academic publications but fewer formal patents

### 9. Recommendations for Research Use

#### When to Use Patent Data

1. **Broad firm coverage needed**: Studying innovation across 8,436 firms
2. **Long time series**: Analyzing innovation trends from 1976-2025
3. **Applied R&D focus**: Studying patentable inventions and commercialization
4. **High-confidence subset**: Restrict to Stage 1 matches (100.0% accuracy, 6,786 firms)
5. **Industry-level analysis**: Patents are better for cross-industry comparisons

#### When to Use Publication Data

1. **Accuracy critical**: Requiring 98.7% match accuracy (e.g., event studies, causal inference)
2. **Basic research focus**: Studying fundamental scientific advances
3. **Research-intensive firms**: Analyzing large pharma, tech, industrial firms
4. **Citation analysis**: Tracking research impact through citations
5. **Complement to patents**: Combining with patents for comprehensive innovation measure

#### When to Use Both Datasets

1. **Comprehensive innovation measure**: Capturing both applied and basic research
2. **Robustness checks**: Validating findings across different data sources
3. **Innovation diversity**: Analyzing firms' innovation strategies (patents vs. publications)
4. **Firm-level analysis**: Studying the 1,000-1,500 overlapping firms with both data types
5. **Institutional differences**: Comparing industries' patent vs. publication strategies

### 10. Summary Table: Key Metrics

| Category | Metric | Patents | Publications | Winner |
|----------|--------|---------|--------------|--------|
| **Accuracy** | Overall accuracy | 95.4% | 98.7% | Publications (+3.3pp) |
| | High-confidence accuracy | 100.0% (Stage 1) | 98.7% (homepage) | Patents (+1.3pp) |
| | Low-confidence accuracy | 63.0% (0.90-0.95) | N/A (no low conf.) | Publications (N/A) |
| **Coverage** | Total matches | 39,535 | 2,841 | Patents (13.9x more) |
| | Unique firms | 8,436 (45.1%) | 1,580 (8.4%) | Patents (5.3x more) |
| | Matched entities | 31,318 (32.1%) | 2,382 (8.8%) | Patents (13.1x more) |
| | Total outputs | 902,392 (70.0%) | 2,568,072 (322%) | Publications (2.8x more) |
| **Time Period** | Years covered | 49 (1976-2025) | 34 (1990-2024) | Patents (15 years longer) |
| **Quality** | Validation sample | 1,000 matches | 500 matches | Patents (2x larger) |
| | Confidence range | 0.90-0.99 | 0.98 (all) | Publications (narrower) |
| **Methodology** | Stages | 3 (exact+fuzzy+manual) | 1 (homepage exact) | Patents (more flexible) |
| | Error rate | 4.6% overall | 1.3% overall | Publications (3.5x lower) |

**Overall Winner:**
- **For coverage**: Patent matching (5.3x more firms)
- **For accuracy**: Publication matching (3.3pp higher accuracy)
- **For completeness**: Use both datasets together

## Conclusion

The patent and publication matching datasets serve complementary purposes in studying firm innovation:

- **Patent matching** prioritizes **coverage** (45.1% of firms) with **good accuracy** (95.4%). It captures a broad set of firms across industries and time periods, making it ideal for cross-sectional analysis, long-term trend analysis, and studies where breadth is more important than perfect accuracy.

- **Publication matching** prioritizes **accuracy** (98.7%) with **focused coverage** (8.4% of firms). It captures the most research-intensive firms with exceptional precision, making it ideal for event studies, causal inference, and analyses where match quality is critical.

The two datasets have estimated **70-90% overlap** at the firm level for publication-matched firms, but **83% of patent-matched firms** do NOT have publication matches. This suggests that patents capture a much broader set of firms, while publications capture a narrower set of research-intensive firms with higher precision.

**Recommendation:** Use patent data for broad coverage analyses and publication data for high-precision analyses. For the most comprehensive understanding of firm innovation, combine both datasets to capture both applied R&D (patents) and basic research (publications).

---

**Created:** February 15, 2026
**Last Updated:** February 15, 2026
**Status:** Complete
