# AI Concepts Extraction - Precision & Recall Evaluation

## Summary

**Date:** 2026-01-27  
**Method:** Comprehensive ACM CCS Taxonomy + Exclusion Rules  
**Source:** OpenAlex concepts dump (64,988 concepts)

## Results

### Overall Statistics
- **Total AI concepts found:** 212
- **Taxonomy size:** 490 concepts
- **Known AI concept IDs:** 18
- **Known concepts found:** 17/18 (94.44% recall)

### Precision Metrics

| Metric | Count | Percentage |
|--------|-------|------------|
| **Definitely AI** | 55 | 25.94% |
| **Probably AI (AI-adjacent)** | 9 | 4.25% |
| **Questionable** | 139 | 65.57% |
| **False Positives** | 9 | 4.25% |

**Precision Estimates:**
- **Conservative (definitely AI only):** 25.94%
- **Moderate (definitely + probably AI):** 30.19%
- **Optimistic (all except FPs):** 95.75%

### Recall Metrics
- **Recall on known AI concepts:** 94.44% (17/18)
- **Missing known concept:** 1 (likely Computer Science parent, which is intentionally excluded as too broad)

## Concept Categories

### ✅ Definitely AI (55 concepts)
Core AI concepts including:
- Artificial intelligence
- Machine learning
- Deep learning
- Neural networks (various types)
- Computer vision
- Natural language processing
- Robotics
- Expert systems
- Knowledge representation
- Automated planning
- Multi-agent systems
- Transformers, GANs, etc.

### ⚠️ Probably AI (9 concepts)
AI-adjacent concepts that are commonly used in AI:
- Logistic regression (used in ML)
- Linear regression (used in ML)
- Search algorithm (heuristic search is AI)
- A* search algorithm (AI search algorithm)
- Algorithmic trading (AI for finance)
- Bioinformatics (often uses AI)
- Medical imaging (often uses AI/ML)
- Image processing (could be CV-related)
- Discourse analysis (NLP-related)
- Drug discovery (often uses AI)
- Negotiation (multi-agent systems)

### ❌ False Positives (9 concepts)
Concepts that are not AI-related:
1. Algorithm (too general)
2. Programming language (too general)
3. C programming language (not AI)
4. Radiology (medical, not necessarily AI)
5. Computational chemistry (not necessarily AI)
6. Molecular dynamics (not necessarily AI)
7. Climate model (not necessarily AI)

### ❓ Questionable (139 concepts)
Concepts that might be AI-related but need manual review:
- Various statistical methods
- General computer science concepts
- Domain-specific applications that may or may not use AI

## Improvements Needed

1. **Better exclusion rules:** Need to exclude overly general terms like "Algorithm", "Programming language"
2. **Context-aware matching:** Some concepts like "Image processing" could be AI-related in CV context, but not always
3. **Domain filtering:** Concepts like "Computational chemistry" and "Molecular dynamics" should be excluded unless explicitly AI-related
4. **Manual review:** The 139 questionable concepts need manual review to determine if they're AI-related

## Recommendations

1. **For high precision filtering:** Use only the 55 "definitely AI" concepts
2. **For balanced filtering:** Use 55 + 9 = 64 concepts (definitely + probably AI)
3. **For comprehensive filtering:** Use all 212 concepts but be aware of potential false positives

## Next Steps

1. Manually review the 139 questionable concepts
2. Add more exclusion rules for general CS concepts
3. Consider using concept hierarchy (ancestors) if available in future dumps
4. Validate against a ground truth dataset of AI papers
