# OpenAlex Concepts API Analysis

## API Endpoint
**URL:** https://api.openalex.org/concepts

## What the API Provides

### 1. **Complete Concept Taxonomy**
- **Total concepts:** 65,026 concepts in the OpenAlex taxonomy
- **Hierarchical structure:** Concepts have levels (0-5), where:
  - Level 0 = Most general (e.g., "Computer science", "Medicine", "Physics")
  - Level 1 = More specific (e.g., "Artificial intelligence", "Humanities")
  - Level 2-5 = Increasingly specific sub-concepts

### 2. **Concept Object Structure**
Each concept returned by the API contains:

```json
{
  "id": "https://openalex.org/C154945302",  // Full concept ID URL
  "display_name": "Artificial intelligence",  // Human-readable name
  "level": 1,  // Hierarchy level (0-5)
  "description": "field of computer science...",  // Concept description
  "works_count": 27327665,  // Number of papers with this concept
  "cited_by_count": 79711244,  // Total citations
  "wikidata": "https://www.wikidata.org/wiki/Q11660",  // Wikidata link
  "wikipedia": "https://en.wikipedia.org/wiki/artificial_intelligence",
  "works_api_url": "https://api.openalex.org/works?filter=concepts.id:154945302",
  "ancestors": null,  // Parent concepts (may need special request)
  "related_concepts": null  // Related concepts (may need special request)
}
```

### 3. **Key Features for AI Paper Filtering**

#### A. **Direct Concept ID Filtering**
The `works_api_url` shows the exact filter syntax:
```
filter=concepts.id:154945302
```
This allows you to directly filter papers by concept ID with **high precision**.

#### B. **Search Functionality**
You can search for concepts by name:
```
https://api.openalex.org/concepts?filter=display_name.search:artificial%20intelligence
```
This returns all concepts matching the search term, including:
- Main concept: "Artificial intelligence" (C154945302)
- Sub-concepts: "Applications of artificial intelligence" (C157170001)
- Related: "Artificial general intelligence" (C162027153)
- etc.

#### C. **Concept Hierarchy Levels**
- **Level 0:** Broad fields (Computer science: C41008148)
- **Level 1:** Major subfields (Artificial intelligence: C154945302)
- **Level 2-5:** Specific topics and techniques

### 4. **AI-Related Concepts Found**

From the API search for "artificial intelligence", we found:

1. **C154945302** - "Artificial intelligence" (Level 1)
   - 27,327,665 works
   - Main AI concept

2. **C157170001** - "Applications of artificial intelligence" (Level 2)
   - 143,352 works

3. **C176777502** - "Anticipation (artificial intelligence)" (Level 2)
   - 54,902 works

4. **C162027153** - "Artificial general intelligence" (Level 2)
   - 8,815 works

5. **C30112582** - "Artificial Intelligence System" (Level 2)
   - 11,678 works

6. **C207453521** - "Artificial intelligence, situated approach" (Level 2)
   - 7,701 works

7. **C26205005** - "Symbolic artificial intelligence" (Level 3)
   - 4,399 works

8. **C44464901** - "Marketing and artificial intelligence" (Level 3)
   - 11,044 works

9. **C91557362** - "Music and artificial intelligence" (Level 2)
   - 3,715 works

### 5. **How to Use for High-Precision AI Filtering**

#### Step 1: Get All AI-Related Concept IDs
Query the API for AI-related concepts:
```python
# Search for AI concepts
ai_concepts = [
    "artificial intelligence",
    "machine learning", 
    "deep learning",
    "neural network",
    "natural language processing",
    "computer vision",
    "reinforcement learning"
]

# For each term, query:
# https://api.openalex.org/concepts?filter=display_name.search:{term}
```

#### Step 2: Extract Concept IDs
From each search result, extract the concept IDs (the numeric part after "C"):
- `https://openalex.org/C154945302` → `154945302` or `C154945302`

#### Step 3: Filter Your Parquet Files
In your flattened parquet files, check all `concepts_*_id` columns:
```python
# AI concept IDs to check
ai_concept_ids = [
    "154945302",  # Artificial intelligence
    "15744967",   # Machine learning (from known list)
    "27788060",   # Deep learning
    # ... etc
]

# Filter papers
ai_papers = df.filter(
    pl.col("concepts_0_id").is_in(ai_concept_ids) |
    pl.col("concepts_1_id").is_in(ai_concept_ids) |
    # ... check all concept ID columns (0-39)
)
```

### 6. **Advantages of Using Concept IDs**

1. **High Precision:** Concept IDs are unambiguous - no text matching errors
2. **Stable:** IDs don't change, unlike display names which might vary
3. **Comprehensive:** Can find all sub-concepts by querying the API
4. **Hierarchical:** Can use parent concepts (Computer Science) to catch broader AI papers
5. **Official Taxonomy:** Uses OpenAlex's curated classification system

### 7. **API Query Examples**

#### Get all concepts (paginated):
```
GET https://api.openalex.org/concepts?page=1&per_page=200
```

#### Search for specific concept:
```
GET https://api.openalex.org/concepts?filter=display_name.search:machine%20learning
```

#### Get specific concept by ID:
```
GET https://api.openalex.org/concepts/C154945302
```

#### Filter works by concept:
```
GET https://api.openalex.org/works?filter=concepts.id:154945302
```

### 8. **Recommended Approach for Your Use Case**

1. **Query OpenAlex API** to get comprehensive list of AI-related concept IDs:
   - Search for: "artificial intelligence", "machine learning", "deep learning", etc.
   - Extract all concept IDs from results
   - Include parent concepts (e.g., Computer Science: C41008148) if you want broader coverage

2. **Build Concept ID List:**
   - Create a JSON file with all AI-related concept IDs
   - Include concept name, level, and works_count for reference

3. **Filter Your Data:**
   - Check all `concepts_*_id` columns (0-39) in your flattened parquet files
   - Match against your AI concept ID list
   - This gives you **high precision** filtering

4. **Optional: Use Hierarchy:**
   - If a paper has Computer Science (C41008148) as a concept, you might want to include it
   - Or be more strict and only include direct AI concepts

### 9. **Known AI Concept IDs (from API and research)**

Based on the API responses and common knowledge:

- **C154945302** - Artificial intelligence (Level 1, 27M works)
- **C15744967** - Machine learning (Level 1, estimated)
- **C27788060** - Deep learning (Level 2, estimated)
- **C121332964** - Neural networks (Level 2, estimated)
- **C119362028** - Natural language processing (Level 2, estimated)
- **C185592680** - Computer vision (Level 1, from API results)
- **C127313188** - Reinforcement learning (Level 2, estimated)

### 10. **Next Steps**

1. Query the OpenAlex API for all AI-related concepts
2. Build a comprehensive list of AI concept IDs
3. Use these IDs to filter your parquet files with high precision
4. This approach is much more reliable than text-based filtering

## Summary

The OpenAlex Concepts API provides:
- ✅ **65,026 concepts** in a hierarchical taxonomy
- ✅ **Stable concept IDs** for precise filtering
- ✅ **Search functionality** to find AI-related concepts
- ✅ **Direct filtering syntax** (`filter=concepts.id:154945302`)
- ✅ **Works count** to understand concept popularity
- ✅ **Hierarchical levels** to understand concept relationships

**This is the ideal approach for high-precision AI paper filtering** - much better than text-based searches on display names!
