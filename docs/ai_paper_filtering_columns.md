# AI Paper Filtering Columns

## Executive Summary

This document identifies all columns in the flattened parquet files that can be used to filter AI-related papers. The analysis was conducted systematically across 30+ sample files to ensure comprehensive coverage.

**Total columns identified: 148**

## Column Categories by Priority

### CRITICAL COLUMNS (Must Check - Highest Accuracy)

These columns should be checked first for the most accurate AI paper identification:

#### 1. Concept Display Names (40 columns)
- `concepts_0_display_name` through `concepts_39_display_name`
- **Why critical**: Concepts are the most granular classification and directly identify AI-related research areas
- **Found**: 462 unique AI-related concept values

#### 2. Concept IDs (40 columns)
- `concepts_0_id` through `concepts_39_id`
- **Why critical**: Concept IDs allow exact matching against known AI concept IDs (e.g., Machine Learning: C15744967, AI: C154945302)
- **Use case**: Can match against predefined list of AI concept IDs from OpenAlex

#### 3. Primary Topic (2 columns)
- `primary_topic.display_name`
- `primary_topic.id`
- **Why critical**: Primary topic is the main classification of the paper
- **Found**: 201 unique AI-related primary topic values

#### 4. Primary Topic Fields/Domains/Subfields (6 columns)
- `topics_0_display_name`
- `topics_0_field_display_name`
- `topics_0_domain_display_name`
- `topics_0_subfield_display_name`
- `topics_0_field_id`
- `topics_0_domain_id`
- `topics_0_subfield_id`
- `topics_0_id`
- **Why critical**: Field/domain/subfield classifications identify Computer Science and related AI fields
- **Found**: 251 unique AI-related topic values

### IMPORTANT COLUMNS (Should Check - Good Coverage)

These columns provide additional coverage to catch papers that might be missed:

#### 5. Secondary Topics (24 columns)
- `topics_1_display_name`, `topics_2_display_name`, `topics_3_display_name`
- `topics_1_field_display_name`, `topics_2_field_display_name`, `topics_3_field_display_name`
- `topics_1_domain_display_name`, `topics_2_domain_display_name`, `topics_3_domain_display_name`
- `topics_1_subfield_display_name`, `topics_2_subfield_display_name`, `topics_3_subfield_display_name`
- `topics_1_id`, `topics_2_id`, `topics_3_id`
- `topics_1_field_id`, `topics_2_field_id`, `topics_3_field_id`
- `topics_1_domain_id`, `topics_2_domain_id`, `topics_3_domain_id`
- `topics_1_subfield_id`, `topics_2_subfield_id`, `topics_3_subfield_id`
- **Why important**: Papers may have AI as a secondary topic rather than primary

#### 6. Keywords (32 columns)
- `keywords_0_display_name` through `keywords_15_display_name`
- `keywords_0_id` through `keywords_15_id`
- **Why important**: Keywords can catch AI-related papers that might not be classified in topics/concepts
- **Found**: 1,026 unique AI-related keyword values

### SUPPLEMENTARY COLUMNS (Optional - For Edge Cases)

These columns can be used for text-based filtering but are less precise:

#### 7. Text Search Columns (2 columns)
- `title`
- `display_name`
- **Why supplementary**: Text search is less precise but can catch edge cases where classification missed AI content
- **Use case**: Full-text keyword search for AI-related terms

## Complete Column List (148 columns)

### Concepts (80 columns)
1. `concepts_0_display_name`
2. `concepts_0_id`
3. `concepts_1_display_name`
4. `concepts_1_id`
5. `concepts_2_display_name`
6. `concepts_2_id`
7. `concepts_3_display_name`
8. `concepts_3_id`
9. `concepts_4_display_name`
10. `concepts_4_id`
11. `concepts_5_display_name`
12. `concepts_5_id`
13. `concepts_6_display_name`
14. `concepts_6_id`
15. `concepts_7_display_name`
16. `concepts_7_id`
17. `concepts_8_display_name`
18. `concepts_8_id`
19. `concepts_9_display_name`
20. `concepts_9_id`
21. `concepts_10_display_name`
22. `concepts_10_id`
23. `concepts_11_display_name`
24. `concepts_11_id`
25. `concepts_12_display_name`
26. `concepts_12_id`
27. `concepts_13_display_name`
28. `concepts_13_id`
29. `concepts_14_display_name`
30. `concepts_14_id`
31. `concepts_15_display_name`
32. `concepts_15_id`
33. `concepts_16_display_name`
34. `concepts_16_id`
35. `concepts_17_display_name`
36. `concepts_17_id`
37. `concepts_18_display_name`
38. `concepts_18_id`
39. `concepts_19_display_name`
40. `concepts_19_id`
41. `concepts_20_display_name`
42. `concepts_20_id`
43. `concepts_21_display_name`
44. `concepts_21_id`
45. `concepts_22_display_name`
46. `concepts_22_id`
47. `concepts_23_display_name`
48. `concepts_23_id`
49. `concepts_24_display_name`
50. `concepts_24_id`
51. `concepts_25_display_name`
52. `concepts_25_id`
53. `concepts_26_display_name`
54. `concepts_26_id`
55. `concepts_27_display_name`
56. `concepts_27_id`
57. `concepts_28_display_name`
58. `concepts_28_id`
59. `concepts_29_display_name`
60. `concepts_29_id`
61. `concepts_30_display_name`
62. `concepts_30_id`
63. `concepts_31_display_name`
64. `concepts_31_id`
65. `concepts_32_display_name`
66. `concepts_32_id`
67. `concepts_33_display_name`
68. `concepts_33_id`
69. `concepts_34_display_name`
70. `concepts_34_id`
71. `concepts_35_display_name`
72. `concepts_35_id`
73. `concepts_36_display_name`
74. `concepts_36_id`
75. `concepts_37_display_name`
76. `concepts_37_id`
77. `concepts_38_display_name`
78. `concepts_38_id`
79. `concepts_39_display_name`
80. `concepts_39_id`

### Topics - Primary (9 columns)
81. `topics_0_display_name`
82. `topics_0_id`
83. `topics_0_field_display_name`
84. `topics_0_field_id`
85. `topics_0_domain_display_name`
86. `topics_0_domain_id`
87. `topics_0_subfield_display_name`
88. `topics_0_subfield_id`
89. `primary_topic.display_name`
90. `primary_topic.id`

### Topics - Secondary (24 columns)
91. `topics_1_display_name`
92. `topics_1_id`
93. `topics_1_field_display_name`
94. `topics_1_field_id`
95. `topics_1_domain_display_name`
96. `topics_1_domain_id`
97. `topics_1_subfield_display_name`
98. `topics_1_subfield_id`
99. `topics_2_display_name`
100. `topics_2_id`
101. `topics_2_field_display_name`
102. `topics_2_field_id`
103. `topics_2_domain_display_name`
104. `topics_2_domain_id`
105. `topics_2_subfield_display_name`
106. `topics_2_subfield_id`
107. `topics_3_display_name`
108. `topics_3_id`
109. `topics_3_field_display_name`
110. `topics_3_field_id`
111. `topics_3_domain_display_name`
112. `topics_3_domain_id`
113. `topics_3_subfield_display_name`
114. `topics_3_subfield_id`

### Keywords (32 columns)
115. `keywords_0_display_name`
116. `keywords_0_id`
117. `keywords_1_display_name`
118. `keywords_1_id`
119. `keywords_2_display_name`
120. `keywords_2_id`
121. `keywords_3_display_name`
122. `keywords_3_id`
123. `keywords_4_display_name`
124. `keywords_4_id`
125. `keywords_5_display_name`
126. `keywords_5_id`
127. `keywords_6_display_name`
128. `keywords_6_id`
129. `keywords_7_display_name`
130. `keywords_7_id`
131. `keywords_8_display_name`
132. `keywords_8_id`
133. `keywords_9_display_name`
134. `keywords_9_id`
135. `keywords_10_display_name`
136. `keywords_10_id`
137. `keywords_11_display_name`
138. `keywords_11_id`
139. `keywords_12_display_name`
140. `keywords_12_id`
141. `keywords_13_display_name`
142. `keywords_13_id`
143. `keywords_14_display_name`
144. `keywords_14_id`
145. `keywords_15_display_name`
146. `keywords_15_id`

### Text Search (2 columns)
147. `title`
148. `display_name`

## Recommended Filtering Strategy

### Strategy 1: High Precision (Recommended)
Check only critical columns to minimize false positives:
- All `concepts_*_display_name` columns (0-39)
- All `concepts_*_id` columns (0-39)
- `topics_0_display_name`
- `topics_0_field_display_name`
- `topics_0_domain_display_name`
- `primary_topic.display_name`

### Strategy 2: High Recall (Comprehensive)
Check all columns to ensure no AI papers are missed:
- All columns listed above (148 total)

### Strategy 3: Balanced (Recommended for Most Use Cases)
Check critical + important columns:
- All concept columns (80 columns)
- All primary topic columns (9 columns)
- All keyword columns (32 columns)
- Total: 121 columns

## AI-Related Search Terms

Based on analysis of 30+ sample files, papers containing these terms in the identified columns should be considered AI-related:

### Core AI Terms
- artificial intelligence
- machine learning
- deep learning
- neural network
- AI
- ML

### Specific AI Techniques
- natural language processing
- NLP
- computer vision
- reinforcement learning
- supervised learning
- unsupervised learning
- transformer
- BERT
- GPT
- LLM (large language model)
- CNN (convolutional neural network)
- RNN (recurrent neural network)
- LSTM
- GAN (generative adversarial network)

### AI-Related Fields
- computer science
- computing
- information technology
- data science
- algorithm
- pattern recognition
- feature extraction
- data mining

## Known OpenAlex AI Concept IDs

For exact matching using concept IDs:
- Machine Learning: `C15744967`
- Artificial Intelligence: `C154945302`
- Deep Learning: `C27788060`
- Neural Networks: `C121332964`
- Natural Language Processing: `C119362028`
- Computer Vision: `C185592680`
- Reinforcement Learning: `C127313188`

## Statistics from Analysis

- **Files analyzed**: 30+ sample files
- **Unique AI concepts found**: 462
- **Unique AI topics found**: 251
- **Unique AI keywords found**: 1,026
- **Unique AI primary topics found**: 201

## Notes

1. **Concepts are most reliable**: Concept classifications are the most granular and accurate for identifying AI papers.

2. **Check all positions**: AI-related content can appear at any position (0-39 for concepts, 0-15 for keywords), not just position 0.

3. **Field/Domain classification**: Papers in Computer Science fields/domains are more likely to be AI-related, but not all CS papers are AI papers.

4. **Text search limitations**: Title and display_name text search should be used as a supplement, not primary filter, due to potential false positives.

5. **Case sensitivity**: All searches should be case-insensitive to catch variations.

## Implementation Example

```python
import polars as pl

# Load flattened parquet file
df = pl.read_parquet("batch_00000_flatten.parquet")

# AI-related search terms
ai_terms = [
    'artificial intelligence', 'machine learning', 'deep learning',
    'neural network', 'ai', 'ml', 'nlp', 'natural language',
    'computer vision', 'reinforcement learning', 'transformer',
    'bert', 'gpt', 'llm', 'cnn', 'rnn', 'lstm', 'gan'
]

# Check all concept display name columns
concept_cols = [f'concepts_{i}_display_name' for i in range(40)]
ai_mask = pl.lit(False)
for col in concept_cols:
    if col in df.columns:
        ai_mask = ai_mask | df[col].str.to_lowercase().str.contains('|'.join(ai_terms))

# Filter AI papers
ai_papers = df.filter(ai_mask)
```
