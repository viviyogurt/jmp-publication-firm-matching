# Prompt for Claude Agent: Patent-Firm Match Validation

## Task Overview

You are tasked with manually validating 1,000 patent assignee-to-firm matches to determine if each assignee is correctly linked to its corresponding CRSP/Compustat firm. Your goal is to check each match and mark whether it is correct, incorrect, or uncertain.

## Context

This validation is part of a research project matching USPTO patent assignees to CRSP/Compustat public firms. The matches were created using a multi-stage matching algorithm:
- **Stage 1**: Exact name matches, ticker matches, contained name matches (high confidence)
- **Stage 2**: Fuzzy string matching with business description validation (medium confidence)
- **Stage 3**: Manual mappings for known large firms (very high confidence)

## Your Task

For each row in the CSV file (`validation_sample_1000.csv`), you need to:

1. **Review the match** between the assignee and the firm
2. **Determine if the match is correct** by checking:
   - Does the assignee name correspond to the firm name?
   - Is the assignee a subsidiary/division of the firm?
   - Could the assignee be the same entity as the firm (with name variations)?
3. **Mark the validation** in the `is_correct` column
4. **Add notes** if needed in the `validation_notes` column

## Validation Criteria

### Mark as "Yes" (Correct Match) if:
- The assignee name is the same as or clearly corresponds to the firm name
- The assignee is a known subsidiary/division of the firm (e.g., "Microsoft Research" → Microsoft)
- The assignee name is a variation/abbreviation of the firm (e.g., "IBM" → International Business Machines)
- The assignee name contains the firm name (e.g., "Google LLC" → Google/Alphabet)
- The business description aligns with the assignee's activities
- The match makes logical sense given the firm's business

### Mark as "No" (Incorrect Match) if:
- The assignee name is clearly a different company
- The assignee is unrelated to the firm (different industry, different location without explanation)
- The names are similar but refer to different entities (e.g., "Apple Inc" vs "Apple Computer" if they're different companies)
- The match appears to be a false positive from fuzzy matching

### Mark as "Uncertain" if:
- You cannot determine with reasonable confidence whether the match is correct
- The names are very similar but you lack sufficient context
- The assignee might be related but you're not certain (e.g., could be a subsidiary but not sure)
- Additional research would be needed to confirm

## Key Columns to Review

### Firm Information:
- **firm_conm**: Company name from the match
- **conm**: Company name from Compustat (may differ slightly)
- **tic**: Ticker symbol (useful for verification)
- **state, city**: Firm location
- **busdesc**: Business description (very helpful for validation)

### Assignee Information:
- **assignee_clean_name**: Normalized assignee name (this is what was matched)
- **clean_name**: Alternative assignee name (should match assignee_clean_name)
- **patent_count_total**: Total patents for this assignee
- **first_patent_year, last_patent_year**: When this assignee filed patents

### Match Information:
- **match_type**: stage1 (exact), stage2 (fuzzy), or stage3 (manual)
- **match_method**: How the match was made (e.g., "exact_conm", "fuzzy_jw_high", "ticker_in_name")
- **match_confidence**: Confidence score (0.90-0.99, higher is better)

## Validation Process

For each row, follow these steps:

1. **Read the firm name** (firm_conm or conm) and **assignee name** (assignee_clean_name)
2. **Compare them**:
   - Are they the same or clearly related?
   - Check if assignee contains firm name or vice versa
   - Look for abbreviations or variations
3. **Check business description** (busdesc) if available:
   - Does it mention activities related to the assignee name?
   - Does it align with what the assignee likely does?
4. **Consider the match method**:
   - "exact_conm" or "exact_conml" → Should be very clear match
   - "ticker_in_name" → Assignee name should contain the ticker
   - "firm_contained_conm" → Firm name should be in assignee name
   - "fuzzy_jw_high" or "fuzzy_jw_medium" → Names should be similar but may have variations
5. **Check confidence level**:
   - High confidence (≥0.95) → Should be more obvious matches
   - Medium confidence (0.90-0.95) → May require more careful review
6. **Make your determination** and mark accordingly

## Examples

### Example 1: Correct Match
- **firm_conm**: "MICROSOFT CORP"
- **assignee_clean_name**: "MICROSOFT"
- **match_method**: "exact_conm"
- **match_confidence**: 0.98
- **Decision**: **Yes** - Clear exact match

### Example 2: Correct Match (Subsidiary)
- **firm_conm**: "MICROSOFT CORP"
- **assignee_clean_name**: "MICROSOFT RESEARCH"
- **match_method**: "firm_contained_conm"
- **match_confidence**: 0.96
- **Decision**: **Yes** - Microsoft Research is a division of Microsoft

### Example 3: Correct Match (Abbreviation)
- **firm_conm**: "INTERNATIONAL BUSINESS MACHINES"
- **assignee_clean_name**: "IBM"
- **match_method**: "abbreviation_dict"
- **match_confidence**: 0.95
- **Decision**: **Yes** - IBM is the abbreviation for International Business Machines

### Example 4: Incorrect Match
- **firm_conm**: "APPLE INC"
- **assignee_clean_name**: "APPLE COMPUTER" (if this is a different, unrelated company)
- **match_method**: "fuzzy_jw_medium"
- **match_confidence**: 0.92
- **Decision**: **No** - If Apple Computer is a different company, this is incorrect

### Example 5: Uncertain Match
- **firm_conm**: "GENERAL TECHNOLOGIES INC"
- **assignee_clean_name**: "GEN TECH"
- **match_method**: "fuzzy_jw_medium"
- **match_confidence**: 0.91
- **busdesc**: Not available or unclear
- **Decision**: **Uncertain** - Could be correct but need more context

## Notes Guidelines

In the `validation_notes` column, add brief notes for:
- **Subsidiaries**: "Subsidiary of [firm name]"
- **Name changes**: "Firm changed name from X to Y"
- **Abbreviations**: "Abbreviation for [full name]"
- **False positives**: Brief explanation of why it's wrong
- **Uncertain cases**: What additional information would help

## Important Considerations

1. **Name Variations**: Companies often have multiple name variations. Be flexible but careful.
2. **Subsidiaries**: Many assignees are subsidiaries or divisions. These are CORRECT matches.
3. **Historical Names**: Firms change names over time. If the assignee uses an old name, it may still be correct.
4. **Fuzzy Matches**: Stage 2 matches may have slight spelling differences but should still be the same entity.
5. **When in Doubt**: Mark as "Uncertain" rather than guessing.

## Output Format

For each row, you should:
1. Update the `is_correct` column with: "Yes", "No", or "Uncertain"
2. Optionally add notes in `validation_notes` column
3. Optionally add your name in `reviewer_name` column (if you want to track who validated)

## Quality Standards

- **Be thorough**: Don't rush through matches
- **Be consistent**: Apply the same criteria throughout
- **Be conservative**: When uncertain, mark "Uncertain" rather than guessing
- **Document edge cases**: Add notes for unusual situations

## Starting the Validation

Begin with row 1 and work through systematically. The file is sorted by confidence (lowest first), so you'll see the most challenging matches early. Take your time with each match - accuracy is more important than speed.

Good luck with the validation!
