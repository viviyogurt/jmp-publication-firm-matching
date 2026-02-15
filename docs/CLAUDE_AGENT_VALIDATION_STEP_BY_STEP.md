# Step-by-Step Validation Prompt for Claude Agent

## Complete Prompt to Copy-Paste

```
You are validating patent assignee-to-firm matches for a research project. I need you to check 1,000 matches and determine if each assignee correctly corresponds to its matched CRSP/Compustat firm.

CSV FILE LOCATION: /home/kurtluo/yannan/jmp/data/processed/linking/validation_sample_1000.csv

YOUR TASK:
1. Read the CSV file
2. For each row (1-1000), validate the match between assignee and firm
3. Fill in the 'is_correct' column with: "Yes", "No", or "Uncertain"
4. Optionally add notes in 'validation_notes' column
5. Save the updated CSV

VALIDATION PROCESS FOR EACH ROW:

Step 1: Read the key information
- Firm name: Check 'firm_conm' or 'conm' column
- Assignee name: Check 'assignee_clean_name' column
- Business description: Check 'busdesc' column (very helpful!)
- Match method: Check 'match_method' column
- Confidence: Check 'match_confidence' column

Step 2: Compare names
- Are they the same? → Likely "Yes"
- Is assignee a variation/abbreviation of firm? → Likely "Yes"
- Does assignee contain firm name? (e.g., "Microsoft Research" contains "Microsoft") → Likely "Yes"
- Are they completely different? → Likely "No"
- Are they similar but unclear? → Likely "Uncertain"

Step 3: Check business description
- Does busdesc mention activities related to assignee name?
- Does it align with what the assignee likely does?
- If busdesc supports the match → More likely "Yes"
- If busdesc contradicts → More likely "No"

Step 4: Consider match method
- "exact_conm" or "exact_conml" → Should be obvious match, likely "Yes"
- "ticker_in_name" → Assignee should contain ticker, likely "Yes"
- "firm_contained_conm" → Firm name in assignee, likely "Yes"
- "fuzzy_jw_high" (confidence ≥0.94) → Names very similar, likely "Yes"
- "fuzzy_jw_medium" (confidence 0.90-0.94) → Names similar but check carefully

Step 5: Make decision
- Mark "Yes" if clearly correct
- Mark "No" if clearly incorrect
- Mark "Uncertain" if cannot determine

Step 6: Add notes (optional but helpful)
- "Subsidiary of [firm]"
- "Name variation"
- "Abbreviation for [full name]"
- "False positive - different company"
- "Need more context"

VALIDATION RULES:

✅ Mark "Yes" (Correct) if:
- Names match exactly or closely
- Assignee is subsidiary/division of firm
- Assignee is abbreviation of firm (IBM = International Business Machines)
- Assignee name contains firm name
- Business description aligns with assignee
- Match makes logical sense

❌ Mark "No" (Incorrect) if:
- Assignee is clearly different company
- Names similar but unrelated entities
- Business description contradicts match
- False positive from fuzzy matching

❓ Mark "Uncertain" if:
- Cannot determine with confidence
- Names similar but need more context
- Could be related but not certain

EXAMPLES FROM ACTUAL DATA:

Example 1:
- firm_conm: "SUMMA INDUSTRIES"
- assignee_clean_name: "UMC INDUSTRIES"
- match_method: "fuzzy_jw_medium"
- match_confidence: 0.9
- Analysis: "SUMMA" vs "UMC" - different names, likely "No" or "Uncertain"

Example 2:
- firm_conm: "ERO INC"
- assignee_clean_name: "ERNO"
- match_method: "fuzzy_jw_medium"
- match_confidence: 0.9
- Analysis: "ERO" vs "ERNO" - very similar, check busdesc. If busdesc mentions "ERO" activities, likely "Yes"

Example 3:
- firm_conm: "MICROSOFT CORP"
- assignee_clean_name: "MICROSOFT"
- match_method: "exact_conm"
- match_confidence: 0.98
- Analysis: Exact match, clearly "Yes"

IMPORTANT NOTES:
1. Subsidiaries and divisions are CORRECT matches (e.g., "Microsoft Research" → Microsoft)
2. Name variations are CORRECT if same entity
3. Be conservative - use "Uncertain" when unsure
4. File is sorted by confidence (lowest first) - harder matches come early
5. Focus on accuracy, not speed

OUTPUT:
Please process all 1,000 rows and provide the updated CSV file with:
- 'is_correct' column filled with "Yes", "No", or "Uncertain"
- 'validation_notes' column with brief notes where helpful
- All other columns unchanged

Start with row 1 and work through systematically. Take your time to be accurate.
```

## Quick Reference Card

**For each row, ask yourself:**

1. **Do the names match?**
   - Same → Yes
   - Similar/variation → Check further
   - Different → No

2. **Is assignee a subsidiary?**
   - Contains firm name → Yes
   - Related division → Yes

3. **Does business description help?**
   - Mentions assignee activities → Yes
   - Contradicts → No
   - Unclear → Uncertain

4. **What's the match method?**
   - Exact match → Usually Yes
   - Fuzzy match → Check carefully
   - Ticker match → Usually Yes

5. **What's the confidence?**
   - High (≥0.95) → Should be obvious
   - Medium (0.90-0.95) → Check carefully

**Final decision:**
- Clear match → Yes
- Clear mismatch → No  
- Unclear → Uncertain
