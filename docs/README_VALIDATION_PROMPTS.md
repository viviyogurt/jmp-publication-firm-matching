# Validation Prompts for Claude Agent

## Overview

Three prompt files have been created to help a Claude agent validate the 1,000 patent-firm matches. Choose the one that best fits your needs.

## Available Prompts

### 1. **CLAUDE_AGENT_FINAL_PROMPT.txt** (Recommended)
- **Size**: 3.5KB
- **Best for**: Direct copy-paste to Claude agent
- **Format**: Concise, actionable instructions
- **Use this if**: You want a ready-to-use prompt with clear validation rules

### 2. **CLAUDE_AGENT_PROMPT_DIRECT.txt**
- **Size**: 2.3KB  
- **Best for**: Quick validation task
- **Format**: Very concise, essential information only
- **Use this if**: You want the shortest, most direct prompt

### 3. **CLAUDE_AGENT_VALIDATION_STEP_BY_STEP.md**
- **Size**: 4.8KB
- **Best for**: Detailed instructions with examples
- **Format**: Comprehensive guide with step-by-step process
- **Use this if**: You want detailed guidance and examples

### 4. **claude_agent_validation_prompt.md**
- **Size**: Larger, comprehensive
- **Best for**: Full documentation and reference
- **Format**: Complete guide with all details
- **Use this if**: You need complete documentation

## Recommended Usage

**For Claude Agent**: Use `CLAUDE_AGENT_FINAL_PROMPT.txt`

This prompt includes:
- Clear task description
- Validation criteria (Yes/No/Uncertain)
- Step-by-step validation process
- Examples from actual data
- Important notes about subsidiaries and name variations

## How to Use

1. **Open the prompt file**: `docs/CLAUDE_AGENT_FINAL_PROMPT.txt`
2. **Copy the entire content** (between the === markers)
3. **Paste to your Claude agent** along with:
   - The CSV file path: `/home/kurtluo/yannan/jmp/data/processed/linking/validation_sample_1000.csv`
   - Or attach the CSV file directly
4. **Let Claude process** all 1,000 rows
5. **Get the updated CSV** with `is_correct` column filled in
6. **Run analysis**: Use `src/02_linking/analyze_validation_results.py` to calculate accuracy

## Validation Sample File

**Location**: `data/processed/linking/validation_sample_1000.csv`

**Structure**:
- 1,000 rows (plus header)
- 23 columns including firm info, assignee info, match info
- Sorted by confidence (lowest first) - harder matches come early

**Key columns to validate**:
- `firm_conm` / `conm`: Firm company name
- `assignee_clean_name`: Assignee name that was matched
- `busdesc`: Business description (very helpful!)
- `match_method`: How match was made
- `match_confidence`: 0.90-0.99

**Columns to fill**:
- `is_correct`: "Yes", "No", or "Uncertain"
- `validation_notes`: Optional notes
- `reviewer_name`: Optional

## After Validation

Once Claude completes the validation:

1. **Save the updated CSV** (with `is_correct` filled in)
2. **Run analysis script**:
   ```bash
   python src/02_linking/analyze_validation_results.py
   ```
3. **Review the accuracy report**: `data/processed/linking/validation_accuracy_report.json`

## Expected Results

Based on our estimated accuracy:
- **Overall accuracy**: ~97.6% (target: >95%)
- **Stage 1 matches**: ~98% accuracy
- **Stage 2 matches**: ~94.7% accuracy

The validation will confirm or adjust these estimates.
