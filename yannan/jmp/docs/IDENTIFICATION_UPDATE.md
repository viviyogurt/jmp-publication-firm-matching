# Research Plan Update: Identification Strategy Improvements

## 🔄 What Changed (v1 → v2)

### Main Changes to Identification Strategy

#### Before (v1):
1. ⚠️ **Lagged variables** (t-1) - Very weak, just reduces simultaneity
2. ⚠️ **Weak IVs** (pre-sample TE, industry TE, distance to universities) - Questionable exogeneity
3. ⚠️ **Firm FE** - Helpful but doesn't solve time-varying endogeneity
4. ✅ **CTO DID** - Mentioned but underemphasized

#### After (v2):
1. ✅ **Alice Corp DID** - ⭐ PRIMARY CAUSAL STRATEGY (quasi-experimental)
2. ✅ **Selection model** - Realistic framing (descriptive/predictive, not causal)
3. ✅ **Parallel trends validation** - Testable assumptions, event-study DID
4. ✅ **Heterogeneous treatment effects** - TE × DID interactions

---

## 📊 New Two-Pronged Approach

### **Approach 1: Selection Model** (Descriptive/Predictive) - Primary

**Goal:** Characterize which firms choose which strategies and why

**Method:** Multinomial logit with rich controls
- Firm fixed effects (time-invariant heterogeneity)
- Manager fixed effects (managerial ability)
- Industry × Year FE (industry-specific shocks)
- Controls: size, age, R&D, cash, leverage, competition

**What This Identifies:**
- ✓ **Descriptive patterns:** Which firm types choose which strategies
- ✓ **Predictive relationships:** How characteristics predict strategy choice
- ✓ **Correlational evidence:** TE correlates with strategy (controlling for observables)
- ✓ **Heterogeneous effects:** Different firm types respond differently

**What This Does NOT Identify:**
- ✗ **Causal effects:** Cannot claim "TE causes Both strategy"
- ✗ **Counterfactuals:** Cannot predict "what if firm X switched strategies"

**Hypotheses:**
- H1: Translation Efficiency Hypothesis (Descriptive)
- H2: Resource-Based Hypothesis (Descriptive)
- H3: Strategic Substitution Hypothesis (Descriptive)
- H4: Competitive Dynamics Hypothesis (Descriptive)

---

### **Approach 2: Alice Corp DID** (Causal) - Causal Add-On

**Natural Experiment:** *Alice Corp v. CLS Bank* (2014)
- Weakened patent protection for software and business method inventions
- Exogenous shock to software firms (treatment group)
- Affects non-software firms minimally (control group)

**Key Specification:**
```stata
Disclosure_Ratio_{i,t} = α + β1·(Post_Alice × Software_Firm) + β2·TE_{i,t-1}
                       + β3·(Post_Alice × TE × Software_Firm) + γ·Controls
                       + μ_i + λ_t + ε
```

**What This Identifies:**
- ✓ **Causal effect:** β1 = LATE of patent protection on disclosure strategy
- ✓ **Mechanism:** β3 = Does TE moderate the response (heterogeneous treatment effect)
- ✓ **Dynamic adjustment:** Event-study DID shows transition over time
- ✓ **For marginal firms:** Software firms affected by Alice (local average treatment effect)

**Key Hypotheses (Causal):**
- **H5: Patent Protection Shock Hypothesis** - Weaker IP → substitution toward publications (β1 < 0)
- **H6: Translation Efficiency Amplification** - High TE firms respond more strongly (β3 < 0)
- **H7: Asymmetric Response** - Patenting decreases more than publications increase

**DID Assumptions & Validation:**
- ✓ **Parallel trends:** Testable (pre-trends 2010-2013, event-study DID)
- ✓ **No anticipation:** Testable (γ_{-1}, γ_{-2} = 0)
- ✓ **No simultaneous shocks:** Control with industry×year FE, placebo tests
- ✓ **SUTVA:** Cluster SE at industry level

---

## 🎯 Why This Is Better

### Improvement 1: Realistic About What We Can Identify

**Before:** Overstated causal claims ("TE causes higher value") with weak IVs
**After:** Honest about identification
- Selection model = descriptive/predictive (rich patterns, robust)
- DID = causal (one specific effect: IP regime → disclosure strategy)

### Improvement 2: Stronger Causal Design

**Before:** Questionable IVs (pre-sample TE correlates with firm quality)
**After:** Exogenous shock (Supreme Court decision, plausibly random to firms)

**Why Alice Corp Is Better:**
- ✓ Truly exogenous (court decision, not firm-controlled)
- ✓ Asymmetric treatment (software vs. non-software firms)
- ✓ Clear mechanism (weakened patent protection → substitution)
- ✓ Testable assumptions (parallel trends, no anticipation)
- ✓ Prior literature support (software patents declined post-Alice)

### Improvement 3: More Transparent About Limitations

**Added sections:**
- "What This Approach Identifies" for each approach
- DID assumptions & validation
- Robustness checks for parallel trends
- Discussion of LATE (local average treatment effect)

### Improvement 4: Better Integration

**Combines both approaches:**
1. **Selection model:** Comprehensive description of firm strategies
2. **DID:** Causal evidence on one specific mechanism (IP regime response)
3. **Combined:** "Which firms respond most to Alice?" (selection model predicts DID heterogeneity)

---

## 📊 New Hypotheses Structure

### Descriptive Hypotheses (Selection Model)
- H1: Translation Efficiency (descriptive)
- H2: Resource-Based (size, age, resources)
- H3: Strategic Substitution (constraints)
- H4: Competitive Dynamics (competition)

### Causal Hypotheses (DID)
- H5: Patent Protection Shock (main DID effect)
- H6: TE Amplification (DID × TE interaction)
- H7: Asymmetric Response (pubs vs. pats)

---

## 📈 Expected Effects (Updated)

**Effect 1: TE → Firm Value** (Descriptive, correlational)
- 15-20% higher Tobin's Q for 1 SD higher TE
- **Identification:** Selection model with firm FE (controls for time-invariant heterogeneity)

**Effect 2: "Both" Strategy Premium** (Descriptive, correlational)
- Firms with "Both" strategy have highest value
- **Identification:** Selection model with rich controls

**Effect 3: Alice Corp Response** (Causal - DID) ⭐ NEW
- 5-10 pp increase in disclosure ratio for software firms post-Alice
- Patenting ↓ 10-15%, Publications ↑ 5-10%
- **Identification:** DID with parallel trends (causal, LATE)

**Effect 4: Heterogeneous Response by TE** (Causal - DID) ⭐ NEW
- High TE: 15-20 pp increase in disclosure ratio
- Low TE: 5-10 pp increase in disclosure ratio
- **Identification:** DID × TE interaction (causal)

**Effect 5: Dynamic Evolution** (Descriptive)
- Lifecycle patterns (young → publish, mature → patent/both)
- **Identification:** Markov transition models (descriptive)

---

## 🔬 Why Alice Corp Works So Well

### 1. Exogenous Variation

**Supreme Court decisions are random from firm perspective:**
- Firms cannot influence SCOTUS docket
- Alice Corp case was about business methods, but affected all software
- Timing unexpected (June 2014 ruling)

### 2. Clear Asymmetric Treatment

**Treatment group (affected):**
- Software firms (NAICS 5112, 3341, 5415)
- Firms with high software patent portfolio
- Estimated: ~200-400 firms

**Control group (unaffected):**
- Hardware firms, biotech firms, industrial equipment
- Firms with low software patent portfolio
- Estimated: ~1,000-2,000 firms

### 3. Testable Mechanism

**Predicted response:**
- Patent protection ↓ → Patenting ↓ (cost-benefit changes)
- Publication ↑ (to signal quality when patents less valuable)
- Net effect: Disclosure_Ratio = Pub/(Pub+Pat) ↑

### 4. Parallel Trends Testable

**Pre-Alice (2010-2013):**
- Software and non-software firms should have similar trends
- Test: Event-study DID for k = -5, -4, -3, -2, -1
- Expected: γ_k = 0 (no differential trends pre-shock)

**Post-Alice (2014-2018):**
- Divergence begins (software firms shift toward publications)
- Test: γ_k for k ≥ 0 captures dynamic adjustment

### 5. Rich Heterogeneity Analysis

**TE × DID interaction:**
- High TE firms: Better at using publications as substitutes → larger response
- Low TE firms: Poorer substitutes → smaller response
- Test: β3 (Post_Alice × TE × Software_Firm)

---

## 📝 Practical Implementation

### Data Requirements (Same as Before)

**Software Firm Identification:**
1. NAICS codes (5112, 3341, 5415) - primary method
2. Patent portfolio pre-Alice (software patent share > 20%) - secondary method
3. Firm descriptions (keyword search: "software", "AI", "machine learning") - validation

**Treatment Variable:**
```python
software_firm[i] = 1 if NAICS in {5112, 3341, 5415} or software_patent_share_2013 > 0.2
post_alice[t] = 1 if year >= 2014
```

**DID Estimation:**
```python
import linearmodels as lm

# Main DID specification
model = lm.PanelOLS(
    disclosure_ratio ~ post_alice * software_firm + te +
                    post_alice * te * software_firm +
    controls,
    entity_effects=True,
    time_effects=True
)

result = model.fit(cov_type='clustered', clusters=industry)
```

---

## ✅ Advantages Over Previous Version

| Aspect | v1 (Old) | v2 (New) |
|--------|-----------|-----------|
| **Main approach** | IV strategy (weak) | Selection model (realistic) |
| **Causal claim** | "TE causes value" (weak IVs) | "IP shock → strategy" (DID) |
| **Transparency** | Vague about limitations | Clear about what identifies what |
| **Assumptions** | Weak IV assumptions unclear | DID assumptions testable |
| **Contribution** | Method + theory | Method + theory + quasi-experiment |
| **Publication target** | JFE (maybe) | JFE/RFS (better with DID) |

---

## 🎯 Summary

**What improved:**
1. ✅ **Honest identification:** Selection model (descriptive) + DID (causal)
2. ✅ **Strong causal design:** Alice Corp natural experiment
3. ✅ **Testable assumptions:** Parallel trends, no anticipation, placebo tests
4. ✅ **Heterogeneous effects:** TE × DID interaction (mechanism test)
5. ✅ **Clear contribution:** First quasi-experimental evidence on IP regime → disclosure

**What stayed the same:**
- ✓ Translation efficiency metric (still innovative!)
- ✓ Complementarity vs. substitution framework
- ✓ Dynamic evolution (lifecycle patterns)
- ✓ Rich data (OpenAlex + PatentsView + CRSP-Compustat)

**File locations:**
- **Updated proposal:** `docs/research/research_plan_idea1_enhanced_v2.md`
- **Original proposal:** `docs/research/research_plan_idea1_enhanced.md` (archived)

---

This is a much stronger proposal! The Alice Corp DID gives you a credible causal claim while the selection model provides comprehensive descriptive patterns. Together, they tell a complete story: **"Which firms choose what strategies, and how do they respond when IP protection changes?"**

Target journals will love the quasi-experimental variation! 🎯
