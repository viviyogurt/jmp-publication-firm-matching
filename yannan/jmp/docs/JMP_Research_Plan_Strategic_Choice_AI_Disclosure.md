# JMP Research Plan: Strategic Choice of AI Knowledge Disclosure

**Working Title:** "To Publish or Patent? Strategic AI Knowledge Disclosure in the Age of Artificial Intelligence"

**Author:** [Your Name]
**Date:** February 14, 2025
**Status:** Initial Research Plan

---

## 1. Research Question

### Primary Question
**When do firms choose to disclose AI knowledge through scientific publications versus legal protection through patents, and how does this strategic choice vary systematically across firms and institutional environments?**

### Secondary Questions

1. **Resource Allocation:** How do financial constraints affect the choice between publications (R) and patents (D)?
2. **Institutional Environment:** How does intellectual property protection strength influence disclosure strategy?
3. **Competitive Dynamics:** How does competitive pressure affect the propensity to publish versus patent?
4. **Firm Capability:** How does firm-level innovation capability moderate the R-D relationship?
5. **Market Valuation:** How do financial markets value different disclosure strategies?

### Why This Matters

**Theoretical Contribution:**
- First **discrete choice model** of publication vs. patent strategy (beyond continuous complementarity/substitution)
- Integrates **proprietary cost theory** (trade secrecy, disclosure costs) with **signaling theory** (publications as quality signals)
- Advances **strategic disclosure literature** by modeling R vs. D as explicit strategic choice

**Empirical Contribution:**
- First comprehensive documentation of **AI disclosure strategy patterns** across firms and time
- **Heterogeneous treatment effects** – when do firms choose each strategy?
- **Counterfactual analysis** – what would firms do under different institutional environments?

**Policy Relevance:**
- Inform **IP policy** – how does patent strength affect AI innovation disclosure?
- Inform **innovation policy** – should government subsidize AI research (publications) or applied development (patents)?
- Inform **competition policy** – do patents stifle follow-on innovation in AI?

---

## 2. Identification Strategy

### Challenge: Endogeneity of Strategic Choice

**Problem:** Firms choose publication/patent strategy based on **unobserved factors** (innovation capability, strategic vision, corporate culture)

**Example:** High-quality firms may both publish AND patent (complementarity), but this could reflect:
- **True complementarity:** Publications enhance patents
- **Unobserved capability:** High-quality firms do everything well
- **Omitted variable bias:** β3 biased upward

---

### Solution: Quasi-Experimental Variation

#### Source of Exogenous Variation 1: Institutional Shocks

**Shock:** Court decisions on AI patent eligibility

**Example:** **Alice Corp v. CLS Bank (2014)** - Supreme Court decision abstract ideas not patent-eligible

**First Stage:**
```
Patent_Eligibility_t = α + π×Alice_Decision_2014 + Controls
```

**Effect:** Post-Alice, AI software patents harder to obtain → firms shift toward publications

**Two-Stage Estimation:**
```
Stage 1: Patent_Eligibility_t = f(Alice_Decision, Time_Fixed_Effects)
Stage 2: Strategy_i,t = α + β1×Patent_Eligibility_t × Firm_Characteristics + Controls
```

**Identification:** Changes in strategy after Alice decision attributed to patent eligibility shock (not firm unobservables)

---

#### Source of Exogenous Variation 2: Government Funding Shocks

**Shock:** **DARPA AI funding** increases (exogenous to firm decisions)

**Logic:** DARPA funding targets **applied AI research** (development, not basic research)

**First Stage:**
```
DARPA_Funding_t = α + π×Federal_AI_Budget + Controls
```

**Effect:** Increased DARPA funding → firms shift toward **patent-oriented** strategy (applied work)

**Two-Stage Estimation:**
```
Stage 1: DARPA_Funding_t = f(Federal_Budget, Political_Factors)
Stage 2: Strategy_i,t = α + β1×DARPA_Funding_t × Firm_Characteristics + Controls
```

---

#### Source of Exogenous Variation 3: Competitor Innovation Shocks

**Shock:** **Major competitor AI breakthrough** (exogenous to firm i)

**Logic:** When competitor publishes breakthrough paper or receives major patent, firm i responds strategically

**Difference-in-Differences:**
```
Strategy_i,t = α + β1×Competitor_Innovation_j,t
               + β2×Post_Shock × Treated_Firm
               + μ_i + λ_t + ε
```

**Where:**
- **Treated_Firm:** Firms in same industry as competitor j
- **Control_Firm:** Firms in different industries
- **Post_Shock:** Period after competitor innovation

**Identification:** Compare strategy changes of treated vs. control firms before/after shock

---

### Alternative Identification: Selection Correction (Heckman)

**Approach:** Model selection into strategy explicitly

**Two-Stage Heckman Correction:**
```
Stage 1 (Selection): P(Strategy = s | X) = Logistic(α_s × X)
Stage 2 (Outcome): Innovation = α + β1×Strategy + β2×Inverse_Mills_Ratio + Controls
```

**Where Inverse Mills Ratio:** Controls for selection on unobservables

**Advantage:** Doesn't require exogenous shock (can use observed data)

**Disadvantage:** Requires **exclusion restriction** (variable affecting selection but not outcome)

---

## 3. Expected Effects and Mechanisms

### Primary Mechanisms

#### Mechanism 1: Resource Constraints (Substitution)

**Prediction:** Financially constrained firms face **budget trade-off** between R and D

**Theoretical Logic:**
```
Fixed R&D Budget = R_Spending + D_Spending

If Budget is fixed:
  More R → Less D (substitution along budget constraint)
```

**Expected Effect:**
```
β3 (R×D interaction) × Cash_Holdings < 0
```

**Interpretation:** R and D are substitutes when financially constrained

**Empirical Test:**
```
Strategy_Probability = α + β1×R + β2×D + β3×(R×D×Cash_Constraint) + Controls

Prediction: β3 < 0 for low-cash firms (substitution)
           β3 > 0 for high-cash firms (complementarity)
```

---

#### Mechanism 2: Appropriability Conditions (Strategic Substitution)

**Prediction:** Strong IP protection → patents more valuable → **substitute** publications with patents

**Theoretical Logic:**
```
Strong IP Protection → High Patent Value → Low Publication Value
Weak IP Protection  → Low Patent Value  → High Publication Value (need to signal quality)
```

**Expected Effect:**
```
Strategy = f(IP_Protection, Firm_Type)

Prediction:
  High IP Protection → Patent_Only or Both (patents valuable)
  Low IP Protection  → Publish_Only or Neither (patents not worth cost)
```

**Empirical Test:**
```
P(Patent_Only | IP_Strength) > P(Patent_Only | IP_Weakness)
P(Publish_Only | IP_Strength) < P(Publish_Only | IP_Weakness)
```

**Industry Variation:**
- **High IP:** Pharmaceuticals (strong patent protection) → Patents favored
- **Low IP:** Software (weak patent protection post-Alice) → Publications favored

---

#### Mechanism 3: Signaling Value (Complementarity)

**Prediction:** Publications **signal quality** → enhance patent value → **complementarity**

**Theoretical Logic (Spence 1973):**
```
High-quality firms publish to signal capability
Markets interpret publications as positive signal
Patents from publishing firms viewed as more credible
```

**Expected Effect:**
```
Patent_Value = α + β1×Publications + Controls
Prediction: β1 > 0 (publications increase patent value)
```

**Market Valuation Test:**
```
CAR_Around_Patent_Grant = α + β1×Prior_Publications + Controls
Prediction: β1 > 0 (patents from publishing firms have higher CAR)
```

---

#### Mechanism 4: Capability Thresholds (Non-Monotonic)

**Prediction:** Relationship between R and D is **non-monotonic** in firm capability

**Theoretical Logic:**
```
Low Capability:    Neither R nor D (can't do either)
Medium Capability: D only (can apply, can't discover)
High Capability:  Both R and D (can afford both)
```

**Expected Effect:**
```
Strategy_Probability = α + β1×Capability + β2×Capability^2 + Controls

Prediction for Both:
  β1 > 0 (increasing capability makes both more likely)
  β2 < 0 (concave - only high-capability firms do both)

Prediction for Patent_Only:
  Inverted U - medium capability firms most likely
```

**Graphical Representation:**
```
Probability of "Both" Strategy:
    |
    |       ________
    |      /        \
    |     /          \
    |    /            \
    |___/              \________
    | Capability  Low  High
```

---

### Summary Table of Expected Effects

| Mechanism | Effect | Interaction | Expected Sign |
|-----------|--------|-------------|---------------|
| **Resource Constraints** | Substitution | × Cash Holdings | β3 < 0 for low cash |
| **Appropriability** | Strategic Substitution | × IP Strength | Patent_Only ↑ with IP |
| **Signaling** | Complementarity | × Firm Quality | β3 > 0 for high quality |
| **Capability** | Non-Monotonic | × Capability^2 | β2 < 0 (concave) |
| **Competition** | Ambiguous | × Competition | Either sign |

---

## 4. Primary Specifications

### Specification 1: Discrete Choice Model (Multinomial Logit)

**Model:**
```
P(Strategy_i,t = s) = exp(X_s,i,t'β) / Σ_{k∈{Pub, Pat, Both, Neither}} exp(X_k,i,t'β)
```

**Where:** X_s,i,t includes:
- **Firm characteristics:** Size, age, cash holdings, R&D intensity, leverage
- **Institutional characteristics:** IP protection strength, competition (HHI), technological opportunity
- **Knowledge characteristics:** Publication quality (citations), patent scope (claims)

**Base Category:** Neither (no publications, no patents)

**Key Coefficients:**
- **β_Pub_Only_R&D:** R&D intensity → choose Publish_Only
- **β_Pat_Only_IP:** IP protection → choose Patent_Only
- **β_Both_Capability:** Firm capability → choose Both

**Interpretation:** How firm/industry characteristics affect probability of each strategy

---

### Specification 2: Nested Logit (Structured Choices)

**Model:**
```
P(Strategy) = P(Nest) × P(Strategy | Nest)
```

**Nest Structure:**
```
Nest 1: Disclosure {Publish_Only, Both}
Nest 2: Protection {Patent_Only, Both}
Nest 3: Passive {Neither}
```

**Logic:**
- Firms first choose **whether to disclose** (Nest 1 vs. Nest 2 vs. Nest 3)
- Then choose **disclosure method** (publication vs. patent vs. both)

**Advantage:** Allows correlation between unobserved factors affecting similar choices

---

### Specification 3: Dynamic Panel Model (Transition Probabilities)

**Model:**
```
P(Strategy_i,t = s | Strategy_i,t-1 = s_prev) = Logistic(α_s + γ×X_i,t-1)
```

**Markov Transition Matrix:**
```
        | Pub(t)  Pat(t)  Both(t)  Neither(t)
--------|---------------------------------------
Pub(t-1)|   p_pp   p_pt     p_pb      p_pn
Pat(t-1)|   p_tp   p_pt     p_tb      p_tn
Both(t-1)|  p_bp   p_bt     p_bb      p_bn
Neither(t-1)| p_np   p_nt     p_nb      p_nn
```

**Key Questions:**
1. **State dependence:** Do firms persist in strategies (diagonal dominance)?
2. **Mobility:** How easily do firms transition between strategies?
3. **Absorbing states:** Is "Neither" absorbing (once out, never return)?

---

### Specification 4: Market Valuation (Event Study)

**Model:**
```
CAR_it = α + β1×Publication_Event + β2×Patent_Event
               + β3×(Publication_Event × Firm_Characteristics)
               + β4×(Patent_Event × Firm_Characteristics)
               + Controls + ε
```

**Event Windows:**
- **Publication event:** [-5, +5] days around publication date
- **Patent event:** [-5, +5] days around patent grant date

**Cross-Sectional Tests:**
```
CAR = α + β1×Event_Type + β2×Event_Type × IP_Protection
            + β3×Event_Type × Cash_Holdings + Controls
```

**Predictions:**
- **β2 > 0 for publications:** Stronger IP → publications more valuable (signal稀缺ity)
- **β2 < 0 for patents:** Stronger IP → patents more valuable
- **β3 > 0:** High-cash firms' publications valued more (credibility)

---

### Specification 5: Innovation Production Function

**Model:**
```
Innovation_Output_i,t = α + β1×R_i,t-1 + β2×D_i,t-1 + β3×(R_i,t-1 × D_i,t-1)
                        + β4×(R_i,t-1 × D_i,t-1 × Z_i,t-1)
                        + γ×Controls + μ_i + λ_t + ε
```

**Where Z = moderator (cash holdings, IP protection, capability, competition)**

**Key Coefficient:** β4 (three-way interaction)

**Expected Signs:**
- **β4 < 0 for Z = Cash_Constraint:** Substitution when constrained
- **β4 > 0 for Z = IP_Protection:** Complementarity when IP strong
- **β4 > 0 for Z = Capability:** Complementarity when capability high

**Innovation Output Measures:**
- **Citation-weighted publications:** Quality-adjusted R
- **Citation-weighted patents:** Quality-adjusted D
- **Future innovation:** Publications/patents in t+1, t+2, t+3

---

## 5. Planned Robustness Checks

### Robustness Check 1: Alternative Classification of Strategies

**Current Classification:**
- **Publish_Only:** ≥1 AI publication, 0 AI patents
- **Patent_Only:** 0 AI publications, ≥1 AI patent
- **Both:** ≥1 AI publication, ≥1 AI patent
- **Neither:** 0 AI publications, 0 AI patents

**Alternative Classifications:**
1. **Threshold-based:** Use ≥5 publications or ≥5 patents (higher threshold)
2. **Value-based:** Use citation-weighted counts (quality-adjusted)
3. **Share-based:** Use publication/patent intensity (per employee or per R&D $)
4. **Growth-based:** Use growth rates in publications/patents

**Test:** Re-estimate models with alternative classifications

**Expected:** Results robust to classification changes

---

### Robustness Check 2: Alternative Estimation Methods

**Current Method:** Multinomial logit (MNL)

**Alternatives:**
1. **Nested logit:** Allows correlation within nests (disclosure, protection)
2. **Mixed logit:** Random coefficients (captures heterogeneity)
3. **Conditional logit:** Alternative-specific characteristics
4. **Ordered logit:** If strategies can be ordered by "disclosure intensity"

**Test:** Re-estimate using alternative estimators

**Expected:** Similar qualitative results

---

### Robustness Check 3: Alternative Instrumental Variables

**Current IVs:**
- IV1: Alice Corp decision (IP shock)
- IV2: DARPA funding (applied R shock)
- IV3: Competitor innovation (competitive shock)

**Alternative IVs:**
1. **AI PhD graduations (regional):** Supply shock → affects R (publications)
2. **USPTO fee changes:** Cost shock → affects D (patents)
3. **AI conference location:** Proximity shock → affects R (conference attendance)
4. **State-level IP court decisions:** Institutional shock → affects D

**Test:** Re-estimate using alternative IVs

**Expected:** First-stage F-statistics > 10 (strong instruments)

---

### Robustness Check 4: Sample Selection Bias

**Concern:** Sample includes only AI-active firms (≥1 publication or patent)

**Heckman Two-Stage Correction:**
```
Stage 1: P(Include_in_Sample) = Logistic(α + γ×X)
Stage 2: Strategy_Model = α + β1×Strategy + β2×IMR + Controls
```

**Where IMR = Inverse Mills Ratio (controls for selection)

**Test:** Compare results with and without IMR correction

**Expected:** Minimal bias (if selection on observables only)

---

### Robustness Check 5: Falsification Tests

**Falsification Test 1: Future Strategies**
```
Strategy_i,t = α + β1×Firm_Characteristics_i,t+1 + Controls
```

**Expected:** β1 ≈ 0 (future characteristics shouldn't predict current strategy)

**Falsification Test 2: Placebo Shocks**
```
Strategy_i,t = α + β1×Sham_Shock_t + Controls
```

**Where Sham_Shock = shock in unrelated industry (e.g., pharma AI patents for software firms)

**Expected:** β1 ≈ 0 (no effect of unrelated shocks)

---

### Robustness Check 6: Alternative Model Specifications

**Test 1: Include Firm Fixed Effects**
```
Strategy_i,t = α + β×X_i,t + μ_i + λ_t + ε
```

**Challenge:** Fixed effects with discrete choice model (computationally intensive)

**Solution:** Conditional logit with fixed effects (Chamberlain's estimator)

**Test 2: Dynamic Model (Lagged Dependent Variable)**
```
Strategy_i,t = α + γ×Strategy_i,t-1 + β×X_i,t + μ_i + λ_t + ε
```

**Expected:** γ > 0 (persistence in strategy)

**Test 3: Accelerated Failure Time (AFT) Model**
```
Hazard(Strategy_Change) = h_0(t) × exp(β×X)
```

**Expected:** Financial constraints accelerate strategy change

---

### Robustness Check 7: Subsample Analysis

**Subsample 1: By Industry**
```
Software_Firms: Estimate model separately
Hardware_Firms: Estimate model separately
Service_Firms: Estimate model separately
```

**Expected:** Different coefficients across industries (heterogeneity)

**Subsample 2: By Time Period**
```
Pre-Alice (2010-2013): Estimate model
Post-Alice (2014-2024): Estimate model
```

**Expected:** Patent-oriented strategies decrease post-Alice

**Subsample 3: By Firm Size**
```
Small_Firms: Estimate model
Large_Firms: Estimate model separately
```

**Expected:** Small firms face tighter resource constraints (more substitution)

---

## 6. Discrete Choice Model Details

### Model Framework

**Multinomial Logit (MNL):**

**Utility of strategy s for firm i at time t:**
```
U_{i,t,s} = V_{i,t,s} + ε_{i,t,s}
V_{i,t,s} = X_s,i,t'β_s
```

**Probability:**
```
P(Strategy_i,t = s) = exp(V_{i,t,s}) / Σ_k exp(V_{i,t,k})
```

**Normalization:** Base category = Neither (V_Neither = 0)

---

### Specification of Variables

**Dependent Variable:**
```
Strategy_i,t ∈ {Publish_Only, Patent_Only, Both, Neither}
```

**Independent Variables (X):**

**Firm-Level Variables:**
- **Log(Assets):** Firm size
- **Log(Age):** Firm age
- **Cash/Assets:** Liquidity (financial constraint measure)
- **Debt/Assets:** Leverage
- **R&D/Assets:** R&D intensity
- **Tobin's Q:** Growth opportunities
- **ROA:** Profitability

**Industry-Level Variables:**
- **HHI:** Herfindahl-Hirschman Index (competition)
- **IP_Strength:** Patent enforcement index (industry-level)
- **Tech_Opportunity:** Industry R&D intensity
- **AI_Intensity:** Share of AI patents in industry

**Knowledge-Level Variables:**
- **Avg_Publication_Citations:** Publication quality (if publishing)
- **Avg_Patent_Claims:** Patent scope (if patenting)

---

### Alternative: Nested Logit

**Nest Structure:**
```
Level 1: Choose Nest {Disclosure, Protection, Passive}
Level 2: Choose Strategy within Nest
```

**Utility:**
```
U_s = V_s + ε_s + ε_nest(s)
```

**Probability:**
```
P(Strategy = s) = P(Nest = n) × P(Strategy = s | Nest = n)
                = [Σ_k∈n exp(V_k)]^λ × [exp(V_s) / Σ_k∈n exp(V_k)]
```

**Where λ = inclusion parameter (0 ≤ λ ≤ 1)**
- λ = 1: Independence of Irrelevant Alternatives (IIA) holds (same as MNL)
- λ < 1: Relaxed IIA (correlation within nests)

**Advantage:** Allows correlation between Publish_Only and Both (both involve disclosure)

---

### Alternative: Mixed Logit

**Model:**
```
U_s = X_s'β_s + ε_s
β_s ~ Distribution(μ_s, Σ_s)  (Random coefficients)
```

**Probability:**
```
P(Strategy = s) = ∫ exp(X_s'β_s) / Σ_k exp(X_k'β_k) f(β_s | μ_s, Σ_s) dβ_s
```

**Estimation:** Simulated Maximum Likelihood (draw β_s from distribution, integrate)

**Advantage:** Captures **preference heterogeneity** across firms (not all firms respond same to X)

---

## 7. Theoretical Model

### Setup

**Economic Environment:**
- **Firm i** chooses strategy s ∈ {Publish_Only, Patent_Only, Both, Neither}
- **Strategy determines:** Knowledge disclosure, legal protection, innovation cost
- **Firm maximizes:** Expected profit minus cost

---

### Knowledge Creation Process

**Firm produces innovation with quality θ ~ Uniform[0, 1]**

**Innovation requires two stages:**
1. **Discovery (R):** Basic research (publications)
2. **Development (D):** Applied research (patents)

**Production function:**
```
Innovation_Value = f(θ, R, D) = θ^α × (1 + γ_R×R) × (1 + γ_D×D)
```

**Where:**
- **θ:** Innate innovation quality (unobserved by firm)
- **R:** Basic research investment (publications)
- **D:** Applied research investment (patents)
- **γ_R, γ_D:** Productivity of R and D

---

### Costs

**Publication cost:**
```
C_P(R) = c_P × R^2
```
- **Quadratic:** Increasing marginal cost (harder to make incremental discoveries)
- **c_P:** Publication cost parameter (depends on research capability)

**Patent cost:**
```
C_D(D) = c_D × D^2 + Fee
```
- **c_D:** Development cost parameter
- **Fee:** Patent filing cost (fixed)

---

### Appropriability & Disclosure

**Publication:**
- **Benefit:** Establishes scientific priority (signal quality, attract talent)
- **Cost:** Reveals knowledge → competitors can copy

**Patent:**
- **Benefit:** Legal protection (exclusivity for 20 years)
- **Cost:** Disclosure in patent application (less revealing than publication)
- **Value:** Depends on **appropriability regime** (α)

**Appropriability parameter α ∈ [0, 1]:**
- **α = 1:** Perfect protection (competitors can't copy)
- **α = 0:** No protection (competitors can freely copy)

---

### Payoffs

**Publish_Only:**
```
π_P = Innovation_Value × (1 - α) × Signal_Bonus - C_P(R)
```

**Patent_Only:**
```
π_D = Innovation_Value × α - C_D(D)
```

**Both:**
```
π_PD = Innovation_Value × [α + (1-α)×Signal_Bonus] - C_P(R) - C_D(D)
```

**Neither:**
```
π_N = 0 (No innovation, no cost)
```

**Where Signal_Bonus = boost to innovation value from publication (signaling quality, attracting talent)

---

### Optimal Strategy

**Firm solves:**
```
max_s∈{P,D,PD,N} π_s
```

**Comparative Statics:**

**1. Effect of Appropriability (α):**
```
d(Patent_Only)/dα > 0  (Patent_Only increases with α)
d(Publish_Only)/dα < 0  (Publish_Only decreases with α)
```

**2. Effect of Publication Costs (c_P):**
```
d(Both)/dc_P < 0  (Both decreases with publication cost)
d(Patent_Only)/dc_P > 0  (Patent_Only increases with publication cost)
```

**3. Effect of Financial Constraints:**
```
If Budget < Cost(Publication) + Cost(Patent):
  Choose Publish_Only OR Patent_Only (substitution)
If Budget ≥ Cost(Publication) + Cost(Patent):
  Choose Both (complementarity)
```

---

### Key Predictions

**Prediction 1: High Appropriability → Patent_Only or Both**
```
P(Patent_Only | α_high) > P(Patent_Only | α_low)
P(Both | α_high) > P(Both | α_low)
```

**Prediction 2: Low Appropriability → Publish_Only or Neither**
```
P(Publish_Only | α_high) < P(Publish_Only | α_low)
```

**Prediction 3: Financial Constraints → Substitution**
```
P(Both | Low_Cash) < P(Both | High_Cash)
P(Publish_Only | Low_Cash) > P(Publish_Only | High_Cash)  OR
P(Patent_Only | Low_Cash) > P(Patent_Only | High_Cash)
```

**Prediction 4: Non-Monotonic Capability Relationship**
```
P(Both | Capability) increases then decreases with capability
```

**Graph:**
```
P(Both)
    |       ____
    |      /    \
    |     /      \
    |    /        \______
    |___/                \__
    | Capability    Low  High
```

---

## 8. Data Sources

### Primary Data Sources

#### 1. AI Publications (OpenAlex)

**Why:** Comprehensive, free, real-time, includes citation data

**Fields to Extract:**
- **Publication title, abstract:** Text analysis
- **Publication date:** Event study timing
- **Author affiliations:** Firm matching
- **Citations:** Publication quality
- **Venue:** Conference/journal (quality assessment)

**Query:**
```python
from openalex import Entities

works = Entities.Works().filter(
    title_search="machine learning OR neural networks OR deep learning OR NLP OR computer vision",
    from_publication_year="2010",
    has_affiliation=True
)
```

**Expected Sample:** 50,000-100,000 AI publications (2010-2024)

---

#### 2. AI Patents (USPTO / PatentsView)

**Why:** Patent data, firm matching, legal protection information

**Fields to Extract:**
- **Patent number:** Unique identifier
- **Grant date:** Event study timing
- **Assignee:** Firm name (disambiguated)
- **CPC classification:** Patent class (identify AI patents)
- **Claims:** Patent scope
- **Forward citations:** Patent quality

**Query:**
```python
import patentsview

patents = patentsview.query(
    assignee="FIRM_NAME",
    CPC_classification=["G06N", "G06N3"],  # AI classes
    app_date_gte="2010-01-01"
)
```

**Expected Sample:** 20,000-50,000 AI patents (2010-2024)

---

#### 3. Financial Data (CRSP & Compustat)

**CRSP:**
- **Stock returns:** Event study CAR calculation
- **Market cap:** Firm size
- **Trading volume:** Liquidity

**Compustat:**
- **Total assets:** Firm size
- **R&D expenses:** R&D intensity
- **Cash holdings:** Financial constraint
- **Debt:** Leverage
- **Age:** Firm age
- **Tobin's Q:** Growth opportunities
- **Industry classification:** HHI, competition

**Merge:** Use GVKEY to link CRSP and Compustat

**Sample:** 5,000-10,000 publicly traded firms (2010-2024)

---

#### 4. Institutional Data (External Sources)

**Appropriability Measures:**
- **IP protection strength:** Fraser Institute IP protection index
- **Court cases:** AI patent litigation data (Stanford IP Litigation Clearinghouse)
- **Alice Corp cases:** List of software patent cases post-2014

**Competition Measures:**
- **HHI:** Census Bureau, Compustat segments
- **TNIC:** Hoberg-Phillips text-based network industries
- **Market share:** CRSP market cap / Industry market cap

**Government Funding:**
- **DARPA awards:** DARPA website, contract database
- **NSF AI grants:** NSF award search
- **SBIR/STTR:** Small Business Innovation Research awards

---

### Data Construction Timeline

| Phase | Task | Duration |
|-------|------|----------|
| **1** | Download AI publications (OpenAlex) | 2 weeks |
| **2** | Download AI patents (USPTO/PatentsView) | 2 weeks |
| **3** | Match firms to publications/patents | 3 weeks |
| **4** | Merge with CRSP/Compustat | 1 week |
| **5** | Construct strategy classification | 1 week |
| **6** | Collect institutional data (IP, competition) | 2 weeks |
| **7** | Build final analysis dataset | 2 weeks |

**Total:** 13 weeks (3 months)

---

## 9. Expected Timeline

### Month 1-3: Data Collection and Cleaning
- Download publications, patents, financial data
- Firm matching (author affiliation → GVKEY)
- Construct strategy classifications
- Collect institutional variables

### Month 4-5: Descriptive Analysis
- Summary statistics by strategy
- Strategy transition matrices
- Trend analysis (2010-2024)
- Visualization (heatmaps, network graphs)

### Month 6-7: Model Estimation
- Estimate discrete choice models
- Estimate production functions
- Event studies
- Robustness checks

### Month 8-9: Writing
- Introduction (motivation, question)
- Literature review
- Methodology
- Results
- Conclusion

### Month 10-12: Revisions
- Faculty feedback
- Conference presentations (AFA, AEA, ECMA)
- Refine analysis, add robustness
- Final submission

---

## 10. Potential Contributions

### Theoretical Contribution
- First **strategic choice model** of publication vs. patent (beyond continuous complementarity/substitution)
- Integrates **proprietary cost theory** with **signaling theory** in unified framework
- Shows **context-dependence** of R-D relationship (when complements, when substitutes)

### Empirical Contribution
- First **discrete choice analysis** of AI disclosure strategies
- Documents **heterogeneous treatment effects** (financial constraints, IP protection, capability)
- **Counterfactual analysis** – how would firms respond to policy changes?

### Policy Contribution
- Inform **IP policy** – does stronger patent protection encourage or discourage publications?
- Inform **innovation policy** – should government subsidize AI research (publications) or applied development (patents)?
- Inform **competition policy** – do large firms crowd out small firms from AI innovation?

---

## 11. Risks and Mitigation

### Risk 1: Endogeneity of Strategic Choice

**Concern:** Firms choose strategy based on unobserved factors (innovation capability, strategy)

**Mitigation:**
- **Instrumental variables:** Alice decision, DARPA funding, competitor shocks
- **Selection correction:** Heckman two-step
- **Fixed effects:** Control for time-invariant firm heterogeneity

### Risk 2: Measurement Error in Strategy Classification

**Concern:** Classification into 4 strategies may be noisy (firm misclassification)

**Mitigation:**
- **Alternative classifications:** Test different thresholds
- **Validation:** Manually verify sample of firms
- **Latent class model:** Allow for probabilistic classification

### Risk 3: Limited Time Series (2010-2024)

**Concern:** 14 years may be insufficient to observe long-run dynamics

**Mitigation:**
- **Focus on dynamic aspects:** Transition probabilities, hazard models
- **Use longer sample for validation:** Include pre-2010 data for robustness

### Risk 4: Unobserved AI Capability

**Concern:** Don't observe "true" AI capability (only publications/patents)

**Mitigation:**
- **Proxy measures:** AI talent hiring, AI job postings, AI product launches
- **Control variables:** R&D spending, analyst coverage (information asymmetry)

---

## 12. Next Steps

### Immediate (This Week)
1. **Download OpenAlex data** for AI publications
2. **Download PatentsView data** for AI patents
3. **Merge with Compustat** using GVKEY
4. **Construct strategy classification** (Publish_Only, Patent_Only, Both, Neither)

### Short-Term (Next Month)
1. **Collect institutional data** (IP protection, competition)
2. **Descriptive analysis** of strategy patterns
3. **Estimate preliminary discrete choice models**
4. **Draft slides for faculty feedback**

### Long-Term (Next 3 Months)
1. **Complete robustness checks**
2. **Develop theoretical model** (formalize setup)
3. **Write full paper**
4. **Submit to conference (AFA, AEA, ECMA)**

---

**Status:** Initial Research Plan – Ready for Discussion

**Date:** February 14, 2025

---

**Prepared by:** [Your Name]
**Project:** JMP – AI Strategic Disclosure
**Target Journals:** Journal of Finance, Journal of Financial Economics, Review of Financial Studies, Econometrica, American Economic Review
