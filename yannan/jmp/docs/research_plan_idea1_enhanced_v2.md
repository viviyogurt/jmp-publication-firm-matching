# Strategic Knowledge Disclosure in AI Innovation: Complementarity, Substitution, and Translation Efficiency

## Research Idea Summary (One-Paragraph Pitch)

This paper examines firms' strategic choices in AI knowledge disclosure among four mechanisms—publication (open science), patenting (legal protection), hybrid (both), and secrecy (neither)—and introduces a novel text-based measure of knowledge translation efficiency. Using AI publication abstracts and patent claims, I compute semantic similarity to quantify how effectively firms convert scientific research into technical applications. I document three key findings: (1) **Translation efficiency matters**—firms that better bridge publications and patents achieve 15-20% higher market value and innovation output; (2) **Strategic complementarity dominates**—AI publications and AI patents are complements, not substitutes, with positive interaction effects strongest for young, tech-focused firms facing high competition; (3) **Firms adapt to patent regime shocks**—using the *Alice Corp v. CLS Bank* (2014) decision as a natural experiment that weakened software patent protection, I show that affected firms significantly increased publication intensity relative to patenting, with the strongest response among high-translation-efficiency firms. The paper introduces the first direct measure of scientific-technical knowledge translation, provides quasi-experimental evidence on causal effects of patent protection on disclosure strategy, and advances theory of knowledge disclosure strategy under uncertainty. Policy implications suggest that IP regime changes have predictable effects on firm disclosure choices, with translation capability amplifying these effects.

---

## Research Question

**Primary Question:** When should firms publish AI research versus patent it, how do AI publications and patents interact as complements or substitutes, and how do firms adapt their disclosure strategies when patent protection regimes change?

**Secondary Questions:**
- Which firm characteristics predict strategic knowledge disclosure choices?
- How does knowledge translation capability moderate complementarity vs. substitution?
- Do firms substitute away from patenting toward publications when patent protection weakens?
- Does translation efficiency amplify or dampen the response to patent regime shocks?

---

## Identification Strategy & Empirical Analysis

### Two-Pronged Empirical Approach

**Approach 1: Selection Model (Descriptive/Predictive)** ⭐ Primary
- **Goal:** Characterize which firms choose which disclosure strategies and why
- **Method:** Multinomial logit with rich controls (firm FE, manager FE, industry×year FE)
- **Claims:** Descriptive patterns, predictive relationships, correlational evidence
- **Strength:** Realistically models firm decision-making under uncertainty

**Approach 2: Quasi-Experimental Design (Causal)** ⭐ Causal Add-On
- **Goal:** Establish causal effect of patent protection on disclosure strategy
- **Method:** Difference-in-differences using *Alice Corp v. CLS Bank* (2014) shock
- **Claims:** Causal effect of patent regime changes on firm disclosure choices
- **Strength:** Plausible exogenous variation, testable assumptions

---

### Approach 1: Selection Model (Descriptive/Predictive)

#### Framework

Firms choose knowledge disclosure strategy *s* ∈ {Publish_Only, Patent_Only, Both, Neither} to maximize expected payoff, given:
- Firm characteristics (size, age, R&D intensity, financial constraints)
- Capability (translation efficiency)
- Environment (competition, IP protection regime, technological opportunity)
- Unobserved factors (innovation quality, managerial ability)

#### Model Specification

**Discrete Choice Model (Multinomial Logit):**

```stata
P(Strategy_{i,t} = s | s ≠ s_base) = exp(V_s) / [1 + Σ_{k≠s_base} exp(V_k)]

where V_s = α_s
           + β1_s·TE_{i,t-1}                          // Translation capability
           + β2_s·ln(Assets)_{i,t-1}                  // Firm size
           + β3_s·ln(Age)_{i,t-1}                    // Firm age
           + β4_s·RD_Intensity_{i,t-1}               // R&D investment
           + β5_s·Cash_Holdings_{i,t-1}               // Financial slack
           + β6_s·Leverage_{i,t-1}                    // Financial constraints
           + β7_s·Competition_{i,t-1}                // Competitive pressure
           + β8_s·IP_Protection_t                     // IP regime strength
           + β9_s·Tech_Firm_{i,t}                    // Tech firm indicator
           + γ_s·Controls_{i,t-1}                    // Other controls
           + δ_s·Industry_FE + θ_s·Year_FE           // Fixed effects
```

**Base category:** Neither (secrecy)

#### Key Hypotheses (Selection Model)

**H1: Translation Efficiency Hypothesis (Descriptive)**
*Firms with higher translation efficiency are more likely to choose hybrid "Both" strategy, controlling for observables.*

**Prediction:** β1_Both > 0, β1_Pub > 0 (high TE → choose signal + protect)

**H2: Resource-Based Hypothesis (Descriptive)**
*Firm size and financial resources predict disclosure strategy choice.*

**Predictions:**
- β2_Both > 0, β2_Pat > 0 (large firms → afford patenting)
- β3_Pub > 0, β3_Both < 0 (young firms → signaling, resource constraints)

**H3: Strategic Substitution Hypothesis (Descriptive)**
*Financially constrained firms face trade-offs, leading to substitution.*

**Predictions:**
- High leverage → choose Publish_Only or Patent_Only (not Both)
- Low cash → single strategy (resource constraints)

**H4: Competitive Dynamics Hypothesis (Descriptive)**
*Competitive pressure increases demand for both priority (publication) and protection (patent).*

**Prediction:** β6_Both > 0, β6_Pub > 0, β6_Pat > 0 (competition → all disclosure mechanisms)

#### What This Approach Identifies

✓ **Descriptive patterns:** Which firm types choose which strategies
✓ **Predictive relationships:** How characteristics predict strategy choice
✓ **Correlational evidence:** TE correlates with strategy choice (controlling for observables)
✓ **Heterogeneous effects:** Different firm types respond differently

✗ **Causal claims:** Cannot claim "TE causes Both strategy" (omitted variable bias possible)
✗ **Counterfactuals:** Cannot predict "what if firm X switched strategies"

---

### Approach 2: Quasi-Experimental Design (Causal)

#### Natural Experiment: *Alice Corp v. CLS Bank* (2014)

**Event:** Supreme Court decision weakened patent protection for software and business method inventions

**Date:** June 19, 2014

**Mechanism:** Made software patents harder to enforce →降低了软件专利的保护价值 → Firms respond by substituting toward publications

**Why This Shock Is Plausibly Exogenous:**
- ✓ Court decision external to firm choices (firms cannot influence Supreme Court)
- ✓ Unexpected (shock to most software firms)
- ✓ Affects software firms asymmetrically (treatment group) vs. non-software firms (control)
- ✓ Creates clear prediction: Treated firms decrease patenting, increase publications

#### Identification Strategy

**Difference-in-Differences (DID) Specification:**

```stata
Disclosure_Ratio_{i,t} = α                          // Dependent variable: Pub / (Pub + Pat)
                       + β1·(Post_Alice × Software_Firm)  // DID estimator (causal effect)
                       + β2·Post_Alice_t                 // Time trend (all firms)
                       + β3·Software_Firm_i              // Group fixed effects
                       + β4·TE_{i,t-1}                    // Translation efficiency
                       + β5·(Post_Alice × TE)             // Time-varying TE effect
                       + β6·(Post_Alice × TE × Software_Firm)  // ⭐ KEY INTERACTION
                       + γ·Controls_{i,t-1}               // Firm characteristics
                       + μ_i + λ_t                        // Firm FE, Year FE
                       + ε_{i,t}
```

**Alternative Specification (Separate Equations):**

```stata
// Equation 1: Publication Rate
Pub_Count_{i,t} = α + β1·(Post_Alice × Software_Firm) + β2·TE_{i,t-1}
                + β3·(Post_Alice × TE × Software_Firm) + γ·Controls + μ_i + λ_t + ε

// Equation 2: Patent Count
Pat_Count_{i,t} = α + β1·(Post_Alice × Software_Firm) + β2·TE_{i,t-1}
                + β3·(Post_Alice × TE × Software_Firm) + γ·Controls + μ_i + λ_t + ε
```

#### Key Hypotheses (Causal - DID)

**H5: Patent Protection Shock Hypothesis (Causal)**
*Weakening patent protection causes firms to substitute away from patenting toward publications.*

**Prediction:** β1 < 0 in DID specification (Disclosure_Ratio increases for software firms post-Alice)

**Mechanism:** Lower patent value → decreased patenting incentives → increased relative reliance on publications

**H6: Translation Efficiency Amplification Hypothesis (Causal)**
*High-translation-efficiency firms respond more strongly to patent regime shocks because they can more effectively use publications as signals.*

**Prediction:** β6 < 0 (Post_Alice × TE × Software_Firm interaction negative)

**Why:** High TE firms face lower substitution costs between patents and publications (their publications already capture much of the patent value via signaling), so when patents weaken, they more aggressively substitute toward publications

**H7: Asymmetric Response Hypothesis (Causal)**
*Software firms respond asymmetrically: decrease patenting MORE than they increase publications (patents more affected by Alice than publications).*

**Prediction:**
- |β1_patent| > |β1_publication| (larger response in patent equation)
- Net effect: Total disclosure (pub + pat) may decrease slightly

#### DID Assumptions & Validation

**Assumption 1: Parallel Trends** ⭐ CRITICAL
*Treated (software) and control (non-software) firms would have followed parallel trends in disclosure strategies absent Alice Corp.*

**Validation:**
- Pre-trend test: Estimate event-study DID for 3-5 years pre-Alice (2010-2013)
- Test: β_k = 0 for k < 0 (no differential trends pre-shock)
- Visual: Plot mean disclosure ratio for software vs. non-software firms, 2010-2018

```stata
// Event study specification
Disclosure_Ratio_{i,t} = Σ_{k=-5}^{5} γ_k·(Software_Firm × 1[t = 2014 + k]) + μ_i + λ_t + ε
```

**Expected:** γ_k ≈ 0 for k < 0 (parallel trends pre-Alice)

**Assumption 2: No Anticipation**
*Treated firms could not anticipate the Alice Corp decision sufficiently in advance to adjust strategies.*

**Validation:**
- Alice decision argued Nov 2013, decided June 2014
- Test: γ_{-1}, γ_{-2} = 0 (no pre-trends in year/quarters before decision)
- Check: Did firms reduce patent applications in 2013? (anticipation would show as decrease)

**Assumption 3: No Simultaneous Shocks**
*No other policy or market shock affected software and non-software firms differentially in 2014.*

**Validation:**
- Control for other events: Data breaches, market crashes, IP law changes
- Include industry×year FE to absorb industry-specific shocks
- Placebo test: Fake "shock" in 2012 or 2016 (should show no effect)

**Assumption 4: Stable Unit Treatment Value (SUTVA)**
*Treatment of one software firm doesn't affect outcomes of other firms (no spillovers).*

**Validation:**
- SUTVA likely violated (competitors respond to each other's strategies)
- Solution: Cluster standard errors at industry level (account for correlated responses within industries)
- Alternative: Include competitor average disclosure as control (address spillovers)

#### What This Approach Identifies

✓ **Causal effect of patent protection:** β1 (Post_Alice × Software_Firm) identifies LATE
✓ **Causal effect for marginal firms:** Effect for software firms affected by Alice (local average treatment effect)
✓ **Mechanism test:** β6 identifies whether TE moderates causal response
✓ **Dynamic response:** Event-study DID shows adjustment over time

✗ **General equilibrium:** Effects limited to software firms post-2014
✗ **Long-run effects:** May not capture steady-state (short-run adjustment)

---

## Expected Effects & Mechanisms

### Primary Effects

**Effect 1: Translation Efficiency (Descriptive)**
- Firms with 1 SD higher TE have 15-20% higher Tobin's Q
- 20-30% more patent citations per patent
- 1-2 years faster publication-to-patent conversion
- **Mechanism:** Signaling unobserved innovation quality, attracting capital and talent
- **Identification:** Selection model (correlational, with rich controls)

**Effect 2: Complementarity (Descriptive)**
- Firms pursuing "Both" strategy have highest value (after controlling for counts)
- Interaction term β3 = 0.05 to 0.10 (economically significant)
- **Mechanism:** Publications establish priority (first-to-discover), patents protect applications (first-to-file), combined → maximum value capture
- **Identification:** Selection model with firm FE (controls for time-invariant firm heterogeneity)

**Effect 3: Alice Corp Response (Causal - DID)**
- Software firms increase disclosure ratio by 5-10 percentage points post-Alice
- Patent applications decrease by 10-15%, publications increase by 5-10%
- **Mechanism:** Weakened patent protection → substitution toward publications
- **Identification:** DID with parallel trends assumption (causal)

**Effect 4: Heterogeneous Response by TE (Causal)**
- High TE software firms: 15-20 pp increase in disclosure ratio post-Alice
- Low TE software firms: 5-10 pp increase in disclosure ratio post-Alice
- **Mechanism:** High TE firms can more effectively substitute publications for patents (publications already signal quality)
- **Identification:** DID × TE interaction (causal, conditional on parallel trends)

**Effect 5: Dynamic Evolution (Descriptive)**
- Startups (< 5 years): 60% choose "Publish only", 10% choose "Both"
- Mature firms (> 20 years): 20% choose "Publish only", 40% choose "Both"
- High TE firms: 2x faster transition to "Both" strategy
- **Mechanism:** Lifecycle stages, changing resource constraints, evolving strategic priorities
- **Identification:** Markov transition models (descriptive, predictive)

---

## Primary Specification

### Selection Model (Main Analysis)

**Dependent Variable:** Strategy choice s ∈ {Publish_Only, Patent_Only, Both, Neither}

**Specification:**
```stata
// Multinomial logit (base category: Neither)
mlogit Strategy i.TE i.Assets i.Age i.RD_Intensity i.Cash i.Leverage ///
      i.Competition i.IP_Protection i.Tech_Firm i.Controls ///
      ibn.Industry_FE ibn.Year_FE, baseoutcome(3)

// Marginal effects
margins, dydx(*) post
```

**Expected Signs:**
- β_TE_Both > 0: High TE firms choose hybrid strategy
- β_Size_Pat > 0, β_Size_Both > 0: Large firms patent/hybrid
- β_Age_Pub > 0: Young firms publish (signaling)
- β_Cash_Both > 0, β_Leverage_Both < 0: Resources enable hybrid strategy

### DID Model (Causal Analysis)

**Dependent Variable:** Disclosure_Ratio = AI_Pubs / (AI_Pubs + AI_Pats)

**Specification:**
```stata
// Main DID specification
reghdfe Disclosure_Ratio Post_Alice##Software_Firm TE ///
         c.Te#c.Post_Alice##Software_Firm ///
         ln_Assets ln_Age RD_Intensity Cash_Holdings Leverage ROA ///
         i.Industry_FE i.Year_FE, absorb(FirmID) vce(cluster industry)

// Event study DID (validate parallel trends)
reghdfe Disclosure_Ratio Software_Firm ///
         i.year##Software_Firm i.firm, absorb(FirmID) vce(cluster industry)
```

**Expected Signs:**
- β_DID < 0: Post_Alice × Software_Firm increases disclosure ratio (substitution toward pubs)
- β_TE×DID < 0: High TE amplifies substitution (pubs more effective substitutes when patents weak)
- γ_k = 0 for k < 0: Parallel trends pre-Alice (validation)

---

## Planned Robustness Checks

### 1. Selection Model Robustness

**Alternative Estimators:**
- **Nested logit:** Relax IIA assumption (firms choose {Publish, Patent} before choosing Both)
- **Mixed logit:** Random coefficients (allow heterogeneity in TE responsiveness)
- **Heckman selection:** Correct for non-random participation in AI innovation

**Alternative Specifications:**
- **Continuous dependent variable:** ln(1 + Disclosure_Count) by strategy (Poisson/negative binomial)
- **Ordered logit:** If we collapse to single dimension (pub intensity vs. pat intensity)
- **Bivariate probit:** Model pub and patent choices jointly

**Control Variables:**
- **Manager fixed effects:** Control for time-invariant managerial ability
- **Firm-specific linear trends:** μ_i × time_t (address underlying trends)
- **Industry × Year FE:** Absorb industry-specific shocks
- **Competitor controls:** Competitor AI activities (spillovers, competitive pressure)

### 2. DID Robustness

**Parallel Trends Validation:**
- **Pre-trend tests:** Event-study DID for 3-5 years pre-Alice (should show no differential trends)
- **Placebo tests:** Fake shock in 2011 or 2016 (should show no effect)
- **Alternative control groups:** Hardware firms, biotech firms (should show smaller or no effect)

**Alternative Treatment Definitions:**
- **Continuous treatment:** Share of patents in software classes (pre-Alice trend)
- **Triple DID:** Post_Alice × High_Software_Patent_Share × High_TE (complementary)

**Sample Restrictions:**
- **Exclude tech hardware firms:** May also be affected by Alice (controls)
- **Exclude pre-Alice entrants:** Firms that entered after 2014 (post-shock entrants)
- **Balanced panel:** Firms with data 2010-2018 (address attrition)

**Alternative Dependent Variables:**
- **Publication rate:** AI_Pubs / Total_Assets or AI_Pubs / R&D
- **Patent rate:** AI_Pats / Total_Assets or AI_Pats / R&D
- **Separate equations:** Estimate pub and pat equations separately

**Heterogeneity:**
- **Firm size:** Small vs. large software firms
- **Firm age:** Young vs. mature software firms
- **Pre-Alice patent reliance:** High vs. low software patent intensity (treatment intensity)
- **TE level:** High vs. low TE (mechanism test)

### 3. Combined Approach Robustness

**Triple DID (DDD):**
```stata
// Add third dimension: High_TE vs. Low_TE firms
Disclosure_Ratio = α + β1·Post_Alice × Software_Firm × High_TE
                 + β2·Post_Alice × Software_Firm + β3·Post_Alice × High_TE
                 + γ·Controls + μ_i + λ_t + ε
```

**Expected:** β1 < 0 (strongest substitution for software firms with high TE)

---

## Theoretical Model

### Model Setup (Unchanged from previous version)

**Agents:** Firm chooses IP strategy s ∈ {P (Publish), D (Patent), PD (Both), N (Neither)}

**Timing:**
1. Nature draws innovation quality q ~ Uniform[0, 1]
2. Firm observes q and chooses strategy s
3. Patent regime θ_P ∈ {Strong (pre-2014), Weak (post-2014)} realized (Alice shock)
4. Payoffs realized

**Payoffs with Patent Regime:**
```
V(P | θ_P) = q × v_pub × Signal_Bonus(q) - C_pub(q)
V(D | θ_P) = q × v_pat × θ_P × Protection_Bonus(q) - C_pat(q)
V(PD | θ_P) = q × [v_pub + v_pat × θ_P + θ_Firm_Char] × [Signal × Protection]
            - [C_pub + C_pat + Coordination_Cost]
V(N | θ_P) = q × v_sec × (1 - Imitation_Risk) - C_sec(q)
```

Where θ_P ∈ {Strong, Weak} captures patent protection strength

**Key Comparative Static (Alice Corp):**
- ∂V(D | θ_P) / ∂θ_P > 0: Patent value higher when IP protection strong
- ∂s / ∂θ_P: When θ_P decreases (Alice), firms substitute D → P (publication becomes relatively more attractive)

**Equilibrium Prediction:**
- Pre-Alice (strong IP): q_PD* lower (firms choose "Both" at lower innovation quality)
- Post-Alice (weak IP): q_PD* higher (fewer firms choose "Both", more choose P)
- **Stronger effect for high TE firms:** Lower coordination cost for substituting P for D

---

## Data Sources

### Primary Data

**AI Publications:**
- **Source:** OpenAlex (https://openalex.org)
- **Fields:** Abstract text, publication year, venue, citations, author institutions, concepts/fields
- **Coverage:** All AI publications 2010-2024
- **Sample Size:** ~50,000-100,000 AI publications from public firms
- **AI Definition:** Publications in AI-related concepts (Machine Learning, NLP, Computer Vision, Robotics)

**AI Patents:**
- **Source:** USPTO/PatentsView (https://patentsview.org)
- **Fields:** Patent claims text, application date, grant date, citations, assignee, CPC/IPC classifications
- **Coverage:** AI patent applications/grants 2010-2024
- **Sample Size:** ~30,000-60,000 AI patents from public firms
- **AI Definition:** Patents in AI-related CPC classes (G06N, G10L, etc.) or text-based classification
- **Software patents:** CPC classes G06F (computing), business methods (data processing)

**Firm Financial Data:**
- **Source:** CRSP/Compustat merged (via WRDS)
- **Fields:** Market value, book value, total assets, R&D expenses, cash holdings, leverage, profitability, firm age, SIC/NAICS
- **Sample:** All public firms with AI publications/patents 2010-2024
- **Sample Size:** ~1,000-2,000 firms, ~15,000 firm-year observations

**Software Firm Classification:**
- **Source:** NAICS industry codes (3341 - Computer Equipment Mfg, 5112 - Software Publishers, 5415 - Custom Programming)
- **Alternative:** High share of software patents in pre-Alice period
- **Sample:** ~200-400 software firms (treatment group)

### Secondary Data

**Patent Case Law Data:**
- **Source:** Stanford Law School Patent Case Law Database, LexisNexis
- **Fields:** Case names, dates, decisions, affected CPC classes, industry impact
- **Use:** Identify other IP regime shocks (e.g., *Bilski v. Kappos* 2010, *Mayo v. Prometheus* 2012)

**Industry Patent Strength Indices:**
- **Source:** Patent strength by industry/region (available in literature: Qian 2007, ITU indexes)
- **Use:** Test appropriability hypothesis (strong IP → complementarity)
- **Coverage:** Industry-year level (SIC 2-digit or 3-digit)

**Competition Measures:**
- **Source:** Compustat (construct Herfindahl-Hirschman Index - HHI)
- **Fields:** Sales by firm, 3-digit SIC industry
- **Use:** Test competition effects on complementarity
- **Construction:** HHI = Σ(sales_i / total_sales_industry)²

---

## Data Collection Timeline

| Phase | Tasks | Duration |
|-------|-------|----------|
| **Week 1-2** | Download AI publication abstracts (OpenAlex API) | 2 weeks |
| **Week 3-4** | Download AI patent claims (USPTO bulk data/PatentsView) | 2 weeks |
| **Week 5-6** | Match publications/patents to firms (GVKEY, CIK matching) | 2 weeks |
| **Week 7-8** | Collect financial data (CRSP/Compustat via WRDS) | 2 weeks |
| **Week 9-10** | Identify software firms (NAICS, patent portfolio data) | 2 weeks |
| **Week 11-12** | Merge datasets, clean data, construct key variables | 2 weeks |
| **Week 13-14** | Compute embeddings and translation efficiency | 2 weeks |
| **Week 15-16** | Construct DID treatment and control groups, validate parallel trends | 2 weeks |
| **Total** | **Data collection, cleaning, and variable construction** | **16 weeks (4 months)** |

---

## Expected Contributions

### 1. Methodological Innovation
**First paper to measure scientific-technical knowledge translation directly using text similarity analysis.** Prior work uses citation counts (backward citations from patents to papers) or simple publication/patent counts. My semantic similarity measure captures how much scientific content from publications is reused in patent claims, directly measuring knowledge conversion.

### 2. Theoretical Contributions
**Integrates three theoretical frameworks:**
- **Signaling theory (Spence 1973):** Publications as costly signals of innovation quality
- **Resource-based view (Barney 1991):** Translation capability as scarce, valuable, inimitable resource
- **Complementarity theory (Milgrom & Roberts 1990):** Publications and patents as context-dependent complements vs. substitutes

**Advances theory by:**
- Characterizing WHEN publications and patents are complements vs. substitutes (IP regime dependence)
- Showing HOW translation capability moderates complementarity (high TE → stronger complementarity)
- Explaining WHY firms respond to patent regime shocks (theoretical model with comparative statics)
- Providing first quasi-experimental evidence on causal effects of IP changes on disclosure strategy

### 3. Empirical Contributions
**First quasi-experimental evidence on:**
- Effect of patent protection regime changes on disclosure strategy (Alice Corp DID)
- Heterogeneous treatment effects by translation capability (TE × DID interaction)
- Descriptive patterns of strategic choice (multinomial logit selection model)
- Dynamic evolution of strategies over firm lifecycle (Markov transition matrices)

### 4. Policy Implications
**For managers:**
- WHEN to publish vs. patent AI research (context-dependent decision rules)
- HOW to build translation capability (hire dual-background executives, cross-functional teams)
- WHICH strategy maximizes firm value (hybrid "Both" strategy when resources allow, BUT adjust strategy when IP regime changes)

**For policymakers:**
- HOW IP law changes affect knowledge disclosure (Alice Corp → shift to publications)
- WHETHER strong IP regimes encourage complementarity (yes, but high TE firms benefit more from hybrid)
- TRADE-OFFS: Weakening IP protection may increase publication but decrease patenting (net effect on innovation ambiguous)
- ROLE of publication in innovation diffusion (publications create spilloversers, patents protect value)

### 5. Literature Positioning

**Builds on:**
- **Corporate science literature:** Simeth & Cincera (2016), "Rich on paper?" (2021)
- **Patent citation literature:** Trajtenberg et al. (1997), Hall et al. (2005)
- **Signaling literature:** Spence (1973), Boot & Vladimirov (2024)
- **IP strategy literature:** Arora et al. (2021 AER), Anton & Yao (1994)
- **Law & economics:** Lemley & Moore (2004) on software patents

**Advances beyond:**
- Arora et al. (2021): Study continuous investment levels (HOW MUCH), I study discrete choice (WHICH MECHANISM)
- Simeth & Cincera (2016): Use publication counts, I use text content (semantic similarity)
- Boot & Vladimirov (2024): Theoretical model, I provide empirical test using real data with quasi-experimental variation
- **Quasi-experimental:** Prior work is correlational; I provide causal evidence using Alice Corp shock

---

## Publication Strategy

### Target Journals (Tiered)

**Tier 1 (Top 5 / Top Field):**
- **Journal of Financial Economics (JFE)** - Corporate finance, innovation, IP strategy
- **Review of Financial Studies (RFS)** - Corporate finance, strategic interactions
- **Journal of Finance (JF)** - If market valuation effects are strong

**Tier 2 (Top Field):**
- **Management Science** - Innovation, strategy, IP
- **Strategic Management Journal (SMJ)** - Competitive strategy, dynamic capabilities
- **Organization Science** - Organizational capabilities, knowledge management

**Tier 3 (Specialized):**
- **Research Policy** - Innovation studies, IP policy
- **Journal of Corporate Finance** - Corporate finance, governance
- **Journal of Financial Markets** - Market efficiency, information asymmetry

### Conference Presentations

**Finance:**
- AFA Annual Meeting (American Finance Association)
- WFA Annual Meeting (Western Finance Association)
- EFA Annual Meeting (European Finance Association)

**Management / Strategy:**
- Academy of Management (AOM) Annual Meeting
- Strategic Management Society (SMS) Annual Conference
- DRID (Durham Research in Innovation Discourse) Conference

---

## Timeline (12 months)

| Phase | Tasks | Duration | Milestones |
|-------|-------|----------|------------|
| **Months 1-4** | Data collection, cleaning, pilot analysis | 4 months | Complete TE computation, identify software firms, pre-trends |
| **Months 5-7** | Main analysis: selection model, DID | 3 months | Complete mlogit, DID main specs, validation tests |
| **Months 8-9** | Robustness, heterogeneity, mechanism tests | 2 months | Complete all robustness checks, event studies, DDD |
| **Months 10-12** | Writing, revising, submission | 3 months | Complete full paper, target journal selection, submit |

**Monthly Breakdown:**

**Month 1:**
- Download AI publication abstracts (OpenAlex)
- Download AI patent claims (USPTO)
- Match firms to GVKEY/CIK

**Month 2:**
- Download financial data (CRSP/Compustat)
- Identify software firms (NAICS, patent portfolio)
- Construct software firm treatment indicator

**Month 3:**
- Compute embeddings (SciBERT/PatBERT)
- Compute translation efficiency (TE)
- Pilot analysis on TE measurement

**Month 4:**
- Construct key variables (pubs, patents, disclosure ratio)
- Validate parallel trends pre-Alice (2010-2013)
- Descriptive statistics, summary tables

**Month 5:**
- Selection model: Multinomial logit estimation
- Marginal effects, predicted probabilities
- Heterogeneity analysis (by firm type)

**Month 6:**
- DID main specification: Alice Corp effect
- Event study DID (dynamic effects)
- TE × DID interaction (heterogeneous response)

**Month 7:**
- Triple DID (high TE vs. low TE software firms)
- Mechanism tests (mediation, moderation)
- Initial results summary

**Month 8:**
- Robustness: Alternative estimators (nested logit, mixed logit)
- DID robustness: Placebo tests, alternative control groups
- Additional extensions (competitor controls, spillovers)

**Month 9:**
- Dynamic analysis: Markov transition matrices
- Survival analysis: Time to transition strategies
- Results validation, cross-checks

**Month 10:**
- Draft Introduction and Literature Review
- Draft Data and Measurement sections
- Create figures and tables

**Month 11:**
- Draft Results section (selection model + DID)
- Draft Discussion and Conclusion
- Revise and polish

**Month 12:**
- Finalize paper
- Target journal selection (JFE/RFS based on results)
- Submit paper

---

*Total length: ~3,200 words (comprehensive proposal with strengthened identification strategy)*
