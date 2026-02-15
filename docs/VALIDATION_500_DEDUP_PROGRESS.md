# MANUAL VALIDATION RESULTS - 500 Matches (DE-DUPLICATED, seed=910)

**Date:** 2026-02-15
**Sample Size:** 500 matches
**Method:** MANUAL verification (de-duplicated dataset)
**Dataset:** `publication_firm_matches_dedup.parquet`
**De-duplication:** Each institution → ONE firm (but one firm can have multiple institutions)

---

## 🎯 Overall Results

### Final Accuracy: TBD

**Status:** IN PROGRESS - Verified 300/500 so far

---

## 📊 De-Duplication Summary

**Strategy:** Group by institution_id, keep best firm match (highest paper count)
- **Before:** 2,811 matches (312 institutions matched to multiple firms)
- **After:** 2,364 matches
- **Removed:** 447 duplicate matches (15.9%)
- **Coverage:**
  - Institutions: 2,364 / 16,278 = 14.52%
  - Firms: 1,454 / 18,709 = 7.77%
  - Papers: 2.06M

---

## ❌ Incorrect Matches Found (First 300 matches)

### 1. AkzoNobel (Canada) - INCORRECT
**Line:** 32
**Match:** AkzoNobel (Canada) → COURTAULDS PLC
**Issue:** AkzoNobel is not Courtaulds

### 2. Biogen (United States) - INCORRECT
**Line:** 99
**Match:** Biogen (United States) → BIOGEN INC-OLD
**Issue:** Should be BIOGEN INC (GVKEY 024468, not 002226)

### 3. Bruker (China) - POTENTIALLY INCORRECT
**Line:** 110
**Match:** Bruker (China) → BRUKER AXS INC
**Issue:** Bruker acquired Bruker AXS, but name is "Bruker" not "Bruker AXS"

### 4. Bruker (Germany) - POTENTIALLY INCORRECT
**Line:** 111
**Match:** Bruker (Germany) → BRUKER AXS INC
**Issue:** Same as above

### 5. Jazz Pharmaceuticals (Italy) - INCORRECT
**Line:** 252
**Match:** Jazz Pharmaceuticals (Italy) → CELATOR PHARMACEUTICALS INC
**Issue:** Jazz Pharmaceuticals is not Celator

### 6. Japan Tobacco (Switzerland) - INCORRECT
**Line:** 251
**Match:** Japan Tobacco (Switzerland) → GALLAHER GROUP PLC
**Issue:** Japan Tobacco is not Gallaher (J Tobacco acquired Gallaher, but institution is Japan Tobacco)

### 7. Lundbeck (Spain) - INCORRECT
**Line:** 278
**Match:** Lundbeck (Spain) → SYNAPTIC PHARMACEUTICAL CORP
**Issue:** Lundbeck is not Synaptic Pharmaceutical

---

## 📝 Verification Progress

**Verified:** 300/500 matches (60% complete)
- **Correct:** 293-295 (depending on Bruker)
- **Incorrect:** 5-7 (depending on Bruker)

**Current Accuracy:** 97.7% - 98.3%

---

**Last Updated:** 2026-02-15
**Status:** 🔵 VERIFICATION IN PROGRESS
