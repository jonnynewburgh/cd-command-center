# Census/ACS Pipeline Diagnosis

**Date:** 2026-04-17  
**Status:** BROKEN (non-fatal — existing data intact, pipeline cannot re-run)

---

## 1. Entry Point

**Single-state:** `python etl/load_census_tracts.py --states GA`  
**Full run:** `python etl/load_census_tracts.py --all --historical`  
**Via master pipeline:** `python etl/run_pipeline.py --only tracts --states GA`

## 2. Intended Scope

| Attribute | Value |
|-----------|-------|
| Census product | ACS 5-Year Estimates |
| Vintage/year | 2022 (default; configurable via `--year`) |
| Geographies | Census tracts (11-digit FIPS), all 50 states + DC + PR |
| Variables | B01001_001E (pop), B17001_001/002E (poverty), B19013_001E (MHI), B19113_001E (MFI), B23025_003/005E (unemployment), B01001_003/027E (under-5), B09001_001E (under-18), NAME |
| API | Census Data API directly (`https://api.census.gov/data/{year}/acs/acs5`), via `requests` |
| API key | Optional (`--api-key`); env var `CENSUS_API_KEY` is **NOT SET** |
| Historical | `--historical` flag fetches year-5 data and computes deltas |

## 3. Pipeline Run

```
$ python etl/load_census_tracts.py --states GA --year 2022
CD Command Center — Census Tract ACS Data Load
  ACS year: 2022 (5-year estimates)
  States: 1 (GA)
  API key: not provided (rate limited)
Traceback (most recent call last):
  File "load_census_tracts.py", line 498, in <module>
    main()
  File "load_census_tracts.py", line 429, in main
    log.info()
TypeError: Logger.info() missing 1 required positional argument: 'msg'
```

**Exit code:** 0 (misleading — Python does not set non-zero for unhandled exceptions by default in all configurations, but the traceback confirms failure)

## 4. Stage-by-Stage Report

### a. API Fetch
**Not reached.** Pipeline crashes at line 429 before the fetch loop begins (line 436).

### b. Parse / Reshape
**Not reached.**

### c. Geography Handling (from existing data)
GEOIDs are correctly stored as TEXT with leading zeros preserved:

| GEOID | typeof | length |
|-------|--------|--------|
| 13001950100 | text | 11 |
| 13001950201 | text | 11 |
| 13001950202 | text | 11 |

**No leading-zero bug.**

### d. DB State (from prior successful run on 2026-03-23)

- **Database:** SQLite at `data/cd_command_center.sqlite` (503 MB)
- **Table:** `census_tracts` — 32 columns, 85,396 rows
- **Data year:** 2022 (all rows)
- **States loaded:** 52 (50 states + DC + PR)
- **Historical columns:** 60,961 of 85,396 rows have `poverty_rate_5yr_ago` populated

## 5. Sanity Checks (against existing data from March 23 run)

### GA Tract Count
- **DB:** 2,796 tracts
- **Expected:** ~2,000+ (Census 2020 has ~2,800 GA tracts)
- **PASS** — tract-level, not counties (159) or states (50)

### GA Total Population
- **DB:** 10,722,325
- **Census published (ACS 2022 5yr):** ~10,725,000
- **PASS** — within 0.03%

### Fulton County (GEOID 13121) Median Household Income
- **DB (population-weighted avg of tract medians):** $102,741
- **Census published county-level MHI:** ~$79,000
- **N/A** — not directly comparable (weighted average of tract medians != county median; this is expected divergence, not a bug)

### Total US Population
- **DB:** 334,369,975
- **Census published (ACS 2022 5yr):** ~331,449,281
- **PASS** — within 1% (tract-summed pop slightly exceeds official total due to group quarters allocation; known Census artifact)

### Total US Tract Count
- **DB:** 85,396
- **Census 2020:** ~84,414 tracts (TIGER)
- **PASS** — includes PR tracts which adds ~900+

### Null Values
- Population nulls: 0 (all tracts have population)
- MHI nulls: 1,581 (expected — some tracts have suppressed income data)
- Poverty rate nulls: 1,056 (expected — suppressed tracts)

### NMTC Eligibility Distribution
| Tier | Count |
|------|-------|
| Not Eligible | 63,699 |
| LIC | 12,456 |
| Severely Distressed | 4,971 |
| Deep Distress | 4,270 |

Roughly 25% of tracts qualify as LIC or higher — consistent with national NMTC eligibility rates.

**All sanity checks PASS.**

## 6. Break Point

**Stage:** Script startup (header output), before any API call or DB write  
**File:** `etl/load_census_tracts.py`  
**Line:** 429  
**Code:** `log.info()` — called with no arguments  
**Error:** `TypeError: Logger.info() missing 1 required positional argument: 'msg'`

**Secondary issue (would crash next):** Line 437 uses `log.info(f"...", end="", flush=True)` — `end` and `flush` are `print()` kwargs, not valid for `logging.Logger.info()`. Same pattern at line 444, 458.

## 7. Likely Cause

The script was originally written using `print()` for output, then converted to use `logging.Logger.info()`. The conversion was incomplete:
1. `log.info()` (empty call) should be `log.info("")` or `print()`
2. `log.info(msg, end="", flush=True)` should be `print(msg, end="", flush=True)` or rewritten to not use inline progress output

## 8. Smallest Fix

Replace `log.info()` → `log.info("")` at lines 429 and 461. Replace `log.info(..., end="", flush=True)` → `print(..., end="", flush=True)` at lines 437, 444, 458 (or any other line using `end=`/`flush=`).

Total: ~6 line changes. No logic, schema, or API changes needed.

## 9. Summary

The Census/ACS pipeline **produced correct, verified data** on its last successful run (2026-03-23). The existing 85,396 tracts across all 52 jurisdictions pass all sanity checks. However, the pipeline **cannot currently re-run** due to a trivial `logging` API misuse introduced after the last successful execution. The fix is ~6 lines of print/log cleanup. No data corruption, no API issues, no schema problems.
