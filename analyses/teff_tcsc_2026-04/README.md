# TN Charter School Facilities Analysis — TEFF / TCSC enrichment pull

Run date: original 2026-04-30; refreshed 2026-05-03 after data quality fixes.
Source: local Postgres at `localhost:5432/cd_command_center` (canonical store
per project memory; SQLite file in `data/` is stale).
Builder: `_build.py`

## What changed in the 2026-05-03 refresh

The original pull surfaced six data quality issues. Four have been fixed in
this refresh:

1. **NMTC Coalition state truncation** (Tennessee → "TE", Pennsylvania → "PE",
   etc., collapsing Michigan/Minnesota/Mississippi/Missouri into "MI" and the
   six "NE-/NEW-" states into "NE"). Patched in
   `etl/load_nmtc_coalition.py` with a proper state-name-to-abbreviation map
   and reloaded — TN coalition rows now resolve to 442 (was 0 under "TN"
   filter, 1,260 under the truncated "TE").
2. **SY 2023-24 charter directory** loaded via
   `fetch_nces_charter_schools.py --states TN --demographics --year 2024`.
   123 schools fetched (5 more than the SY 2022-23 file).
3. **Race/ethnicity demographics** filled for 116 of 127 TN charter rows
   via the same `--demographics` re-run. ELL and SPED endpoints returned no
   charter rows for TN (upstream API gap, not a loader bug).
4. **Census tract assignment** completed for all 127 TN charter rows via
   `assign_census_tracts.py --states TN`. Output 3 (tract context) is now
   built.

Two issues remain upstream-blocked: `year_opened` is not in the Urban
Institute directory endpoint, and `seasch` repo-wide fill (10.8%) is bounded
by what the API returns for non-TN states.

## Pipelines used

| Pipeline | Source | Last refresh in this run |
|---|---|---|
| Charter schools (`schools` table) | NCES CCD via Urban Institute Education Data Portal — https://educationdata.urban.org/api/v1/schools/ccd/directory/ | 2026-05-03 (year=2024 + demographics) |
| Census tract assignment | FCC Area API (primary) + Census Bureau geocoder (fallback) | 2026-05-03 (TN-scoped, all 127 rows assigned) |
| NMTC projects (`nmtc_projects`) | CDFI Fund NMTC Public Data 2024 | 2026-03-23 (unchanged) |
| NMTC Coalition (`nmtc_coalition_projects`) | NMTC Coalition TLR 2024 — https://nmtccoalition.org/nmtc-fact-sheet/ | 2026-05-03 (reloaded with state-truncation patch) |
| Census tracts (`census_tracts`) | ACS 5-year via Census API | unchanged |

## Outputs

| File | Rows | Notes |
|---|---:|---|
| `TN_charter_schools_roster.csv` | 127 | 114 Open, others Closed/Pending. Two-year mix: 123 SY 2023-24, 4 SY 2022-23 stragglers (schools missing from the 2024 directory; likely closed/merged). |
| `TN_charter_NMTC_deals.csv` | 0 | **Headers only** — neither NMTC dataset carries an end-use = charter classification. See "NMTC charter identification" below. |
| `TN_charter_tract_context.csv` | 127 | Joined to census_tracts on `census_tract_id`. 74/127 (58%) flagged `nmtc_lic_qualified`. |

## Output 1 — column-level fill rates (127 rows)

| Column | Populated | Notes |
|---|---|---|
| `ncessch` | 127/127 | NCES school ID |
| `seasch` | 127/127 | TN coverage is 100%; repo-wide it remains ~10.8% (upstream gap). |
| `school_name`, `city`, `latitude`, `longitude` | 127/127 | |
| `county` | 127/127 | Stored upstream as 7-char string (e.g. `4747157`); normalized to county name via TN FIPS lookup in `_build.py`. 5 distinct counties present in the data: Shelby (76), Davidson (37), Hamilton (9), Rutherford (3), Knox (2). |
| `grades_offered` | 127/127 | Computed as `grade_low-grade_high`. |
| `enrollment_total` | 116/127 | 11 missing (mix of pending/closed schools and zero-enrollment edge cases). |
| `pct_black`, `pct_hispanic`, `pct_white` | 116/127 | Filled via the `--demographics` re-run (race endpoint). 11 rows missing (race endpoint returned no enrollment for them). |
| `pct_other` | 116/127 | **Computed**, not stored: `max(0, 100 − black − hispanic − white)`. Captures Asian, multiracial, AI/AN, NHPI residual. Null when any input is null. |
| `frpl_pct` | **0/127** | TN charter rows have no value in the directory endpoint's `free_or_reduced_price_lunch` field for SY 2023-24 or SY 2022-23. Likely caused by Community Eligibility Provision adoption in TN (district-level FRPL no longer reported per-school). Fix path: pull district-level CEP/FRPL from a state-DOE source. |
| `pct_ell` | **0/127** | LEP-status endpoint at `/ccd/enrollment/2024/grade-99/lep-status-2/` returned no charter-school rows for TN. Same for SY 2022-23. Upstream gap, not a loader bug. |
| `pct_swd` | **0/127** | Same upstream gap on disability-status-2 endpoint. |
| `authorizer` | 127/127 | Mapped from `lea_name`. Distribution includes Memphis-Shelby County Schools, Davidson County, Achievement School District, Tennessee Public Charter School Commission, Hamilton County, Knox County, Rutherford County. Note: this is *operating LEA*, not always *authorizer* — TPCSC and ASD are state-level authorizers but in some rows their charters appear under the operating district. |
| `charter_year_opened` | **0/127** | Not in the Urban Institute directory endpoint (`year_opened` is hardcoded to None in `fetch_nces_charter_schools.py:357`). Fix path: pull from TN DOE charter directory or NCES ELSI. |
| `data_year` | 127/127 | 123 = 2024 (SY 2023-24), 4 = 2023 (SY 2022-23). |

## Output 2 — NMTC charter identification (still empty)

The Postgres NMTC datasets carry these counts for TN:

| Source | TN rows | What's available |
|---|---:|---|
| `nmtc_projects` (CDFI Fund Public Data) | 149 | `project_type` ∈ {NRE, RE, SPE, CDE}; `project_description` is null for all 149 TN rows |
| `nmtc_coalition_projects` (Coalition TLR) | 442 | `project_type` (= QALICB Type) ∈ {NRE, RE, SPE}; "Purpose of Investment" is a financing-type field (Business Financing / Real Estate – Construction / Real Estate – Rehab) with no charter/school/academy mentions |

Neither dataset has an end-use category that identifies charter schools.
Output 2 stays headers-only. See "Next steps" for what would unblock this.

## Output 3 — census tract context

127/127 TN charter schools joined to the `census_tracts` table on the
11-digit GEOID.

- **Statewide TN median family income used:** $73,448 (median of all
  TN tract MFIs in `census_tracts.median_family_income`).
- **`ami_pct`** = school's tract MFI / statewide TN MFI.
- **`nmtc_lic_qualified`** = `poverty_rate >= 20%` OR `ami_pct <= 0.80`
  (CDFI Fund Low-Income Community criteria).
- **74/127 (58%)** of TN charter schools sit in NMTC-LIC-qualified tracts.
- **`distressed_community`** = `census_tracts.is_nmtc_eligible` boolean
  flag (a richer 4-tier eligibility classification computed during census
  load). 53 rows are flagged distressed.

## Data quality flags

1. **FRPL / ELL / SWD remain 0/127.** Verified upstream — the Urban
   Institute API does not return per-charter values for these in TN. Likely
   driven by TN's adoption of Community Eligibility Provision for FRPL and
   by sparse charter coverage on the LEP/disability enrollment endpoints.
2. **`year_opened` is 0/127.** Hardcoded to None in the loader because the
   directory endpoint does not include it. Needs a different source.
3. **Two-year row mix (123 SY 2024 + 4 SY 2023).** The SY 2024 fetch
   upserts on `nces_id`; 4 schools present in the SY 2022-23 file did not
   appear in the SY 2023-24 file and remain at the older year. Likely
   closed schools that the upstream removed from the directory; treat
   their data as 1 year stale.
4. **Authorizer is operating-LEA, not always authorizer-of-record.** TN's
   state-level authorizers (TPCSC, ASD) appear for some rows but
   district-authorized charters in Memphis-Shelby/Davidson/Hamilton/Knox
   show the *district*, not the *authorizer*, in the `lea_name` field.
5. **NMTC datasets are unchanged in this refresh.** Only the Coalition
   state-truncation patch was applied; the underlying QALICB Type values
   are unchanged. The dataset still cannot identify charter projects.
6. **SQLite file in `data/` is stale.** Project canonical store is local
   Postgres. The earlier SQLite-based read of this analysis showed
   pre-patch values (1,260 "TE" coalition rows, 0 census tracts) and
   should be ignored.

## What is NOT verified

- That `data_year=2024` corresponds to SY 2023-24 (Urban Institute
  conventions; not cross-checked against NCES source files).
- That the school-status field reflects current operating status. NCES /
  Urban updates lag the actual closure events by 6-18 months.
- That `pct_black + pct_hispanic + pct_white ≤ 100` in all rows (residual
  `pct_other` formula assumes this; floors at 0).
- That the TN statewide MFI used ($73,448, the median of TN tract MFIs)
  matches the figure CDFI Fund uses for its annual NMTC LIC determination.
  CDFI Fund publishes statewide medians from a specific ACS vintage; if
  the local `census_tracts` table is on a different vintage, individual
  `nmtc_lic_qualified` flags could disagree at the margins.

## Next steps to harden this output

1. **Load the CDFI Fund Compliance Report** as a sibling to
   `nmtc_projects`. The Compliance Report Excel carries an end-use
   category that distinguishes charter schools, K-12, and other
   facility types. Estimate: half-day for a new
   `etl/load_nmtc_compliance.py` + view that joins on CDFI project ID.
2. **Pull `year_opened`** from a different source. Options: the TN DOE
   charter directory (state-specific) or NCES ELSI bulk export
   (national). Both would require a small new loader.
3. **Fill FRPL** via state CEP file (TN DOE publishes district-level
   CEP percentages) or a school-meals microdata source. NCES per-school
   FRPL coverage is collapsing nationally; this is a multi-state pattern.
4. **Audit `seasch` join logic** repo-wide. TN's 100% fill is not the
   norm; document where seasch comes from in `fetch_nces_schools.py` /
   `fetch_nces_charter_schools.py` and add a coverage threshold test.
5. **Verify the LIC threshold against CDFI Fund's published list** for
   one or two known-NMTC-eligible TN tracts, to confirm the local
   `census_tracts.is_nmtc_eligible` flag and the computed
   `nmtc_lic_qualified` agree on edge cases.
