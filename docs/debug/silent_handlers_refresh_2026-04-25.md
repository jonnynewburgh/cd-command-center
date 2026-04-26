---
title: Silent handlers refresh — Phase 1 Step 0
date: 2026-04-25
scope: db.py only — re-grep against the H1-H15 list in postgres_readiness_inventory_2026-04-22.md
status: Pre-fix snapshot. Used to drive Phase 1 Step 2 sweep.
---

# Silent handlers refresh — 2026-04-25

## Why this doc exists

The inventory at `postgres_readiness_inventory_2026-04-22.md` is 12+
days old. Several handlers were resolved by recent commits
(`bf4ed8a`, `11097d6`, `7d8382a`). This refresh re-greps `db.py` for
`except Exception` and rebuilds the DANGEROUS/RISKY/LOW-RISK/
ACCEPTABLE classification fresh, so Phase 1 Step 2 operates against
current state, not stale state.

## Counts

- `except Exception(\s+as\s+\w+)?\s*:` in `db.py`: **62 sites**
- Bare `except:`: **0**
- Inventory baseline: 64 sites → 2 fewer post-`bf4ed8a`

DANGEROUS count: **22** (below the 30-site hard-stop threshold; the
phase split holds).

## DANGEROUS sites (active masks)

| # | Line | Function | On except | Status vs inventory | Cross-ref |
|---:|---:|---|---|---|---|
| H6 | 2241 | `get_fqhc_by_id` | `return {}` | unchanged | A1 silent-404 |
| H7 | 2260 | `get_ece_by_id` | `return {}` | unchanged | A1 silent-404 |
| H8 | 2274 | `get_nmtc_project_by_id` | `return {}` | raw-? fixed in `11097d6`; handler still masks dict(row) | A1 silent-404 |
| H9a | 2317 | `get_nearby_facilities` schools branch | `pass` | unchanged | masks map |
| H9b | 2325 | `get_nearby_facilities` fqhc branch | `pass` | unchanged | masks map |
| H9c | 2333 | `get_nearby_facilities` ece branch | `pass` | unchanged | masks map |
| H9d | 2341 | `get_nearby_facilities` nmtc branch | `pass` | unchanged | masks map |
| H11 | 2983 | `get_user_notes` | `notes = []` | raw-? fixed in `11097d6`; handler still masks dict(row) | A1 silent-empty |
| H12 | 3035 | `get_bookmarks` | `bookmarks = []` | unchanged | A1+B6 silent-empty (dict(row) bug live) |
| H13 | 3079 | `is_bookmarked` | `found = False` | unchanged | LOW priority |
| H14 | 3275 | `_compute_for_ein` audit lookup | `pass` | unchanged | raw-? on `financial_ratios` still live (deferred to Phase 3) |
| H15a | 4157 | `get_school_tearsheet_data` enrollment_history | `result["..."] = []` | unchanged | DEAD path |
| H15b | 4181 | tearsheet accountability | `[]` | unchanged | DEAD path |
| H15c | 4196 | tearsheet state_averages | `{}` | unchanged | DEAD path |
| H15d | 4208 | tearsheet cpf_scores | `[]` | unchanged | DEAD path |
| H15e | 4222 | tearsheet financials_990 | `[]` | unchanged | DEAD path |
| H15f | 4231 | tearsheet financial_ratios | `{}` | unchanged | DEAD path |
| H15g | 4280 | tearsheet nearby_schools | `[]` | unchanged | DEAD path |
| H15h | 4298 | tearsheet census_tract | `{}` | unchanged | DEAD path |
| NEW-1 | 4010 | `get_federal_audit_by_id` | `df = pd.DataFrame()` | NEW (added since 2026-04-09) | silent-404 mask, same shape as H6/H7 |
| NEW-2 | 4110 | `get_headstart_by_id` | `df = pd.DataFrame()` | NEW (added since 2026-04-09) | silent-404 mask, same shape as H6/H7 |
| NEW-3 | 2287 | `get_nmtc_projects_by_cde` | `df = pd.DataFrame()` | inventory missed | raw-? site (no `adapt_sql`) hidden by silent except |

**Total DANGEROUS: 22.** Below the 30 hard-stop threshold (1.5×
inventory's 20). Phase split is correct.

## Resolved (per recent commits)

| Original ID | Function | Resolved by | Note |
|---|---|---|---|
| H1 | `get_schools` for-table fallback | `bf4ed8a` | try/except + for-loop removed; `adapt_sql` wraps query; exceptions now propagate |
| H2 | `get_school_by_id` | `bf4ed8a` | try/except removed; uses `adapt_sql` + `dict(row)` (dict(row) bug now loud) |
| H3 | `get_school_states` | `bf4ed8a` | try/except removed; query has no params, no raw-? |
| H4 | `get_school_summary` | `bf4ed8a` | try/except removed; query has no `?` placeholders |
| H5 | `upsert_school` | `bf4ed8a` | for-loop fallback removed; uses `adapt_sql` |
| H10 | `batch_update_school_census_tracts` | `bf4ed8a` | for-loop fallback removed; uses `adapt_sql` |

## RISKY readers (silent swallow, lower blast radius)

38 sites. Decoration-only target for Phase 1 Step 3 (stretch).

```
1749  get_fqhc                 → df = pd.DataFrame()
1763  get_fqhc_states          → states = []
1790  get_fqhc_summary         → result = {}
1847  get_ece_centers          → df = pd.DataFrame()
1863  get_ece_states           → states = []
1890  get_ece_summary          → result = {}
1913  _search_table            → return pd.DataFrame()
2550  get_peer_nmtc_projects   → df = pd.DataFrame()
2569  get_operator_schools     → df = pd.DataFrame()
2584  get_operator_fqhc        → df = pd.DataFrame()
2628  get_990_history          → df = pd.DataFrame()
2684  get_cdfis                → df = pd.DataFrame()
2700  get_cdfi_states          → states = []
2744  get_state_programs       → df = pd.DataFrame()
2760  get_program_states       → states = []
2843  get_service_gaps         → df = pd.DataFrame()
2881  get_enrollment_history   → df = pd.DataFrame()
2943  get_cdfi_awards          → df = pd.DataFrame()
2960  get_cdfi_award_states    → states = []
3127  get_documents            → df = pd.DataFrame()
3193  get_financial_ratios     → df = pd.DataFrame()
3352  get_market_rates         → df = pd.DataFrame()
3380  get_latest_rates         → df = pd.DataFrame()
3409  search_org               → df = pd.DataFrame()
3445  get_hud_ami              → df = pd.DataFrame()
3481  get_hud_fmr              → df = pd.DataFrame()
3522  get_cra_institutions     → df = pd.DataFrame()
3564  get_cra_assessment_areas → df = pd.DataFrame()
3616  get_sba_loans            → df = pd.DataFrame()
3640  get_sba_summary          → result = {}
3688  get_hmda_activity        → df = pd.DataFrame()
3739  get_bls_unemployment     → df = pd.DataFrame()
3789  get_bls_qcew             → df = pd.DataFrame()
3849  get_scsc_cpf             → df = pd.DataFrame()
3912  get_nmtc_coalition_projects → df = pd.DataFrame()
3996  get_federal_audits       → df = pd.DataFrame()
4035  get_federal_audit_programs → df = pd.DataFrame()
4093  get_headstart_programs   → df = pd.DataFrame()
```

## LOW-RISK / ACCEPTABLE

| Line | Function | Body | Why low-risk |
|---:|---|---|---|
| 69 | `_try_exec` SAVEPOINT branch | `cur.execute("ROLLBACK TO SAVEPOINT _safe")` | Intentional schema-migration tolerance |
| 74 | `_try_exec` non-SAVEPOINT branch | `pass` | Intentional schema-migration tolerance |

## Severity totals

| Severity | Count | Plan |
|---|---:|---|
| DANGEROUS | 22 | Phase 1 Step 2 — log-and-continue / log-and-raise / remove |
| RISKY | 38 | Phase 1 Step 3 (stretch) — `logger.exception(...)` decoration only |
| LOW-RISK / ACCEPTABLE | 2 | leave as-is |
| **Total** | **62** | matches `db.py` re-grep |

## Deferred (do not touch in Phase 1)

These are bug-class crossings flagged for later phases. Capture only;
do not chase mid-session.

- **H8 raw-? on H14**: `_compute_for_ein` audit lookup at db.py:3266 has raw `?` placeholders not wrapped in `adapt_sql`. Belongs to Phase 3 (raw-? sweep).
- **H8 dict(row)** at db.py:2273 — Phase 4 dict(row) sweep.
- **H11 dict(row)** at db.py:2982 — Phase 4 dict(row) sweep.
- **H12 dict(row)** at db.py:3034 — Phase 4 dict(row) sweep.
- **NEW-3 raw-?**: `get_nmtc_projects_by_cde` at db.py:2284 — raw `?` not wrapped in `adapt_sql`. Phase 3.

---

## Post-Phase-1 smoke test findings (2026-04-26)

Now that the silent masks are gone, called previously-masked db
functions directly against Postgres with valid IDs to surface what was
hidden. This sizes the Phase 4 architectural decision.

### Confirmed live bugs (now loud)

| Site | Function | Bug |
|---|---|---|
| db.py:1426 | `get_school_by_id` | `dict(row)` TypeError on psycopg2 tuple |
| db.py:2285 | `get_nmtc_project_by_id` (H8) | `dict(row)` TypeError on psycopg2 tuple |
| db.py:3008 | `get_user_notes` (H11) | `[dict(row) for row in ...]` TypeError |
| db.py:3064 | `get_bookmarks` (H12) | `[dict(row) for row in ...]` TypeError |

H11 and H12 only raise when there's data to return — empty cursor
swallows the bug. Test harness inserted a note and a bookmark to
trigger the cursor path; both raised.

### dict(row) full sweep — 7 sites

Fresh grep against db.py for `dict(row)` and `[dict(row) for`:

| Line | Function | Status |
|---|---|---|
| 1426 | `get_school_by_id` | LIVE (raised in smoke test) |
| 1491 | `get_census_tract` | likely LIVE (same shape; not smoke-tested) |
| 1685 | `get_nmtc_project_summary` | likely LIVE (single-row aggregate) |
| 2285 | `get_nmtc_project_by_id` | LIVE (raised) |
| 3008 | `get_user_notes` | LIVE (raised) |
| 3064 | `get_bookmarks` | LIVE (raised) |
| 3187 | `delete_document` | likely LIVE; ALSO has raw-? (no `adapt_sql`) |

Functions using `dict(zip(cols, row))` instead of `dict(row)` work
correctly on psycopg2 tuples and need no fix: `get_fqhc_by_id`,
`get_ece_by_id`, `get_fqhc_summary`, `get_school_summary` etc.

### Phase 4 Decision A vs B — resolved

**Decision B is correct.** 7 sites is well under the 25-site Decision A
threshold in `postgres-migration-plan.md`. A `row_to_dict(cur, row)`
helper + 7 site-specific edits is a 30-60 minute job. Decision A
(`RealDictCursor`) would touch every SELECT path's tuple-index access
across db.py — multiple sessions of mechanical work for no extra
benefit at this surface size.

### Other findings (deferred)

- **H9 `get_nearby_facilities` schools branch — signature drift bug**:
  calls `get_schools(active_only=False)` but `active_only` is not a
  current kwarg on `get_schools`. Raises `TypeError`. Not a Postgres
  bug; pre-existing. ~5 min fix; flag for a 30-min slot.
- **L2 `save_user_note` lastrowid**: returns 0 instead of real id on
  Postgres. Already in Phase 4 scope.

### What didn't surface (silent paths still safe)

H6, H7, NEW-1, NEW-2 returned full records correctly. Their callers
(FastAPI 404 wrappers) will continue to work as designed. Real
Postgres errors will now produce 500s with stack traces instead of
fake 404s — desired behavior.
