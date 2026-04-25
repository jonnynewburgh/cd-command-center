# Postgres migration plan

Forward-looking execution plan derived from the diagnostic
audit at `docs/debug/postgres_readiness_inventory_2026-04-22.md`.
This is the sequencing doc; the inventory is the evidence doc.
When they disagree, re-run the inventory.

---

## Goal

Get the FastAPI + Postgres stack to a deployable state. The
readiness inventory documents 9 known Postgres-compat bug classes
across ~70 confirmed sites, with an honest 28-44 hour effort
estimate spread across 8-12 sessions. This plan reorders that
work into 6 phases with explicit prerequisites so each phase's
verification can be trusted — the prior approach (per-function
fixes, per-symptom sessions) kept rediscovering bug classes
because the silent-handler substrate hid every defect from
verification. Phasing trades a small amount of mechanical
overhead (re-greps, working agreement) for a large amount of
convergence reliability.

---

## Sequence

### Phase 1 — Silent exception handler audit (bug class 7)

- **Objective.** Eliminate the masking layer so subsequent phases
  can be verified.
- **Bug classes addressed.** Class 7 (silent exception handlers).
- **Estimated effort.** 4-6 hours. Inventory says ~6-10 if every
  RISKY reader gets touched; this phase explicitly scopes to the
  ~20 DANGEROUS sites in `db.py` plus minimal logging
  decoration on the ~40 RISKY readers.
- **Sessions.** 1 dedicated session, 4-6 hours.
- **Prerequisites.** None.
- **Success criteria.**
  - 20 DANGEROUS handlers (H1-H15 in the inventory) either log
    `logger.exception(...)` and continue, log and re-raise, or
    have the handler removed.
  - The ~40 RISKY readers in `db.py` decorated with
    `logger.exception(...)` even where the silent default
    behavior is preserved.
  - A failing query against any `get_*` function in `db.py`
    surfaces in logs (verified by deliberately running one
    raw-? query against Postgres and confirming the trace
    appears).
  - `archive/app.py` and the `get_school_tearsheet_data`
    section explicitly classified DEAD or REVIVED — no
    in-between.
- **Hard stops.**
  - Crossing into a fix for raw-? or dict(row) mid-session.
    Capture in this plan's Open Questions, do not chase.
  - Discovering more than ~30 DANGEROUS sites on re-grep
    (1.5x the inventory). If that happens, stop and re-scope:
    the phase split was wrong.

### Phase 2 — Operational logging fix (bug class 4, L1 only)

- **Objective.** Restore `data_loads` pipeline run history on
  Postgres so subsequent phases can be verified by re-running
  the pipeline.
- **Bug classes addressed.** Class 4 (cur.lastrowid), one site
  only: L1 `db.py:1292 log_load_start`.
- **Estimated effort.** 1-2 hours.
- **Sessions.** Half-session (≤2 hours), or front-load into the
  Phase 3 session if scope discipline allows (it usually
  doesn't — keep separate).
- **Prerequisites.** Phase 1 (otherwise the verification "did
  log_load_finish actually update the row?" can't be trusted).
- **Success criteria.**
  - `log_load_start` returns a non-None `run_id` on Postgres
    (rewrite to `INSERT ... RETURNING id` + `cur.fetchone()[0]`).
  - `log_load_finish` UPDATE matches exactly one row.
  - `validate.py`'s `data_loads` check returns sensible counts
    after a manual pipeline run on Postgres.
  - Spot-check: `SELECT id, pipeline, status FROM data_loads
    ORDER BY id DESC LIMIT 5` shows recent runs transitioning
    `running → success`.
- **Hard stops.**
  - Touching L2 `save_user_note` or L3 `save_document` in this
    phase. They belong to Phase 4 because they're entangled with
    the dict(row) decision.
  - Discovering a fourth lastrowid site. If found, capture in
    Open Questions and continue.

### Phase 3 — Raw-? sweep + INSERT OR IGNORE remaining + chained-execute remaining (bug classes 1, 2, 3)

- **Objective.** Close the three "mechanical templated rewrite"
  bug classes that share the `adapt_sql` family of fixes.
- **Bug classes addressed.** Class 1 (~21 raw-? remaining), class
  2 (2 remaining: `state_programs` schema work + verify
  `load_cra_lending.py` branch), class 3 (~8 chained-execute
  remaining).
- **Estimated effort.** 8-12 hours.
- **Sessions.** 3 sessions, ~3-4 hours each. Suggested split:
  - Session 3A: ETL raw-? blockers + chained-execute in
    fetch_bmf_eins / fetch_990_irs (both ACTIVE).
  - Session 3B: FastAPI raw-? sites (A1 loud-crash + silent-404
    overlap) — explicitly does not yet fix dict(row), only
    raw-?.
  - Session 3C: remaining LATENT chained-execute + remaining
    INSERT OR IGNORE (`state_programs` schema migration).
- **Prerequisites.** Phases 1 and 2.
  - Phase 1 because otherwise a raw-? fix that "looked
    successful" might actually still be silently masked.
  - Phase 2 because each session ends with a pipeline run as
    its convergence test, which requires `data_loads` logging
    to work.
- **Success criteria.**
  - Zero raw-? sites in active code paths (re-grep at session
    start of each sub-session).
  - Zero `INSERT OR IGNORE` sites in active code paths
    (`state_programs` rewritten or table re-keyed; `load_cra_lending.py`
    SQLite branch unchanged because it's correctly gated).
  - Zero chained `cur.execute(...).fetchone()` sites in active
    code paths. LATENT sites in PDF-scraping scripts may remain
    if their upstream conditions still block end-to-end run.
  - `python etl/run_pipeline.py --dry-run` runs cleanly on
    Postgres.
- **Hard stops.**
  - Discovering a dict(row) bug mid-session. Note it in Open
    Questions for Phase 4. Do not fix.
  - Re-grep finds more than ~32 raw-? sites (1.5x the
    inventory). Pause and re-scope.
  - `state_programs` schema migration surfaces unexpected
    duplicate-row data. That's a 1-hour-minimum side-quest;
    split into its own micro-session rather than continuing.

### Phase 4 — Remaining cur.lastrowid + dict(row) sweep (bug classes 4, 5)

- **Objective.** Make the FastAPI dashboard pass an
  end-to-end smoke test on Postgres.
- **Bug classes addressed.** Class 4 (L2 `save_user_note`,
  L3 `save_document`), class 5 (dict(row) — full sweep,
  inventory currently incomplete).
- **Estimated effort.** 3-5 hours, with high variance depending
  on the architectural decision in the first 30 minutes:
  - Decision A: switch `get_connection()` to
    `psycopg2.extras.RealDictCursor`. Larger blast radius
    (every SELECT path uses tuple-index access; sweeping
    them takes ~4-8 hours of mechanical work).
  - Decision B: introduce `row_to_dict(cur, row)` helper +
    fix sites individually as they surface. Smaller blast
    radius per session, but pushes some sites into Phase 6
    "found during reconciliation."
  - Recommend Decision B for this phase, with Decision A
    revisited only if Phase 6 keeps surfacing new dict(row)
    sites.
- **Sessions.** 1 session, 3-5 hours.
- **Prerequisites.** Phase 3 (because dict(row) sites and raw-?
  sites are commonly co-located in the same function — fixing
  one without the other leaves the function half-broken and
  re-verification is wasted work).
- **Success criteria.**
  - `save_user_note` and `save_document` return a non-None id
    on Postgres (rewritten with `INSERT ... RETURNING id`).
  - 6 confirmed dict(row) readers fixed: `get_school_by_id`,
    `get_fqhc_by_id`, `get_ece_by_id`, `get_nmtc_project_by_id`,
    `get_user_notes`, `get_bookmarks`.
  - Fresh full-repo grep for `[dict(row) for row in cur` and
    `dict(row)\b` and `conn.row_factory` — net new sites
    classified DEAD/LATENT/ACTIVE.
  - Smoke test: hit `/notes`, `/bookmarks`, `/schools/{id}`,
    `/fqhc/{id}`, `/ece/{id}`, `/nmtc/{id}` against Postgres
    and confirm non-empty payloads where data exists.
- **Hard stops.**
  - dict(row) re-grep finds more than ~25 sites. That's the
    Decision A signal — pause, switch to RealDictCursor, and
    re-plan Phase 4 as a multi-session sweep.
  - The tearsheet S2 `row_factory = sqlite3.Row` site requires
    its own sub-decision (revive or delete). Capture in Open
    Questions; do not chase mid-session.

### Phase 5 — SQLite-only function rewrites (bug class 6)

- **Objective.** Make `validate.py` and the BLS scripts portable
  to a Postgres-only environment.
- **Bug classes addressed.** Class 6 (3 SQL function sites + 3
  direct-sqlite3 sub-bugs).
- **Estimated effort.** 2-4 hours.
- **Sessions.** 1 session.
- **Prerequisites.** Phase 4 (so the smoke test is meaningful;
  validate.py's data_loads check requires Phase 2 anyway).
- **Success criteria.**
  - `validate.py` runs end-to-end on Postgres without
    `datetime('now', ...)` syntax errors.
  - `python etl/fetch_bls_unemployment.py --all-counties` and
    `python etl/fetch_bls_qcew.py --all-counties` route
    through `db.get_connection()`, not hardcoded
    `sqlite3.connect()`.
  - `get_school_tearsheet_data` either deleted (DEAD path
    confirmed) or rewritten to not poison `row_factory` (LIVE
    path).
  - Vestigial `import sqlite3` removed from
    `etl/load_census_tracts.py` and `etl/load_sample_data.py`.
- **Hard stops.**
  - The `--all-counties` re-route requires a `db.get_county_fips_from_tracts()`
    helper — that's a small new db.py function, fine. If it
    turns out to need additional cross-cutting helpers, split
    out as a micro-session.

### Phase 6 — Schema reconciliation + data reconciliation (bug classes 8, 9)

- **Objective.** Achieve cross-backend parity. SQLite and
  Postgres serve the same query the same answer.
- **Bug classes addressed.** Class 8 (schema drift, including
  vestigial `charter_schools`), class 9 (data drift on ≥6
  known tables: `irs_990`, `schools`, Atlanta/Fulton search,
  `cdes`, `fqhc`).
- **Estimated effort.** 4-8 hours, with significant variance
  depending on what's surfaced during reconciliation.
- **Sessions.** 1-2 sessions.
- **Prerequisites.** Phases 1-5 all complete. Reconciliation
  before the loaders are reliable just sets up the next
  divergence.
- **Success criteria.**
  - `charter_schools` table dropped (A4); for-table fallback
    loops in `db.py` (H1, H5, H10) removed.
  - Per-table audit complete for the 6 known drifted tables;
    canonical-source decision recorded; resync runs executed.
  - `validate.py` reports parity for every shared table on
    both backends.
  - Top 10 `get_*` functions return identical row counts on
    both backends for a fixed query input.
- **Hard stops.**
  - Discovering a new bug class during reconciliation. Capture
    in Open Questions; do not fix mid-phase. Reconciliation
    findings are the leading indicator that a Phase N earlier
    sweep was incomplete.
  - Per-table reconciliation surfacing data quality issues
    that go beyond Postgres compat (e.g. wrong source year,
    wrong state filter). Split into a separate data-quality
    workstream.

---

## Total

Estimated **22-37 hours across 7-9 dedicated sessions.** Aligns
with the inventory's 28-44 estimate (lower because phasing allows
concurrent work within a single session — e.g. Phase 3A handles
raw-? + chained-execute in the same ETL file at the same time
rather than two passes).

Stretch upper bound if Phase 4 forces Decision A
(RealDictCursor): add 4-6 hours and 1 extra session, putting
total at **26-43 hours / 8-10 sessions.**

---

## Anti-patterns observed

- **Per-function fix sessions discover new bug classes faster
  than they close them.** Phase-based sweeps required.
- **Silent exception handlers caused verification to lie.** Don't
  ship Postgres-compat fixes during sessions where Phase 1's
  active-bug-masking subset isn't yet complete.
- **Initial inventory always under-counts (1.5-2x rule observed).**
  Each phase should re-inventory at session start, not rely on
  prior counts. Apply the 1.5x rule to budget per phase.
- **Scope expansion mid-session is correct when staying within
  the current bug class but always wrong when crossing into a
  new bug class.** The single biggest cause of half-finished
  sessions has been bug-class-crossing scope creep.
- **Pick the right target for the slot you have.** When a
  30-minute slot is available, pick a target that fits the slot.
  Don't start a complex function fix in 30 minutes. Don't start
  Phase 1 audit in 30 minutes. Use the session-length guidance
  below to match target to budget. Punt un-shippable-in-30
  targets to longer blocks.

---

## Working agreement

- **One phase per session minimum** for the phases that require
  sustained focus (Phase 1 active-bug-masking subset, Phase 6).
  For phases that decompose cleanly into per-function/per-file
  units (Phases 2-5), it is fine to interleave 30-minute slots
  inside a phase, as long as session boundaries fall on
  function/file boundaries — never mid-fix.
- **Re-grep at start of each phase.** Inventory drift is normal.
- **Phase-specific session-length rules:**
  - **Phase 1 (silent handlers):** the ~13 DANGEROUS handlers
    masking known active bugs need a **2-3 hour minimum block**
    — they require holding both the failing query and its
    masking handler in head at the same time, and switching
    contexts mid-handler causes regressions. The remaining
    ~7 DANGEROUS + ~40 RISKY handlers can be done in any
    session length once that subset is closed.
  - **Phase 2 (L1 logging fix):** 1-2 hours total; can be split
    into two 30-minute slots (slot 1: diagnose + propose
    `INSERT ... RETURNING id` shape; slot 2: apply + verify a
    pipeline run on Postgres updates `data_loads` correctly).
  - **Phase 3 (raw-? + INSERT OR IGNORE + chained-execute):**
    per-function or per-file fixes. Simple functions fit
    30-minute slots; reserve **60-90 minute** blocks for complex
    functions with COALESCE preserve-if-null logic
    (`upsert_nmtc_project`, `upsert_lea_accountability`).
  - **Phase 4 (lastrowid + dict(row)):** lastrowid sites fit
    30-min slots. The dict(row) Decision A/B spike requires a
    **60-90 min dedicated block** (full-repo re-grep + read
    every confirmed call site + commit to an approach).
    Application of the chosen approach can be batched into
    30-min slots after.
  - **Phase 5 (SQLite-only functions):** 30-min slots per site.
  - **Phase 6 (schema + data reconciliation):** **2-3 hour
    blocks.** Holding both backends' state — SQLite row counts,
    Postgres row counts, canonical-source decision per table —
    requires sustained focus that 30-min slots break.
- **After each phase, update this plan doc and the project
  pipeline doc** — record what shipped, what slipped to the
  next phase, what new sites surfaced. Refresh the 30-minute
  slot menu (below) so the next available slot can grab a
  target without re-diagnosing.
- **Verification is the bottleneck, not editing.** Each phase's
  success criteria are the binding constraint. If the
  verification step is skipped, the phase is not done — even
  if every site listed is touched.

---

## Session length guidance

Match the work to the slot you have, not the other way around.

**30-minute slots work for:**
- Single-function Postgres-compat fixes when the function is
  simple (no COALESCE preserve-if-null logic, no shared
  helpers, ON CONFLICT key obvious from the table schema).
- Diagnostic / inventory passes for narrow, scoped questions
  (e.g. "how many lastrowid sites", "does X table have a
  UNIQUE constraint").
- Plan and doc updates.
- Verification of prior session's work (pipeline run, smoke
  test of 2-3 endpoints).
- Phase 5 SQL function rewrites (each site is independent and
  bounded).
- Any 30-minute slot menu item below.

**60-90 minute slots required for:**
- dict(row) Decision A/B spike (full-repo re-grep + read every
  confirmed call site + commit to an approach).
- Complex function fixes with COALESCE preserve-if-null logic
  (`upsert_nmtc_project`, `upsert_lea_accountability`).
- Phase 1 single-handler resolution if the handler touches
  multiple callers (need to read each caller's failure mode
  before deciding log-and-continue vs. log-and-raise).
- `state_programs` schema migration (B5 site 1) — schema
  decision, dedupe of accumulated rows, function rewrite, reload.

**2-3 hour blocks required for:**
- Phase 1 silent handler audit (the ~13 active-bug-masking
  subset). This is the one piece that genuinely cannot flex —
  shorter slots cause regressions because each handler's fix
  depends on knowing what the other handlers around it are
  doing.
- Phase 6 schema + data reconciliation (per-table audit +
  canonical-source decision + resync run).
- Any phase that requires cross-backend mental state (holding
  both SQLite and Postgres behaviors in head simultaneously).

---

## Open questions for future-me

- **Is FastAPI+Postgres actually the strategic target,** or
  should this effort be deprioritized in favor of finishing
  pipelines on Streamlit+SQLite? (Strategic question, not
  addressed by this plan. Affects whether 22-37 hours is worth
  spending now vs. deferring.)
- **Once Postgres is ready, what's the cutover plan from
  Streamlit+SQLite to FastAPI+Postgres?** (Out of scope for
  this plan; needs its own session. Touches the dashboard repo
  separately.)
- **Tearsheet revival decision.** `get_school_tearsheet_data`
  (db.py:4196) is currently DEAD on Postgres because of the
  `row_factory = sqlite3.Row` poisoning. Either revive it
  (Phase 5 work + Phase 4 dict(row) sweep includes its 8
  per-section handlers) or delete it. No middle ground.
- **dict(row) architecture decision (Decision A vs B).** Should
  ideally be made at the start of Phase 4, but the inputs for
  the decision are still unclear: a fresh full-repo grep is
  needed first. Recommend a 30-min spike at the start of Phase
  4 dedicated to that grep before committing to A or B.

---

## 30-minute slot menu

Grab one of these when a 30-minute slot opens up. Each is sized
to ship in a single 30-minute block (fix + verify + commit).
Listed in rough order of independence — top items have the
lowest risk of scope expansion mid-session.

- `upsert_cde_allocation` (B1) — likely simple, similar shape
  to `upsert_cdfi_award` shipped in `fa915ef`.
- `upsert_enrollment_history` (B1) — likely simple; history
  tables are usually flat and the (nces_id, school_year)
  conflict key is already declared.
- L1 `log_load_start` lastrowid fix (Phase 2, first half) —
  rewrite to `INSERT ... RETURNING id`. Diagnose + propose
  shape only; second slot applies + verifies.
- L2 `save_user_note` lastrowid fix (Phase 4) — bare INSERT,
  RETURNING id rewrite. Verify FastAPI `/notes` POST returns
  a non-null `note_id`.
- L3 `save_document` lastrowid fix (Phase 4) — same shape as
  L2.
- `validate.py:372` datetime function rewrite (Phase 5) —
  branch on `_IS_POSTGRES`; mirror the `db.py:3400` pattern
  (`CURRENT_DATE - INTERVAL '%s days'`).
- Any Phase 3 chained-execute fix from the LATENT list:
  `compute_financial_ratios.py:70`, `fetch_lea_accountability.py:291-294`,
  `fetch_nmtc_award_books.py:287-290 / 318 / 319-321`,
  `patch_pct_asian.py:117-120 / 122` (note: 117-120 is also
  CHAIN+PLACEHOLDER and rides along with an `adapt_sql` wrap).

**Maintenance rule.** After any item ships, remove it from this
menu and replace it with a fresh ready-to-grab target from the
inventory. Keep ~3-5 items live so the next available slot can
grab one without re-diagnosing. If the menu drops below 3, that
is a signal to spend a 30-min slot on a re-grep + repopulate
pass before continuing fixes.
