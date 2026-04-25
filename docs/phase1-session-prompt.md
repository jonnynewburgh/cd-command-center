# Phase 1 session-opening prompt

Drop-in prompt for the next dedicated 2-3 hour block on the silent
handler audit (Phase 1 of `docs/postgres-migration-plan.md`). Drafted
2026-04-25, post-commits `bf4ed8a` / `11097d6` / `7d8382a` — those
already removed the silent excepts on H1, H2, H5, H10, so the
DANGEROUS count is closer to ~10-13, not the inventory's 20.

Re-read this prompt and the migration plan's session-length section
before starting. Edit before pasting if priorities have shifted.

---

Phase 1 from `docs/postgres-migration-plan.md`: silent exception
handler audit. **Block 2-3 hours minimum.** This is the
prerequisite-clearing work for the rest of the migration — every
prior phase's verification could lie because these handlers were
swallowing the very errors that proved the fix was wrong. Read the
plan + the inventory at
`docs/debug/postgres_readiness_inventory_2026-04-22.md` (sections
2.1, 2.5, 2.6) before touching code.

**Step 0 — re-inventory (15 min, mandatory).** The H1-H15 list in
the inventory is 12+ days old and several handlers were already
removed by recent commits:
  - `bf4ed8a` removed for-table fallback loops, killing H1, H2, H5,
    H10's silent excepts.
  - `11097d6` fixed raw-? in H8 (`get_nmtc_project_by_id`) and H11
    (`get_user_notes`) — but their surrounding silent excepts
    remain and now have a different bug class (dict(row)) to
    swallow.

  First action: re-grep `db.py` for `except Exception` (and bare
  `except:`) and rebuild the DANGEROUS / RISKY / LOW-RISK /
  ACCEPTABLE classification fresh. Compare against H1-H15 in the
  inventory; record what's been resolved, what still exists, and
  any new sites the prior inventory missed. Save the refreshed
  list as `docs/debug/silent_handlers_refresh_<today>.md` — the
  next session needs this evidence even if the fix sweep gets
  interrupted.

  Hard stop on this step: if the refreshed DANGEROUS count exceeds
  ~30 (1.5x the inventory's 20), pause and re-scope. The phase
  split was wrong.

**Step 1 — wire logging (10 min).** `db.py` has no logger. Add at
the top of the file (after imports):
```python
import logging
logger = logging.getLogger(__name__)
```
Decide whether to add a default `logging.basicConfig(...)`
somewhere (probably no — that's caller's job; just expose the
logger). Confirm by running
`python -c "import db, logging; logging.basicConfig(level=logging.DEBUG); ..."`
and triggering one known-bad query against Postgres (a raw-? site
that you haven't fixed yet — there are several
`pd.read_sql_query(query, ...)` sites in `db.py` lines 1505, 1562,
etc. that lack `adapt_sql`). Confirm the exception trace appears.

**Step 2 — DANGEROUS sweep (2 hours).** For each remaining
DANGEROUS handler from your refreshed list, decide one of three:
  - **log-and-continue** — replace `pass` / `continue` / `return {}`
    with `logger.exception("<context>")` then the original silent
    default. Use this when the silent default is behaviorally
    correct (e.g., empty state-filter dropdown is acceptable
    while the bug is being fixed) but the diagnostic blackout
    isn't.
  - **log-and-raise** — `logger.exception(...); raise`. Use this
    when the silent default is masking a bug whose silent
    presentation is itself harmful (e.g., empty bookmarks sidebar
    with no error to investigate).
  - **remove the handler entirely** — let the exception propagate
    naturally. Use this when the try/except adds no value.

  Each decision needs to read the call site (FastAPI router, ETL
  script, etc.) to understand the silent default's downstream
  effect. Don't decide blindly. Record decision + reason inline in
  the commit message.

  Priority order (highest first, based on what masks A1 / known
  live bugs):
  - H6 `get_fqhc_by_id` — silent-404 mask
  - H7 `get_ece_by_id` — silent-404 mask
  - H8 `get_nmtc_project_by_id` — silent-404 mask (raw-? half
    already fixed in `11097d6`; handler decision still pending)
  - H11 `get_user_notes` — silent-empty mask (raw-? half already
    fixed in `11097d6`; handler decision still pending)
  - H12 `get_bookmarks` — silent-empty mask (the dict(row) bug
    behind it)
  - H9 `get_nearby_facilities` (4 handlers) — masks the entire map
  - H3 `get_school_states`, H4 `get_school_summary` — silent
    dropdown / silent summary blank
  - H13 `is_bookmarked` — low priority (False is a valid answer)
  - H14 `_compute_for_ein` audit lookup — tolerable behaviorally
  - H15 `get_school_tearsheet_data` (8 handlers) — DEAD path;
    classify DEAD or REVIVED, no in-between

**Step 3 — RISKY decoration (1 hour, stretch).** ~40 RISKY readers
throughout db.py (inventory's section 2.2). Add `logger.exception(...)`
decoration only — keep the silent-default behavior, just stop
blacking out the diagnostics. If time runs out before this step,
ship Step 2 alone — the DANGEROUS work is the load-bearing part.

**Step 4 — convergence test (15 min).** Trigger one deliberately-
broken query against Postgres for a function whose handler you
just fixed. Confirm the log line appears with a stack trace. Then
run `validate.py` and `python etl/run_pipeline.py --dry-run` —
both should still pass (this phase doesn't change any data flow).

**Hard stops — do NOT cross any of these:**
- Fixing raw-? or dict(row) bugs you encounter while reading
  handler call sites. Capture them in a single "deferred to Phase
  3/4" note in the commit message; do not chase. Bug-class
  crossing is the #1 cause of half-finished sessions per the
  migration plan's anti-patterns.
- Touching `archive/app.py`. Out of scope.
- Refactoring unrelated code in db.py. Whitespace-only changes
  from auto-formatters are fine; logic changes outside handler
  bodies are not.
- Removing a try/except whose silent default is intentional
  flow-control (inventory's ACCEPTABLE category — typically
  attribute checks like `try: x.foo except AttributeError: x = None`).
  Touching ACCEPTABLE handlers wastes time and adds noise.
- Working past the 3-hour mark. If you're not done by then, pick
  a clean stopping point (end of a handler decision, not
  mid-handler), commit what you have with a
  "WIP: Phase 1 partial — N of M DANGEROUS resolved" message, and
  stop. Phase 1 explicitly tolerates being split if the splits
  fall on handler boundaries.

**Commit message:** `Phase 1: silent handler audit — N DANGEROUS
resolved` (full or partial). Include in the body: per-handler
decision (log+continue / log+raise / remove) with one-line reason,
refreshed inventory file path, deferred-bug list, convergence test
result.

**Ship as a single commit** if the session completes Steps 0-2.
Multiple commits OK if you split mid-session for natural
checkpoints — but don't push individual handler fixes; bundle
them.
