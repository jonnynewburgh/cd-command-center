# archive/

Retired code kept for reference only. Nothing here is on the runtime path
or imported by `api/`, `etl/run_pipeline.py`, or `db.py`.

## Contents

- **`app.py`** — original Streamlit dashboard. Replaced by the FastAPI
  backend in `api/` plus the Next.js frontend in the sibling
  `cd-command-center-dashboard` repo.
- **`tmp_make_ga_authorizer_files.py`** — one-off script used while
  seeding the charter authorizer registry tables.
- **`etl/fetch_nces_charter_schools.py`** — superseded by
  `etl/fetch_nces_schools.py --charter-only`.
- **`etl/fetch_edfacts.py`** — superseded by `etl/fetch_edfacts_auto.py`,
  which is what `run_pipeline.py` calls.
- **`etl/fetch_state_accountability.py`**, **`etl/fetch_lea_accountability.py`**,
  **`etl/load_lea_accountability.py`** — earlier per-state accountability
  loaders. The pipeline uses EDFacts (`fetch_edfacts_auto.py`) instead.
- **`etl/fetch_nmtc_award_books.py`** — preceded
  `etl/load_nmtc_awards_2024_2025.py`.
- **`etl/audit_backend_parity.py`** — debug/parity script from the
  Streamlit-to-FastAPI migration.
- **`etl/explore_fac.py`** — exploratory script used while building
  `etl/fetch_fac.py`.

If you need to delete these, do so in a single commit so the history is
clear; nothing here is referenced by anything outside of `archive/` and
historical docs.
