# CD Command Center

Backend + ETL for a community-development-finance deal-origination platform.
Consolidates ~30 federal/state data sources (schools, FQHCs, NMTC projects,
census demographics, 990-PFs, CRA, HMDA, FRED rates, Single Audits, Head
Start, and more) behind a single FastAPI service backed by SQLite locally
and PostgreSQL in production.

The Next.js frontend lives in a sibling repo: `cd-command-center-dashboard`.

## Quick start (local)

Requirements: Python 3.11.

```bash
pip install -r requirements.txt
cp .env.example .env             # fill in API keys you have
alembic upgrade head             # build/upgrade the SQLite schema
uvicorn api.main:app --reload    # http://localhost:8000/docs
```

To populate the database, run the ETL:

```bash
python etl/run_pipeline.py                    # everything that auto-downloads
python etl/run_pipeline.py --states GA TX     # state-filtered
python etl/run_pipeline.py --only schools fqhc 990
```

`validate.py` runs automatically at the end of the pipeline. To run it
on its own:

```bash
python validate.py            # full sweep
python validate.py schools    # one target
python validate.py --strict   # exit 1 on any issue
```

## Repository layout

| Path | What's there |
|------|--------------|
| `api/` | FastAPI app + per-resource routers |
| `db.py` | All database access — single point of change for SQLite↔Postgres |
| `etl/` | Per-source ingestion scripts (`fetch_*.py`, `load_*.py`) and `run_pipeline.py` |
| `validate.py` | Per-table data-quality checks (row counts, null rates, ranges, FK joins) |
| `migrations/` | Alembic — `db.init_db()` is frozen at `f19ded25b983`; new tables go here |
| `models/` | Pydantic response schemas |
| `utils/` | Geo, maps, export, PDF extraction helpers |
| `analyses/` | Ad-hoc notebooks/scripts (not part of the pipeline) |
| `archive/` | Retired code (the original Streamlit app) |
| `data/raw/` | Manually-downloaded source files (gitignored except small seed CSVs) |

## Required environment variables

See `.env.example`. Only `DATABASE_URL` is strictly required; the rest are
needed only for specific data sources:

- `FRED_API_KEY` — FRED market rates (free)
- `CENSUS_API_KEY` — ACS demographics (optional, raises rate limits)
- `BLS_API_KEY` — BLS employment data (optional)
- `FAC_API_KEY` — Federal Audit Clearinghouse (free, https://api.data.gov/signup/)
- `HSES_USERNAME` / `HSES_PASSWORD` — Head Start PIR (account required)

## Production deployment

`render.yaml` deploys this as a Render web service:
- builds with `pip install -r requirements.txt && alembic upgrade head`
- starts `uvicorn api.main:app`
- mounts a 1 GB disk at `/data` for SQLite (or set `DATABASE_URL` to a
  Postgres URL in the Render dashboard for prod)
- env vars are declared with `sync: false`; set values in the Render dashboard

`DATA_REFRESH_SCHEDULE.md` documents the daily/monthly/quarterly refresh
cadence per source.

## Migrating SQLite → PostgreSQL

`db.py` is the single layer that handles dialect differences. Set
`DATABASE_URL` to a `postgresql://...` URL, run `alembic upgrade head` on
the empty Postgres DB, then re-run the ETL.

## More

- Architecture details, build phases, conventions, and per-source command
  examples: `CLAUDE.md`
- Refresh schedule per source: `DATA_REFRESH_SCHEDULE.md`
- Agent guardrails: `AGENTS.md`
