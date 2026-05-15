# cd-command-center — Security Review Profile

Custom security-scan instructions for this repo. Two uses:

1. **Claude Code `/security-review`** — copy this file's guidance into
   `.claude/commands/security-review.md` (or just paste it when asking Claude Code
   for a full-codebase review).
2. **GitHub Action** (`anthropics/claude-code-security-review`) — point the
   `custom-security-scan-instructions` input at this file. It gets appended to the
   audit prompt.

> Note: both `/security-review` and the Action are **diff-aware** — they only look
> at changed files. Do a one-time *full-codebase* review first (ask Claude Code to
> review the whole repo against this profile), then let the diff-based tooling
> handle everything after.

---

## Repo context

`cd-command-center` is a **data-pipeline repo**, not a typical web app. Stack:

- **ETL**: Python pipelines pulling from federal data APIs — FRED, Census/ACS, BLS,
  FAC (Federal Audit Clearinghouse), HRSA HSES, FDIC, CRA, HMDA, LIHTC, IRS 990/990-PF,
  CDFI Fund TLR. Orchestrated by `.github/workflows/pipeline.yml` (weekly cron +
  `workflow_dispatch` with a pipeline-picker input).
- **Data layer**: SQLAlchemy over SQLite (local + Render disk fallback) and
  PostgreSQL (intended prod). `db.py` treats non-`postgresql://` `DATABASE_URL`
  values as SQLite paths. Alembic migrations run on deploy (`alembic upgrade head`).
- **API**: FastAPI app served by `uvicorn api.main:app` on Render. **Publicly
  reachable.** Render auto-deploys on git push to the connected branch.
- **QA**: `validate.py --strict` is the data-quality gate.
- **Secrets**: `.env` (gitignored), `.env.example` lists `DATABASE_URL`,
  `FRED_API_KEY`, `CENSUS_API_KEY`, `BLS_API_KEY`, `FAC_API_KEY`,
  `HSES_USERNAME`/`HSES_PASSWORD`. Render uses `render.yaml` with `sync: false`.
  GitHub Actions uses repo Secrets.

There is **no user-facing authentication** on the API. Treat every API endpoint as
anonymously reachable from the internet.

---

## Priority surfaces — review these first

### 1. The FastAPI app (`api/`) — HIGHEST PRIORITY
This is the only internet-exposed component. For **every** endpoint:
- Does any path/query/body parameter flow into a SQL query? Confirm it goes through
  SQLAlchemy parameter binding — **never** f-strings, `.format()`, `%`, or string
  concatenation into `text()` / raw cursor execution.
- What can an anonymous caller read? Is any endpoint returning more than intended
  (full table dumps, internal IDs, row counts that leak structure)?
- CORS config — is `allow_origins` set to `*`? Scope it.
- Is the DB connection used by the API a least-privilege role, or does it have
  DDL/write rights it doesn't need for a read API?
- Error responses — do stack traces, SQL errors, or file paths leak to the client?
- Any endpoint that triggers a pipeline run, file read, or shell call = remote
  code/command execution risk. Flag immediately.

### 2. Secret handling — HIGH PRIORITY
- **Known issue**: a hardcoded `FRED_API_KEY` exists in older notes. Check whether
  any notes file containing it was ever committed (`git log --all -p -S` for the key
  fragment). If it was committed, the key is burned — rotate it and scrub history.
- No secret should appear in: source files, committed notes/markdown, `render.yaml`,
  workflow YAML `run:` blocks, log output, or API error responses.
- Confirm `.env`, `*.sqlite`, `data/`, and any local-credential files are gitignored.
- Flag any `print()`/`logger` call that could emit a connection string or key.

### 3. SQL construction in ETL pipelines — HIGH PRIORITY
- Pipelines ingest external (federal) data and write it to the DB. Every INSERT/
  UPSERT/UPDATE must use bound parameters, not interpolated values — malformed or
  hostile field values in source data should never be able to alter a query.
- `validate.py` and any dynamic query builders: same rule.
- Check for `eval()`, `exec()`, `pickle.load()`, or `pd.read_pickle()` on anything
  derived from downloaded data.

### 4. GitHub Actions workflow (`.github/workflows/pipeline.yml`) — MEDIUM
- The `workflow_dispatch` pipeline-picker input: confirm it's a `choice` type with a
  fixed option list. If it's a free-text `string` interpolated into a `run:` step
  (`python pipeline.py ${{ inputs.pipeline }}`), that's a script-injection vector —
  pass it as an `env:` var instead.
- No step should `echo` secrets or the contents of `DATABASE_URL`.
- Check `permissions:` — the workflow should have the minimum scope it needs.
- Pin third-party actions to a full commit SHA, not a floating tag.

### 5. Dependencies & deploy config — MEDIUM
- Run `pip-audit` against `requirements.txt`; flag known CVEs in fastapi, uvicorn,
  sqlalchemy, alembic, pandas, requests, and transitive deps.
- `render.yaml`: confirm no secret has `sync: true` or a literal value.
- `alembic upgrade head` runs on every deploy — note (operational, not strictly
  security) that a bad migration auto-applies to prod with no gate.
- HSES uses username/password basic auth — confirm it's only ever read from env and
  sent over HTTPS, never logged.

---

## Tuning — what NOT to flag

This is a small internal data tool, not a multi-tenant SaaS. To keep signal high,
**deprioritize or suppress**:

- Denial-of-service / rate-limiting findings on the API (no auth layer expected yet;
  it's a known, accepted gap — separate from injection risk).
- Generic "missing input validation" with no demonstrated injection or exposure path.
- Missing security headers (CSP, HSTS) on the API unless it serves HTML to browsers.
- Verbose-logging findings in local-only scripts that never run in prod.
- Timing-attack / crypto-nitpick findings on code that handles no auth secrets.

**Always flag regardless of the above**: anything that exposes a secret, allows SQL
injection, allows command/code execution, or returns more data to an anonymous
caller than intended.

---

## After the first full pass

Once the one-time full review is clean:
- Run `/security-review` in Claude Code before pushing any non-trivial change.
- If you adopt a PR workflow, wire up the GitHub Action so every PR gets reviewed
  automatically. If you push straight to `main`, the Action does nothing for you —
  the local `/security-review` habit is your control instead.
