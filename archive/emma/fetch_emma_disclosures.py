"""
etl/fetch_emma_disclosures.py — Pull annual audited financial statement PDFs
from EMMA (MSRB) for 501(c)(3) conduit borrowers.

================================================================================
!! BLOCKED — DO NOT RUN AGAINST emma.msrb.org !!

MSRB's Terms of Use (reviewed 2026-05-18) explicitly prohibit:
  "use or allow others to use any data mining, crawling, 'scraping', robot or
   similar automated or data gathering or extraction method, or any manual
   process, to access, acquire, monitor or copy any portion of the Website,
   Content or Services, or otherwise systematically download or store Content"

This script's design (enumerate obligors → list disclosures → download PDFs)
is exactly the activity prohibited above. Politeness (sleeps, single-thread,
backoff) does not cure the violation — the ToS bans the activity itself, not
just aggressive variants.

The Alembic migration, db.py accessors, and the fetch_990_irs.py EIN-query
hook are kept in place as latent infrastructure for a legitimate source:
  - MSRB's paid Continuing Disclosure subscription feed
  - Commercial relicensors (DPC DATA, Merritt Research, SOLVE, Munistatistics)
  - Bloomberg / Refinitiv (if a seat is available)
  - Direct outreach to borrowers / trustees

DO NOT fill in the ENDPOINTS dict against emma.msrb.org. Repurpose this file
only when a licensed data path is in place; at that point most of the polite-
HTTP plumbing is irrelevant anyway (vendor APIs supply their own auth + SLAs).

  -- Blocked 2026-05-18 after MSRB ToS review
================================================================================

Phase 1 of the EMMA ETL. See docs/emma_etl_brief.md for the full scope.

DATA SOURCE: EMMA — Electronic Municipal Market Access (emma.msrb.org)
  Public website, no API key. The EMMA UI is backed by XHR JSON endpoints
  which we use directly (HTML scraping is the fallback if a JSON endpoint
  isn't available for a given screen).

POLITE-SCRAPER POLICY (do not violate):
  - Single concurrent request (no thread/async fan-out).
  - >= 1 s sleep between requests.
  - Exponential backoff with jitter on 429 / 5xx (max 5 retries).
  - Descriptive User-Agent identifying the ETL + a contact email.
  - On-disk cache for issuer lists / document indexes so re-runs don't
    re-hit EMMA for unchanged metadata.
  - PDF downloads skipped when the local file already exists and is
    non-empty (SHA256 still recorded).
  - Stop and surface to the user if rate-limiting becomes aggressive or
    if MSRB returns a ToS notice in headers/body.

ENDPOINTS (Phase 1 — TO BE FILLED IN AFTER UI/DEVTOOLS RECON):
  The four routes the script needs. Capture them by opening the EMMA UI
  in a browser with the Network tab filtered to XHR/Fetch, then paste
  the URL templates into the ENDPOINTS dict below.

    1. browse/search obligors by state + sector (returns paged list of
       obligors with id, name, state, sector, CUSIP6s)
    2. list continuing-disclosure documents for an obligor (paged,
       filterable by document category)
    3. get document metadata (filing date, period end, title, doc id)
    4. download document PDF (typically a content-disposition URL keyed
       on emma_doc_id)

  Until those are filled in, --enumerate-issuers and --list-disclosures
  raise NotImplementedError with a pointer back to this docstring.

OUTPUTS:
  - data/raw/emma/_index/issuers_<state>.json    cached obligor lists
  - data/raw/emma/_index/disclosures_<obligor>.json   per-obligor doc index
  - data/raw/emma/<cusip6>/<emma_doc_id>.pdf     downloaded PDFs
  - logs/emma_<timestamp>.log                    structured run log

USAGE:
    python etl/fetch_emma_disclosures.py --states GA              # one state
    python etl/fetch_emma_disclosures.py --states GA CA TX NY TN  # default Phase-1 set
    python etl/fetch_emma_disclosures.py --states GA --since 2024-01-01   # incremental
    python etl/fetch_emma_disclosures.py --obligor OBL12345        # single obligor
    python etl/fetch_emma_disclosures.py --states GA --skip-downloads  # metadata only
    python etl/fetch_emma_disclosures.py --states GA --dry-run     # parse + log only
    python etl/fetch_emma_disclosures.py --match-eins              # BMF EIN match only

PHASE-2 (not built here):
  Financial-statement extraction from the PDFs is a separate project.
"""

import argparse
import hashlib
import json
import logging
import os
import random
import re
import string
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

EMMA_BASE      = "https://emma.msrb.org"
RAW_DIR        = Path("data/raw/emma")
INDEX_DIR      = RAW_DIR / "_index"
LOG_DIR        = Path("logs")

CONTACT_EMAIL  = os.environ.get("EMMA_CONTACT_EMAIL", "jonnynewburgh@gmail.com")
USER_AGENT     = (
    f"cd-command-center-emma-etl/1.0 (+contact: {CONTACT_EMAIL}) "
    f"polite-scraper single-thread sleep>=1s"
)

# Polite-scraping knobs
REQUEST_SLEEP  = 1.1   # seconds between successful requests
BACKOFF_BASE   = 2.0   # seconds, doubled each retry
BACKOFF_MAX    = 60.0
MAX_RETRIES    = 5
HTTP_TIMEOUT   = 60

# Phase-1 default state set (per user, 2026-05-18)
DEFAULT_STATES = ["GA", "CA", "TX", "NY", "TN"]

# EMMA document categories we keep in Phase 1. Other categories
# (operating data, event notices, official statements, rating changes)
# are explicitly out-of-scope for Phase 1.
PHASE1_CATEGORIES = {
    "Annual Financial Information",
    "Audited Financial Statements",
}


# ---------------------------------------------------------------------------
# ENDPOINTS — TO BE FILLED IN AFTER DEVTOOLS RECON
# ---------------------------------------------------------------------------
# Until these are confirmed by walking the EMMA UI with Network panel open,
# the issuer/disclosure enumeration steps will refuse to run.
#
# Expected shape after recon (placeholders — replace once captured):
#   ENDPOINTS = {
#       "search_obligors": "https://emma.msrb.org/.../api/.../search?state={state}&sector={sector}&page={page}",
#       "list_disclosures": "https://emma.msrb.org/.../api/.../obligor/{obligor_id}/cd?category=annual&page={page}",
#       "doc_metadata":    "https://emma.msrb.org/.../api/.../doc/{doc_id}",
#       "doc_download":    "https://emma.msrb.org/.../api/.../doc/{doc_id}/download",
#   }
ENDPOINTS: dict[str, str] = {}


def _require_endpoint(key: str) -> str:
    url = ENDPOINTS.get(key)
    if not url:
        raise NotImplementedError(
            f"EMMA endpoint '{key}' not configured. "
            f"Capture the XHR URL from emma.msrb.org devtools and paste it "
            f"into ENDPOINTS at the top of {__file__}. See module docstring."
        )
    return url


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"emma_{ts}.log"

    fmt = "%(asctime)s %(levelname)-7s %(message)s"
    root = logging.getLogger("emma")
    root.setLevel(logging.INFO)
    root.handlers.clear()

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter(fmt))
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter(fmt))
    root.addHandler(fh)
    root.addHandler(sh)
    root.info("log file: %s", log_path)
    return root


log = logging.getLogger("emma")


# ---------------------------------------------------------------------------
# Polite HTTP client
# ---------------------------------------------------------------------------

class PoliteSession:
    """Single-thread, sleep-between-requests, backoff-on-failure HTTP client.

    Every request method (get / get_json / download) goes through _request()
    which applies the per-request sleep + backoff policy. The class is *not*
    thread-safe by design — Phase 1 is explicitly single-concurrent.
    """

    def __init__(self):
        self._s = requests.Session()
        self._s.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/html;q=0.8",
        })
        self._last_request_at = 0.0

    def _sleep_polite(self):
        elapsed = time.monotonic() - self._last_request_at
        wait = REQUEST_SLEEP - elapsed
        if wait > 0:
            time.sleep(wait)

    def _request(self, method: str, url: str, **kwargs):
        for attempt in range(1, MAX_RETRIES + 1):
            self._sleep_polite()
            try:
                resp = self._s.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
                self._last_request_at = time.monotonic()
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES:
                    raise
                wait = min(BACKOFF_MAX,
                           BACKOFF_BASE * (2 ** (attempt - 1))) + random.random()
                log.warning("network error on %s (%s) — retry %d in %.1fs",
                            url, exc, attempt, wait)
                time.sleep(wait)
                continue

            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt == MAX_RETRIES:
                    log.error("giving up on %s after %d retries (HTTP %d)",
                              url, attempt, resp.status_code)
                    resp.raise_for_status()
                wait = min(BACKOFF_MAX,
                           BACKOFF_BASE * (2 ** (attempt - 1))) + random.random()
                # Honor Retry-After if present
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        wait = max(wait, float(ra))
                    except ValueError:
                        pass
                log.warning("HTTP %d on %s — retry %d in %.1fs",
                            resp.status_code, url, attempt, wait)
                time.sleep(wait)
                continue

            # Surface MSRB ToS notices if EMMA ever returns one
            ct = resp.headers.get("Content-Type", "")
            if "tos" in ct.lower() or "terms" in ct.lower():
                log.warning("possible ToS response header on %s: %s", url, ct)
            return resp
        raise RuntimeError(f"unreachable: exhausted retries for {url}")

    def get(self, url: str, **kw):
        return self._request("GET", url, **kw)

    def get_json(self, url: str, **kw):
        r = self.get(url, **kw)
        r.raise_for_status()
        return r.json()

    def download(self, url: str, dest: Path) -> tuple[int, str]:
        """Stream a binary file to dest. Returns (size_bytes, sha256_hex)."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        sha = hashlib.sha256()
        size = 0
        with self._request("GET", url, stream=True) as r:
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    sha.update(chunk)
                    size += len(chunk)
        tmp.replace(dest)
        return size, sha.hexdigest()


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

_STRIP_RE = re.compile(r"[^A-Z0-9 ]+")
_WS_RE   = re.compile(r"\s+")


def normalize_org_name(name: str) -> str:
    """Uppercased, punctuation stripped, single-spaced — the BMF match key."""
    if not name:
        return ""
    n = name.upper()
    n = _STRIP_RE.sub(" ", n)
    n = _WS_RE.sub(" ", n).strip()
    return n


def hash_obligor_id(name_norm: str, state: str) -> str:
    """Deterministic obligor_id when EMMA doesn't expose its own."""
    h = hashlib.sha1(f"{name_norm}|{state}".encode("utf-8")).hexdigest()[:16]
    return f"H_{h}"


# ---------------------------------------------------------------------------
# Disk cache
# ---------------------------------------------------------------------------

def _safe_filename(s: str) -> str:
    keep = set(string.ascii_letters + string.digits + "_-.")
    return "".join(c if c in keep else "_" for c in s)[:120]


def _cache_load(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None


def _cache_save(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


# ---------------------------------------------------------------------------
# EMMA endpoint wrappers (Phase-1 stubs — fill in after recon)
#
# Each function below is the spot where the captured EMMA XHR URL gets used.
# Until ENDPOINTS is populated, they raise NotImplementedError pointing the
# operator back to the module docstring.
# ---------------------------------------------------------------------------

def search_obligors(session: PoliteSession, state: str,
                    sector: str | None = None) -> list[dict]:
    """Return all nonprofit conduit obligors in a state (and optional sector).

    Each returned dict should have at least:
      obligor_id, obligor_name, state, sector, cusip6_list (list[str]),
      first_seen (date, optional), last_seen (date, optional)
    """
    cache_key = f"issuers_{state}{'_' + sector if sector else ''}.json"
    cache_path = INDEX_DIR / cache_key
    cached = _cache_load(cache_path)
    if cached is not None:
        log.info("issuers cache hit: %s (%d rows)", cache_key, len(cached))
        return cached

    url_tpl = _require_endpoint("search_obligors")

    results: list[dict] = []
    page = 1
    while True:
        url = url_tpl.format(state=state, sector=sector or "", page=page)
        log.info("search_obligors state=%s sector=%s page=%d", state, sector, page)
        data = session.get_json(url)
        rows, has_next = _parse_obligor_search_response(data)
        results.extend(rows)
        if not has_next or not rows:
            break
        page += 1

    _cache_save(cache_path, results)
    return results


def _parse_obligor_search_response(data) -> tuple[list[dict], bool]:
    """Adapter from raw EMMA JSON → normalized obligor dicts. TO IMPLEMENT
    once the endpoint shape is known."""
    raise NotImplementedError(
        "Parse the raw obligor-search response into normalized dicts here. "
        "Return (rows, has_next_page)."
    )


def list_disclosures(session: PoliteSession, obligor_id: str) -> list[dict]:
    """Return all continuing-disclosure documents for an obligor that fall
    into PHASE1_CATEGORIES (audited financials / annual financial info).

    Each dict should have at least:
      emma_doc_id, filing_date, period_end_date, document_category,
      document_subcategory, document_title, cusips (list[str]),
      source_url
    """
    cache_path = INDEX_DIR / f"disclosures_{_safe_filename(obligor_id)}.json"
    cached = _cache_load(cache_path)
    if cached is not None:
        log.debug("disclosures cache hit: %s (%d)", obligor_id, len(cached))
        return cached

    url_tpl = _require_endpoint("list_disclosures")

    rows: list[dict] = []
    page = 1
    while True:
        url = url_tpl.format(obligor_id=obligor_id, page=page)
        data = session.get_json(url)
        page_rows, has_next = _parse_disclosure_list_response(data)
        rows.extend(page_rows)
        if not has_next or not page_rows:
            break
        page += 1

    rows = [r for r in rows if r.get("document_category") in PHASE1_CATEGORIES]
    _cache_save(cache_path, rows)
    return rows


def _parse_disclosure_list_response(data) -> tuple[list[dict], bool]:
    """Adapter from raw EMMA JSON → normalized disclosure dicts. TO IMPLEMENT
    once the endpoint shape is known."""
    raise NotImplementedError(
        "Parse the raw disclosure-list response into normalized dicts here. "
        "Return (rows, has_next_page)."
    )


def pdf_download_url(emma_doc_id: str) -> str:
    """Return the canonical PDF URL for a document. TO IMPLEMENT once the
    endpoint shape is known."""
    url_tpl = _require_endpoint("doc_download")
    return url_tpl.format(doc_id=emma_doc_id)


# ---------------------------------------------------------------------------
# Phase steps
# ---------------------------------------------------------------------------

def enumerate_issuers(session: PoliteSession, states: list[str]) -> int:
    """Step 1: walk states (and sectors), upsert emma_issuers rows."""
    n_total = 0
    for st in states:
        try:
            rows = search_obligors(session, st)
        except NotImplementedError as e:
            log.error("issuer enumeration blocked: %s", e)
            raise
        except Exception as exc:
            log.exception("search_obligors failed for state=%s: %s", st, exc)
            continue

        log.info("state=%s obligors_found=%d", st, len(rows))
        for r in rows:
            name = (r.get("obligor_name") or "").strip()
            if not name:
                log.warning("obligor row missing name: %s", r)
                continue
            norm = normalize_org_name(name)
            obligor_id = r.get("obligor_id") or hash_obligor_id(norm, st)
            record = {
                "obligor_id":              obligor_id,
                "obligor_name":            name,
                "obligor_name_normalized": norm,
                "state":                   st,
                "sector":                  r.get("sector"),
                "cusip6_list":             r.get("cusip6_list") or [],
                "first_seen":              r.get("first_seen"),
                "last_seen":               r.get("last_seen"),
            }
            db.upsert_emma_issuer(record)
            n_total += 1
    log.info("issuer enumeration complete: %d upserts", n_total)
    return n_total


def enumerate_disclosures(session: PoliteSession,
                          obligor_ids: Iterable[str],
                          since: date | None = None) -> tuple[int, int]:
    """Step 2: for each obligor, list disclosures and upsert pending rows.

    Returns (docs_found, docs_in_phase1_categories)."""
    found, kept = 0, 0
    for oid in obligor_ids:
        try:
            rows = list_disclosures(session, oid)
        except NotImplementedError:
            raise
        except Exception:
            log.exception("list_disclosures failed for obligor=%s", oid)
            continue

        for r in rows:
            found += 1
            cat = r.get("document_category")
            if cat not in PHASE1_CATEGORIES:
                continue
            if since:
                fd = r.get("filing_date")
                if fd and str(fd) < since.isoformat():
                    continue
            doc_id = r.get("emma_doc_id")
            if not doc_id:
                log.warning("disclosure missing emma_doc_id (obligor=%s): %s",
                            oid, r)
                continue
            db.upsert_emma_disclosure({
                "emma_doc_id":          doc_id,
                "obligor_id":           oid,
                "filing_date":          r.get("filing_date"),
                "period_end_date":      r.get("period_end_date"),
                "document_category":    cat,
                "document_subcategory": r.get("document_subcategory"),
                "document_title":       r.get("document_title"),
                "cusips":               r.get("cusips") or [],
                "source_url":           r.get("source_url"),
                "download_status":      "pending",
            })
            kept += 1
    log.info("disclosure enumeration complete: found=%d kept=%d", found, kept)
    return found, kept


def download_pdfs(session: PoliteSession) -> tuple[int, int, int]:
    """Step 3: walk emma_disclosures rows with download_status='pending' (or
    NULL) and pull the PDF. Returns (ok, skipped_existing, failed)."""
    pending = db.get_emma_disclosures_pending_download()
    log.info("pending PDF downloads: %d", len(pending))

    ok, skipped, failed = 0, 0, 0
    for _, row in pending.iterrows():
        doc_id = row["emma_doc_id"]
        oblig  = row["obligor_id"]
        cusips_raw = row.get("cusips") or "[]"
        if isinstance(cusips_raw, str):
            try:
                cusips = json.loads(cusips_raw) or []
            except ValueError:
                cusips = []
        else:
            cusips = list(cusips_raw) if cusips_raw else []
        cusip6 = (cusips[0][:6] if cusips else "unknown").upper()

        dest = RAW_DIR / cusip6 / f"{_safe_filename(doc_id)}.pdf"
        rel  = str(dest.relative_to(RAW_DIR.parent.parent)) \
               if RAW_DIR.is_absolute() else str(dest)

        if dest.exists() and dest.stat().st_size > 0:
            # Idempotent: re-record without re-downloading.
            try:
                sha = hashlib.sha256(dest.read_bytes()).hexdigest()
            except OSError as exc:
                log.warning("could not hash existing %s: %s", dest, exc)
                failed += 1
                continue
            db.update_emma_disclosure_download(
                doc_id, str(dest), sha, dest.stat().st_size, "ok"
            )
            skipped += 1
            continue

        try:
            url = pdf_download_url(doc_id)
            size, sha = session.download(url, dest)
        except NotImplementedError:
            raise
        except Exception as exc:
            log.warning("download failed doc=%s obligor=%s: %s",
                        doc_id, oblig, exc)
            db.update_emma_disclosure_download(doc_id, None, None, None, "failed")
            failed += 1
            continue

        # Cheap sanity: PDF magic bytes
        try:
            head = dest.read_bytes()[:4]
            if head[:4] != b"%PDF":
                log.warning("doc=%s: downloaded file is not a PDF (head=%r)",
                            doc_id, head)
                db.update_emma_disclosure_download(
                    doc_id, str(dest), sha, size, "skipped_non_pdf"
                )
                failed += 1
                continue
        except OSError:
            pass

        db.update_emma_disclosure_download(doc_id, str(dest), sha, size, "ok")
        ok += 1
        if (ok + failed) % 25 == 0:
            log.info("downloads: ok=%d skipped=%d failed=%d", ok, skipped, failed)

    log.info("download phase complete: ok=%d skipped=%d failed=%d",
             ok, skipped, failed)
    return ok, skipped, failed


def match_eins_to_bmf(min_score: float = 0.85) -> tuple[int, int]:
    """Step 4: match emma_issuers (no EIN yet) to the IRS BMF by
    normalized name + state. Returns (attempted, matched)."""
    # Lazy import — BMF download is heavy and only needed for this step.
    from etl.fetch_bmf_eins import download_bmf, load_bmf, find_best_match

    log.info("downloading IRS BMF (cached if present)...")
    paths = download_bmf()
    if not paths:
        log.error("no BMF files available; aborting EIN match")
        return 0, 0
    bmf = load_bmf(paths, ntee_prefix="")
    log.info("BMF loaded: %d tax-exempt orgs", len(bmf))

    # State-keyed index of (name, ein, zip, ntee, *fin) tuples
    index: dict[str, list] = {}
    for _, r in bmf.iterrows():
        st = r["STATE"]
        index.setdefault(st, []).append((
            r["NAME"], r["EIN"], r.get("ZIP", "") or "", r.get("NTEE_CD", ""),
            None, None, None,
        ))

    pending = db.get_emma_issuers_pending_ein_match()
    log.info("issuers pending EIN match: %d", len(pending))
    attempted, matched = 0, 0
    for _, row in pending.iterrows():
        attempted += 1
        name = row.get("obligor_name") or ""
        st   = (row.get("state") or "").upper()
        if not name or not st:
            continue
        result = find_best_match(name, st, index, min_score, "")
        if not result:
            continue
        ein, bmf_name, score = result
        db.update_emma_issuer_ein(
            row["obligor_id"], str(ein).zfill(9), float(score), "bmf_name_state"
        )
        matched += 1
        if matched % 100 == 0:
            log.info("EIN match progress: %d / %d (matched=%d)",
                     attempted, len(pending), matched)
    rate = (100.0 * matched / attempted) if attempted else 0.0
    log.info("EIN match: attempted=%d matched=%d (%.1f%%)",
             attempted, matched, rate)
    return attempted, matched


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    # Hard block: refuse to start. See top-of-file notice.
    sys.stderr.write(
        "REFUSING TO RUN: emma.msrb.org scraping is prohibited by MSRB ToS.\n"
        "See top-of-file notice in etl/fetch_emma_disclosures.py for the\n"
        "blocking decision (2026-05-18) and the list of legitimate data\n"
        "paths to pursue instead.\n"
    )
    sys.exit(2)

    parser = argparse.ArgumentParser(
        description="EMMA continuing-disclosure ETL (Phase 1)"
    )
    parser.add_argument(
        "--states", nargs="+", default=DEFAULT_STATES, metavar="ST",
        help=f"State subset to enumerate (default: {' '.join(DEFAULT_STATES)})",
    )
    parser.add_argument(
        "--sectors", nargs="+", default=None, metavar="SECTOR",
        help="Optional EMMA sector filter (e.g. 'Education' 'Health Care')",
    )
    parser.add_argument(
        "--obligor", metavar="OBLIGOR_ID",
        help="Refresh a single obligor (skips state-wide enumeration)",
    )
    parser.add_argument(
        "--since", metavar="YYYY-MM-DD",
        help="Incremental: only keep disclosures with filing_date >= this",
    )
    parser.add_argument(
        "--skip-downloads", action="store_true",
        help="Enumerate metadata only; skip PDF download phase",
    )
    parser.add_argument(
        "--match-eins", action="store_true",
        help="Run BMF EIN matching only (skip enumeration + downloads)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Do not write to DB; log what would happen",
    )
    parser.add_argument(
        "--min-score", type=float, default=0.85,
        help="BMF name-match minimum score (default: 0.85)",
    )
    args = parser.parse_args()

    _setup_logging()
    log.info("CD Command Center — EMMA Phase 1 ETL")
    log.info("states=%s sectors=%s obligor=%s since=%s skip_downloads=%s "
             "match_eins=%s dry_run=%s",
             args.states, args.sectors, args.obligor, args.since,
             args.skip_downloads, args.match_eins, args.dry_run)

    since = None
    if args.since:
        since = datetime.strptime(args.since, "%Y-%m-%d").date()

    if args.dry_run:
        log.warning("--dry-run is informational only in Phase 1; DB writes "
                    "happen through db.py upsert functions which always commit. "
                    "Use --skip-downloads / --match-eins to scope a real run instead.")

    if not ENDPOINTS and not args.match_eins:
        log.error("EMMA endpoints are not configured (see module docstring). "
                  "Run with --match-eins to operate on already-loaded issuers, "
                  "or paste the captured XHR URLs into ENDPOINTS and re-run.")
        sys.exit(2)

    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    session = PoliteSession()
    summary = {
        "states":            args.states,
        "obligors_upserted": 0,
        "docs_found":        0,
        "docs_kept":         0,
        "pdfs_ok":           0,
        "pdfs_skipped":      0,
        "pdfs_failed":       0,
        "eins_attempted":    0,
        "eins_matched":      0,
        "errors":            [],
    }

    try:
        if args.match_eins:
            summary["eins_attempted"], summary["eins_matched"] = \
                match_eins_to_bmf(min_score=args.min_score)
        else:
            # Step 1: issuers
            if args.obligor:
                obligor_ids = [args.obligor]
            else:
                summary["obligors_upserted"] = enumerate_issuers(
                    session, [s.upper() for s in args.states]
                )
                obligor_ids = [
                    r["obligor_id"]
                    for _, r in db.get_emma_issuers_pending_ein_match().iterrows()
                    # Note: this iterates *all* pending issuers across states.
                    # Filtering to just this-run's states isn't necessary for
                    # correctness — disclosure listing is idempotent per obligor.
                ]

            # Step 2: disclosures
            f, k = enumerate_disclosures(session, obligor_ids, since=since)
            summary["docs_found"] = f
            summary["docs_kept"]  = k

            # Step 3: downloads
            if not args.skip_downloads:
                ok, sk, fl = download_pdfs(session)
                summary["pdfs_ok"]      = ok
                summary["pdfs_skipped"] = sk
                summary["pdfs_failed"]  = fl
            else:
                log.info("--skip-downloads: PDF download phase skipped")

            # Step 4: EIN match
            summary["eins_attempted"], summary["eins_matched"] = \
                match_eins_to_bmf(min_score=args.min_score)

    except NotImplementedError as exc:
        log.error("blocked: %s", exc)
        summary["errors"].append(str(exc))
    except Exception as exc:
        log.exception("unexpected error in main pipeline: %s", exc)
        summary["errors"].append(str(exc))

    # End-of-run summary
    log.info("=" * 60)
    log.info("RUN SUMMARY")
    log.info("=" * 60)
    for k, v in summary.items():
        log.info("  %-20s %s", k, v)


if __name__ == "__main__":
    main()
