"""
etl/fetch_bmf_eins.py — Match charter schools to EINs using the IRS
Exempt Organizations Business Master File (BMF).

The IRS publishes the BMF monthly as four regional CSV files covering all
~2 million tax-exempt organizations. Each record includes EIN, org name,
city, state, zip, and NTEE code. NTEE code B29 = Charter Schools; all B
codes cover education.

This script:
  1. Downloads and caches the four BMF regional CSVs from the IRS website
  2. Filters to education NTEE codes (B* by default, or B29 for charters only)
  3. For each unlinked charter school operator in the schools table, finds the
     best-scoring BMF entry by normalized name + state match
  4. Writes matched EINs to schools.ein
  5. Upserts a lightweight record into irs_990 (data_source="IRS BMF")

WHY BMF vs. ProPublica API:
  ProPublica requires one API call per org with a 0.3s rate limit, making
  8,000+ orgs a multi-hour job. The BMF is a one-time download (~200MB)
  that lets us match everything locally in seconds. Coverage improves from
  ~62% to ~80%+ for charter schools because the BMF includes orgs that
  don't appear in ProPublica search results.

AFTER RUNNING:
  Run fetch_990_data.py --schools --overwrite to enrich newly-linked schools
  with full ProPublica financial data (revenue, expenses, assets).

Usage:
    python etl/fetch_bmf_eins.py                    # match all unlinked charters
    python etl/fetch_bmf_eins.py --dry-run           # preview matches, no DB write
    python etl/fetch_bmf_eins.py --ntee B29          # charter schools NTEE only
    python etl/fetch_bmf_eins.py --ntee B            # all education NTEE codes
    python etl/fetch_bmf_eins.py --states CA TX      # limit by state
    python etl/fetch_bmf_eins.py --limit 200         # test on 200 orgs
    python etl/fetch_bmf_eins.py --min-score 0.7     # stricter matching
    python etl/fetch_bmf_eins.py --force-download    # re-download even if cached
    python etl/fetch_bmf_eins.py --all-schools       # also match non-charter schools
"""

import argparse
import os
import re
import sys
import zipfile
import io

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

# ---------------------------------------------------------------------------
# IRS BMF download URLs — four regional files covering all 50 states + DC
# Published monthly; no authentication required.
# IRS source page: https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf
# ---------------------------------------------------------------------------
BMF_URLS = [
    "https://www.irs.gov/pub/irs-soi/eo1.csv",  # Northeast
    "https://www.irs.gov/pub/irs-soi/eo2.csv",  # Southeast / Mid-Atlantic
    "https://www.irs.gov/pub/irs-soi/eo3.csv",  # Midwest
    "https://www.irs.gov/pub/irs-soi/eo4.csv",  # West / South / Territories
]

# Local cache directory
BMF_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "raw", "bmf",
)

# BMF column names as published by the IRS
BMF_COLUMNS = [
    "EIN", "NAME", "ICO", "STREET", "CITY", "STATE", "ZIP", "GROUP",
    "SUBSECTION", "AFFILIATION", "CLASSIFICATION", "RULING", "DEDUCTIBILITY",
    "FOUNDATION", "ACTIVITY", "ORGANIZATION", "STATUS", "TAX_PERIOD",
    "ASSET_CD", "INCOME_CD", "FILING_REQ_CD", "PF_FILING_REQ_CD", "ACCT_PD",
    "ASSET_AMT", "INCOME_AMT", "REVENUE_AMT", "NTEE_CD", "SORT_NAME",
]

DOWNLOAD_TIMEOUT = 120
DEFAULT_MIN_SCORE = 0.7  # higher than ProPublica (0.5) — BMF has more noise

# Common words to strip when normalizing school/org names for comparison
_STOP_WORDS = {
    "the", "a", "an", "of", "for", "and", "in", "at", "to", "inc", "llc",
    "corp", "school", "schools", "charter", "education", "educational",
    "academy", "academies",
}

# Suffixes to strip from LEA names before matching (mirrors fetch_990_data.py)
_LEA_SUFFIXES = [
    " Unified School District", " City School District",
    " Union High School District", " Union Elementary District",
    " Union School District", " Elementary School District",
    " High School District", " School District",
    " Charter District", " District",
]


# ---------------------------------------------------------------------------
# Name normalization helpers
# ---------------------------------------------------------------------------

def _words(text: str) -> set:
    return set(re.findall(r"[a-z]+", (text or "").lower()))


def _meaningful_words(text: str) -> set:
    # Also filter single-character tokens — they arise from dotted abbreviations
    # like "P.L.C." and create noisy false-positive matches
    return {w for w in _words(text) - _STOP_WORDS if len(w) > 1}


def _clean_lea_name(name: str) -> str:
    """Strip district/school suffixes and NCES ID noise from a LEA name."""
    if not name:
        return name
    cleaned = name.strip()
    # Strip trailing parenthetical NCES IDs like "(273398)" or "(1000166)"
    cleaned = re.sub(r"\s*\(\d+\)\s*$", "", cleaned).strip()
    # Strip district-type suffixes
    for suffix in _LEA_SUFFIXES:
        if cleaned.lower().endswith(suffix.lower()):
            cleaned = cleaned[: -len(suffix)].strip()
            break
    return cleaned


def _dba_variants(name: str) -> list[str]:
    """
    Return name variants for orgs using a d.b.a. pattern.
    "American Charter Schools Foundation d.b.a. Alta Vista High"
    returns ["American Charter Schools Foundation", "Alta Vista High"]
    so we try matching both the legal name and the operating name.
    """
    for pat in (r"\s+d\.b\.a\.\s+", r"\s+dba\s+", r"\s+d/b/a\s+"):
        parts = re.split(pat, name, flags=re.IGNORECASE)
        if len(parts) == 2:
            return [p.strip() for p in parts]
    return [name]


def _match_score(query: str, candidate: str) -> float:
    """
    Fraction of meaningful query words found in the candidate name.
    Returns 0.0–1.0. Requires the first meaningful query word (brand/network
    identifier) to appear in the candidate to prevent false positives.
    """
    q_words = _meaningful_words(query)
    c_words = _meaningful_words(candidate)
    if not q_words:
        return 0.0

    # First meaningful word must appear verbatim (brand/network check)
    ordered = [w for w in re.findall(r"[a-z]+", query.lower()) if w not in _STOP_WORDS]
    if ordered and ordered[0] not in c_words:
        return 0.0

    return len(q_words & c_words) / len(q_words)


# ---------------------------------------------------------------------------
# BMF download + load
# ---------------------------------------------------------------------------

def _cache_path(url: str) -> str:
    filename = url.split("/")[-1]
    return os.path.join(BMF_CACHE_DIR, filename)


def download_bmf(force: bool = False) -> list[str]:
    """
    Download all four IRS BMF regional CSV files, caching them locally.
    Returns list of local file paths. Skips files already on disk unless
    force=True.
    """
    os.makedirs(BMF_CACHE_DIR, exist_ok=True)
    paths = []

    for url in BMF_URLS:
        path = _cache_path(url)
        if os.path.exists(path) and not force:
            size_mb = os.path.getsize(path) / 1_048_576
            print(f"  Cached: {os.path.basename(path)} ({size_mb:.1f} MB)")
            paths.append(path)
            continue

        print(f"  Downloading {url} ...", end=" ", flush=True)
        try:
            resp = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            resp.raise_for_status()
            with open(path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            size_mb = os.path.getsize(path) / 1_048_576
            print(f"{size_mb:.1f} MB")
            paths.append(path)
        except Exception as e:
            print(f"FAILED — {e}")
            print(f"    Manual download: https://www.irs.gov/pub/irs-soi/{os.path.basename(path)}")
            print(f"    Save as: {path}")

    return paths


def load_bmf(paths: list[str], ntee_prefix: str = "B") -> pd.DataFrame:
    """
    Load and concatenate the BMF CSV files, filtering to the given NTEE prefix.
    ntee_prefix="B"   -> all education orgs
    ntee_prefix="B29" -> charter schools only
    """
    dfs = []
    for path in paths:
        try:
            df = pd.read_csv(
                path,
                names=BMF_COLUMNS,
                header=0,
                dtype=str,
                low_memory=False,
                encoding="latin-1",
            )
            dfs.append(df)
        except Exception as e:
            print(f"  Warning: could not read {path} — {e}")

    if not dfs:
        return pd.DataFrame(columns=BMF_COLUMNS)

    bmf = pd.concat(dfs, ignore_index=True)

    # Normalize
    bmf["EIN"]     = bmf["EIN"].str.strip().str.zfill(9)
    bmf["NAME"]    = bmf["NAME"].str.strip()
    bmf["STATE"]   = bmf["STATE"].str.strip().str.upper()
    bmf["ZIP"]     = bmf["ZIP"].str.strip().str[:5]
    bmf["NTEE_CD"] = bmf["NTEE_CD"].fillna("").str.strip().str.upper()

    # Filter to requested NTEE prefix
    bmf = bmf[bmf["NTEE_CD"].str.startswith(ntee_prefix.upper())].copy()
    bmf = bmf.dropna(subset=["EIN", "NAME", "STATE"])
    bmf = bmf[bmf["EIN"].str.len() == 9]
    bmf = bmf[bmf["NAME"].str.len() > 0]

    return bmf.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def _build_index(bmf: pd.DataFrame) -> dict:
    """
    Build a state-keyed lookup dict:
      { "CA": [(normalized_name, ein, full_name, zip, ntee, asset_amt, income_amt, revenue_amt), ...] }
    Pre-computing normalized names speeds up the inner match loop.
    """
    index = {}
    for _, row in bmf.iterrows():
        state = row["STATE"]
        if state not in index:
            index[state] = []
        index[state].append((
            row["NAME"],                              # original name (for display)
            row["EIN"],
            row.get("ZIP", ""),
            row.get("NTEE_CD", ""),
            _safe_float(row.get("ASSET_AMT")),
            _safe_float(row.get("INCOME_AMT")),
            _safe_float(row.get("REVENUE_AMT")),
        ))
    return index


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        v = float(str(val).replace(",", ""))
        return v if v != 0 else None
    except (TypeError, ValueError):
        return None


def find_best_match(
    query_name: str,
    state: str,
    index: dict,
    min_score: float,
    query_zip: str = "",
) -> tuple[str, str, float] | None:
    """
    Search the BMF index for the best match to query_name in the given state.
    Returns (ein, bmf_name, score) or None if no match exceeds min_score.

    Zip proximity adds a 0.05 bonus — enough to break ties but not to
    override a clearly better name match in a different zip.
    """
    candidates = index.get(state, [])
    if not candidates:
        return None

    q_meaningful = _meaningful_words(query_name)
    if len(q_meaningful) < 2:
        # Single-word queries are too ambiguous — require at least 2 keywords
        return None

    best_score = 0.0
    best_ein = None
    best_name = None

    for (bmf_name, ein, bmf_zip, ntee, *_) in candidates:
        score = _match_score(query_name, bmf_name)
        if score == 0.0:
            continue
        # Small zip-match bonus
        if query_zip and bmf_zip and query_zip[:5] == bmf_zip[:5]:
            score = min(1.0, score + 0.05)
        if score > best_score:
            best_score = score
            best_ein = ein
            best_name = bmf_name

    if best_score >= min_score:
        return best_ein, best_name, best_score
    return None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_unlinked_operators(states=None, limit=None, all_schools=False) -> list[dict]:
    """
    Return distinct charter school operators (by lea_id) that don't yet have
    an EIN. If all_schools=True, also include non-charter open schools.
    """
    conn = db.get_connection()
    cur = conn.cursor()

    conditions = ["(ein IS NULL OR ein = '')"]
    params = []

    if not all_schools:
        conditions.append("is_charter = 1")

    conditions.append("school_status = 'Open'")

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    where = "WHERE " + " AND ".join(conditions)

    cur.execute(
        f"SELECT DISTINCT lea_id, lea_name, school_name, state, zip_code "
        f"FROM schools {where} ORDER BY state, lea_name",
        params,
    )
    rows = cur.fetchall()
    conn.close()

    # Deduplicate by lea_id
    seen = set()
    orgs = []
    for lea_id, lea_name, school_name, state, zip_code in rows:
        key = lea_id or f"{school_name}|{state}"
        if key in seen:
            continue
        seen.add(key)
        search_name = lea_name or school_name
        if search_name:
            orgs.append({
                "lea_id":      lea_id,
                "search_name": _clean_lea_name(search_name),
                "raw_name":    search_name,
                "state":       state,
                "zip":         (zip_code or "")[:5],
            })

    if limit:
        orgs = orgs[:limit]

    return orgs


def _link_ein_to_schools(lea_id, school_name, state, ein, all_schools=False):
    """Write the matched EIN back to all schools with this lea_id."""
    conn = db.get_connection()
    cur = conn.cursor()
    charter_filter = "" if all_schools else "AND is_charter = 1"
    if lea_id:
        cur.execute(
            f"UPDATE schools SET ein = ?, updated_at = CURRENT_TIMESTAMP "
            f"WHERE lea_id = ? {charter_filter}",
            (ein, lea_id),
        )
    else:
        cur.execute(
            f"UPDATE schools SET ein = ?, updated_at = CURRENT_TIMESTAMP "
            f"WHERE school_name = ? AND state = ? {charter_filter}",
            (ein, school_name, state),
        )
    conn.commit()
    conn.close()


def _upsert_bmf_990(ein: str, bmf_row_data: dict):
    """
    Insert a lightweight irs_990 record from BMF data.
    Only populates fields available in the BMF (no detailed financials).
    Running fetch_990_data.py --overwrite afterwards upgrades to full data.
    """
    record = {
        "ein":          ein,
        "org_name":     bmf_row_data.get("name"),
        "city":         bmf_row_data.get("city"),
        "state":        bmf_row_data.get("state"),
        "ntee_code":    bmf_row_data.get("ntee_cd"),
        "total_assets": bmf_row_data.get("asset_amt"),
        "total_revenue": bmf_row_data.get("revenue_amt"),
        "data_source":  "IRS BMF",
    }
    # Drop None values so we don't overwrite real ProPublica data with nulls
    record = {k: v for k, v in record.items() if v is not None}
    db.upsert_990(record)


# ---------------------------------------------------------------------------
# Main matching loop
# ---------------------------------------------------------------------------

def match_schools(
    bmf: pd.DataFrame,
    states=None,
    limit=None,
    min_score=DEFAULT_MIN_SCORE,
    dry_run=False,
    verbose=False,
    all_schools=False,
) -> int:
    """Match unlinked schools against the BMF. Returns count of new matches."""
    print(f"  Building BMF lookup index ({len(bmf):,} education orgs)...")
    index = _build_index(bmf)

    # Build a lookup for full BMF row data by EIN (for writing to irs_990)
    bmf_by_ein = {}
    for _, row in bmf.iterrows():
        ein = row["EIN"]
        bmf_by_ein[ein] = {
            "name":       row["NAME"],
            "city":       row.get("CITY", ""),
            "state":      row["STATE"],
            "ntee_cd":    row.get("NTEE_CD", ""),
            "asset_amt":  _safe_float(row.get("ASSET_AMT")),
            "revenue_amt": _safe_float(row.get("REVENUE_AMT")),
        }

    orgs = _get_unlinked_operators(states=states, limit=limit, all_schools=all_schools)
    total = len(orgs)
    print(f"  Unlinked operators to match: {total:,}")
    if total == 0:
        print("  Nothing to do.")
        return 0

    matched = 0
    unmatched = 0

    for i, org in enumerate(orgs, 1):
        query  = org["search_name"]
        state  = org["state"]
        zip_   = org["zip"]

        result = find_best_match(query, state, index, min_score, zip_)

        # Try d.b.a. variants — legal name AND operating name
        if not result:
            for variant in _dba_variants(query):
                if variant != query:
                    result = find_best_match(variant, state, index, min_score, zip_)
                    if result:
                        break

        # Try the raw (uncleaned) name as a last resort
        if not result and org["raw_name"] != query:
            result = find_best_match(org["raw_name"], state, index, min_score, zip_)

        if result:
            ein, bmf_name, score = result
            matched += 1
            if verbose or dry_run:
                print(f"  HIT [{i:,}] {query[:45]:<45} ({state}) "
                      f"-> {bmf_name[:45]:<45}  EIN={ein}  score={score:.2f}")
            if not dry_run:
                _link_ein_to_schools(
                    org["lea_id"], org["raw_name"], state, ein, all_schools
                )
                if ein in bmf_by_ein:
                    _upsert_bmf_990(ein, bmf_by_ein[ein])
        else:
            unmatched += 1
            if verbose:
                print(f"  --- [{i:,}] {query[:45]:<45} ({state})  no match")

        if not verbose and not dry_run and i % 500 == 0:
            print(f"  [{i:,}/{total:,}] matched: {matched:,}, unmatched: {unmatched:,}")

    return matched


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Match charter schools to EINs using the IRS Exempt Org BMF"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print matches without writing to the database",
    )
    parser.add_argument(
        "--ntee", default="B",
        help="NTEE code prefix to include (default: B = all education; use B29 for charters only)",
    )
    parser.add_argument(
        "--states", nargs="+", metavar="ST",
        help="Limit to specific states (e.g. CA TX NY)",
    )
    parser.add_argument(
        "--limit", type=int,
        help="Max number of operators to attempt matching",
    )
    parser.add_argument(
        "--min-score", type=float, default=DEFAULT_MIN_SCORE,
        help=f"Minimum name match score to accept (default: {DEFAULT_MIN_SCORE})",
    )
    parser.add_argument(
        "--force-download", action="store_true",
        help="Re-download BMF files even if already cached",
    )
    parser.add_argument(
        "--all-schools", action="store_true",
        help="Also match non-charter open schools (default: charters only)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print every match attempt (matched and unmatched)",
    )
    args = parser.parse_args()

    db.init_db()

    print("CD Command Center — IRS BMF EIN Matcher")
    print(f"  NTEE filter:   {args.ntee}* (education)")
    print(f"  Min score:     {args.min_score}")
    if args.states:
        print(f"  States:        {' '.join(args.states)}")
    if args.dry_run:
        print("  Mode:          DRY RUN (no DB writes)")
    print()

    # Step 1: Download / load from cache
    print("BMF files:")
    paths = download_bmf(force=args.force_download)
    if not paths:
        print("No BMF files available. Exiting.")
        sys.exit(1)
    print()

    # Step 2: Load and filter
    print(f"Loading BMF (NTEE prefix '{args.ntee}')...")
    bmf = load_bmf(paths, ntee_prefix=args.ntee)
    print(f"  {len(bmf):,} education organizations loaded")
    ntee_counts = bmf["NTEE_CD"].value_counts().head(10)
    for code, count in ntee_counts.items():
        print(f"    {code:6}  {count:,}")
    print()

    if bmf.empty:
        print("No matching NTEE records found. Try --ntee B for all education orgs.")
        sys.exit(1)

    # Step 3: Match
    print("Matching against schools table...")
    new_matches = match_schools(
        bmf,
        states=args.states,
        limit=args.limit,
        min_score=args.min_score,
        dry_run=args.dry_run,
        verbose=args.verbose,
        all_schools=args.all_schools,
    )
    print()

    # Step 4: Summary
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM schools WHERE is_charter = 1")
    total_charters   = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM schools WHERE is_charter = 1 AND ein IS NOT NULL AND ein != ''")
    linked_charters  = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM irs_990")
    total_990        = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM irs_990 WHERE data_source = 'IRS BMF'")
    bmf_source_count = cur.fetchone()[0]
    conn.close()

    if args.dry_run:
        print(f"DRY RUN — {new_matches:,} matches found (nothing written)")
    else:
        print(f"New EINs matched this run:  {new_matches:,}")
        print(f"Charter schools with EIN:   {linked_charters:,} / {total_charters:,} "
              f"({linked_charters/total_charters*100:.1f}%)")
        print(f"irs_990 total records:      {total_990:,}")
        print(f"  of which from IRS BMF:    {bmf_source_count:,}")
        print()
        if new_matches > 0:
            print("Next step: enrich new matches with full financial data:")
            print("  python etl/fetch_990_data.py --schools --overwrite")


if __name__ == "__main__":
    main()
