"""
etl/fetch_990_data.py — Fetch IRS 990 financial data for charter schools and FQHCs.

Uses the ProPublica Nonprofit Explorer API (free, no API key required) to:
  1. Search for each organization by name + state
  2. Match the best result using name similarity
  3. Fetch the organization's most recent 990 financial data
  4. Store the EIN back in the facility table (schools or fqhc)
  5. Store the financial data in the irs_990 table

ProPublica API docs: https://projects.propublica.org/nonprofits/api

WHY this approach:
  Charter schools and FQHCs are 501(c)(3) nonprofits that file Form 990 with
  the IRS. The 990 shows revenue, expenses, assets, and program spending —
  key financial health indicators for deal origination. ProPublica aggregates
  this data and provides a free search API.

  We search by the school's LEA name (operator/district name) rather than the
  school site name because the LEA is usually the legal nonprofit entity that
  files the 990. For FQHCs, we search by health_center_name.

MATCHING STRATEGY:
  The ProPublica API returns a ranked list of results. We pick the first result
  whose name has significant word overlap with our search term. This isn't
  perfect — some manual cleanup may be needed — but it handles most cases.

RUNNING TIME:
  Each organization requires 2 API calls (search + org detail). With 0.3s sleep
  between calls and ~8,000 charter schools, a full run takes several hours.
  Use --limit or --states to run in batches. Re-running is safe — already-linked
  facilities are skipped unless you pass --overwrite.

Usage:
    python etl/fetch_990_data.py --schools              # charter schools only
    python etl/fetch_990_data.py --fqhc                 # health centers only
    python etl/fetch_990_data.py                        # both
    python etl/fetch_990_data.py --states CA TX NY      # limit by state
    python etl/fetch_990_data.py --limit 100            # test on 100 orgs
    python etl/fetch_990_data.py --overwrite            # re-fetch even linked orgs
"""

import argparse
import sys
import os
import time
import re

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

PROPUBLICA_SEARCH_URL = "https://projects.propublica.org/nonprofits/api/v2/search.json"
PROPUBLICA_ORG_URL    = "https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json"

# Seconds to sleep between API calls — ProPublica asks for polite usage
API_SLEEP = 0.3

# Minimum word overlap (fraction of query words found in result name) to accept a match
MIN_MATCH_SCORE = 0.5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _words(text: str) -> set:
    """Lowercase alphabetic words in a string — used for name matching."""
    return set(re.findall(r"[a-z]+", text.lower()))

# Common filler words that don't help distinguish organizations.
# NOTE: keep "academy" OUT of here — it's the primary word in many charter names.
_STOP_WORDS = {"the", "a", "an", "of", "for", "and", "in", "at", "to", "inc",
               "llc", "corp", "school", "schools", "charter"}

def _match_score(query: str, result: str) -> float:
    """
    Returns fraction of meaningful query words found in the result name.
    Ignores common stop words. Returns 0.0–1.0.
    """
    q_words = _words(query) - _STOP_WORDS
    r_words = _words(result) - _STOP_WORDS
    if not q_words:
        return 0.0
    return len(q_words & r_words) / len(q_words)


def search_propublica(name: str, state: str) -> list[dict]:
    """
    Search ProPublica for a nonprofit by name and state.
    Returns list of result dicts (may be empty).
    """
    try:
        resp = requests.get(
            PROPUBLICA_SEARCH_URL,
            params={"q": name, "state[id]": state},
            timeout=10,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        return data.get("organizations", [])
    except Exception:
        return []


def fetch_org_detail(ein: str) -> dict:
    """
    Fetch the organization detail page from ProPublica, which includes
    a list of 990 filings with financial data.
    Returns a dict with 'organization' and 'filings_with_data' keys, or {}.
    """
    try:
        resp = requests.get(
            PROPUBLICA_ORG_URL.format(ein=ein),
            timeout=10,
        )
        if resp.status_code != 200:
            return {}
        return resp.json()
    except Exception:
        return {}


def best_match(query_name: str, results: list[dict]) -> dict | None:
    """
    Pick the best matching organization from ProPublica search results.
    Returns the result dict or None if no result meets the threshold.
    """
    best = None
    best_score = 0.0
    for r in results:
        score = _match_score(query_name, r.get("name", ""))
        if score > best_score:
            best_score = score
            best = r
    if best_score >= MIN_MATCH_SCORE:
        return best
    return None


def extract_financials(org_detail: dict) -> dict:
    """
    Pull the most recent year's financial data from a ProPublica org detail response.
    Returns a flat dict ready to upsert into irs_990.
    """
    org = org_detail.get("organization", {})
    filings = org_detail.get("filings_with_data", [])

    # Sort filings newest-first and take the first one
    filings_sorted = sorted(filings, key=lambda f: f.get("tax_prd_yr", 0), reverse=True)
    filing = filings_sorted[0] if filings_sorted else {}

    ein_raw = org.get("ein", "")
    ein = str(ein_raw).zfill(9) if ein_raw else None

    total_revenue  = filing.get("totrevenue")
    total_expenses = filing.get("totfuncexpns")

    # net_income isn't a direct field — compute it if we have both
    net_income = None
    if total_revenue is not None and total_expenses is not None:
        try:
            net_income = float(total_revenue) - float(total_expenses)
        except (TypeError, ValueError):
            pass

    return {
        "ein":                      ein,
        "org_name":                 org.get("name"),
        "city":                     org.get("city"),
        "state":                    org.get("state"),
        "ntee_code":                org.get("ntee_code"),
        "subsection_code":          org.get("subsection_code"),
        "total_revenue":            _safe_float(total_revenue),
        "total_expenses":           _safe_float(total_expenses),
        "total_assets":             _safe_float(filing.get("totassetsend")),
        "net_income":               net_income,
        "program_service_revenue":  _safe_float(filing.get("prgmservrev")),
        "program_service_expenses": _safe_float(filing.get("progrmserviceexp")),
        "officer_compensation":     _safe_float(filing.get("compnsatncurrofcr")),
        "tax_year":                 filing.get("tax_prd_yr"),
        "filing_pdf_url":           filing.get("pdf_url"),
        "data_source":              "ProPublica",
    }


def _safe_float(val) -> float | None:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Per-facility-type fetch functions
# ---------------------------------------------------------------------------

def fetch_990_for_charter_schools(states=None, limit=None, overwrite=False, verbose=False):
    """
    For each charter school, search ProPublica using the LEA name (the
    operator/district entity that files the 990), link the EIN, and store
    the financial data.
    """
    conn = db.get_connection()
    cur = conn.cursor()

    conditions = ["is_charter = 1", "school_status = 'Open'"]
    params = []

    if not overwrite:
        conditions.append("(ein IS NULL OR ein = '')")
    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    where = "WHERE " + " AND ".join(conditions)

    # Fetch all rows and deduplicate BEFORE applying the limit so we don't
    # accidentally process the same LEA operator multiple times.
    cur.execute(
        f"SELECT DISTINCT lea_id, lea_name, school_name, state "
        f"FROM schools {where} ORDER BY state, lea_name",
        params,
    )
    rows = cur.fetchall()
    conn.close()

    # Deduplicate by lea_id so we don't search for the same operator twice.
    # For charter schools each school often has its own LEA — in that case
    # lea_id is unique per school and we use lea_name as-is.
    seen_lea = set()
    orgs = []
    for lea_id, lea_name, school_name, state in rows:
        if lea_id and lea_id in seen_lea:
            continue
        seen_lea.add(lea_id)
        search_name = lea_name or school_name
        if search_name:
            orgs.append({"lea_id": lea_id, "name": search_name, "state": state})

    # Apply limit after dedup
    if limit:
        orgs = orgs[:limit]

    total = len(orgs)
    print(f"  Searching 990s for {total:,} charter school operators...")

    linked = 0
    failed = 0

    for i, org in enumerate(orgs, 1):
        results = search_propublica(org["name"], org["state"])
        time.sleep(API_SLEEP)

        if verbose:
            print(f"\n  [{i}] Search: '{org['name']}' ({org['state']})")
            print(f"       API returned {len(results)} results")
            for r in results[:3]:
                score = _match_score(org["name"], r.get("name", ""))
                print(f"       • {r.get('name')} — score={score:.2f}")

        match = best_match(org["name"], results)
        if not match:
            if verbose:
                print(f"       → NO MATCH (threshold={MIN_MATCH_SCORE})")
            failed += 1
            continue

        ein = str(match.get("ein", "")).zfill(9)

        # Fetch financial detail for this EIN
        detail = fetch_org_detail(ein)
        time.sleep(API_SLEEP)

        if not detail:
            failed += 1
            continue

        financials = extract_financials(detail)
        if not financials.get("ein"):
            failed += 1
            continue

        db.upsert_990(financials)

        # Link the EIN to all schools with this lea_id
        conn = db.get_connection()
        cur = conn.cursor()
        if org["lea_id"]:
            cur.execute(
                "UPDATE schools SET ein = ?, updated_at = CURRENT_TIMESTAMP "
                "WHERE lea_id = ? AND is_charter = 1",
                (ein, org["lea_id"]),
            )
        else:
            cur.execute(
                "UPDATE schools SET ein = ?, updated_at = CURRENT_TIMESTAMP "
                "WHERE school_name = ? AND state = ? AND is_charter = 1",
                (ein, org["name"], org["state"]),
            )
        conn.commit()
        conn.close()
        linked += 1

        if i % 100 == 0 or i == total:
            print(f"  [{i:,}/{total:,}] {linked:,} linked, {failed} not matched")

    print(f"  Charter school operators: {linked:,} linked, {failed} not matched")
    return linked


def fetch_990_for_fqhc(states=None, limit=None, overwrite=False, verbose=False):
    """
    For each FQHC parent organization, search ProPublica using health_center_name,
    link the EIN, and store the financial data.
    """
    conn = db.get_connection()
    cur = conn.cursor()

    conditions = ["is_active = 1"]
    params = []

    if not overwrite:
        conditions.append("(ein IS NULL OR ein = '')")
    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    where = "WHERE " + " AND ".join(conditions)

    # Fetch all then apply limit after dedup (same pattern as charter schools)
    cur.execute(
        f"SELECT DISTINCT health_center_name, state "
        f"FROM fqhc {where} ORDER BY state, health_center_name",
        params,
    )
    rows = cur.fetchall()
    conn.close()

    if limit:
        rows = rows[:limit]

    total = len(rows)
    print(f"  Searching 990s for {total:,} FQHC parent organizations...")

    linked = 0
    failed = 0

    for i, (hc_name, state) in enumerate(rows, 1):
        if not hc_name:
            failed += 1
            continue

        results = search_propublica(hc_name, state)
        time.sleep(API_SLEEP)

        if verbose:
            print(f"\n  [{i}] Search: '{hc_name}' ({state})")
            print(f"       API returned {len(results)} results")
            for r in results[:3]:
                score = _match_score(hc_name, r.get("name", ""))
                print(f"       • {r.get('name')} — score={score:.2f}")

        match = best_match(hc_name, results)
        if not match:
            if verbose:
                print(f"       → NO MATCH (threshold={MIN_MATCH_SCORE})")
            failed += 1
            continue

        ein = str(match.get("ein", "")).zfill(9)

        detail = fetch_org_detail(ein)
        time.sleep(API_SLEEP)

        if not detail:
            failed += 1
            continue

        financials = extract_financials(detail)
        if not financials.get("ein"):
            failed += 1
            continue

        db.upsert_990(financials)

        # Link the EIN to all sites belonging to this health center org
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE fqhc SET ein = ? WHERE health_center_name = ? AND state = ?",
            (ein, hc_name, state),
        )
        conn.commit()
        conn.close()
        linked += 1

        if i % 100 == 0 or i == total:
            print(f"  [{i:,}/{total:,}] {linked:,} linked, {failed} not matched")

    print(f"  FQHC organizations: {linked:,} linked, {failed} not matched")
    return linked


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch IRS 990 data for charter schools and FQHCs via ProPublica"
    )
    parser.add_argument("--schools",   action="store_true", help="Process charter schools only")
    parser.add_argument("--fqhc",      action="store_true", help="Process health centers only")
    parser.add_argument("--states",    nargs="+", metavar="ST", help="Limit to specific states (e.g. CA TX)")
    parser.add_argument("--limit",     type=int, help="Max organizations to process per facility type")
    parser.add_argument("--overwrite", action="store_true", help="Re-fetch even already-linked facilities")
    parser.add_argument("--verbose",   action="store_true", help="Print search query and top API results for each org")
    args = parser.parse_args()

    # Ensure all tables and columns exist (adds ein column to schools/fqhc if missing)
    db.init_db()

    # If neither flag is set, do both
    do_schools = args.schools or (not args.schools and not args.fqhc)
    do_fqhc    = args.fqhc    or (not args.schools and not args.fqhc)

    print("CD Command Center — IRS 990 Data Fetch")
    print(f"  Source:   ProPublica Nonprofit Explorer API")
    print(f"  Targets:  {'Charter schools' if do_schools else ''} {'FQHCs' if do_fqhc else ''}".strip())
    if args.states:
        print(f"  States:   {args.states}")
    if args.limit:
        print(f"  Limit:    {args.limit} per facility type")
    print()

    if do_schools:
        print("Charter schools:")
        fetch_990_for_charter_schools(
            states=args.states,
            limit=args.limit,
            overwrite=args.overwrite,
            verbose=args.verbose,
        )
        print()

    if do_fqhc:
        print("FQHCs:")
        fetch_990_for_fqhc(
            states=args.states,
            limit=args.limit,
            overwrite=args.overwrite,
            verbose=args.verbose,
        )
        print()

    summary = db.get_990_summary()
    print("Database now contains:")
    print(f"  Total 990 records:          {summary['total_990_records']:,}")
    print(f"  Charter schools linked:     {summary['linked_charter_schools']:,}")
    print(f"  FQHC sites linked:          {summary['linked_fqhc_sites']:,}")
    print()
    print("Done.")


if __name__ == "__main__":
    main()
