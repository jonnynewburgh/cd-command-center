"""
etl/fetch_fac.py — Load Federal Audit Clearinghouse (FAC) Single Audit data.

The Federal Audit Clearinghouse is the federal registry of Single Audits — every
non-federal entity expending $1M+ ($750K pre-FY25) in federal awards in a fiscal
year is required to submit one. This is a HORIZONTAL data source: it layers across
schools (charter networks), fqhc, cdfi_directory, ece_centers, and any other
EIN-keyed entity table via auditee_ein.

For deal origination this is gold:
    - EIN-keyed → joins straight into irs_990 and your existing entity tables
    - Total federal expenditures (a clean proxy for organizational scale)
    - Per-program detail by ALN (Assistance Listing Number, formerly CFDA)
    - Findings flags (going concern, material weakness, etc.) as risk signals
    - Auditee contact data (CFOs, EDs) for outreach

Data source:
    GSA's FAC at https://www.fac.gov/ (replaced the legacy Census Bureau FAC in
    October 2023). Public REST API at https://api.fac.gov, PostgREST-style.
    Federal public-domain data; no commercial restrictions.

API key:
    Free at https://api.data.gov/signup/. The same key works for many federal
    APIs. Pass via --api-key or set FAC_API_KEY env var.

Important coverage caveats:
    - The new FAC only contains audits submitted on/after Oct 1, 2023. Older
      audits live in the legacy Census FAC and are not loaded by this script.
    - Single Audit threshold: $750K for FYs starting before 10/1/2024,
      $1M for FYs starting on/after 10/1/2024. Both appear in the data.
    - Entities below the threshold are not in FAC even if they receive federal $.

Two-table design:
    federal_audits          — one row per audit submission (header + auditee + opinion)
    federal_audit_programs  — one row per (report_id, award_reference); the line items
                              with ALN, amount expended, loan vs. grant flag, etc.

Idempotency:
    - federal_audits is upserted on report_id (PK)
    - federal_audit_programs is upserted on (report_id, award_reference) composite PK
    - Re-running with the same filter is safe; updated audits get refreshed via
      resubmission_version tracking

Usage:
    # Test load — one state, one year (~hundreds of audits)
    python etl/fetch_fac.py --state CO --year 2024

    # Multiple years for one state
    python etl/fetch_fac.py --state CO --years 2023 2024

    # All states, recent year (~tens of thousands of audits)
    python etl/fetch_fac.py --all-states --year 2024

    # Pass API key explicitly
    python etl/fetch_fac.py --api-key YOUR_KEY --state CO --year 2024

    # Or set FAC_API_KEY env var and omit --api-key
    export FAC_API_KEY=your_data_gov_key
    python etl/fetch_fac.py --state CO --year 2024

Tip:
    Start small. The first run for a new state/year prints how many audits were
    found before fetching line items, so you can sanity-check before committing
    to a long run.
"""

import argparse
import os
import sys
import time
from typing import Iterable

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

FAC_BASE = "https://api.fac.gov"
PAGE_SIZE = 500          # PostgREST max per request without explicit Range header
REQUEST_DELAY = 0.5      # be polite; api.data.gov default rate limit is 1000/hr


# All US states + DC + territories that show up in FAC submissions
ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "AS", "GU", "MP", "PR", "VI",
]


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def yn_to_bool(v) -> bool | None:
    """FAC stores most boolean fields as 'Yes'/'No' strings. Normalize to bool."""
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("y", "yes", "true", "1"):
        return True
    if s in ("n", "no", "false", "0"):
        return False
    return None


def to_int(v) -> int | None:
    """Coerce to int, tolerating empty strings, None, and stringified numbers."""
    if v is None or v == "":
        return None
    try:
        return int(float(v))  # float() handles '10919275.00'
    except (ValueError, TypeError):
        return None


def to_str(v) -> str | None:
    """Empty strings → None, otherwise stringified."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def construct_aln(prefix: str | None, extension: str | None) -> str | None:
    """Build the conventional ALN string '14.126' from FAC's split fields."""
    p = to_str(prefix)
    e = to_str(extension)
    if not p or not e:
        return None
    return f"{p}.{e}"


# ---------------------------------------------------------------------------
# FAC API client
# ---------------------------------------------------------------------------

def fac_request(endpoint: str, params: dict, api_key: str) -> list[dict]:
    """
    Hit a FAC PostgREST endpoint and return parsed JSON.
    Caller is responsible for pagination via 'offset' / 'limit' params.
    Retries up to 3 times on connection errors and rate limits.
    """
    url = f"{FAC_BASE}/{endpoint}"
    headers = {
        "X-Api-Key": api_key,
        "Accept": "application/json",
    }
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=90)
            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"  WARNING: rate limited (429), sleeping {wait}s (attempt {attempt+1}/3)...", file=sys.stderr)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            wait = 30 * (attempt + 1)
            print(f"  WARNING: {type(e).__name__}, sleeping {wait}s (attempt {attempt+1}/3)...", file=sys.stderr)
            time.sleep(wait)
    # Final attempt — let it raise
    resp = requests.get(url, params=params, headers=headers, timeout=120)
    resp.raise_for_status()
    return resp.json()


def paginate(endpoint: str, base_params: dict, api_key: str) -> Iterable[dict]:
    """Yield records from a FAC endpoint, paging through with offset/limit."""
    offset = 0
    while True:
        params = {**base_params, "limit": str(PAGE_SIZE), "offset": str(offset)}
        batch = fac_request(endpoint, params, api_key)
        if not batch:
            return
        for record in batch:
            yield record
        if len(batch) < PAGE_SIZE:
            return
        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)


# ---------------------------------------------------------------------------
# Field mapping: raw FAC response → our table rows
# ---------------------------------------------------------------------------

def map_general_record(raw: dict) -> dict:
    """Map a /general response record to a federal_audits row dict."""
    return {
        "report_id":              to_str(raw.get("report_id")),

        "auditee_ein":            to_str(raw.get("auditee_ein")),
        "auditee_uei":            to_str(raw.get("auditee_uei")),
        "auditee_name":           to_str(raw.get("auditee_name")),
        "entity_type":            to_str(raw.get("entity_type")),
        "is_multiple_eins":       yn_to_bool(raw.get("is_multiple_eins")),

        "auditee_address_line_1": to_str(raw.get("auditee_address_line_1")),
        "auditee_city":           to_str(raw.get("auditee_city")),
        "auditee_state":          to_str(raw.get("auditee_state")),
        "auditee_zip":            to_str(raw.get("auditee_zip")),

        "auditee_contact_name":   to_str(raw.get("auditee_contact_name")),
        "auditee_contact_title":  to_str(raw.get("auditee_contact_title")),
        "auditee_email":          to_str(raw.get("auditee_email")),
        "auditee_phone":          to_str(raw.get("auditee_phone")),
        "auditee_certify_name":   to_str(raw.get("auditee_certify_name")),
        "auditee_certify_title":  to_str(raw.get("auditee_certify_title")),
        "auditee_certified_date": to_str(raw.get("auditee_certified_date")),

        "audit_year":             to_int(raw.get("audit_year")),
        "fy_start_date":          to_str(raw.get("fy_start_date")),
        "fy_end_date":            to_str(raw.get("fy_end_date")),
        "audit_period_covered":   to_str(raw.get("audit_period_covered")),
        "audit_type":             to_str(raw.get("audit_type")),

        "total_amount_expended":  to_int(raw.get("total_amount_expended")),
        "dollar_threshold":       to_int(raw.get("dollar_threshold")),

        "gaap_results":           to_str(raw.get("gaap_results")),
        "is_going_concern":           yn_to_bool(raw.get("is_going_concern_included")),
        "is_material_weakness":       yn_to_bool(raw.get("is_internal_control_material_weakness_disclosed")),
        "is_significant_deficiency":  yn_to_bool(raw.get("is_internal_control_deficiency_disclosed")),
        "is_material_noncompliance":  yn_to_bool(raw.get("is_material_noncompliance_disclosed")),
        "is_low_risk_auditee":        yn_to_bool(raw.get("is_low_risk_auditee")),
        "agencies_with_prior_findings": to_str(raw.get("agencies_with_prior_findings")),

        "cognizant_agency":       to_str(raw.get("cognizant_agency")),
        "oversight_agency":       to_str(raw.get("oversight_agency")),

        "auditor_firm_name":      to_str(raw.get("auditor_firm_name")),
        "auditor_ein":            to_str(raw.get("auditor_ein")),
        "auditor_state":          to_str(raw.get("auditor_state")),
        "auditor_city":           to_str(raw.get("auditor_city")),
        "auditor_zip":            to_str(raw.get("auditor_zip")),
        "auditor_address_line_1": to_str(raw.get("auditor_address_line_1")),
        "auditor_country":        to_str(raw.get("auditor_country")),
        "auditor_contact_name":   to_str(raw.get("auditor_contact_name")),
        "auditor_contact_title":  to_str(raw.get("auditor_contact_title")),
        "auditor_email":          to_str(raw.get("auditor_email")),
        "auditor_phone":          to_str(raw.get("auditor_phone")),
        "auditor_certify_name":   to_str(raw.get("auditor_certify_name")),
        "auditor_certify_title":  to_str(raw.get("auditor_certify_title")),
        "auditor_certified_date": to_str(raw.get("auditor_certified_date")),

        "submitted_date":         to_str(raw.get("submitted_date")),
        "fac_accepted_date":      to_str(raw.get("fac_accepted_date")),
        "resubmission_version":   to_int(raw.get("resubmission_version")),
    }


def map_award_record(raw: dict) -> dict:
    """Map a /federal_awards response record to a federal_audit_programs row dict."""
    prefix = to_str(raw.get("federal_agency_prefix"))
    extension = to_str(raw.get("federal_award_extension"))
    return {
        "report_id":               to_str(raw.get("report_id")),
        "award_reference":         to_str(raw.get("award_reference")),

        "aln":                     construct_aln(prefix, extension),
        "federal_agency_prefix":   prefix,
        "federal_award_extension": extension,
        "federal_program_name":    to_str(raw.get("federal_program_name")),

        "amount_expended":         to_int(raw.get("amount_expended")),
        "federal_program_total":   to_int(raw.get("federal_program_total")),

        "is_loan":                 yn_to_bool(raw.get("is_loan")),
        "loan_balance":            to_int(raw.get("loan_balance")),
        "is_direct":               yn_to_bool(raw.get("is_direct")),
        "is_passthrough_award":    yn_to_bool(raw.get("is_passthrough_award")),
        "passthrough_amount":      to_int(raw.get("passthrough_amount")),
        "is_major":                yn_to_bool(raw.get("is_major")),

        "cluster_name":            to_str(raw.get("cluster_name")),
        "other_cluster_name":      to_str(raw.get("other_cluster_name")),
        "state_cluster_name":      to_str(raw.get("state_cluster_name")),
        "cluster_total":           to_int(raw.get("cluster_total")),

        "findings_count":          to_int(raw.get("findings_count")),
        "audit_report_type":       to_str(raw.get("audit_report_type")),
    }


# ---------------------------------------------------------------------------
# Per-(state, year) load
# ---------------------------------------------------------------------------

def load_state_year(state: str, year: int, api_key: str, dry_run: bool = False) -> tuple[int, int]:
    """
    Load all audits for one (state, year) into federal_audits + federal_audit_programs.
    Returns (audits_loaded, programs_loaded).
    """
    print(f"\n-> Fetching audits for state={state}, year={year}")

    # 1) Pull all audit headers for this state/year
    base_params = {
        "auditee_state": f"eq.{state}",
        "audit_year":    f"eq.{year}",
    }

    audit_rows = []
    seen_report_ids = []
    for raw in paginate("general", base_params, api_key):
        row = map_general_record(raw)
        if row["report_id"]:
            audit_rows.append(row)
            seen_report_ids.append(row["report_id"])

    print(f"  Found {len(audit_rows)} audit(s)")
    if not audit_rows:
        return (0, 0)

    if dry_run:
        print("  (dry-run; not writing to database)")
        return (len(audit_rows), 0)

    # 2) Pull federal_awards for these audits.
    # PostgREST supports "in.(...)" filters, but URL length caps total report_ids
    # per request. Batch in chunks of 50 to stay safely under URL limits.
    program_rows = []
    BATCH = 50
    for i in range(0, len(seen_report_ids), BATCH):
        chunk = seen_report_ids[i : i + BATCH]
        ids_param = "in.(" + ",".join(chunk) + ")"
        for raw in paginate("federal_awards", {"report_id": ids_param}, api_key):
            row = map_award_record(raw)
            if row["report_id"] and row["award_reference"]:
                program_rows.append(row)

    print(f"  Found {len(program_rows)} program line item(s)")

    # 3) Upsert in dependency order: parent then children
    audits_n = db.upsert_rows("federal_audits", audit_rows, unique_cols=["report_id"])
    programs_n = db.upsert_rows(
        "federal_audit_programs",
        program_rows,
        unique_cols=["report_id", "award_reference"],
    )
    print(f"  Loaded: {audits_n} audits, {programs_n} programs")
    return (audits_n, programs_n)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(
        description="Load Federal Audit Clearinghouse (FAC) Single Audit data."
    )
    p.add_argument(
        "--api-key",
        default=os.environ.get("FAC_API_KEY"),
        help="data.gov API key. Free at https://api.data.gov/signup/. Or set FAC_API_KEY env var.",
    )
    p.add_argument("--state", help="Two-letter state code, e.g. CO. Mutually exclusive with --all-states.")
    p.add_argument("--all-states", action="store_true", help="Load all states + DC + territories.")
    p.add_argument("--year", type=int, help="Single audit year, e.g. 2024.")
    p.add_argument("--years", type=int, nargs="+", help="Multiple audit years, e.g. --years 2023 2024.")
    p.add_argument("--dry-run", action="store_true", help="Fetch headers only; don't write to DB.")
    args = p.parse_args()

    if not args.api_key:
        print(
            "Error: --api-key or FAC_API_KEY env var required.\n"
            "Get a free key at https://api.data.gov/signup/",
            file=sys.stderr,
        )
        return 2

    if not args.state and not args.all_states:
        print("Error: must specify --state CO or --all-states.", file=sys.stderr)
        return 2
    if args.state and args.all_states:
        print("Error: --state and --all-states are mutually exclusive.", file=sys.stderr)
        return 2

    if not args.year and not args.years:
        print("Error: must specify --year 2024 or --years 2023 2024.", file=sys.stderr)
        return 2

    states = ALL_STATES if args.all_states else [args.state.upper()]
    years = args.years if args.years else [args.year]

    # Make sure tables exist before loading anything
    db.init_db()

    run_id = db.log_load_start("fac")
    total_audits = 0
    total_programs = 0
    error = None
    try:
        for year in years:
            for state in states:
                a, p_ = load_state_year(state, year, args.api_key, dry_run=args.dry_run)
                total_audits += a
                total_programs += p_
    except Exception as e:
        error = str(e)
        print(f"\nERROR: {error}", file=sys.stderr)
        raise
    finally:
        db.log_load_finish(run_id, rows_loaded=total_audits + total_programs, error=error)

    print(f"\nDone. Total: {total_audits} audits + {total_programs} programs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
