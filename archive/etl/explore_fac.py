"""
Exploratory script for the Federal Audit Clearinghouse (FAC) API.

Goal: hit /general and /federal_awards once with small filters and print
the field names + a sample record from each, so we can design the
federal_audits / federal_audit_programs schema against REAL response data
rather than guessing.

Writes nothing to the database. Throwaway/inspection only.

Usage:
    export FAC_API_KEY=your_data_gov_key
    python etl/explore_fac.py

    # or pass on CLI:
    python etl/explore_fac.py --api-key YOUR_KEY

Get a free key at https://api.data.gov/signup/
FAC API docs: https://www.fac.gov/api/
"""

import argparse
import json
import os
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

FAC_BASE = "https://api.fac.gov"


def fac_get(endpoint: str, params: dict, api_key: str, limit: int = 5) -> list:
    """Hit a FAC PostgREST endpoint and return parsed JSON."""
    params = {**params, "limit": str(limit)}
    url = f"{FAC_BASE}/{endpoint}?{urlencode(params)}"
    req = Request(url, headers={
        "X-Api-Key": api_key,
        "Accept": "application/json",
    })
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"HTTP {e.code} on {url}\n{body}", file=sys.stderr)
        raise
    except URLError as e:
        print(f"URL error on {url}: {e}", file=sys.stderr)
        raise


def describe(label: str, records: list) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {label}  ({len(records)} record(s) returned)")
    print(f"{'=' * 70}")
    if not records:
        print("  (no records — try widening the filter)")
        return
    sample = records[0]
    print(f"\n  Field names ({len(sample)} total):")
    for k in sorted(sample.keys()):
        v = sample[k]
        v_repr = "<null>" if v is None else (repr(v)[:60] + ("…" if len(repr(v)) > 60 else ""))
        print(f"    {k:40s} = {v_repr}")
    print(f"\n  Full first record (JSON):")
    print(json.dumps(sample, indent=2, default=str))


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--api-key",
        default=os.environ.get("FAC_API_KEY"),
        help="data.gov API key. Or set FAC_API_KEY env var.",
    )
    p.add_argument("--state", default="CO", help="Auditee state filter (default: CO)")
    p.add_argument("--year", default="2024", help="Audit year filter (default: 2024)")
    args = p.parse_args()

    if not args.api_key:
        print(
            "Error: --api-key or FAC_API_KEY env var required.\n"
            "Get a free key at https://api.data.gov/signup/",
            file=sys.stderr,
        )
        return 2

    print(f"Probing FAC API at {FAC_BASE}")
    print(f"Filter: auditee_state={args.state}, audit_year={args.year}, limit=5")

    # 1) /general — one row per audit submission. Audit-level metadata.
    general = fac_get(
        "general",
        {
            "auditee_state": f"eq.{args.state}",
            "audit_year": f"eq.{args.year}",
        },
        args.api_key,
        limit=5,
    )
    describe("/general — audit metadata", general)

    # If we got an audit, pull federal awards for the same report_id so the
    # two responses are linkable and we can see the join key in action.
    if general:
        report_id = general[0].get("report_id")
        if report_id:
            awards = fac_get(
                "federal_awards",
                {"report_id": f"eq.{report_id}"},
                args.api_key,
                limit=10,
            )
            describe(
                f"/federal_awards — program line items for report_id={report_id}",
                awards,
            )
        else:
            print("\n(no report_id field on /general response — checking what the join key is called)")

    print("\nDone. Use the field names above to design federal_audits + federal_audit_programs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
