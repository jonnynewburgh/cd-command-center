"""
etl/validate_authorizer_registry.py -- sanity-check the authorizer registry
against NACSA's statutory landscape.

For every row in `authorizers`, confirms the `authorizer_kind` (LEA, ICB,
SEA, HEI, NFP, NEG, ...) is one of the kinds NACSA records as statutorily
permitted in that state. Mismatches mean either:
  - the authorizer is mis-typed (e.g., GA's SCSC labeled LEA when it's ICB)
  - the NACSA seed is stale relative to a recent statutory change
  - a state-specific builder is emitting a kind NACSA doesn't track

The check is trivial today (GA only -- 11 authorizers in {LEA, ICB},
NACSA-permitted set for GA is {LEA, ICB, SEA}), but earns its keep as
new state builders come online: catches drift before it leaks into the
school-authorizer links.

Usage:
    python etl/validate_authorizer_registry.py
    python etl/validate_authorizer_registry.py --states GA TN
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db  # noqa: E402


def _permitted_kinds_by_state() -> dict[str, set[str]]:
    """Return {state_usps: {kind, ...}} from statutory_charter_authorizer_policy."""
    df = db._pd_read_sql(
        """
        SELECT state_usps, nacsa_col_1, nacsa_col_2, nacsa_col_3, nacsa_col_4
        FROM statutory_charter_authorizer_policy
        """
    )
    out: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        state = (row["state_usps"] or "").strip().upper()
        if not state:
            continue
        kinds: set[str] = set()
        for col in ("nacsa_col_1", "nacsa_col_2", "nacsa_col_3", "nacsa_col_4"):
            cell = row.get(col)
            if cell is None or str(cell).strip() in ("", "nan"):
                continue
            for part in str(cell).split(","):
                token = part.strip().upper()
                if token:
                    kinds.add(token)
        out[state] = kinds
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Validate authorizer.authorizer_kind values against the NACSA statutory landscape"
    )
    parser.add_argument(
        "--states", nargs="*", default=None,
        help="Only check these states (USPS codes). Defaults to all states with authorizers.",
    )
    args = parser.parse_args()

    permitted = _permitted_kinds_by_state()

    where = ""
    params: list = []
    if args.states:
        placeholders = ",".join("?" * len(args.states))
        where = f"WHERE state IN ({placeholders})"
        params = [s.upper() for s in args.states]

    authorizers = db._pd_read_sql(
        f"""
        SELECT id, state, name, authorizer_kind, source_system
        FROM authorizers
        {where}
        ORDER BY state, name
        """,
        params,
    )

    states_present = sorted(authorizers["state"].dropna().unique())
    print(f"Checked {len(authorizers)} authorizers across {len(states_present)} state(s): "
          f"{', '.join(states_present) if states_present else '(none)'}")

    issues = []
    no_kind = []
    no_statute = []
    for _, row in authorizers.iterrows():
        state = (row["state"] or "").strip().upper()
        name = row["name"]
        kind = (row["authorizer_kind"] or "").strip().upper()

        if not kind:
            no_kind.append((state, name))
            continue
        if state not in permitted:
            no_statute.append((state, name, kind))
            continue
        if kind not in permitted[state]:
            issues.append((state, name, kind, sorted(permitted[state])))

    print()
    if issues:
        print(f"FAIL: {len(issues)} authorizer(s) with a kind not statutorily permitted:")
        for state, name, kind, allowed in issues:
            print(f"  {state}  {name!r:50}  kind={kind}  allowed={allowed}")
    else:
        print("OK: every authorizer's kind is in its state's NACSA permitted set.")

    if no_kind:
        print(f"\nWARN: {len(no_kind)} authorizer(s) missing authorizer_kind (cannot validate):")
        for state, name in no_kind:
            print(f"  {state}  {name}")

    if no_statute:
        print(f"\nWARN: {len(no_statute)} authorizer(s) in states not in the NACSA seed:")
        for state, name, kind in no_statute:
            print(f"  {state}  {name}  kind={kind}")

    sys.exit(1 if issues else 0)


if __name__ == "__main__":
    main()
