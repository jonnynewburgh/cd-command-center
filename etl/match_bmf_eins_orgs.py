"""
etl/match_bmf_eins_orgs.py — Match organizations in entity tables to EINs
using the IRS Exempt Organizations Business Master File (BMF).

Generalization of fetch_bmf_eins.py (which is school-specific) to populate
the `ein` column on:
  - ece_centers          (name=provider_name, NTEE P*/B*)
  - headstart_programs   (name=grantee_name,  any NTEE — broad filter)
  - cdfi_directory       (name=cdfi_name,     any NTEE — banks/CUs won't match)
  - nmtc_projects        (name=project_name,  any NTEE — currently 0 matches; project_name is NULL on all rows because the 2024 CDFI Fund release strips it. Recover by loading older release or extracting the NMTC Coalition PDF.)

Not included: nmtc_coalition_projects has no real org name to match against
(only purpose-of-investment categories), so EIN matching from this table
would only yield false positives.

Reuses match_score / find_best_match / load_bmf from fetch_bmf_eins.py.

Usage:
    python etl/match_bmf_eins_orgs.py                     # all tables
    python etl/match_bmf_eins_orgs.py --tables headstart_programs cdfi_directory
    python etl/match_bmf_eins_orgs.py --dry-run
    python etl/match_bmf_eins_orgs.py --min-score 0.8
    python etl/match_bmf_eins_orgs.py --force-download
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db
from etl.fetch_bmf_eins import (
    download_bmf,
    load_bmf,
    find_best_match,
    DEFAULT_MIN_SCORE,
)


# Per-table config. NTEE prefix "" means no filter (load all of BMF — ~1.8M orgs,
# heavier RAM but needed when grantees span many sectors).
TABLE_CONFIGS = {
    "ece_centers": {
        "name_col":  "provider_name",
        "state_col": "state",
        "zip_col":   "zip_code",
        "ntee":      "",
    },
    "headstart_programs": {
        "name_col":  "grantee_name",
        "state_col": "state",
        "zip_col":   "zip_code",
        "ntee":      "",
    },
    "cdfi_directory": {
        "name_col":  "cdfi_name",
        "state_col": "state",
        "zip_col":   None,
        "ntee":      "",
    },
    "nmtc_projects": {
        "name_col":  "project_name",
        "state_col": "state",
        "zip_col":   "zip_code",
        "ntee":      "",
    },
    # nmtc_coalition_projects intentionally omitted — has no org name column,
    # only purpose_of_investment (a category). See module docstring.
}


def _build_index_all_ntee(bmf):
    """State-keyed lookup like fetch_bmf_eins._build_index, but no NTEE filter
    is implied — caller already filtered (or chose not to)."""
    index = {}
    for _, row in bmf.iterrows():
        state = row["STATE"]
        if state not in index:
            index[state] = []
        index[state].append((
            row["NAME"], row["EIN"], row.get("ZIP", "") or "", row.get("NTEE_CD", ""),
        ))
    return index


def _get_unique_orgs(table, name_col, state_col, zip_col):
    """Return distinct (name, state, zip) tuples where ein IS NULL."""
    conn = db.get_connection()
    cur = conn.cursor()
    zip_select = f", MIN({zip_col})" if zip_col else ", ''"
    sql = (
        f"SELECT {name_col}, {state_col}{zip_select} "
        f"FROM {table} "
        f"WHERE (ein IS NULL OR ein = '') "
        f"  AND {name_col} IS NOT NULL AND {name_col} != '' "
        f"  AND {state_col} IS NOT NULL AND {state_col} != '' "
        f"GROUP BY {name_col}, {state_col} "
        f"ORDER BY {state_col}, {name_col}"
    )
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return [
        {"name": r[0].strip(), "state": r[1].strip().upper(), "zip": (r[2] or "")[:5]}
        for r in rows if r[0] and r[1]
    ]


def _link_ein(table, name_col, state_col, name, state, ein):
    """Update all rows in the table matching (name, state) with the EIN."""
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute(
        f"UPDATE {table} SET ein = %s WHERE {name_col} = %s AND {state_col} = %s",
        (ein, name, state),
    )
    rowcount = cur.rowcount
    conn.commit()
    conn.close()
    return rowcount


def match_table(table, cfg, index, min_score, dry_run, verbose):
    name_col, state_col, zip_col = cfg["name_col"], cfg["state_col"], cfg["zip_col"]
    print(f"\n--- {table} ---")
    orgs = _get_unique_orgs(table, name_col, state_col, zip_col)
    total = len(orgs)
    print(f"  Unique unlinked orgs: {total:,}")
    if total == 0:
        return {"table": table, "unique": 0, "matched": 0, "rows_updated": 0}

    matched = 0
    rows_updated = 0
    for i, org in enumerate(orgs, 1):
        result = find_best_match(org["name"], org["state"], index, min_score, org["zip"])
        if result:
            ein, bmf_name, score = result
            matched += 1
            if verbose:
                print(f"  HIT [{i:,}] {org['name'][:45]:<45} ({org['state']}) "
                      f"-> {bmf_name[:45]:<45}  EIN={ein}  score={score:.2f}")
            if not dry_run:
                rows_updated += _link_ein(table, name_col, state_col, org["name"], org["state"], ein)
        if not verbose and i % 1000 == 0:
            print(f"  [{i:,}/{total:,}]  matched: {matched:,}")

    print(f"  Matched: {matched:,} / {total:,} ({100.0*matched/total:.1f}%)")
    print(f"  Rows updated: {rows_updated:,}")
    return {"table": table, "unique": total, "matched": matched, "rows_updated": rows_updated}


def main():
    parser = argparse.ArgumentParser(
        description="Match entity-table orgs to EINs via the IRS BMF"
    )
    parser.add_argument("--tables", nargs="+", choices=list(TABLE_CONFIGS.keys()),
                        help="Specific tables to match (default: all)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--min-score", type=float, default=DEFAULT_MIN_SCORE)
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    tables = args.tables or list(TABLE_CONFIGS.keys())

    print("CD Command Center — BMF EIN Matcher (entity tables)")
    print(f"  Min score:  {args.min_score}")
    print(f"  Tables:     {', '.join(tables)}")
    if args.dry_run:
        print("  Mode:       DRY RUN")
    print()

    print("BMF files:")
    paths = download_bmf(force=args.force_download)
    if not paths:
        print("No BMF files available. Exiting.")
        sys.exit(1)
    print()

    print("Loading BMF (no NTEE filter — full master file)...")
    bmf = load_bmf(paths, ntee_prefix="")
    print(f"  {len(bmf):,} tax-exempt organizations loaded")
    print()

    print("Building state-keyed index...")
    index = _build_index_all_ntee(bmf)
    print(f"  {sum(len(v) for v in index.values()):,} indexed across {len(index)} states")

    summary = []
    for table in tables:
        try:
            summary.append(match_table(table, TABLE_CONFIGS[table], index,
                                       args.min_score, args.dry_run, args.verbose))
        except Exception as exc:
            print(f"  ERROR on {table}: {exc}")
            summary.append({"table": table, "error": str(exc)})

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for s in summary:
        if "error" in s:
            print(f"  {s['table']:30s} ERROR: {s['error']}")
        else:
            pct = 100.0 * s["matched"] / s["unique"] if s["unique"] else 0
            print(f"  {s['table']:30s} {s['matched']:>6,} / {s['unique']:>6,} "
                  f"({pct:5.1f}%) — {s['rows_updated']:,} rows updated")


if __name__ == "__main__":
    main()
