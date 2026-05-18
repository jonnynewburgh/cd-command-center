"""
Load data/seed/authorizers/nacsa_statutory_landscape.csv into
statutory_charter_authorizer_policy (plus source metadata from the sidecar YAML).

Requires the Alembic migration that creates statutory_charter_authorizer_policy:
    alembic upgrade head

Usage:
    python etl/load_statutory_charter_authorizer_policy.py
    python etl/load_statutory_charter_authorizer_policy.py --csv path/to/custom.csv
"""

import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db


def _repo_root():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_meta():
    """Parse minimal keys from the YAML sidecar without PyYAML."""
    meta_path = os.path.join(
        _repo_root(),
        "data",
        "seed",
        "authorizers",
        "nacsa_statutory_landscape.meta.yaml",
    )
    if not os.path.isfile(meta_path):
        return None, None
    source_url, retrieved = None, None
    with open(meta_path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s.startswith("source_url:"):
                source_url = s[len("source_url:") :].strip()
            elif s.startswith("retrieved:"):
                retrieved = s[len("retrieved:") :].strip().strip('"').strip("'")
    return source_url, retrieved


def main():
    parser = argparse.ArgumentParser(
        description="Load NACSA statutory charter authorizer landscape seed into SQLite/Postgres"
    )
    default_csv = os.path.join(
        _repo_root(),
        "data",
        "seed",
        "authorizers",
        "nacsa_statutory_landscape.csv",
    )
    parser.add_argument("--csv", default=default_csv, help="Path to nacsa_statutory_landscape.csv")
    args = parser.parse_args()

    if not os.path.isfile(args.csv):
        print(f"ERROR: CSV not found: {args.csv}")
        sys.exit(1)

    db.init_db()
    source_url, retrieved = _load_meta()

    df = pd.read_csv(args.csv, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    n = 0
    for _, row in df.iterrows():
        rec = {
            "state_usps": str(row["state_usps"]).strip().upper(),
            "state_name": str(row["state_name"]).strip(),
            "allowed_by_law": _clean(row.get("allowed_by_law")),
            "appeal_only": _clean(row.get("appeal_only")),
            "limited_jurisdiction": _clean(row.get("limited_jurisdiction")),
            "allowed_not_operating": _clean(row.get("allowed_not_operating")),
            "source_url": source_url,
            "retrieved": retrieved,
        }
        db.upsert_statutory_charter_authorizer_policy(rec)
        n += 1

    print(f"Upserted {n} rows into statutory_charter_authorizer_policy.")


def _clean(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() != "nan" else None


if __name__ == "__main__":
    main()
