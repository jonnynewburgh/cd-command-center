"""etl/load_hrsa_shortage_areas.py — Load HRSA HPSA + MUA designation registries.

Sources (manually downloaded to data/raw/fqhcs/):
- BCD_HPSA_FCT_DET_PC.xlsx — Primary Care HPSAs
- BCD_HPSA_FCT_DET_MH.xlsx — Mental Health HPSAs
- BCD_HPSA_FCT_DET_DH.xlsx — Dental HPSAs
- MUA_DET.xlsx          — Medically Underserved Areas / Populations

HRSA Health Professional Shortage Area (HPSA) and Medically Underserved
Area / Population (MUA/P) designations are the federal eligibility
gates for FQHC funding, NHSC scholar/loan repayment, J-1 visa waivers,
and Medicare HPSA bonus payments. Every census tract that's covered
by an active designation is "underwriting-eligible" for these federal
streams.

Each designation has multiple "component" rows in HRSA's source files
(one per geographic component — county, county subdivision, or tract).
We preserve component granularity so callers can query by county FIPS
or census tract without running a spatial join against HRSA's polygon
shapefiles.

Usage:
    # Load all 3 HPSA disciplines + MUA in one shot (default)
    python etl/load_hrsa_shortage_areas.py

    # Load just one file
    python etl/load_hrsa_shortage_areas.py --hpsa-pc
    python etl/load_hrsa_shortage_areas.py --hpsa-mh
    python etl/load_hrsa_shortage_areas.py --hpsa-dh
    python etl/load_hrsa_shortage_areas.py --mua

    # Override default paths
    python etl/load_hrsa_shortage_areas.py --hpsa-pc-file path/to/file.xlsx

    # Skip rows with status='Withdrawn' (default keeps everything)
    python etl/load_hrsa_shortage_areas.py --active-only
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Optional

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db


DEFAULT_DIR = os.path.join("data", "raw", "fqhcs")
DEFAULT_FILES = {
    "PC":  "BCD_HPSA_FCT_DET_PC.xlsx",
    "MH":  "BCD_HPSA_FCT_DET_MH.xlsx",
    "DH":  "BCD_HPSA_FCT_DET_DH.xlsx",
    "MUA": "MUA_DET.xlsx",
}


# ---------------------------------------------------------------------------
# Column mappings: HRSA verbose header → DB column.
# Match is case-insensitive, whitespace-collapsed.
# ---------------------------------------------------------------------------

HPSA_MAP = {
    "hpsa id":                              "hpsa_id",
    "hpsa name":                            "hpsa_name",
    "designation type":                     "designation_type",
    "hpsa score":                           "hpsa_score",
    "hpsa status":                          "hpsa_status",
    "hpsa designation date":                "designation_date",
    "hpsa designation last update date":    "last_update_date",
    "withdrawn date":                       "withdrawn_date",
    "hpsa withdrawn date string":           "withdrawn_date_string",  # discarded; date is canonical

    "state abbreviation":                   "state_abbr",
    "primary state abbreviation":           "state_abbr_alt",
    "state fips code":                      "state_fips",
    "primary state fips code":              "state_fips_alt",
    "common state county fips code":        "county_fips",
    "common county name":                   "county_name",
    "county equivalent name":               "county_name_alt",
    "common postal code":                   "postal_code",
    "hpsa postal code":                     "postal_code_alt",
    "metropolitan indicator":               "metro_indicator",
    "hpsa metropolitan indicator code":     "metro_indicator_code",
    "rural status":                         "rural_status",
    "rural status code":                    "rural_status_code",
    "latitude":                             "latitude",
    "longitude":                            "longitude",

    "hpsa geography identification number": "hpsa_geo_id",
    "hpsa designation population":          "designation_population",
    "hpsa estimated served population":     "served_population",
    "hpsa estimated underserved population":"underserved_population",
    "hpsa resident civilian population":    "resident_civilian_pop",
    "% of population below 100% poverty":   "pct_below_100pct_poverty",
    "hpsa formal ratio":                    "formal_ratio",
    "hpsa fte":                             "hpsa_fte",
    "hpsa shortage":                        "hpsa_shortage",
    "hpsa provider ratio goal":             "provider_ratio_goal",
    "hpsa degree of shortage":              "degree_of_shortage",

    "hpsa component name":                  "component_name",
    "hpsa component type description":      "component_type",
    "hpsa component source identification number": "component_source_id",
    "bhcmis organization identification number":   "bhcmis_org_id",
}


MUA_MAP = {
    "mua/p id":                             "mua_id",
    "mua/p service area name":              "mua_name",
    "designation type":                     "designation_type",
    "mua/p status description":             "mua_status",
    "imu score":                            "imu_score",
    "designation date":                     "designation_date",
    "mua/p update date":                    "update_date",
    "medically underserved area/population (mua/p) withdrawal date": "withdrawal_date",
    "break in designation":                 "break_in_designation",

    "population type":                      "population_type",
    "medically underserved area/population (mua/p) metropolitan description": "metro_indicator",

    "state abbreviation":                   "state_abbr",
    "state fips code":                      "state_fips",
    "state and county federal information processing standard code": "county_fips",
    "complete county name":                 "county_name",
    "county subdivision name":              "county_subdivision_name",
    "census tract":                         "census_tract",
    "rural status description":             "rural_status",

    "medically underserved area/population (mua/p) component geographic name":            "component_name",
    "medically underserved area/population (mua/p) component geographic type description":"component_type",

    "percent of population with incomes at or below 100 percent of the u.s. federal poverty level": "pct_below_100pct_poverty",
    "percentage of population age 65 and over":         "pct_age_65_plus",
    "infant mortality rate":                            "infant_mortality_rate",
    "providers per 1000 population":                    "providers_per_1000",
    "designation population in a medically underserved area/population (mua/p)": "designation_population",
    "medically underserved area/population (mua/p) total resident civilian population":  "total_population",
}


def _norm(s: object) -> str:
    if s is None: return ""
    return " ".join(str(s).lower().split()).strip()


def _coerce_int(v) -> Optional[int]:
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    s = str(v).strip().replace(",", "")
    if not s or s.lower() in ("not applicable", "n/a", "-", "—"): return None
    try:
        return int(round(float(s)))
    except ValueError:
        return None


def _coerce_float(v) -> Optional[float]:
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    s = str(v).strip().replace(",", "").replace("%", "")
    if not s or s.lower() in ("not applicable", "n/a", "-", "—"): return None
    try:
        return float(s)
    except ValueError:
        return None


def _coerce_date(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    if isinstance(v, str):
        s = v.strip()
        if not s or s.lower() in ("not applicable", "n/a"): return None
    try:
        return pd.to_datetime(v).date().isoformat()
    except Exception:
        return None


def _coerce_text(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    s = str(v).strip()
    return s or None


# Per-column type coercion. Fields not listed default to text.
INT_COLS  = {"designation_population", "served_population", "underserved_population",
             "resident_civilian_pop", "total_population"}
FLOAT_COLS = {"hpsa_score", "hpsa_fte", "hpsa_shortage", "pct_below_100pct_poverty",
              "imu_score", "pct_age_65_plus", "infant_mortality_rate",
              "providers_per_1000", "latitude", "longitude"}
DATE_COLS = {"designation_date", "last_update_date", "withdrawn_date",
             "update_date", "withdrawal_date"}


def _coerce(col: str, value):
    if col in INT_COLS:   return _coerce_int(value)
    if col in FLOAT_COLS: return _coerce_float(value)
    if col in DATE_COLS:  return _coerce_date(value)
    return _coerce_text(value)


# ---------------------------------------------------------------------------
# Per-source load functions
# ---------------------------------------------------------------------------

def _build_records(df: pd.DataFrame, mapping: Dict[str, str], extra: Dict) -> List[Dict]:
    """Map verbose HRSA columns to DB columns, coercing each cell to the
    declared type. `extra` carries fields injected from the file context
    (discipline, source_file).
    """
    norm_to_db: Dict[str, str] = {_norm(k): v for k, v in mapping.items()}
    src_to_db: Dict[str, str] = {}
    for col in df.columns:
        n = _norm(col)
        if n in norm_to_db:
            src_to_db[col] = norm_to_db[n]

    records = []
    for _, row in df.iterrows():
        rec: Dict = dict(extra)
        for src_col, db_col in src_to_db.items():
            # If we mapped two source columns to the same DB column (e.g.,
            # state_abbr vs state_abbr_alt), keep the first non-null.
            if rec.get(db_col) is not None:
                continue
            rec[db_col] = _coerce(db_col, row[src_col])
        # Drop the *_alt fallback aliases — they were only used to fill the
        # canonical column when the primary was null.
        for alt in ("state_abbr_alt", "state_fips_alt", "county_name_alt",
                    "postal_code_alt", "metro_indicator_code", "rural_status_code",
                    "withdrawn_date_string"):
            rec.pop(alt, None)
        records.append(rec)
    return records


def _post_process_hpsa(rec: Dict) -> Dict:
    """Apply HPSA-specific fallbacks (alt columns → canonical)."""
    # No-op now since _build_records discards alt columns; canonical
    # values from those fields are already merged via setdefault-like
    # behavior in _build_records.
    return rec


def load_hpsa(path: str, discipline: str, status_filter: Optional[List[str]] = None) -> int:
    print(f"\n=== HPSA {discipline}: {path} ===")
    if not os.path.exists(path):
        print(f"  [skip] file not found")
        return 0

    xls = pd.ExcelFile(path)
    sheet = xls.sheet_names[0] if "SHORTAGE" not in xls.sheet_names[0] else xls.sheet_names[0]
    # Always use the first non-Data-Dictionary sheet
    sheet = next((s for s in xls.sheet_names if "dictionary" not in s.lower()), xls.sheet_names[0])
    df = pd.read_excel(xls, sheet_name=sheet, dtype=object)
    print(f"  rows: {len(df):,}")

    extra = {"discipline": discipline, "source_file": os.path.basename(path)}
    records = _build_records(df, HPSA_MAP, extra)

    # Carry alt-column fallbacks: re-pass the rows to fill from alt columns
    # when the canonical was missing. (Simpler: re-run mapping with alt
    # columns mapped to canonical names.)
    fallback_map = {
        "primary state abbreviation":   "state_abbr",
        "primary state fips code":      "state_fips",
        "county equivalent name":       "county_name",
        "hpsa postal code":             "postal_code",
    }
    fb_records = _build_records(df, fallback_map, extra)
    for rec, fb in zip(records, fb_records):
        for k, v in fb.items():
            if rec.get(k) is None:
                rec[k] = v

    if status_filter:
        before = len(records)
        records = [r for r in records if (r.get("hpsa_status") or "") in status_filter]
        print(f"  After status filter {status_filter}: {len(records):,} of {before:,}")

    return _insert_records("hrsa_hpsa_designations", records)


def load_mua(path: str, status_filter: Optional[List[str]] = None) -> int:
    print(f"\n=== MUA: {path} ===")
    if not os.path.exists(path):
        print(f"  [skip] file not found")
        return 0

    xls = pd.ExcelFile(path)
    sheet = next((s for s in xls.sheet_names if "dictionary" not in s.lower()), xls.sheet_names[0])
    df = pd.read_excel(xls, sheet_name=sheet, dtype=object)
    print(f"  rows: {len(df):,}")

    extra = {"source_file": os.path.basename(path)}
    records = _build_records(df, MUA_MAP, extra)

    # Census tract: drop "Not Applicable" placeholder, keep numeric strings only
    for r in records:
        ct = r.get("census_tract")
        if ct and ct.strip().lower() in ("not applicable", "n/a"):
            r["census_tract"] = None

    if status_filter:
        before = len(records)
        records = [r for r in records if (r.get("mua_status") or "") in status_filter]
        print(f"  After status filter {status_filter}: {len(records):,} of {before:,}")

    return _insert_records("hrsa_mua_designations", records)


# ---------------------------------------------------------------------------
# Bulk insert. These tables don't have natural unique keys (designations can
# have multiple component rows that share the same hpsa_id/county_fips), so
# the run sequence is: TRUNCATE once at start, then INSERT per source.
# `_truncated` tracks which tables have been wiped this run so subsequent
# discipline loads (PC → MH → DH all hit hrsa_hpsa_designations) don't
# clobber rows the earlier load just inserted.
# ---------------------------------------------------------------------------

_truncated: set = set()


def _insert_records(table: str, records: List[Dict]) -> int:
    if not records:
        return 0
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        (table,),
    )
    valid_cols = {r[0] for r in cur.fetchall()}
    valid_cols.discard("id")
    valid_cols.discard("loaded_at")

    cleaned = []
    for r in records:
        cleaned.append({k: v for k, v in r.items() if k in valid_cols and v is not None})

    cols = sorted({k for r in cleaned for k in r.keys()})

    if table not in _truncated:
        cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY")
        _truncated.add(table)

    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"

    from psycopg2.extras import execute_batch
    execute_batch(
        cur, sql,
        [[r.get(c) for c in cols] for r in cleaned],
        page_size=2000,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"  Loaded: {len(cleaned):,} rows into {table}")
    return len(cleaned)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default=DEFAULT_DIR, help="Directory containing the HRSA xlsx files")
    p.add_argument("--hpsa-pc-file", help="Override path to BCD_HPSA_FCT_DET_PC.xlsx")
    p.add_argument("--hpsa-mh-file", help="Override path to BCD_HPSA_FCT_DET_MH.xlsx")
    p.add_argument("--hpsa-dh-file", help="Override path to BCD_HPSA_FCT_DET_DH.xlsx")
    p.add_argument("--mua-file",     help="Override path to MUA_DET.xlsx")
    p.add_argument("--hpsa-pc",      action="store_true", help="Load PC HPSAs only")
    p.add_argument("--hpsa-mh",      action="store_true", help="Load MH HPSAs only")
    p.add_argument("--hpsa-dh",      action="store_true", help="Load DH HPSAs only")
    p.add_argument("--mua",          action="store_true", help="Load MUA only")
    p.add_argument("--active-only",  action="store_true",
                   help="Skip Withdrawn / Proposed For Withdrawal rows")
    args = p.parse_args()

    status_filter = ["Designated"] if args.active_only else None

    # If no specific source flag is set, run all four
    run_all = not any([args.hpsa_pc, args.hpsa_mh, args.hpsa_dh, args.mua])

    if run_all or args.hpsa_pc:
        path = args.hpsa_pc_file or os.path.join(args.dir, DEFAULT_FILES["PC"])
        load_hpsa(path, discipline="PC", status_filter=status_filter)
    if run_all or args.hpsa_mh:
        path = args.hpsa_mh_file or os.path.join(args.dir, DEFAULT_FILES["MH"])
        load_hpsa(path, discipline="MH", status_filter=status_filter)
    if run_all or args.hpsa_dh:
        path = args.hpsa_dh_file or os.path.join(args.dir, DEFAULT_FILES["DH"])
        load_hpsa(path, discipline="DH", status_filter=status_filter)
    if run_all or args.mua:
        path = args.mua_file or os.path.join(args.dir, DEFAULT_FILES["MUA"])
        load_mua(path, status_filter=status_filter)


if __name__ == "__main__":
    main()
