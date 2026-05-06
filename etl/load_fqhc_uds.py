"""etl/load_fqhc_uds.py — Load HRSA UDS grantee-level reports into fqhc_uds_reports.

Source files are HRSA's H80 (Section 330 awardees) and LAL (look-alikes)
xlsx workbooks. Both share the same UIID-coded structure:
- `HealthCenterInfo` sheet: identity (BHCMISID, GrantNumber, name, state, funding flags)
- `Table4`, `Table5`, `Table8A`, `Table9D`, `Table9E`: numeric data, columns
  named by UIID code (e.g., `T4_L8_Ca` = "Total Medicaid, 0-17 years old")
- `Table6BClinicalmeasures` and `Table7Clinicalmeasures`: precomputed
  percentage columns (e.g., `%ofPatientswithControlledBloodPressure`)
- `UIIDInfo` sheet: dictionary mapping UIID → (table, row, col, row_title,
  col_title). Stable across recent years; printed by --columns-only.

Source download (Akamai-protected — must be a real browser):
- https://data.hrsa.gov/topics/health-centers/uds  (H80 + LAL links)
Save to data/raw/fqhcs/, then run.

Usage:
    # Inspect: print every sheet's UIID columns and the UIIDInfo decoder
    python etl/load_fqhc_uds.py --file data/raw/fqhcs/h80-2024.xlsx --columns-only

    # Dry run — extract and print the first record without writing
    python etl/load_fqhc_uds.py --file data/raw/fqhcs/h80-2024.xlsx --year 2024 --dry-run

    # Real load
    python etl/load_fqhc_uds.py --file data/raw/fqhcs/h80-2024.xlsx --year 2024
    python etl/load_fqhc_uds.py --file data/raw/fqhcs/lal-2024.xlsx --year 2024

    # Filter to states
    python etl/load_fqhc_uds.py --file data/raw/fqhcs/h80-2024.xlsx --year 2024 --states GA TX
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Callable, Dict, List, Optional

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db


# ---------------------------------------------------------------------------
# Identity columns (from HealthCenterInfo sheet)
# ---------------------------------------------------------------------------

IDENTITY_COLS = {
    "GrantNumber":      "grant_number",
    "BHCMISID":         "org_bhcmis_id",
    "HealthCenterName": "health_center_name",
    "HealthCenterState": "state",
}


def _grantee_type(funding_chc, funding_mhc, funding_ho, funding_ph) -> Optional[str]:
    """Build a slash-joined grantee-type string from the four Funding* flags.

    HRSA flags health centers with one or more of these designations on
    HealthCenterInfo. Values in 2024 are 'TRUE'/'FALSE' (sometimes 'Y'/'N'
    in older years):
      CHC (Community Health Center, 330e)
      MHC (Migrant Health Center, 330g)
      HO  (Health Care for the Homeless, 330h)
      PH  (Public Housing Primary Care, 330i)
    """
    def _truthy(v) -> bool:
        return str(v or "").strip().upper() in ("Y", "YES", "TRUE", "1")

    flags = []
    if _truthy(funding_chc): flags.append("CHC")
    if _truthy(funding_mhc): flags.append("MHC")
    if _truthy(funding_ho):  flags.append("HO")
    if _truthy(funding_ph):  flags.append("PH")
    return "/".join(flags) if flags else None


def _num(v) -> float:
    """Coerce a UDS cell to float. None / blank / non-numeric → 0.0 (so sums work)."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("$", "").replace("%", "")
    if not s or s in ("-", "—", "N/A", "n/a"):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _ratio(numer: float, denom: float) -> Optional[float]:
    """Percentage of numer/denom. Returns None if denom is zero or near-zero."""
    if denom <= 0:
        return None
    return round((numer / denom) * 100, 2)


# ---------------------------------------------------------------------------
# Per-sheet extractors
#
# Each extractor takes a row dict (one grantee's row from one sheet) and
# returns a partial record that gets merged into the grantee's main record.
# Missing source columns are tolerated — if a year/file lacks a UIID, the
# extractor will get None and emit None for the derived measure.
# ---------------------------------------------------------------------------

def extract_table4(row: Dict) -> Dict:
    """Patients, income tiers, payer mix, special populations."""
    g = lambda k, _r=row: _r.get(k.lower()) if k.lower().startswith("t") else _r.get(k)  # case-insensitive UIID lookup
    total_patients = _num(g("T4_L6_Ca"))
    total_insured  = _num(g("T4_L12_Ca")) + _num(g("T4_L12_Cb"))

    return {
        "total_patients":           int(total_patients) if total_patients else None,
        "pct_below_100pct_poverty": _ratio(_num(g("T4_L1_Ca")), total_patients),
        "pct_100_to_200_poverty":   _ratio(_num(g("T4_L2_Ca")) + _num(g("T4_L3_Ca")), total_patients),
        "pct_uninsured":            _ratio(_num(g("T4_L7_Ca"))  + _num(g("T4_L7_Cb")),  total_insured),
        "pct_medicaid":             _ratio(_num(g("T4_L8_Ca"))  + _num(g("T4_L8_Cb")),  total_insured),
        "pct_medicare":             _ratio(_num(g("T4_L9_Ca"))  + _num(g("T4_L9_Cb")),  total_insured),
        "pct_other_public":         _ratio(_num(g("T4_L10_Ca")) + _num(g("T4_L10_Cb")), total_insured),
        "pct_private_insurance":    _ratio(_num(g("T4_L11_Ca")) + _num(g("T4_L11_Cb")), total_insured),
        "patients_agricultural":    int(_num(g("T4_L16_Ca"))) or None,
        "patients_homeless":        int(_num(g("T4_L23_Ca"))) or None,
        "patients_school_based":    int(_num(g("T4_L24_Ca"))) or None,
        "patients_veterans":        int(_num(g("T4_L25_Ca"))) or None,
        "patients_public_housing":  int(_num(g("T4_L26_Ca"))) or None,
    }


def extract_table5(row: Dict) -> Dict:
    """Staffing FTEs and visits.

    Table 5 columns: Ca = FTEs, Cb = Clinic Visits, Cb2 = Virtual Visits.
    Each visit-type total is the sum of clinic + virtual visits on the
    appropriate "Total <Service>" line.
    """
    g = lambda k, _r=row: _r.get(k.lower()) if k.lower().startswith("t") else _r.get(k)

    medical_cb   = _num(g("T5_L15_Cb")) + _num(g("T5_L15_Cb2"))
    dental_cb    = _num(g("T5_L19_Cb")) + _num(g("T5_L19_Cb2"))
    mh_cb        = _num(g("T5_L20_Cb")) + _num(g("T5_L20_Cb2"))
    sud_cb       = _num(g("T5_L21_Cb")) + _num(g("T5_L21_Cb2"))
    vision_cb    = _num(g("T5_L22d_Cb")) + _num(g("T5_L22d_Cb2"))
    enabling_cb  = _num(g("T5_L29_Cb")) + _num(g("T5_L29_Cb2"))

    total_clinical = (
        _num(g("T5_L15_Ca")) + _num(g("T5_L19_Ca")) +
        _num(g("T5_L20_Ca")) + _num(g("T5_L21_Ca")) +
        _num(g("T5_L22d_Ca"))
    )
    bh = _num(g("T5_L20_Ca")) + _num(g("T5_L21_Ca"))

    return {
        "physicians_fte":           _num(g("T5_L8_Ca"))   or None,
        "np_pa_cnm_fte":            _num(g("T5_L10a_Ca")) or None,
        "nurses_fte":               _num(g("T5_L11_Ca"))  or None,
        "dentists_fte":             _num(g("T5_L16_Ca"))  or None,
        "bh_providers_fte":         bh                    or None,
        "total_clinical_fte":       total_clinical        or None,
        "total_fte":                _num(g("T5_L34_Ca"))  or None,

        "medical_visits":           int(medical_cb)   or None,
        "dental_visits":            int(dental_cb)    or None,
        "mental_health_visits":     int(mh_cb)        or None,
        "substance_use_visits":     int(sud_cb)       or None,
        "vision_visits":            int(vision_cb)    or None,
        "enabling_services_visits": int(enabling_cb)  or None,
        "total_visits":             int(medical_cb + dental_cb + mh_cb + sud_cb + vision_cb) or None,
    }


def extract_table6b_clinical(row: Dict) -> Dict:
    """Quality of care % measures (precomputed by HRSA)."""
    g = lambda k, _r=row: _r.get(k.lower()) if k.lower().startswith("t") else _r.get(k)
    return {
        "cervical_cancer_screening_pct":   _num(g("%ofPatientstestedPap")) or None,
        "breast_cancer_screening_pct":     _num(g("%ofPatientswithMammogram")) or None,
        "colorectal_cancer_screening_pct": _num(g("%ofAdultswithAppropriateScreeningforColorectalCancer")) or None,
        "depression_screening_pct":        _num(g("%PatientsScreenedforDepressionandFollowupPlanDocumentedasAppropriate")) or None,
        "tobacco_screening_pct":           _num(g("%PatientsAssessedforTobaccoUseandProvidedInterventionIfaTobaccoUser")) or None,
    }


def extract_table7_clinical(row: Dict) -> Dict:
    """Diabetes A1c control + hypertension control. Race-stratified by row;
    we keep just the 'all races' (or first non-stratified) values per grantee."""
    g = lambda k, _r=row: _r.get(k.lower()) if k.lower().startswith("t") else _r.get(k)
    return {
        "diabetes_a1c_poor_control_pct": _num(g("%ofPatientswithHbA1c>9%")) or None,
        "hypertension_control_pct":      _num(g("%ofPatientswithControlledBloodPressure")) or None,
    }


def extract_table8a(row: Dict) -> Dict:
    """Total accrued costs after facility/non-clinical allocation."""
    g = lambda k, _r=row: _r.get(k.lower()) if k.lower().startswith("t") else _r.get(k)
    return {
        "total_costs": int(_num(g("T8a_L17_Cc"))) or None,
    }


def extract_table9d(row: Dict) -> Dict:
    """Patient service revenue collected (across all payers)."""
    g = lambda k, _r=row: _r.get(k.lower()) if k.lower().startswith("t") else _r.get(k)
    return {
        "patient_service_revenue": int(_num(g("T9d_L14_Cb"))) or None,
        "self_pay_revenue":        int(_num(g("T9d_L13_Cb"))) or None,
    }


def extract_table9e(row: Dict) -> Dict:
    """Federal/state/private grant revenue."""
    g = lambda k, _r=row: _r.get(k.lower()) if k.lower().startswith("t") else _r.get(k)
    return {
        "bphc_grant_revenue":    int(_num(g("T9e_L1_Ca"))) or None,
        "other_federal_revenue": int(_num(g("T9e_L5_Ca"))) or None,
        "state_local_revenue":   int(_num(g("T9e_L6_Ca")) + _num(g("T9e_L6a_Ca")) + _num(g("T9e_L7_Ca"))) or None,
        "private_grant_revenue": int(_num(g("T9e_L8_Ca"))) or None,
    }


# Sheet name → extractor function. Sheet name lookups are done on a
# normalized (lower, alphanum-only) form so 2020 and 2024 capitalization
# differences don't matter.
SHEET_EXTRACTORS: List[tuple[str, Callable[[Dict], Dict]]] = [
    ("table4",                  extract_table4),
    ("table5",                  extract_table5),
    ("table6bclinicalmeasures", extract_table6b_clinical),
    ("table7clinicalmeasures",  extract_table7_clinical),
    ("table8a",                 extract_table8a),
    ("table9d",                 extract_table9d),
    ("table9e",                 extract_table9e),
]


def _norm_sheet(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _resolve_sheet(xls: pd.ExcelFile, target: str) -> Optional[str]:
    target_norm = _norm_sheet(target)
    for sheet in xls.sheet_names:
        if _norm_sheet(sheet) == target_norm:
            return sheet
    return None


# UDS sheets include a verbose-label "header" row 0 with BHCMISID = "-",
# and dash-placeholder rows (`---`, `----`) between actual grantee rows.
# Both must be skipped before extraction.
_DASH_BHCMISIDS = {"-", "--", "---", "----"}


def _is_real_row(bhcmisid) -> bool:
    if bhcmisid is None or (isinstance(bhcmisid, float) and pd.isna(bhcmisid)):
        return False
    s = str(bhcmisid).strip()
    return bool(s) and s not in _DASH_BHCMISIDS


# 2020-vintage Table9E used verbose column names instead of UIID codes.
# Map them to the canonical UIIDs (lowercase) so the rest of the loader is
# format-agnostic. Only the columns the extractors actually read are listed
# — the rest fall through to whitespace-stripping and land in raw_metrics_json.
_LEGACY_VERBOSE_TO_UIID = {
    "TotalBPHCGrants(SumofLines1g+1k+1q)-Amount(a)":              "t9e_l1_ca",
    "TotalOtherFederalGrants(SumofLines2through3b)-Amount(a)":    "t9e_l5_ca",
    "StateGovernmentGrantsandContracts-Amount(a)":                 "t9e_l6_ca",
    "State/LocalIndigentCarePrograms-Amount(a)":                   "t9e_l6a_ca",
    "LocalGovernmentGrantsandContracts-Amount(a)":                 "t9e_l7_ca",
    "Foundation/PrivateGrantsandContracts-Amount(a)":              "t9e_l8_ca",
}


def _normalize_uid_col(c: str) -> str:
    """Normalize a UIID-style column to a canonical form for matching.

    HRSA's UIID conventions drift across reporting years:
    - Pre-2022: spaces inside codes ('T4 L8 Ca'), uppercase table letters ('T9D', 'T9E').
    - 2022-2023: underscores ('T4_L8_Ca'), uppercase table letters ('T9D_L14_Cb').
    - 2024: underscores, lowercase table letters ('T9d_L14_Cb').

    Canonical form (used everywhere in the loader): underscores + lowercase.
    Identity columns (BHCMISID, GrantNumber, etc.) lose all whitespace.
    """
    import re
    s = str(c)
    if re.match(r"^T\d", s):
        return re.sub(r"\s+", "_", s.strip()).lower()
    return re.sub(r"\s+", "", s)


def _strip_spaces_in_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to the canonical form (see _normalize_uid_col).
    Applied to every sheet before extraction so MEASURE_MAP can use a single
    naming convention regardless of source file vintage. Verbose-named columns
    from 2020-vintage Table9E are mapped to UIID codes via _LEGACY_VERBOSE_TO_UIID.
    """
    df = df.copy()
    new_cols = []
    for c in df.columns:
        n = _normalize_uid_col(c)
        new_cols.append(_LEGACY_VERBOSE_TO_UIID.get(n, n))
    df.columns = new_cols
    return df


def parse_workbook(path: str, year: int) -> List[Dict]:
    """Build one record per grantee by joining all relevant sheets on GrantNumber."""
    print(f"  Reading: {path}")
    xls = pd.ExcelFile(path)

    info_sheet = _resolve_sheet(xls, "HealthCenterInfo")
    if info_sheet is None:
        raise SystemExit(f"  ERROR: HealthCenterInfo sheet not found in {path}")

    info = pd.read_excel(xls, sheet_name=info_sheet, dtype=object)
    info = _strip_spaces_in_columns(info)
    info = info[info["BHCMISID"].apply(_is_real_row)]
    print(f"  HealthCenterInfo: {len(info):,} grantees")

    by_grant: Dict[str, Dict] = {}
    for _, row in info.iterrows():
        grant = row.get("GrantNumber")
        if grant is None or (isinstance(grant, float) and pd.isna(grant)):
            continue
        grant = str(grant).strip()
        if not grant or grant in _DASH_BHCMISIDS:
            continue

        rec = {
            "grant_number":       grant,
            "data_year":          year,
            "org_bhcmis_id":      str(row.get("BHCMISID")).strip() if pd.notna(row.get("BHCMISID")) else None,
            "health_center_name": str(row.get("HealthCenterName")).strip() if pd.notna(row.get("HealthCenterName")) else None,
            "state":              str(row.get("HealthCenterState")).strip().upper() if pd.notna(row.get("HealthCenterState")) else None,
            "grantee_type":       _grantee_type(row.get("FundingCHC"), row.get("FundingMHC"), row.get("FundingHO"), row.get("FundingPH")),
        }
        by_grant[grant] = rec

    # Apply each extractor across its sheet
    raw_metrics: Dict[str, Dict] = {g: {} for g in by_grant}

    for target, extractor in SHEET_EXTRACTORS:
        sheet = _resolve_sheet(xls, target)
        if sheet is None:
            print(f"  [skip] sheet matching '{target}' not in workbook")
            continue
        df = pd.read_excel(xls, sheet_name=sheet, dtype=object)
        df = _strip_spaces_in_columns(df)
        df = df[df["BHCMISID"].apply(_is_real_row)]
        print(f"  {sheet}: {len(df):,} real rows")

        # Some sheets (Table7Clinicalmeasures) have multiple rows per grantee
        # — race-stratified. Keep the first row for each grantee.
        for _, srow in df.iterrows():
            grant = srow.get("GrantNumber")
            if grant is None or (isinstance(grant, float) and pd.isna(grant)):
                continue
            grant = str(grant).strip()
            if grant in _DASH_BHCMISIDS or grant not in by_grant:
                continue

            row_dict = srow.to_dict()
            # raw_metrics: keep all non-null values for the JSONB blob
            for k, v in row_dict.items():
                if k in ("GrantNumber", "BHCMISID"): continue
                if v is None or (isinstance(v, float) and pd.isna(v)): continue
                # don't overwrite a raw value already captured from another sheet
                raw_metrics[grant].setdefault(f"{sheet}.{k}", v)

            measures = extractor(row_dict)
            for k, v in measures.items():
                if v is None: continue
                # First non-null wins (e.g., race-stratified rows on Table7Clinical)
                by_grant[grant].setdefault(k, v)

    # Attach raw_metrics_json blob and source_file
    fname = os.path.basename(path)
    for grant, rec in by_grant.items():
        rec["source_file"] = fname
        raw = raw_metrics.get(grant, {})
        rec["raw_metrics_json"] = json.dumps(
            {k: (v if isinstance(v, (int, float, str, bool)) else str(v)) for k, v in raw.items()},
            default=str,
        )

    return list(by_grant.values())


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def upsert_records(records: List[Dict]) -> int:
    if not records:
        return 0
    all_cols = sorted({k for r in records for k in r.keys()})
    placeholders = ", ".join(["%s"] * len(all_cols))
    col_list = ", ".join(all_cols)
    update_set = ", ".join(f"{c}=EXCLUDED.{c}" for c in all_cols if c not in ("grant_number", "data_year"))

    sql = f"""
        INSERT INTO fqhc_uds_reports ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (grant_number, data_year) DO UPDATE SET {update_set}, loaded_at = CURRENT_TIMESTAMP
    """

    conn = db.get_connection()
    cur = conn.cursor()
    n = 0
    try:
        for rec in records:
            cur.execute(sql, [rec.get(c) for c in all_cols])
            n += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    return n


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def print_columns(path: str) -> None:
    xls = pd.ExcelFile(path)
    print(f"Workbook: {path}")
    print(f"Sheets ({len(xls.sheet_names)}): {xls.sheet_names}\n")
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet, nrows=0, dtype=object)
        print(f"=== {sheet} ({len(df.columns)} cols) ===")
        for c in df.columns:
            print(f"  {c}")
        print()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--file", required=True)
    p.add_argument("--year", type=int)
    p.add_argument("--states", nargs="+")
    p.add_argument("--columns-only", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if not os.path.exists(args.file):
        print(f"ERROR: file not found: {args.file}")
        sys.exit(2)

    if args.columns_only:
        print_columns(args.file)
        return

    if args.year is None:
        print("ERROR: --year is required (e.g., --year 2024)")
        sys.exit(2)

    records = parse_workbook(args.file, args.year)
    print(f"\n  Parsed: {len(records):,} grantees")

    if args.states:
        states = {s.upper() for s in args.states}
        records = [r for r in records if (r.get("state") or "").upper() in states]
        print(f"  After state filter {sorted(states)}: {len(records):,}")

    if args.dry_run:
        if records:
            print("\n  Sample record:")
            for k, v in sorted(records[0].items()):
                if k == "raw_metrics_json":
                    print(f"    {k}: <{len(v)} chars>")
                else:
                    print(f"    {k}: {v}")
        return

    n = upsert_records(records)
    print(f"\nDone. Upserted: {n:,} grantee-year rows into fqhc_uds_reports.")


if __name__ == "__main__":
    main()
