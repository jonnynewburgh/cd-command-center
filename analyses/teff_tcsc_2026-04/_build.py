"""
Build TN charter facilities analysis outputs.
Reads from local Postgres (DATABASE_URL env var or default below).
Hard rules from request:
  - No fabrication; null is acceptable
  - Do not silently fix the ncessch->seasch join blocker
  - Skip Output 3 if census tracts not assigned (do not call external geocoders)
"""
import csv
import os
import psycopg2
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
OUT = REPO / "analyses" / "teff_tcsc_2026-04"

DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:JNiscool123@localhost:5432/cd_command_center",
)


def conn():
    return psycopg2.connect(DSN)


# TN county FIPS -> name mapping. Reference data, not fabricated values.
TN_COUNTY_FIPS = {
    "47037": "Davidson County",
    "47065": "Hamilton County",
    "47093": "Knox County",
    "47113": "Madison County",
    "47125": "Montgomery County",
    "47149": "Rutherford County",
    "47157": "Shelby County",
    "47163": "Sullivan County",
}


def normalize_county(raw):
    """schools.county is stored upstream as a 7-char string like '4747157'
    (state FIPS 47 prefixed onto a 5-digit GEOID). Take the trailing 5 chars
    and look up the name. Return raw fallback if unknown."""
    if not raw:
        return None
    s = str(raw).strip()
    fips5 = s[-5:] if len(s) >= 5 else s
    return TN_COUNTY_FIPS.get(fips5, f"FIPS:{fips5}")


def grades_offered(low, high):
    if low is None and high is None:
        return None
    return f"{low or ''}-{high or ''}".strip("-") or None


def pct_other(black, hispanic, white):
    """Residual non-Black/Hispanic/White population percentage. Captures Asian,
    Multi, AI/AN, NHPI, and any other category not separately reported. Returns
    None if any input is null (don't compute on incomplete data)."""
    if black is None or hispanic is None or white is None:
        return None
    other = 100.0 - (black + hispanic + white)
    return round(max(0.0, other), 2)


def build_output_1(c):
    cur = c.cursor()
    cur.execute("""
        SELECT nces_id, seasch, school_name, city, county,
               latitude, longitude, grade_low, grade_high,
               enrollment, pct_free_reduced_lunch,
               pct_black, pct_hispanic, pct_white,
               pct_ell, pct_sped,
               lea_name, year_opened, data_year,
               school_status
        FROM schools
        WHERE state = 'TN' AND is_charter = 1
        ORDER BY data_year DESC, school_name
    """)
    rows = cur.fetchall()

    out_path = OUT / "TN_charter_schools_roster.csv"
    cols = [
        "ncessch", "seasch", "school_name", "city", "county",
        "latitude", "longitude", "grades_offered", "enrollment_total",
        "frpl_pct", "pct_black", "pct_hispanic", "pct_white", "pct_other",
        "pct_ell", "pct_swd", "authorizer", "charter_year_opened", "data_year",
    ]
    n_open = 0
    n_total = 0
    by_year = {}
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            (ncessch, seasch, name, city, county, lat, lon,
             gl, gh, enroll, frpl, blk, hsp, wht, ell, sped,
             lea, yr_open, dy, status) = r
            n_total += 1
            by_year[dy] = by_year.get(dy, 0) + 1
            if status == "Open":
                n_open += 1
            w.writerow([
                ncessch,
                seasch if seasch else "",
                name,
                city,
                normalize_county(county),
                lat,
                lon,
                grades_offered(gl, gh),
                enroll if enroll is not None else "",
                frpl if frpl is not None else "",
                blk if blk is not None else "",
                hsp if hsp is not None else "",
                wht if wht is not None else "",
                pct_other(blk, hsp, wht) if pct_other(blk, hsp, wht) is not None else "",
                ell if ell is not None else "",
                sped if sped is not None else "",
                lea if lea else "",
                yr_open if yr_open is not None else "",
                dy if dy is not None else "",
            ])
    return {"path": out_path, "rows": n_total, "open": n_open, "by_year": by_year}


def build_output_2(c):
    """NMTC charter projects in TN. The NMTC datasets in Postgres do not carry
    an 'end use = charter school' identifier:
      - nmtc_projects.project_type ∈ {NRE, RE, SPE, CDE}
      - nmtc_coalition_projects.project_type ∈ {NRE, RE, SPE, CDE}
      - 'Purpose of Investment' on the Coalition source classifies financing
        type, not end use; no charter/school/academy mentions for TN
    Per spec: write headers-only CSV and flag in README."""
    cur = c.cursor()
    cur.execute("SELECT COUNT(*) FROM nmtc_projects WHERE state IN ('TN','Tennessee')")
    n_projects_tn = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM nmtc_coalition_projects WHERE state='TN'")
    n_coalition_tn = cur.fetchone()[0]
    out_path = OUT / "TN_charter_NMTC_deals.csv"
    cols = ["project_name", "city", "allocation_year", "qei_amount",
            "cde_name", "project_type", "census_tract"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
    return {"path": out_path, "rows": 0,
            "tn_projects": n_projects_tn, "tn_coalition": n_coalition_tn}


def build_output_3(c):
    """Census tract context per ncessch in Output 1. Joins schools.census_tract_id
    against census_tracts on the 11-digit GEOID. Computes:
      - poverty_rate (pass-through)
      - median_family_income (pass-through)
      - ami_pct = school MFI / TN-wide MFI
      - nmtc_lic_qualified = poverty_rate>=0.20 OR ami_pct<=0.80
      - distressed_community = is_nmtc_eligible flag (boolean)
    """
    cur = c.cursor()
    # Compute statewide MFI (TN). Use median_family_income column when present.
    cur.execute("""
        SELECT median_family_income FROM census_tracts
        WHERE state='TN' AND median_family_income IS NOT NULL
        ORDER BY median_family_income
    """)
    mfis = [row[0] for row in cur.fetchall()]
    if not mfis:
        return {"path": None, "rows": 0, "skipped": True,
                "reason": "no TN census_tracts with median_family_income"}
    tn_mfi = mfis[len(mfis) // 2]  # median of medians

    cur.execute("""
        SELECT s.nces_id, s.school_name, s.census_tract_id,
               t.poverty_rate, t.median_family_income, t.is_nmtc_eligible
        FROM schools s
        LEFT JOIN census_tracts t ON s.census_tract_id = t.census_tract_id
        WHERE s.state='TN' AND s.is_charter=1
        ORDER BY s.school_name
    """)
    rows = cur.fetchall()

    out_path = OUT / "TN_charter_tract_context.csv"
    cols = ["ncessch", "school_name", "census_tract", "poverty_rate",
            "median_family_income", "ami_pct", "nmtc_lic_qualified",
            "distressed_community"]
    n_total = 0
    n_lic = 0
    n_joined = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            ncessch, name, tract, pov, mfi, distressed = r
            n_total += 1
            if pov is not None or mfi is not None:
                n_joined += 1
            ami = round(mfi / tn_mfi, 4) if (mfi and tn_mfi) else None
            lic = None
            if pov is not None or ami is not None:
                # poverty_rate stored as percent (0-100); LIC threshold is >= 20%
                # ami_pct is fraction; LIC threshold is <= 0.80
                lic = bool((pov is not None and pov >= 20.0) or
                           (ami is not None and ami <= 0.80))
                if lic:
                    n_lic += 1
            w.writerow([
                ncessch, name, tract or "",
                pov if pov is not None else "",
                mfi if mfi is not None else "",
                ami if ami is not None else "",
                "true" if lic is True else ("false" if lic is False else ""),
                "true" if distressed == 1 else ("false" if distressed == 0 else ""),
            ])
    return {"path": out_path, "rows": n_total, "joined": n_joined,
            "lic_qualified": n_lic, "tn_mfi_used": tn_mfi}


def quality_report(c):
    cur = c.cursor()
    cur.execute("""
        SELECT
          COUNT(*),
          SUM(CASE WHEN seasch IS NOT NULL AND seasch != '' THEN 1 ELSE 0 END),
          SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END),
          SUM(CASE WHEN enrollment IS NOT NULL THEN 1 ELSE 0 END),
          SUM(CASE WHEN pct_free_reduced_lunch IS NOT NULL THEN 1 ELSE 0 END),
          SUM(CASE WHEN pct_ell IS NOT NULL THEN 1 ELSE 0 END),
          SUM(CASE WHEN pct_sped IS NOT NULL THEN 1 ELSE 0 END),
          SUM(CASE WHEN pct_black IS NOT NULL THEN 1 ELSE 0 END),
          SUM(CASE WHEN year_opened IS NOT NULL THEN 1 ELSE 0 END),
          SUM(CASE WHEN census_tract_id IS NOT NULL AND census_tract_id != '' THEN 1 ELSE 0 END),
          SUM(CASE WHEN school_status='Open' THEN 1 ELSE 0 END),
          MIN(data_year), MAX(data_year)
        FROM schools WHERE state='TN' AND is_charter=1
    """)
    return cur.fetchone()


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    c = conn()
    o1 = build_output_1(c)
    o2 = build_output_2(c)
    o3 = build_output_3(c)
    qr = quality_report(c)
    print("Output 1:", o1)
    print("Output 2:", o2)
    print("Output 3:", o3)
    print("Quality:", qr)
    c.close()
    return o1, o2, o3, qr


if __name__ == "__main__":
    main()
