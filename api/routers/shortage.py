"""
api/routers/shortage.py — HRSA shortage-area designation endpoints.

HPSA = Health Professional Shortage Area (PC / MH / DH disciplines).
MUA  = Medically Underserved Area / Population.

These are federal eligibility flags for FQHC funding, NHSC scholar
placement, J-1 visa waivers, and Medicare HPSA bonus payments.
"""

from typing import Optional, Literal
from fastapi import APIRouter, Query, HTTPException

import db
from api.deps import clean_dict, df_to_records

router = APIRouter()


@router.get("/summary")
def shortage_summary():
    """Top-level coverage stats for HPSA + MUA tables."""
    return db.get_shortage_summary()


@router.get("/hpsa/county/{county_fips}")
def hpsa_for_county(
    county_fips: str,
    discipline: Optional[Literal["PC", "MH", "DH"]] = Query(default=None),
    active_only: bool = Query(default=True),
):
    """HPSA component rows covering a county. Pass discipline=PC|MH|DH to filter."""
    df = db.get_hpsa_for_county(county_fips, discipline=discipline, active_only=active_only)
    return {"items": df_to_records(df), "total": len(df)}


@router.get("/hpsa/facility/{bhcmis_org_id}")
def hpsa_for_facility(bhcmis_org_id: str, active_only: bool = Query(default=True)):
    """HPSAs auto-designated for an FQHC organization (facility-level designation)."""
    df = db.get_hpsa_for_facility(bhcmis_org_id, active_only=active_only)
    return {"items": df_to_records(df), "total": len(df)}


@router.get("/mua/county/{county_fips}")
def mua_for_county(county_fips: str, active_only: bool = Query(default=True)):
    """MUA component rows covering a county."""
    df = db.get_mua_for_county(county_fips, active_only=active_only)
    return {"items": df_to_records(df), "total": len(df)}


@router.get("/mua/tract/{census_tract}")
def mua_for_tract(census_tract: str, active_only: bool = Query(default=True)):
    """MUA component rows for a specific census tract code (e.g., '9301.01')."""
    df = db.get_mua_for_tract(census_tract, active_only=active_only)
    return {"items": df_to_records(df), "total": len(df)}


@router.get("/site/{bhcmis_id}")
def shortage_for_site(bhcmis_id: str):
    """Resolve an FQHC site to its HPSA + MUA context (for FQHC detail page badges)."""
    summary = db.get_shortage_summary_for_site(bhcmis_id)
    if not summary:
        raise HTTPException(status_code=404, detail="FQHC site not found")
    return clean_dict(summary)
