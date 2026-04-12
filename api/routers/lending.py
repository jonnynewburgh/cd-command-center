"""
api/routers/lending.py — Lending and credit market data endpoints.

Endpoints:
  GET /lending/hmda               — HMDA mortgage activity by census tract
  GET /lending/hmda/{tract_id}    — HMDA data for a single tract (all years)
  GET /lending/sba                — SBA 7(a) and 504 loan records
  GET /lending/sba/summary        — Aggregate SBA stats (count, amount, jobs)
  GET /lending/cra/institutions   — CRA-examined banks
  GET /lending/cra/areas          — CRA assessment areas (bank service territories)
"""

from fastapi import APIRouter, Query
from typing import Optional
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
import db
from api.deps import df_to_records

router = APIRouter()


@router.get("/hmda")
def list_hmda(
    state:           Optional[str]   = Query(None),
    county_fips:     Optional[str]   = Query(None),
    report_year:     Optional[int]   = Query(None),
    min_denial_rate: Optional[float] = Query(None, description="Min denial rate 0-1, e.g. 0.3 = 30%"),
    limit:           int             = Query(500),
):
    df = db.get_hmda_activity(
        state=state,
        county_fips=county_fips,
        report_year=report_year,
        min_denial_rate=min_denial_rate,
        limit=limit,
    )
    return {"data": df_to_records(df), "total": len(df)}


@router.get("/hmda/{tract_id}")
def hmda_for_tract(tract_id: str, report_year: Optional[int] = Query(None)):
    df = db.get_hmda_activity(census_tract_id=tract_id, report_year=report_year, limit=None)
    return {"data": df_to_records(df), "total": len(df)}


@router.get("/sba")
def list_sba_loans(
    state:           Optional[str] = Query(None),
    year:            Optional[int] = Query(None),
    program:         Optional[str] = Query(None, description="7a or 504"),
    census_tract_id: Optional[str] = Query(None),
    zip_code:        Optional[str] = Query(None),
    naics_code:      Optional[str] = Query(None, description="NAICS prefix, e.g. 72 for hospitality"),
    limit:           int           = Query(500),
):
    df = db.get_sba_loans(
        state=state,
        year=year,
        program=program,
        census_tract_id=census_tract_id,
        zip_code=zip_code,
        naics_code=naics_code,
        limit=limit,
    )
    return {"data": df_to_records(df), "total": len(df)}


@router.get("/sba/summary")
def sba_summary(
    state: Optional[str] = Query(None),
    year:  Optional[int] = Query(None),
):
    return db.get_sba_summary(state=state, year=year)


@router.get("/cra/institutions")
def list_cra_institutions(
    state:       Optional[str] = Query(None),
    report_year: Optional[int] = Query(None),
    asset_size:  Optional[str] = Query(None, description="Large | Intermediate Small | Small"),
    search:      Optional[str] = Query(None, description="Substring search on institution name"),
    limit:       Optional[int] = Query(None),
):
    df = db.get_cra_institutions(
        state=state,
        report_year=report_year,
        asset_size=asset_size,
        search=search,
        limit=limit,
    )
    return {"data": df_to_records(df), "total": len(df)}


@router.get("/cra/areas")
def list_cra_areas(
    state:         Optional[str] = Query(None),
    report_year:   Optional[int] = Query(None),
    respondent_id: Optional[str] = Query(None, description="FFIEC respondent ID"),
    county_fips:   Optional[str] = Query(None, description="5-digit county FIPS"),
):
    df = db.get_cra_assessment_areas(
        state=state,
        report_year=report_year,
        respondent_id=respondent_id,
        county_fips=county_fips,
    )
    return {"data": df_to_records(df), "total": len(df)}
