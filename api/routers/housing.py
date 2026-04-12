"""
api/routers/housing.py — Housing cost and labor market data endpoints.

Endpoints:
  GET /housing/ami                 — HUD Area Median Income limits
  GET /housing/fmr                 — HUD Fair Market Rents
  GET /housing/unemployment        — BLS unemployment by county/MSA
  GET /housing/qcew                — BLS QCEW employment and wages by county
"""

from fastapi import APIRouter, Query
from typing import Optional
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
import db
from api.deps import df_to_records

router = APIRouter()


@router.get("/ami")
def list_hud_ami(
    fiscal_year: Optional[int] = Query(None, description="HUD fiscal year, e.g. 2024"),
    state:       Optional[str] = Query(None),
    fips:        Optional[str] = Query(None, description="HUD area FIPS code"),
):
    """
    Return HUD Area Median Income limits.
    Standard benchmark: limit_80_pct = 80% AMI (Low Income threshold).
    """
    df = db.get_hud_ami(fiscal_year=fiscal_year, state=state, fips=fips)
    return {"data": df_to_records(df), "total": len(df)}


@router.get("/fmr")
def list_hud_fmr(
    fiscal_year: Optional[int] = Query(None, description="HUD fiscal year, e.g. 2025"),
    state:       Optional[str] = Query(None),
    fips:        Optional[str] = Query(None, description="HUD FMR area FIPS code"),
):
    """
    Return HUD Fair Market Rents.
    fmr_2br is the standard 2-bedroom benchmark used by most housing programs.
    """
    df = db.get_hud_fmr(fiscal_year=fiscal_year, state=state, fips=fips)
    return {"data": df_to_records(df), "total": len(df)}


@router.get("/unemployment")
def list_unemployment(
    area_fips:    Optional[str] = Query(None, description="5-digit county FIPS or MSA code"),
    state:        Optional[str] = Query(None),
    area_type:    Optional[str] = Query(None, description="county or msa"),
    start_period: Optional[str] = Query(None, description="YYYY-MM"),
    end_period:   Optional[str] = Query(None, description="YYYY-MM"),
    months:       Optional[int] = Query(None, description="Return last N months"),
):
    df = db.get_bls_unemployment(
        area_fips=area_fips,
        state=state,
        area_type=area_type,
        start_period=start_period,
        end_period=end_period,
        months=months,
    )
    return {"data": df_to_records(df), "total": len(df)}


@router.get("/qcew")
def list_qcew(
    area_fips:     Optional[str] = Query(None, description="5-digit county FIPS"),
    state:         Optional[str] = Query(None),
    year:          Optional[int] = Query(None),
    quarter:       Optional[int] = Query(None, description="1-4 or 0 for annual"),
    industry_code: Optional[str] = Query(None, description="NAICS code or '10' for all industries"),
    ownership_code:Optional[str] = Query(None, description="0=total, 5=private, 1=federal"),
    limit:         int           = Query(500),
):
    df = db.get_bls_qcew(
        area_fips=area_fips,
        state=state,
        year=year,
        quarter=quarter,
        industry_code=industry_code,
        ownership_code=ownership_code,
        limit=limit,
    )
    return {"data": df_to_records(df), "total": len(df)}
