"""
api/routers/accountability.py — School accountability score endpoints.

Endpoints:
  GET /accountability/scsc-cpf — SCSC Comprehensive Performance Framework scores (GA charters)
"""

from typing import Optional
from fastapi import APIRouter, Query
import db
from api.deps import df_to_records

router = APIRouter()


@router.get("/scsc-cpf")
def list_scsc_cpf(
    school_year: Optional[str] = Query(None, description="e.g. 2023-24"),
    nces_id:     Optional[str] = Query(None),
    school_name: Optional[str] = Query(None, description="Substring match"),
    designation: Optional[str] = Query(None, description="Filter by academic or ops designation, e.g. Exceeds"),
):
    """SCSC CPF accountability scores for GA charter schools."""
    df = db.get_scsc_cpf(
        school_year=school_year,
        nces_id=nces_id,
        school_name=school_name,
        designation=designation,
    )
    return {"data": df_to_records(df), "total": len(df)}
