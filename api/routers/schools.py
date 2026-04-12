"""
api/routers/schools.py — School endpoints.

All filters mirror the parameters on db.get_schools().
Charter-only mode is exposed as a query flag rather than a separate URL
so the dashboard can toggle it without changing routes.
"""

from typing import Optional, List
from fastapi import APIRouter, Query, HTTPException
import db
from api.deps import df_to_records, clean_dict

router = APIRouter()


@router.get("")
def list_schools(
    states: Optional[List[str]] = Query(default=None, description="State abbreviations, e.g. CA TX"),
    charter_only: bool = Query(default=False),
    nmtc_eligible_only: bool = Query(default=False),
    min_enrollment: Optional[int] = None,
    max_enrollment: Optional[int] = None,
    risk_tiers: Optional[List[str]] = Query(default=None, description="High, Medium, Low"),
    min_survival_score: Optional[float] = None,
    max_survival_score: Optional[float] = None,
    school_status: Optional[List[str]] = Query(default=None, description="Open, Closed, etc."),
    county: Optional[str] = None,
    census_tract_id: Optional[str] = None,
):
    """
    Return schools matching the given filters.
    Omit all filters to get every school in the database.
    """
    df = db.get_schools(
        states=states,
        charter_only=charter_only,
        nmtc_eligible_only=nmtc_eligible_only,
        min_enrollment=min_enrollment,
        max_enrollment=max_enrollment,
        risk_tiers=risk_tiers,
        min_survival_score=min_survival_score,
        max_survival_score=max_survival_score,
        school_status=school_status,
        county=county,
        census_tract_id=census_tract_id,
    )
    return df_to_records(df)


@router.get("/summary")
def school_summary(charter_only: bool = False):
    """High-level counts for the dashboard header."""
    return db.get_school_summary(charter_only=charter_only)


@router.get("/states")
def school_states():
    """Sorted list of states that have school data."""
    return db.get_school_states()


@router.get("/{nces_id}")
def get_school(nces_id: str):
    """Full detail for a single school by NCES ID."""
    record = db.get_school_by_id(nces_id)
    if not record:
        raise HTTPException(status_code=404, detail="School not found")
    return clean_dict(record)


@router.get("/{nces_id}/enrollment-history")
def school_enrollment_history(nces_id: str):
    """Year-by-year enrollment for a single school (for trend charts)."""
    df = db.get_enrollment_history(nces_id)
    return df_to_records(df)


@router.get("/{nces_id}/990")
def school_990(nces_id: str):
    """990 financial data for the operator linked to this school."""
    record = db.get_990_for_school(nces_id)
    if not record:
        raise HTTPException(status_code=404, detail="No 990 data linked to this school")
    return clean_dict(record)
