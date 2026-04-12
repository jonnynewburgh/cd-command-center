"""
api/routers/nmtc.py — NMTC project, CDE allocation, and Coalition project endpoints.
"""

from typing import Optional, List
from fastapi import APIRouter, Query, HTTPException
import db
from api.deps import df_to_records, clean_dict

router = APIRouter()


@router.get("/projects")
def list_nmtc_projects(
    states: Optional[List[str]] = Query(default=None),
    census_tract_id: Optional[str] = None,
    cde_name: Optional[str] = None,
    project_type: Optional[str] = Query(default=None, description="Real Estate or Non-Real Estate"),
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
):
    """Return NMTC projects matching the given filters."""
    df = db.get_nmtc_projects(
        states=states,
        census_tract_id=census_tract_id,
        cde_name=cde_name,
        project_type=project_type,
        min_year=min_year,
        max_year=max_year,
    )
    return df_to_records(df)


@router.get("/projects/summary")
def nmtc_summary():
    """High-level NMTC investment totals."""
    return db.get_nmtc_project_summary()


@router.get("/projects/{cdfi_project_id}")
def get_nmtc_project(cdfi_project_id: str):
    """Full detail for a single NMTC project."""
    record = db.get_nmtc_project_by_id(cdfi_project_id)
    if not record:
        raise HTTPException(status_code=404, detail="NMTC project not found")
    return clean_dict(record)


@router.get("/projects/{cdfi_project_id}/peer-comps")
def nmtc_peer_comps(
    cdfi_project_id: str,
    n: int = Query(default=10, description="Number of comparable deals to return"),
):
    """
    Return comparable NMTC deals for a given project.
    Matches on project_type, state, and approximate QLICI size.
    """
    base = db.get_nmtc_project_by_id(cdfi_project_id)
    if not base:
        raise HTTPException(status_code=404, detail="NMTC project not found")
    df = db.get_peer_nmtc_projects(
        project_type=base.get("project_type"),
        state=base.get("state"),
        qlici_amount=base.get("qlici_amount"),
        exclude_id=cdfi_project_id,
        top_n=n,
    )
    return df_to_records(df)


@router.get("/cdes")
def list_cdes(
    states: Optional[List[str]] = Query(default=None),
):
    """Return CDE allocation records, optionally filtered by state."""
    df = db.get_cde_allocations(states=states)
    return df_to_records(df)


@router.get("/coalition")
def list_coalition_projects(
    state:           Optional[str]  = Query(None, description="2-letter state"),
    cde_name:        Optional[str]  = Query(None, description="Substring match on CDE name"),
    investment_year: Optional[int]  = Query(None),
    matched_only:    bool           = Query(False, description="Only projects matched to CDFI Fund records"),
    limit:           int            = Query(500),
):
    """Return NMTC Coalition transaction-level project records."""
    df = db.get_nmtc_coalition_projects(
        state=state,
        cde_name=cde_name,
        investment_year=investment_year,
        matched_only=matched_only,
        limit=limit,
    )
    return {"data": df_to_records(df), "total": len(df)}
