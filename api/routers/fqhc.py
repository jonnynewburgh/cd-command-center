"""
api/routers/fqhc.py — FQHC / health center endpoints.
"""

from typing import Optional, List
from fastapi import APIRouter, Query, HTTPException
import db
from api.deps import clean_dict, paginate

router = APIRouter()


@router.get("")
def list_fqhc(
    states: Optional[List[str]] = Query(default=None),
    active_only: bool = Query(default=True),
    site_types: Optional[List[str]] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
    sort: Optional[str] = Query(default=None, description="Sort key: name, state, city, type"),
    sort_dir: str = Query(default="asc", pattern="^(asc|desc)$"),
):
    """Return a page of FQHC sites matching the given filters.

    Response shape: `{items, total, limit, offset}`.
    """
    df = db.get_fqhc(
        states=states,
        active_only=active_only,
        site_types=site_types,
        limit=limit,
        offset=offset,
        sort_by=sort,
        sort_dir=sort_dir,
    )
    return paginate(df, limit=limit, offset=offset)


@router.get("/summary")
def fqhc_summary():
    """High-level FQHC counts."""
    return db.get_fqhc_summary()


@router.get("/states")
def fqhc_states():
    """Sorted list of states that have FQHC data."""
    return db.get_fqhc_states()


@router.get("/{bhcmis_id}")
def get_fqhc_site(bhcmis_id: str):
    """Full detail for a single FQHC site by BHCMIS ID."""
    record = db.get_fqhc_by_id(bhcmis_id)
    if not record:
        raise HTTPException(status_code=404, detail="FQHC site not found")
    return clean_dict(record)


@router.get("/{bhcmis_id}/990")
def fqhc_990(bhcmis_id: str):
    """990 financial data for the operator linked to this FQHC site."""
    record = db.get_990_for_fqhc(bhcmis_id)
    if not record:
        raise HTTPException(status_code=404, detail="No 990 data linked to this FQHC site")
    return clean_dict(record)
