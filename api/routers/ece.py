"""
api/routers/ece.py — Early care and education (ECE) center endpoints.
"""

from typing import Optional, List
from fastapi import APIRouter, Query, HTTPException
import db
from api.deps import df_to_records, clean_dict

router = APIRouter()


@router.get("")
def list_ece(
    states: Optional[List[str]] = Query(default=None),
    active_only: bool = Query(default=True),
    facility_types: Optional[List[str]] = Query(default=None),
    accepts_subsidies: Optional[bool] = None,
    min_capacity: Optional[int] = None,
):
    """Return ECE centers matching the given filters."""
    df = db.get_ece_centers(
        states=states,
        active_only=active_only,
        facility_types=facility_types,
        accepts_subsidies=accepts_subsidies,
        min_capacity=min_capacity,
    )
    return df_to_records(df)


@router.get("/summary")
def ece_summary():
    """High-level ECE counts."""
    return db.get_ece_summary()


@router.get("/states")
def ece_states():
    """Sorted list of states that have ECE data."""
    return db.get_ece_states()


@router.get("/{license_id}")
def get_ece_center(license_id: str):
    """Full detail for a single ECE center by license ID."""
    record = db.get_ece_by_id(license_id)
    if not record:
        raise HTTPException(status_code=404, detail="ECE center not found")
    return clean_dict(record)
