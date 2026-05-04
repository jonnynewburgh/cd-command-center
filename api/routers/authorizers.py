"""
api/routers/authorizers.py — Charter authorizer registry (statute snapshot + entities).

Endpoints:
  GET /authorizers/statutory-policy  — NACSA statutory authorizer-type columns by state/DC
  GET /authorizers                   — operational authorizer entities (from state / manual load)
  GET /authorizers/school-links     — school ↔ authorizer rows for a year or school
"""

from typing import List, Optional

from fastapi import APIRouter, Query

import db
from api.deps import df_to_records

router = APIRouter()


@router.get("/statutory-policy")
def list_statutory_policy(
    state: Optional[str] = Query(
        None, min_length=2, max_length=2, description="USPS state code, e.g. GA"
    ),
):
    """Statutory landscape: which authorizer types are permitted, from the NACSA seed."""
    df = db.get_statutory_charter_authorizer_policy(
        state_usps=state.upper() if state else None,
    )
    return {"data": df_to_records(df), "total": len(df)}


@router.get("/")
def list_authorizers(
    states: Optional[List[str]] = Query(None, description="Filter by state USPS codes"),
    name: Optional[str] = Query(None, description="Substring match on authorizer name"),
    kind: Optional[str] = Query(None, description="SEA, LEA, ICB, HEI, NEG, NPO"),
    include_inactive: bool = Query(False),
):
    """Named authorizing bodies loaded from open data or manual entry."""
    df = db.get_authorizers(
        states=states,
        name_substring=name,
        authorizer_kind=kind,
        active_only=not include_inactive,
    )
    return {"data": df_to_records(df), "total": len(df)}


@router.get("/school-links")
def list_school_authorizers(
    nces_school_id: Optional[str] = Query(None),
    authorizer_id: Optional[int] = Query(None),
    school_year: Optional[str] = Query(None, description="e.g. 2023-24"),
    states: Optional[List[str]] = Query(None),
):
    """Links between NCES schools and authorizers."""
    df = db.get_school_authorizers(
        nces_school_id=nces_school_id,
        authorizer_id=authorizer_id,
        school_year=school_year,
        states=states,
    )
    return {"data": df_to_records(df), "total": len(df)}
