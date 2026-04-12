"""
api/routers/cdfis.py — CDFI directory, state programs, and award data.
"""

from typing import Optional, List
from fastapi import APIRouter, Query
import db
from api.deps import df_to_records

router = APIRouter()


@router.get("")
def list_cdfis(
    states: Optional[List[str]] = Query(default=None),
    cdfi_type: Optional[str] = Query(default=None, description="Loan Fund, Bank, Credit Union, etc."),
):
    """Return certified CDFIs, optionally filtered by state and type."""
    df = db.get_cdfis(states=states, cdfi_type=cdfi_type)
    return df_to_records(df)


@router.get("/states")
def cdfi_states():
    """Sorted list of states that have CDFI directory data."""
    return db.get_cdfi_states()


@router.get("/awards")
def cdfi_awards(
    states: Optional[List[str]] = Query(default=None),
    programs: Optional[List[str]] = Query(
        default=None,
        description="FA, BEA, CMF, NMTC, etc.",
    ),
    min_year: Optional[int] = None,
):
    """
    Return CDFI Fund award data (FA, BEA, CMF, etc.) filtered by state, program, and year.
    """
    df = db.get_cdfi_awards(states=states, programs=programs, min_year=min_year)
    return df_to_records(df)


@router.get("/awards/states")
def cdfi_award_states():
    """Sorted list of states that have CDFI award data."""
    return db.get_cdfi_award_states()


@router.get("/state-programs")
def state_programs(
    state: Optional[str] = Query(default=None, description="Two-letter state abbreviation"),
):
    """
    Return state-level financing incentive programs (historic tax credits,
    state NMTCs, etc.), optionally filtered to a single state.
    """
    df = db.get_state_programs(state=state)
    return df_to_records(df)


@router.get("/state-programs/states")
def program_states():
    """Sorted list of states that have incentive program data."""
    return db.get_program_states()
