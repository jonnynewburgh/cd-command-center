"""
api/routers/headstart.py — Head Start / Early Head Start program endpoints.

Endpoints:
  GET /headstart                                          — List programs with filters
  GET /headstart/{grant_number}/{program_number}/{pir_year} — Full detail for one program
"""

from typing import Optional
from fastapi import APIRouter, Query, HTTPException
import db
from api.deps import df_to_records, clean_dict

router = APIRouter()


@router.get("")
def list_headstart(
    state:           Optional[str] = Query(None, description="2-letter state"),
    program_type:    Optional[str] = Query(None, description="HS, EHS, Migrant, AIAN"),
    pir_year:        Optional[int] = Query(None),
    grantee_name:    Optional[str] = Query(None, description="Substring match on grantee name"),
    zip_code:        Optional[str] = Query(None),
    census_tract_id: Optional[str] = Query(None),
    limit:           int           = Query(500),
):
    df = db.get_headstart_programs(
        state=state,
        program_type=program_type,
        pir_year=pir_year,
        grantee_name=grantee_name,
        zip_code=zip_code,
        census_tract_id=census_tract_id,
        limit=limit,
    )
    return {"data": df_to_records(df), "total": len(df)}


@router.get("/{grant_number}/{program_number}/{pir_year}")
def get_headstart_program(grant_number: str, program_number: str, pir_year: int):
    """Full detail for a single Head Start program."""
    record = db.get_headstart_by_id(grant_number, program_number, pir_year)
    if not record:
        raise HTTPException(status_code=404, detail="Head Start program not found")
    return clean_dict(record)
