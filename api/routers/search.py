"""
api/routers/search.py — Cross-entity search and geographic proximity endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Query, HTTPException
import db
from api.deps import df_to_records

router = APIRouter()


@router.get("")
def search(
    q: str = Query(..., min_length=2, description="Search term (name, city, state, ID)"),
):
    """
    Search across schools, NMTC projects, CDEs, FQHCs, and ECE centers by name or city.
    Returns a dict with keys: schools, projects, cdes, fqhc, ece.
    Each value is a list of matching records (up to 200 per category).
    """
    results = db.search_all(q)
    return {
        key: df_to_records(df)
        for key, df in results.items()
    }


@router.get("/nearby")
def nearby(
    lat: float = Query(..., description="Latitude of the center point"),
    lon: float = Query(..., description="Longitude of the center point"),
    radius: float = Query(default=1.0, description="Search radius in miles", ge=0.1, le=50.0),
):
    """
    Return all facility types within `radius` miles of the given lat/lon.
    Returns a dict with keys: schools, fqhc, ece, nmtc.
    Each value is a list of records with a 'distance_miles' column added.
    """
    results = db.get_nearby_facilities(lat=lat, lon=lon, radius_miles=radius)
    return {
        key: df_to_records(df)
        for key, df in results.items()
    }


@router.get("/org")
def search_org(
    q: str = Query(..., min_length=2, description="Org name or EIN"),
):
    """
    Search IRS 990 records by organization name or EIN.
    Returns up to 50 matching orgs with their most recent financial data.
    """
    df = db.search_org(q)
    return df_to_records(df)
