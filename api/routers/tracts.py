"""
api/routers/tracts.py — Census tract endpoints.

Tracts are the geographic backbone of the whole tool. Every facility has a
census_tract_id that joins back here for eligibility tiers, demographics, etc.
"""

from typing import Optional, List, Literal
from fastapi import APIRouter, Query, HTTPException
import db
from api.deps import df_to_records, clean_dict, paginate

router = APIRouter()


@router.get("")
def list_tracts(
    states: Optional[List[str]] = Query(default=None),
    nmtc_eligible_only: bool = Query(default=False),
    eligibility_tiers: Optional[List[str]] = Query(
        default=None,
        description="LIC, Severely Distressed, Deep Distress, Not Eligible",
    ),
    min_poverty_rate: Optional[float] = None,
    max_median_income: Optional[int] = None,
    county_fips: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
    sort: Optional[str] = Query(default=None, description="Sort key: state, poverty, income, tract, county"),
    sort_dir: str = Query(default="asc", pattern="^(asc|desc)$"),
):
    """Return a page of census tracts matching the given filters.

    Response shape: `{items, total, limit, offset}`.
    """
    df = db.get_census_tracts(
        states=states,
        nmtc_eligible_only=nmtc_eligible_only,
        eligibility_tiers=eligibility_tiers,
        min_poverty_rate=min_poverty_rate,
        max_median_income=max_median_income,
        county_fips=county_fips,
        limit=limit,
        offset=offset,
        sort_by=sort,
        sort_dir=sort_dir,
    )
    return paginate(df, limit=limit, offset=offset)


@router.get("/summary")
def tract_summary():
    """High-level counts for the census tract dashboard header."""
    return db.get_census_tract_summary()


@router.get("/states")
def tract_states():
    """Sorted list of states that have census tract data."""
    return db.get_census_tract_states()


@router.get("/service-gaps")
def service_gaps(
    asset_class: Literal["ece", "fqhc", "schools"] = Query(
        default="ece",
        description="Which facility type to check for gaps",
    ),
    states: Optional[List[str]] = Query(default=None),
    min_poverty_rate: float = Query(default=20.0),
    top_n: int = Query(default=50, le=200),
):
    """
    Find high-poverty census tracts with zero facilities of the given type.
    Ranked by need score (population × poverty rate).
    """
    df = db.get_service_gaps(
        states=states,
        asset_class=asset_class,
        min_poverty_rate=min_poverty_rate,
        top_n=top_n,
    )
    return df_to_records(df)


@router.get("/{census_tract_id}")
def get_tract(census_tract_id: str):
    """Full detail for a single census tract by 11-digit FIPS code."""
    record = db.get_census_tract(census_tract_id)
    if not record:
        raise HTTPException(status_code=404, detail="Census tract not found")
    return clean_dict(record)
