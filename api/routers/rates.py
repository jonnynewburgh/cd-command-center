"""
api/routers/rates.py — Market rate endpoints (FRED data: SOFR, Treasuries, etc.).

These power the rate ticker and benchmark comparison tools in the dashboard.
"""

from typing import Optional, List
from fastapi import APIRouter, Query
import db
from api.deps import df_to_records

router = APIRouter()

# Human-readable labels for each FRED series ID
SERIES_LABELS = {
    "SOFR":    "SOFR (Overnight)",
    "FEDFUNDS": "Fed Funds Rate",
    "DPRIME":  "Prime Rate",
    "DGS1MO":  "1-Month Treasury",
    "DGS3MO":  "3-Month Treasury",
    "DGS6MO":  "6-Month Treasury",
    "DGS1":    "1-Year Treasury",
    "DGS2":    "2-Year Treasury",
    "DGS3":    "3-Year Treasury",
    "DGS5":    "5-Year Treasury",
    "DGS7":    "7-Year Treasury",
    "DGS10":   "10-Year Treasury",
    "DGS20":   "20-Year Treasury",
    "DGS30":   "30-Year Treasury",
}


@router.get("/latest")
def latest_rates():
    """
    Return the most recent value for every rate series in the database.
    Used for the dashboard rate ticker / summary cards.
    """
    df = db.get_latest_rates()
    return df_to_records(df)


@router.get("")
def rate_history(
    series: Optional[List[str]] = Query(
        default=None,
        description="FRED series IDs to include (e.g. SOFR DGS10). Omit for all.",
    ),
    days: Optional[int] = Query(
        default=90,
        description="Return the last N days of history. Use 0 for all history.",
        ge=0,
    ),
    start_date: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD"),
):
    """
    Return rate history for charting.  Defaults to the last 90 days for all series.
    Pass days=0 to return full history (can be large).
    """
    df = db.get_market_rates(
        series_ids=series,
        days=(days if days and days > 0 else None),
        start_date=start_date,
        end_date=end_date,
    )
    return df_to_records(df)


@router.get("/series")
def available_series():
    """Return the list of rate series labels available in the database."""
    return [
        {"series_id": k, "label": v}
        for k, v in SERIES_LABELS.items()
    ]
