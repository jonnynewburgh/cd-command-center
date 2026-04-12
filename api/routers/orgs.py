"""
api/routers/orgs.py — Organization / 990 financial data endpoints.

An "org" in this context is a nonprofit operator identified by EIN.
The same org may run multiple schools, FQHC sites, or ECE centers.
"""

from fastapi import APIRouter, HTTPException
import db
from api.deps import df_to_records, clean_dict

router = APIRouter()


@router.get("/{ein}/990")
def get_990(ein: str):
    """
    Most recent 990 financial snapshot for a given EIN.
    Includes revenue, expenses, net assets, and key balance sheet fields.
    """
    record = db.get_990_by_ein(ein)
    if not record:
        raise HTTPException(status_code=404, detail="No 990 record found for this EIN")
    return clean_dict(record)


@router.get("/{ein}/990/history")
def get_990_history(ein: str):
    """
    Multi-year 990 history for a given EIN (for trend charts).
    Returns one row per tax year, sorted newest first.
    """
    df = db.get_990_history(ein)
    return df_to_records(df)


@router.get("/{ein}/schools")
def org_schools(ein: str):
    """All schools linked to this EIN (multi-site operator view)."""
    df = db.get_operator_schools(ein)
    return df_to_records(df)


@router.get("/{ein}/fqhc")
def org_fqhc(ein: str):
    """All FQHC sites linked to this EIN."""
    df = db.get_operator_fqhc(ein)
    return df_to_records(df)


@router.get("/{ein}/ratios")
def org_ratios(ein: str):
    """
    Computed financial ratios for this EIN (acid ratio, leverage, 3yr avg CF).
    Returns one row per fiscal year.
    """
    df = db.get_financial_ratios(ein)
    return df_to_records(df)


@router.get("/{ein}/ratios/latest")
def org_latest_ratios(ein: str):
    """Most recent computed financial ratios for this EIN."""
    record = db.get_latest_financial_ratios(ein)
    if not record:
        raise HTTPException(status_code=404, detail="No financial ratios found for this EIN")
    return clean_dict(record)


@router.get("/{ein}/documents")
def org_documents(ein: str):
    """Uploaded documents (audits, financials) for this EIN."""
    df = db.get_documents(ein=ein)
    return df_to_records(df)
