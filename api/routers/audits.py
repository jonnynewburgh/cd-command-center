"""
api/routers/audits.py — Federal Audit Clearinghouse (Single Audit) endpoints.

Endpoints:
  GET /audits              — List federal audits with filters
  GET /audits/{report_id}  — Full detail for a single audit
  GET /audits/{report_id}/programs — Program-level line items for an audit
"""

from typing import Optional
from fastapi import APIRouter, Query, HTTPException
import db
from api.deps import df_to_records, clean_dict

router = APIRouter()


@router.get("")
def list_audits(
    state:            Optional[str]  = Query(None, description="2-letter state"),
    audit_year:       Optional[int]  = Query(None),
    ein:              Optional[str]  = Query(None, description="Auditee EIN"),
    entity_type:      Optional[str]  = Query(None, description="non-profit, state, local, tribal, higher-ed"),
    has_findings:     Optional[bool] = Query(None, description="Only audits with material weakness or noncompliance"),
    is_going_concern: Optional[bool] = Query(None, description="Only going-concern opinions"),
    limit:            int            = Query(500),
):
    df = db.get_federal_audits(
        state=state,
        audit_year=audit_year,
        ein=ein,
        entity_type=entity_type,
        has_findings=has_findings or None,
        is_going_concern=is_going_concern or None,
        limit=limit,
    )
    return {"data": df_to_records(df), "total": len(df)}


@router.get("/{report_id}")
def get_audit(report_id: str):
    """Full detail for a single audit by report_id."""
    record = db.get_federal_audit_by_id(report_id)
    if not record:
        raise HTTPException(status_code=404, detail="Audit not found")
    return clean_dict(record)


@router.get("/{report_id}/programs")
def audit_programs(report_id: str):
    """Program-level line items (federal awards) for an audit."""
    df = db.get_federal_audit_programs(report_id)
    return {"data": df_to_records(df), "total": len(df)}
