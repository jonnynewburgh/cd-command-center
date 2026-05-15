"""
api/main.py — FastAPI application for CD Command Center.

This is the single entry point for the REST API that powers the Next.js dashboard.
All data access goes through db.py (never raw SQL here).

Run locally:
    uvicorn api.main:app --reload --port 8000

The dashboard (Next.js) should proxy /api/* to this server, or talk to it
directly at http://localhost:8000 during local development.
"""

import sys
import os

# Make sure the repo root (where db.py lives) is on the Python path regardless
# of where uvicorn is invoked from.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import db

logger = logging.getLogger(__name__)

from api.routers import (
    schools,
    nmtc,
    fqhc,
    ece,
    tracts,
    search,
    rates,
    orgs,
    notes,
    cdfis,
    lending,
    housing,
    audits,
    headstart,
    accountability,
    authorizers,
    shortage,
)

app = FastAPI(
    title="CD Command Center API",
    description="REST API for community development deal origination data.",
    version="1.0.0",
)

# ---------------------------------------------------------------------------
# CORS — scoped to the dashboard origins. Override via CORS_ORIGINS env var
# (comma-separated) to add staging / preview URLs without a code change.
# ---------------------------------------------------------------------------
_DEFAULT_CORS_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
    "https://command-center.jhnadvising.com",
]
_cors_env = os.environ.get("CORS_ORIGINS", "").strip()
_cors_origins = (
    [o.strip() for o in _cors_env.split(",") if o.strip()]
    if _cors_env
    else _DEFAULT_CORS_ORIGINS
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Startup — ensure tables exist (idempotent)
# ---------------------------------------------------------------------------
@app.on_event("startup")
def startup():
    db.init_db()


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(schools.router, prefix="/schools",  tags=["Schools"])
app.include_router(nmtc.router,    prefix="/nmtc",     tags=["NMTC"])
app.include_router(fqhc.router,    prefix="/fqhc",     tags=["FQHC"])
app.include_router(ece.router,     prefix="/ece",      tags=["ECE"])
app.include_router(tracts.router,  prefix="/tracts",   tags=["Census Tracts"])
app.include_router(search.router,  prefix="/search",   tags=["Search"])
app.include_router(rates.router,   prefix="/rates",    tags=["Market Rates"])
app.include_router(orgs.router,    prefix="/orgs",     tags=["Organizations / 990"])
app.include_router(notes.router,   prefix="/notes",    tags=["Notes & Bookmarks"])
app.include_router(cdfis.router,   prefix="/cdfis",    tags=["CDFIs & Awards"])
app.include_router(lending.router, prefix="/lending",  tags=["Lending & Credit"])
app.include_router(housing.router,        prefix="/housing",        tags=["Housing & Labor"])
app.include_router(audits.router,         prefix="/audits",         tags=["Federal Audits"])
app.include_router(headstart.router,      prefix="/headstart",      tags=["Head Start"])
app.include_router(accountability.router, prefix="/accountability", tags=["Accountability"])
app.include_router(authorizers.router, prefix="/authorizers", tags=["Authorizers"])
app.include_router(shortage.router,    prefix="/shortage",    tags=["HRSA Shortage Areas"])


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok"}
