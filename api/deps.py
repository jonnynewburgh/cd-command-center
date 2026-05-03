"""
api/deps.py — Shared helpers for API routers.
"""
import json

import pandas as pd


def df_to_records(df: pd.DataFrame) -> list:
    """
    Convert a pandas DataFrame to a JSON-safe list of dicts.

    Why json.loads(df.to_json(...)) instead of df.to_dict('records'):
    on a 2,000-row 44-col schools page the to_json path runs ~2x faster
    than a pure-Python NaN/NaT scrub loop (28 ms vs 64 ms in the bench
    script run during P1 #5 triage on 2026-05-01). pandas' to_json is
    C-implemented; Python loops over hundreds of thousands of cells are
    not.

    `date_format='iso'` is explicit because pandas deprecated the default
    'epoch' (millisecond ints) starting in 4.x — without it every list
    endpoint logs a Pandas4Warning, and a future pandas release will
    flip the contract under us. ISO 8601 is also the format the Next.js
    consumer already expects, so this also fixes a latent format-drift
    risk.
    """
    if df is None or df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso"))


def paginate(df: pd.DataFrame, limit: int, offset: int) -> dict:
    """Wrap a paged DataFrame in the standard list-endpoint envelope.

    The DataFrame is expected to carry the unpaginated total in
    df.attrs["total"], set by the db.get_* helpers when limit is non-None.
    Falls back to len(df) if the attr is missing (e.g. unbounded internal
    callers that go through paginate() anyway).
    """
    items = df_to_records(df)
    total = df.attrs.get("total", len(items)) if df is not None else 0
    return {
        "items": items,
        "total": int(total),
        "limit": int(limit),
        "offset": int(offset),
    }


def clean_dict(d: dict) -> dict:
    """Replace float NaN values in a plain dict with None."""
    import math
    return {
        k: (None if isinstance(v, float) and math.isnan(v) else v)
        for k, v in d.items()
    }
