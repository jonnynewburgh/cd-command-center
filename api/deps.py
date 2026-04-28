"""
api/deps.py — Shared helpers for API routers.
"""
import json
import pandas as pd


def df_to_records(df: pd.DataFrame) -> list:
    """
    Convert a pandas DataFrame to a JSON-safe list of dicts.
    NaN / NaT values become None (null in JSON) automatically via to_json().
    """
    if df is None or df.empty:
        return []
    return json.loads(df.to_json(orient="records"))


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
