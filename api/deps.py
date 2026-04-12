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


def clean_dict(d: dict) -> dict:
    """Replace float NaN values in a plain dict with None."""
    import math
    return {
        k: (None if isinstance(v, float) and math.isnan(v) else v)
        for k, v in d.items()
    }
