"""
utils/export.py — CSV and report export functions.
"""

import pandas as pd
import io


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """
    Convert a DataFrame to CSV bytes for Streamlit's download_button.

    Usage in a page:
        csv = df_to_csv_bytes(df)
        st.download_button("Download CSV", data=csv, file_name="results.csv", mime="text/csv")
    """
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


def format_school_export(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and rename columns for a clean charter school CSV export.
    Drops internal columns like 'id', 'created_at', etc.
    """
    display_cols = {
        "nces_id": "NCES ID",
        "school_name": "School Name",
        "lea_name": "LEA Name",
        "state": "State",
        "city": "City",
        "county": "County",
        "census_tract_id": "Census Tract",
        "enrollment": "Enrollment",
        "school_status": "Status",
        "year_opened": "Year Opened",
        "grade_low": "Grade Low",
        "grade_high": "Grade High",
        "pct_free_reduced_lunch": "% FRL",
        "pct_ell": "% ELL",
        "pct_sped": "% SPED",
        "pct_black": "% Black",
        "pct_hispanic": "% Hispanic",
        "survival_score": "Survival Score",
        "survival_risk_tier": "Risk Tier",
        "accountability_score": "LEA Accountability Score",
        "accountability_rating": "LEA Rating",
        "latitude": "Latitude",
        "longitude": "Longitude",
    }

    # Keep only columns that exist in the DataFrame
    cols_to_keep = {k: v for k, v in display_cols.items() if k in df.columns}
    return df[list(cols_to_keep.keys())].rename(columns=cols_to_keep)
