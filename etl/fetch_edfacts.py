"""
etl/fetch_edfacts.py — Download and load EDFacts federal LEA accountability data.

Data source: US Department of Education EDFacts Data Files
  https://www2.ed.gov/about/inits/ed/edfacts/data-files/index.html

Three files are used:
  1. Math proficiency by LEA
  2. Reading/ELA proficiency by LEA
  3. Cohort graduation rate by LEA

AUTO-DOWNLOAD:
  Run without file arguments to auto-download from the US Dept of Education:

    python etl/fetch_edfacts.py --year 2023         # auto-download all three files
    python etl/fetch_edfacts.py --year 2023 --states CA TX NY  # auto-download, filter states

  EDFacts covers all 50 states — this is the best single-source approach for
  full national coverage of math/reading proficiency and graduation rates.

  EDFacts file URL pattern (school year ending in {year}):
    Math:  https://www2.ed.gov/about/inits/ed/edfacts/data-files/math-achievement-lea-sy{YY1}{YY2}.csv
    RLA:   https://www2.ed.gov/about/inits/ed/edfacts/data-files/rla-achievement-lea-sy{YY1}{YY2}.csv
    Grad:  https://www2.ed.gov/about/inits/ed/edfacts/data-files/acgr-lea-sy{YY1}{YY2}.csv
    (e.g. 2022-23 school year → sy2223)

  Note: The US DOE sometimes packages these as zip files. This script handles both.

The key parsing challenge: EDFacts uses range codes instead of exact percentages.
  e.g., "GE50LE75" means "at least 50% and at most 75%"
  We convert these to the midpoint: (50 + 75) / 2 = 62.5

Usage:
    # Auto-download all three files:
    python etl/fetch_edfacts.py --year 2023

    # Use local files:
    python etl/fetch_edfacts.py --year 2023 \\
        --math-file data/raw/edfacts_math_2023.csv \\
        --rla-file  data/raw/edfacts_rla_2023.csv \\
        --grad-file data/raw/edfacts_grad_2023.csv

    # Filter to specific states:
    python etl/fetch_edfacts.py --year 2023 --states CA TX NY
"""

import argparse
import sys
import os
import re
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from utils.downloader import download_file, download_and_extract_zip


def _edfacts_urls(year: int) -> dict:
    """
    Build EDFacts file download URLs for a given school year ending.
    year=2023 means the 2022-23 school year (sy2223).
    """
    yy1 = str(year - 1)[-2:]   # e.g. 2023 → "22"
    yy2 = str(year)[-2:]        # e.g. 2023 → "23"
    sy = f"sy{yy1}{yy2}"        # e.g. "sy2223"
    base = "https://www2.ed.gov/about/inits/ed/edfacts/data-files"
    return {
        "math": f"{base}/math-achievement-lea-{sy}.csv",
        "rla":  f"{base}/rla-achievement-lea-{sy}.csv",
        "grad": f"{base}/acgr-lea-{sy}.csv",
        # Some years are published as zip files
        "math_zip": f"{base}/math-achievement-lea-{sy}.zip",
        "rla_zip":  f"{base}/rla-achievement-lea-{sy}.zip",
        "grad_zip": f"{base}/acgr-lea-{sy}.zip",
    }


def _local_edfacts_paths(year: int) -> dict:
    return {
        "math": f"data/raw/edfacts_math_{year}.csv",
        "rla":  f"data/raw/edfacts_rla_{year}.csv",
        "grad": f"data/raw/edfacts_grad_{year}.csv",
    }


def _download_edfacts_file(year: int, file_type: str, force: bool = False) -> str | None:
    """
    Download one EDFacts file (math, rla, or grad). Tries the CSV URL first,
    then the zip URL. Returns local path on success, or None on failure.
    """
    urls = _edfacts_urls(year)
    locals_ = _local_edfacts_paths(year)
    local = locals_[file_type]
    labels = {"math": "Math proficiency", "rla": "Reading/ELA proficiency", "grad": "Graduation rate"}
    label = labels.get(file_type, file_type)

    # Try CSV URL first
    try:
        return download_file(
            url=urls[file_type],
            dest_path=local,
            description=f"EDFacts {label} (LEA, {year})",
            force=force,
        )
    except RuntimeError:
        pass

    # Fall back to zip URL
    try:
        zip_local = local.replace(".csv", ".zip")
        return download_and_extract_zip(
            url=urls[f"{file_type}_zip"],
            zip_dest=zip_local,
            extract_pattern="*.csv",
            extract_dest=local,
            description=f"EDFacts {label} zip (LEA, {year})",
            force=force,
        )
    except RuntimeError as e:
        print(f"  Could not download EDFacts {label} for {year}: {e}")
        return None


# ---------------------------------------------------------------------------
# Range code parser
# ---------------------------------------------------------------------------

def parse_edfacts_pct(value_str):
    """
    Convert an EDFacts percentage range code to a numeric midpoint.

    EDFacts suppresses small values and reports ranges instead of exact numbers.
    We convert ranges to their midpoint so we have a numeric value to store.

    Examples:
        "GE50LE75"  -> 62.5   (between 50 and 75, midpoint)
        "GT25LT50"  -> 37.5   (between 25 and 50)
        "GE95"      -> 97.5   (95 or above, we use 100 as the upper bound)
        "LT5"       ->  2.5   (below 5, we use 0 as the lower bound)
        "PS"        -> None   (privacy-suppressed — too few students to report)
        "50.2"      -> 50.2   (already a plain number — just parse it)
        NaN / None  -> None

    Args:
        value_str: string or float from an EDFacts CSV cell

    Returns:
        float or None
    """
    if value_str is None or (isinstance(value_str, float) and np.isnan(value_str)):
        return None

    s = str(value_str).strip()

    # Already a plain number — return as-is
    try:
        return float(s)
    except ValueError:
        pass

    # Privacy-suppressed or not-applicable codes
    if s.upper() in ("PS", "N/A", "–", "-", "", "NULL"):
        return None

    # Parse range codes like GE50LE75, GT25LT50, GE95, LT5
    # Pattern: optional (GE|GT) + lower number + optional (LE|LT) + optional upper number
    pattern = r"^(GE|GT)?(\d+(?:\.\d+)?)(LE|LT)?(\d+(?:\.\d+)?)?$"
    match = re.match(pattern, s, re.IGNORECASE)

    if not match:
        return None

    lower_op, lower_val, upper_op, upper_val = match.groups()
    lower = float(lower_val) if lower_val else 0.0
    upper = float(upper_val) if upper_val else 100.0

    return round((lower + upper) / 2, 1)


# ---------------------------------------------------------------------------
# Column name helpers
# ---------------------------------------------------------------------------

def normalize_cols(df):
    """Lowercase and strip whitespace from all column names."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def find_col(df, candidates):
    """
    Return the first column name from the candidates list that exists in df.
    Returns None if none of the candidates match.
    This handles the fact that EDFacts column names change slightly between years.
    """
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ---------------------------------------------------------------------------
# File loaders — one per data type
# ---------------------------------------------------------------------------

def load_math_file(filepath, year, states=None):
    """
    Load an EDFacts math proficiency CSV file.

    Returns a DataFrame with columns: lea_id, state, proficiency_math, data_year
    Returns an empty DataFrame if the file can't be parsed.
    """
    print(f"Reading math file: {filepath}")
    try:
        df = pd.read_csv(filepath, dtype=str, low_memory=False)
    except Exception as e:
        print(f"  Error reading file: {e}")
        return pd.DataFrame()

    df = normalize_cols(df)

    # EDFacts LEA ID column name varies by year
    lea_col = find_col(df, ["leaid", "lea_id", "ncessch", "district_id", "st_leaid"])
    state_col = find_col(df, ["stateabb", "state_abbr", "state", "st", "stabb"])
    # The "all students" proficiency column — EDFacts may call it different things
    pct_col = find_col(df, [
        "pctprof", "pct_prof", "all_pctprof", "pctproficient",
        "all_students_pctprof", "pct_at_or_above_proficient",
    ])

    if not lea_col:
        print(f"  Error: could not find LEA ID column. Available columns: {list(df.columns)}")
        return pd.DataFrame()

    if not pct_col:
        print(f"  Error: could not find proficiency column. Available columns: {list(df.columns)}")
        return pd.DataFrame()

    result = pd.DataFrame()
    result["lea_id"] = df[lea_col].str.strip()
    result["proficiency_math"] = df[pct_col].apply(parse_edfacts_pct)
    result["data_year"] = year

    if state_col:
        result["state"] = df[state_col].str.strip()

    # Drop rows with no usable LEA ID
    result = result[result["lea_id"].notna() & (result["lea_id"] != "")]

    # Optional state filter
    if states and "state" in result.columns:
        result = result[result["state"].isin(states)]

    print(f"  Loaded {len(result)} LEA math records")
    return result


def load_rla_file(filepath, year, states=None):
    """
    Load an EDFacts reading/language arts proficiency CSV file.

    Same structure as load_math_file but for reading proficiency.
    Returns a DataFrame with columns: lea_id, state, proficiency_reading, data_year
    """
    print(f"Reading RLA file: {filepath}")
    try:
        df = pd.read_csv(filepath, dtype=str, low_memory=False)
    except Exception as e:
        print(f"  Error reading file: {e}")
        return pd.DataFrame()

    df = normalize_cols(df)

    lea_col = find_col(df, ["leaid", "lea_id", "ncessch", "district_id", "st_leaid"])
    state_col = find_col(df, ["stateabb", "state_abbr", "state", "st", "stabb"])
    pct_col = find_col(df, [
        "pctprof", "pct_prof", "all_pctprof", "pctproficient",
        "all_students_pctprof", "pct_at_or_above_proficient",
    ])

    if not lea_col or not pct_col:
        print(f"  Error: missing required columns. Available: {list(df.columns)}")
        return pd.DataFrame()

    result = pd.DataFrame()
    result["lea_id"] = df[lea_col].str.strip()
    result["proficiency_reading"] = df[pct_col].apply(parse_edfacts_pct)
    result["data_year"] = year

    if state_col:
        result["state"] = df[state_col].str.strip()

    result = result[result["lea_id"].notna() & (result["lea_id"] != "")]

    if states and "state" in result.columns:
        result = result[result["state"].isin(states)]

    print(f"  Loaded {len(result)} LEA reading records")
    return result


def load_grad_file(filepath, year, states=None):
    """
    Load an EDFacts adjusted cohort graduation rate CSV file.

    Returns a DataFrame with columns: lea_id, state, graduation_rate, data_year
    """
    print(f"Reading graduation file: {filepath}")
    try:
        df = pd.read_csv(filepath, dtype=str, low_memory=False)
    except Exception as e:
        print(f"  Error reading file: {e}")
        return pd.DataFrame()

    df = normalize_cols(df)

    lea_col = find_col(df, ["leaid", "lea_id", "ncessch", "district_id", "st_leaid"])
    state_col = find_col(df, ["stateabb", "state_abbr", "state", "st", "stabb"])
    grad_col = find_col(df, [
        "cohortgradrate", "grad_rate", "gradrate", "adjcohortgradrate",
        "all_cohortgradrate", "pct_graduated",
    ])

    if not lea_col or not grad_col:
        print(f"  Error: missing required columns. Available: {list(df.columns)}")
        return pd.DataFrame()

    result = pd.DataFrame()
    result["lea_id"] = df[lea_col].str.strip()
    result["graduation_rate"] = df[grad_col].apply(parse_edfacts_pct)
    result["data_year"] = year

    if state_col:
        result["state"] = df[state_col].str.strip()

    result = result[result["lea_id"].notna() & (result["lea_id"] != "")]

    if states and "state" in result.columns:
        result = result[result["state"].isin(states)]

    print(f"  Loaded {len(result)} LEA graduation records")
    return result


# ---------------------------------------------------------------------------
# Merge and load
# ---------------------------------------------------------------------------

def merge_and_load(math_df, rla_df, grad_df, year):
    """
    Merge math, reading, and graduation DataFrames on lea_id, then upsert
    each row into the lea_accountability table.

    We only pass non-None values to upsert_lea_accountability() so that
    EDFacts data doesn't overwrite state-reported scores that are already
    in the table from a previous load.

    Returns:
        (loaded_count, error_count)
    """
    # Start with math as the base (it typically has the broadest coverage)
    if math_df.empty and rla_df.empty and grad_df.empty:
        print("No data to load.")
        return 0, 0

    # Build a combined frame by merging on lea_id
    # Use whichever non-empty frame is the starting point
    if not math_df.empty:
        merged = math_df.copy()
    elif not rla_df.empty:
        merged = rla_df[["lea_id", "data_year"]].copy()
        if "state" in rla_df.columns:
            merged["state"] = rla_df["state"]
    else:
        merged = grad_df[["lea_id", "data_year"]].copy()
        if "state" in grad_df.columns:
            merged["state"] = grad_df["state"]

    # Left-join reading data
    if not rla_df.empty and "proficiency_reading" in rla_df.columns:
        rla_slim = rla_df[["lea_id", "proficiency_reading"]].copy()
        merged = merged.merge(rla_slim, on="lea_id", how="left")
    else:
        merged["proficiency_reading"] = None

    # Left-join graduation data
    if not grad_df.empty and "graduation_rate" in grad_df.columns:
        grad_slim = grad_df[["lea_id", "graduation_rate"]].copy()
        merged = merged.merge(grad_slim, on="lea_id", how="left")
    else:
        merged["graduation_rate"] = None

    print(f"Upserting {len(merged)} LEA records into lea_accountability...")
    loaded = 0
    errors = 0

    for _, row in merged.iterrows():
        record = {
            "lea_id":               row.get("lea_id"),
            "state":                row.get("state"),
            "proficiency_math":     row.get("proficiency_math"),
            "proficiency_reading":  row.get("proficiency_reading"),
            "graduation_rate":      row.get("graduation_rate"),
            "data_year":            year,
        }

        # Remove None/NaN values so we don't overwrite existing data with NULL
        record = {
            k: v for k, v in record.items()
            if v is not None and not (isinstance(v, float) and np.isnan(v))
        }

        if not record.get("lea_id"):
            errors += 1
            continue

        try:
            db.upsert_lea_accountability(record)
            loaded += 1
        except Exception as e:
            print(f"  Error upserting lea_id={record.get('lea_id')}: {e}")
            errors += 1

    print(f"  Done: {loaded} loaded, {errors} errors")
    return loaded, errors


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Load EDFacts federal LEA accountability data into the lea_accountability table"
    )
    parser.add_argument(
        "--year", type=int, required=True,
        help="School year ending (e.g. 2023 = the 2022-23 school year)",
    )
    parser.add_argument(
        "--states", nargs="+",
        help="Optional: only load these states (e.g. --states CA TX NY)",
    )
    parser.add_argument(
        "--math-file",
        help="Path to local EDFacts math proficiency CSV. If omitted, auto-downloads.",
    )
    parser.add_argument(
        "--rla-file",
        help="Path to local EDFacts reading/language arts proficiency CSV. If omitted, auto-downloads.",
    )
    parser.add_argument(
        "--grad-file",
        help="Path to local EDFacts graduation rate CSV. If omitted, auto-downloads.",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download files even if recent local copies exist.",
    )
    args = parser.parse_args()

    db.init_db()

    locals_ = _local_edfacts_paths(args.year)

    # Resolve math file — auto-download if not provided
    math_file = args.math_file
    if not math_file:
        print("Auto-downloading EDFacts math proficiency file...")
        math_file = _download_edfacts_file(args.year, "math", force=args.force_download)
        if not math_file:
            math_file = locals_["math"] if os.path.exists(locals_["math"]) else None
    elif not os.path.exists(math_file):
        print(f"Error: math file not found: {math_file}")
        sys.exit(1)

    # Resolve RLA file
    rla_file = args.rla_file
    if not rla_file:
        print("Auto-downloading EDFacts reading/ELA proficiency file...")
        rla_file = _download_edfacts_file(args.year, "rla", force=args.force_download)
        if not rla_file:
            rla_file = locals_["rla"] if os.path.exists(locals_["rla"]) else None
    elif not os.path.exists(rla_file):
        print(f"Error: RLA file not found: {rla_file}")
        sys.exit(1)

    # Resolve grad file
    grad_file = args.grad_file
    if not grad_file:
        print("Auto-downloading EDFacts graduation rate file...")
        grad_file = _download_edfacts_file(args.year, "grad", force=args.force_download)
        if not grad_file:
            grad_file = locals_["grad"] if os.path.exists(locals_["grad"]) else None
    elif not os.path.exists(grad_file):
        print(f"Error: graduation file not found: {grad_file}")
        sys.exit(1)

    if not math_file and not rla_file and not grad_file:
        print("Error: could not download or find any EDFacts files.")
        print()
        print("Download manually from:")
        print("  https://www2.ed.gov/about/inits/ed/edfacts/data-files/index.html")
        print()
        yy1 = str(args.year - 1)[-2:]
        yy2 = str(args.year)[-2:]
        sy = f"sy{yy1}{yy2}"
        print(f"  Math: math-achievement-lea-{sy}.csv")
        print(f"  RLA:  rla-achievement-lea-{sy}.csv")
        print(f"  Grad: acgr-lea-{sy}.csv")
        sys.exit(1)

    math_df = pd.DataFrame()
    rla_df = pd.DataFrame()
    grad_df = pd.DataFrame()

    if math_file:
        math_df = load_math_file(math_file, args.year, states=args.states)

    if rla_file:
        rla_df = load_rla_file(rla_file, args.year, states=args.states)

    if grad_file:
        grad_df = load_grad_file(grad_file, args.year, states=args.states)

    merge_and_load(math_df, rla_df, grad_df, args.year)

    print("EDFacts load complete.")


if __name__ == "__main__":
    main()
