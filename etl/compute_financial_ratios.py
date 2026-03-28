"""
etl/compute_financial_ratios.py — Compute financial ratios from IRS 990 data.

Reads irs_990_history (multi-year 990 filings) and computes financial health
ratios per EIN per fiscal year. Results go into the financial_ratios table.

Ratios computed from 990 data (~approximate, marked acid_ratio_990):
  Acid ratio:    cash_savings / (accounts_payable + accrued_expenses)
                 Measures liquidity: can the org pay short-term obligations?
                 < 1.0 = potential cash stress; > 1.5 = healthy

  Leverage:      total_liabilities / unrestricted_net_assets
                 Measures debt burden relative to net equity.
                 > 1.0 = liabilities exceed equity (higher risk)

  Operating CF:  (total_revenue - total_expenses) as 990 net income proxy.
                 avg_operating_cash_flow = 3-year rolling average per EIN.

Note: 990-derived ratios are approximate. The audit-quality ratios
(acid_ratio_audit) require uploaded audit PDFs and are set separately
via the document upload workflow in the dashboard.

Usage:
    python etl/compute_financial_ratios.py          # all EINs in irs_990_history
    python etl/compute_financial_ratios.py --limit 500  # test run
"""

import argparse
import os
import sys
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db


def safe_divide(numerator, denominator):
    """Return numerator/denominator, or None if denominator is zero/None."""
    try:
        n = float(numerator)
        d = float(denominator)
        if d == 0:
            return None
        return round(n / d, 4)
    except (TypeError, ValueError):
        return None


def compute_ratios(limit: int = None):
    """
    Pull all IRS 990 history records, compute ratios, upsert into financial_ratios.
    Returns the number of rows upserted.
    """
    conn = db.get_connection()

    try:
        sql = """
            SELECT ein, tax_year, total_revenue, total_expenses,
                   cash_savings, accounts_payable, accrued_expenses,
                   total_liabilities, unrestricted_net_assets
            FROM irs_990_history
            WHERE ein IS NOT NULL
            ORDER BY ein, tax_year
        """
        if limit:
            # Get a subset of EINs for testing
            eins_sql = f"SELECT DISTINCT ein FROM irs_990_history LIMIT {limit}"
            ein_rows = conn.execute(eins_sql).fetchall()
            ein_list = [r[0] for r in ein_rows]
            placeholders = ",".join("?" * len(ein_list))
            sql = f"""
                SELECT ein, tax_year, total_revenue, total_expenses,
                       cash_savings, accounts_payable, accrued_expenses,
                       total_liabilities, unrestricted_net_assets
                FROM irs_990_history
                WHERE ein IN ({placeholders})
                ORDER BY ein, tax_year
            """
            df = pd.read_sql_query(sql, conn, params=ein_list)
        else:
            df = pd.read_sql_query(sql, conn)

    finally:
        conn.close()

    if df.empty:
        print("  No irs_990_history records found.")
        return 0

    print(f"  Processing {len(df):,} filings across {df['ein'].nunique():,} EINs...")

    # Coerce numeric columns
    num_cols = ["total_revenue", "total_expenses", "cash_savings",
                "accounts_payable", "accrued_expenses",
                "total_liabilities", "unrestricted_net_assets"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Sort for rolling window
    df = df.sort_values(["ein", "tax_year"])

    # --- Per-year ratios ---
    df["net_income_proxy"] = df["total_revenue"] - df["total_expenses"]

    # Acid ratio: cash / (AP + accrued)
    df["current_liabilities"] = df["accounts_payable"].fillna(0) + df["accrued_expenses"].fillna(0)
    df["acid_ratio_990"] = df.apply(
        lambda r: safe_divide(r["cash_savings"], r["current_liabilities"])
        if r["current_liabilities"] and r["current_liabilities"] > 0 else None,
        axis=1,
    )

    # Leverage ratio: total_liabilities / unrestricted_net_assets
    df["leverage_ratio"] = df.apply(
        lambda r: safe_divide(r["total_liabilities"], r["unrestricted_net_assets"]),
        axis=1,
    )

    # 3-year rolling average operating cash flow (net income proxy)
    df["avg_operating_cash_flow"] = (
        df.groupby("ein")["net_income_proxy"]
        .transform(lambda s: s.rolling(window=3, min_periods=1).mean().round(0))
    )

    rows = []
    now = datetime.utcnow().isoformat()
    for _, r in df.iterrows():
        rows.append({
            "ein":                      r["ein"],
            "fiscal_year":              int(r["tax_year"]),
            "acid_ratio_990":           r["acid_ratio_990"] if pd.notna(r.get("acid_ratio_990")) else None,
            "acid_ratio_audit":         None,   # set only via audit upload
            "leverage_ratio":           r["leverage_ratio"] if pd.notna(r.get("leverage_ratio")) else None,
            "avg_operating_cash_flow":  float(r["avg_operating_cash_flow"]) if pd.notna(r.get("avg_operating_cash_flow")) else None,
            "cash_and_equivalents":     float(r["cash_savings"]) if pd.notna(r.get("cash_savings")) else None,
            "accounts_payable":         float(r["accounts_payable"]) if pd.notna(r.get("accounts_payable")) else None,
            "accrued_expenses":         float(r["accrued_expenses"]) if pd.notna(r.get("accrued_expenses")) else None,
            "current_liabilities_audit":None,
            "unrestricted_net_assets":  float(r["unrestricted_net_assets"]) if pd.notna(r.get("unrestricted_net_assets")) else None,
            "total_liabilities":        float(r["total_liabilities"]) if pd.notna(r.get("total_liabilities")) else None,
            "total_debt":               None,
            "has_audit_data":           0,
            "data_source":              "irs_990_history",
            "calculated_at":            now,
        })

    total_loaded = db.upsert_rows("financial_ratios", rows, unique_cols=["ein", "fiscal_year"])
    return total_loaded


def main():
    parser = argparse.ArgumentParser(
        description="Compute financial ratios from IRS 990 history data"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit to N EINs (for testing). Default: all EINs.",
    )
    args = parser.parse_args()

    print("CD Command Center — Financial Ratios Computation")
    print(f"  Source: irs_990_history")
    if args.limit:
        print(f"  EIN limit: {args.limit}")
    print()

    db.init_db()
    run_id = db.log_load_start("financial_ratios")

    try:
        total = compute_ratios(limit=args.limit)
    except Exception as e:
        db.log_load_finish(run_id, rows_loaded=0, error=str(e))
        raise

    db.log_load_finish(run_id, rows_loaded=total)
    print(f"Done. Total ratio rows upserted: {total:,}")


if __name__ == "__main__":
    main()
