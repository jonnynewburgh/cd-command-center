"""
etl/train_survival_model.py — Train the charter school survival model.

Loads all charter schools from the database, builds the feature matrix,
trains a calibrated RandomForest classifier, evaluates it with
cross-validation, saves the model to models/charter_survival.pkl, and
bulk-updates the survival_score / survival_risk_tier columns in the
schools table.

Usage:
    python etl/train_survival_model.py                  # train + update DB
    python etl/train_survival_model.py --dry-run        # train only, skip DB update
    python etl/train_survival_model.py --evaluate-only  # metrics without saving

Minimum data requirement:
    At least 10 closed charter schools in the DB.
    With fewer than that the model falls back to heuristics.
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

# Allow running from project root or from etl/ directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from models.charter_survival import CharterSurvivalModel

MODEL_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "models", "charter_survival.pkl"
)

MIN_CLOSED_SCHOOLS = 10  # refuse to train if fewer closed examples than this


def load_training_data() -> pd.DataFrame:
    """Load all charter schools from the database."""
    conn = db.get_connection()
    df = pd.read_sql_query(
        """
        SELECT
            s.nces_id, s.school_name, s.state,
            s.enrollment, s.year_opened, s.data_year,
            s.pct_free_reduced_lunch, s.pct_ell, s.pct_sped,
            s.pct_black, s.pct_hispanic, s.pct_white,
            s.grade_low, s.grade_high, s.school_status,
            l.accountability_score
        FROM schools s
        LEFT JOIN lea_accountability l ON s.lea_id = l.lea_id
        WHERE s.is_charter = 1
          AND s.school_status IN ('Open', 'Closed')
        """,
        conn,
    )
    conn.close()
    return df


def print_data_summary(df: pd.DataFrame):
    print("=" * 60)
    print("TRAINING DATA SUMMARY")
    print("=" * 60)
    print(f"Total charter schools:  {len(df)}")
    print(f"  Open:                 {(df.school_status == 'Open').sum()}")
    print(f"  Closed:               {(df.school_status == 'Closed').sum()}")
    print(f"\nStates:                 {df.state.nunique()}")
    print(f"  {', '.join(sorted(df.state.unique()))}")
    print()
    print("Feature coverage (non-null %):")
    for col in ["enrollment", "year_opened", "pct_free_reduced_lunch",
                "pct_ell", "pct_sped", "pct_black", "pct_hispanic"]:
        pct = 100 * df[col].notna().mean()
        print(f"  {col:<28} {pct:5.1f}%")
    print()


def evaluate_model(model: CharterSurvivalModel, df: pd.DataFrame):
    """Run cross-validation and print detailed metrics."""
    from sklearn.metrics import classification_report, roc_auc_score
    from sklearn.model_selection import StratifiedKFold, cross_val_predict

    X = model._build_feature_matrix(df, fit=False)
    # Align columns to the trained pipeline's expectations
    all_cols = model.NUMERIC_FEATURES + model.state_cols
    for col in all_cols:
        if col not in X.columns:
            X[col] = 0.0
    X = X[[c for c in all_cols if c in X.columns]]

    y = (df["school_status"] == "Open").astype(int)

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    probs = cross_val_predict(
        model.pipeline, X, y, cv=cv, method="predict_proba"
    )[:, 1]
    preds = (probs >= 0.5).astype(int)

    print("=" * 60)
    print("CROSS-VALIDATION METRICS (5-fold stratified)")
    print("=" * 60)
    print(f"ROC-AUC:  {roc_auc_score(y, probs):.3f}")
    print()
    print(classification_report(y, preds, target_names=["Closed", "Open"]))

    # Score distribution
    open_scores   = probs[y == 1]
    closed_scores = probs[y == 0]
    print(f"Predicted survival probability:")
    print(f"  Open schools   mean={open_scores.mean():.2f}  "
          f"median={np.median(open_scores):.2f}")
    print(f"  Closed schools mean={closed_scores.mean():.2f}  "
          f"median={np.median(closed_scores):.2f}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Train charter school survival model")
    parser.add_argument("--dry-run",        action="store_true",
                        help="Train and evaluate but don't save model or update DB")
    parser.add_argument("--evaluate-only",  action="store_true",
                        help="Load existing model and print evaluation metrics only")
    parser.add_argument("--no-db-update",   action="store_true",
                        help="Save model but skip updating survival scores in DB")
    parser.add_argument("--verbose",        action="store_true", default=True)
    args = parser.parse_args()

    # Load data
    print("Loading charter school data from database...")
    df = load_training_data()
    print_data_summary(df)

    n_closed = (df.school_status == "Closed").sum()
    if n_closed < MIN_CLOSED_SCHOOLS:
        print(f"ERROR: Only {n_closed} closed charter schools found "
              f"(need at least {MIN_CLOSED_SCHOOLS}).")
        print("Load more real NCES data with: python etl/fetch_nces_schools.py")
        print("Survival scores will continue using the heuristic model.")
        sys.exit(1)

    model = CharterSurvivalModel()

    if args.evaluate_only:
        if not os.path.exists(MODEL_PATH):
            print(f"No saved model found at {MODEL_PATH}. Train first.")
            sys.exit(1)
        print(f"Loading existing model from {MODEL_PATH}...")
        model.load(MODEL_PATH)
        # Need to rebuild state_cols for feature matrix alignment
        model._build_feature_matrix(df, fit=True)  # just to set state_cols
        model.load(MODEL_PATH)  # reload to get the saved state_cols
        evaluate_model(model, df)
        return

    # Train
    print("Training model...")
    model.train(df, verbose=args.verbose)

    # Evaluate
    evaluate_model(model, df)

    if args.dry_run:
        print("Dry run — model not saved, DB not updated.")
        return

    # Save model
    model.save(MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")

    if args.no_db_update:
        print("Skipping DB update (--no-db-update).")
        return

    # Score all charter schools and update DB
    print("\nScoring all charter schools and updating database...")
    all_charters = load_training_data()  # includes Pending schools too
    # Also load Pending schools (they weren't in training but need scores)
    conn = db.get_connection()
    pending = pd.read_sql_query(
        "SELECT * FROM schools WHERE is_charter = 1 AND school_status = 'Pending'",
        conn,
    )
    conn.close()

    if not pending.empty:
        all_charters = pd.concat([all_charters, pending], ignore_index=True)

    results = model.predict_batch(all_charters)
    all_charters["survival_score"]     = results["survival_score"].values
    all_charters["survival_risk_tier"] = results["survival_risk_tier"].values

    db.bulk_update_survival_scores(all_charters[["nces_id", "survival_score", "survival_risk_tier"]])

    print(f"Updated {len(all_charters)} charter school records.")
    print()

    # Summary of score distribution
    tier_counts = results["survival_risk_tier"].value_counts()
    print("Risk tier distribution:")
    for tier in ["Low", "Medium", "High", "Unknown"]:
        n = tier_counts.get(tier, 0)
        print(f"  {tier:<10} {n:>5}")

    print("\nDone. Re-run this script after loading new school data to refresh scores.")


if __name__ == "__main__":
    main()
