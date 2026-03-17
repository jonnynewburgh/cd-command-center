"""
models/charter_survival.py — Charter school survival prediction model.

Estimates the probability that a charter school remains open over the next 3 years.

The model is a calibrated RandomForest pipeline that handles missing data via
median imputation and encodes categorical features (state, grade span).

Two operating modes:
  1. Trained mode: loads a saved sklearn pipeline from charter_survival.pkl.
     Activate by calling model.load('models/charter_survival.pkl').
  2. Heuristic mode (default): rule-based score used when no trained model
     exists (e.g. first run before etl/train_survival_model.py has been run).

To (re)train:
    python etl/train_survival_model.py

Features used:
  enrollment_log          — log(1 + enrollment); size proxy for stability
  years_open              — years since year_opened (clipped 0–40)
  pct_free_reduced_lunch  — poverty proxy
  pct_ell                 — English language learner share
  pct_sped                — special education share
  grade_span_elem         — school serves elementary grades (binary)
  grade_span_middle       — school serves middle grades (binary)
  grade_span_high         — school serves high school grades (binary)
  state_*                 — one-hot state dummies

Missing values: median imputation applied to all numeric features before
fitting. The imputer's medians are saved inside the pipeline so prediction
is consistent with training.
"""

import os
import pickle
import warnings

import numpy as np
import pandas as pd


# Grades that count as elementary / middle / high for feature engineering
_ELEM_GRADES  = {"PK", "KG", "K", "1", "2", "3", "4", "5"}
_MIDDLE_GRADES = {"6", "7", "8"}
_HIGH_GRADES   = {"9", "10", "11", "12"}


def _grade_flags(grade_low, grade_high):
    """Return (is_elem, is_middle, is_high) binary flags from grade strings."""
    gl = str(grade_low).strip().upper()  if pd.notna(grade_low)  else ""
    gh = str(grade_high).strip().upper() if pd.notna(grade_high) else ""
    return (
        int(gl in _ELEM_GRADES or gh in _ELEM_GRADES),
        int(gl in _MIDDLE_GRADES or gh in _MIDDLE_GRADES),
        int(gl in _HIGH_GRADES or gh in _HIGH_GRADES),
    )


class CharterSurvivalModel:
    """
    Wrapper around a survival (open/close) classifier for charter schools.

    Usage — trained mode:
        m = CharterSurvivalModel()
        m.load("models/charter_survival.pkl")
        results = m.predict_batch(df)  # df has schools table columns

    Usage — first run (heuristic):
        m = CharterSurvivalModel()
        results = m.predict_batch(df)  # falls back to rule-based scoring
    """

    # Numeric features the sklearn pipeline expects (in order)
    NUMERIC_FEATURES = [
        "enrollment_log",
        "years_open",
        "pct_free_reduced_lunch",
        "pct_ell",
        "pct_sped",
        "grade_span_elem",
        "grade_span_middle",
        "grade_span_high",
    ]

    def __init__(self):
        # pipeline: sklearn Pipeline with imputer + classifier; None = heuristic
        self.pipeline   = None
        # state columns seen during training (needed to align one-hot dummies)
        self.state_cols = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, path: str):
        """Load a previously trained pipeline from a pickle file."""
        with open(path, "rb") as f:
            saved = pickle.load(f)
        self.pipeline   = saved["pipeline"]
        self.state_cols = saved["state_cols"]

    def save(self, path: str):
        """Save the trained pipeline to a pickle file."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump({"pipeline": self.pipeline, "state_cols": self.state_cols}, f)

    def train(self, df: pd.DataFrame, verbose: bool = True):
        """
        Train the survival pipeline on historical charter school records.

        Args:
            df: DataFrame with schools table columns plus 'school_status'.
                Rows with school_status 'Closed' are the negative class (0).
                Rows with school_status 'Open' are the positive class (1).
            verbose: print training summary to stdout.
        """
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.impute import SimpleImputer
        from sklearn.metrics import classification_report, roc_auc_score
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        from sklearn.pipeline import Pipeline

        df = df[df["school_status"].isin(["Open", "Closed"])].copy()
        df["target"] = (df["school_status"] == "Open").astype(int)

        X = self._build_feature_matrix(df, fit=True)
        y = df["target"]

        if verbose:
            print(f"Training set: {len(df)} rows  "
                  f"({y.sum()} open / {(y==0).sum()} closed)")
            print(f"Features ({len(X.columns)}): {list(X.columns)}")

        # Build pipeline: impute → calibrated RF
        rf = RandomForestClassifier(
            n_estimators=300,
            max_depth=8,
            min_samples_leaf=5,
            random_state=42,
            class_weight="balanced",
            n_jobs=-1,
        )
        calibrated = CalibratedClassifierCV(rf, cv=5, method="isotonic")
        pipe = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("clf",     calibrated),
        ])

        # Cross-validation AUC
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cv_aucs = cross_val_score(pipe, X, y, cv=cv, scoring="roc_auc")
        if verbose:
            print(f"\nCV ROC-AUC: {cv_aucs.mean():.3f} ± {cv_aucs.std():.3f}")

        # Fit on full dataset
        pipe.fit(X, y)
        self.pipeline = pipe

        # Feature importances from the underlying RF (before calibration wrapper)
        try:
            raw_rf   = pipe.named_steps["clf"].calibrated_classifiers_[0].estimator
            importances = pd.Series(raw_rf.feature_importances_, index=X.columns)
            if verbose:
                print("\nTop feature importances:")
                print(importances.sort_values(ascending=False).head(10).to_string())
        except Exception:
            pass

        if verbose:
            print("\nModel trained successfully.")

    def predict_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Score a batch of charter schools.

        Args:
            df: DataFrame with schools table columns.

        Returns:
            DataFrame with columns:
              survival_score     — float 0–1 (probability of staying open)
              survival_risk_tier — 'Low', 'Medium', or 'High'
        """
        df = df.copy()

        if self.pipeline is not None:
            scores = self._predict_with_pipeline(df)
        else:
            scores = self._predict_heuristic(df)

        return pd.DataFrame({
            "survival_score":     np.round(scores, 3),
            "survival_risk_tier": [self._score_to_tier(s) for s in scores],
        })

    def predict_one(self, school: dict) -> dict:
        """Score a single school (dict of column values)."""
        result = self.predict_batch(pd.DataFrame([school]))
        return {
            "survival_score":     result["survival_score"].iloc[0],
            "survival_risk_tier": result["survival_risk_tier"].iloc[0],
        }

    # ------------------------------------------------------------------
    # Feature engineering
    # ------------------------------------------------------------------

    def _build_feature_matrix(self, df: pd.DataFrame, fit: bool = False) -> pd.DataFrame:
        """
        Convert raw schools-table columns into the feature matrix.

        Args:
            df:  DataFrame with raw school columns.
            fit: If True, learn state_cols from this data. If False, use
                 saved state_cols (for prediction on new data).
        """
        out = pd.DataFrame(index=df.index)

        # Numeric: log-enrollment
        enrollment = pd.to_numeric(df.get("enrollment", 0), errors="coerce").fillna(0)
        out["enrollment_log"] = np.log1p(enrollment)

        # Numeric: years open
        ref_year   = pd.to_numeric(df.get("data_year",   2024), errors="coerce").fillna(2024)
        year_open  = pd.to_numeric(df.get("year_opened", np.nan), errors="coerce")
        out["years_open"] = (ref_year - year_open).clip(0, 40)  # NaN stays NaN → imputed

        # Numeric: poverty / demographic ratios
        for col in ["pct_free_reduced_lunch", "pct_ell", "pct_sped"]:
            out[col] = pd.to_numeric(df.get(col, np.nan), errors="coerce")

        # Grade span binary flags
        grade_flags = df.apply(
            lambda r: _grade_flags(r.get("grade_low"), r.get("grade_high")), axis=1
        )
        out["grade_span_elem"]   = [f[0] for f in grade_flags]
        out["grade_span_middle"] = [f[1] for f in grade_flags]
        out["grade_span_high"]   = [f[2] for f in grade_flags]

        # Categorical: state one-hot
        state_dummies = pd.get_dummies(df.get("state", pd.Series(["UNK"] * len(df))),
                                        prefix="state", dtype=float)
        if fit:
            self.state_cols = list(state_dummies.columns)
        else:
            # Align to training columns (add missing, drop extras)
            for col in self.state_cols:
                if col not in state_dummies.columns:
                    state_dummies[col] = 0.0
            state_dummies = state_dummies[[c for c in self.state_cols if c in state_dummies.columns]]

        out = pd.concat([out, state_dummies], axis=1)
        return out

    def _predict_with_pipeline(self, df: pd.DataFrame) -> np.ndarray:
        """Use the trained pipeline to predict survival probabilities."""
        X = self._build_feature_matrix(df, fit=False)

        # Ensure column order matches training
        all_cols = self.NUMERIC_FEATURES + self.state_cols
        for col in all_cols:
            if col not in X.columns:
                X[col] = 0.0
        X = X[[c for c in all_cols if c in X.columns]]

        probs = self.pipeline.predict_proba(X)[:, 1]
        return np.round(probs, 3)

    # ------------------------------------------------------------------
    # Heuristic fallback (used when no trained model exists)
    # ------------------------------------------------------------------

    def _predict_heuristic(self, df: pd.DataFrame) -> np.ndarray:
        """
        Rule-based survival score. Used only when no trained model is loaded.

        Logic:
          - Base probability of 0.60
          - Larger enrollment → small boost (stability signal)
          - New school (< 3 years) → penalty (high early failure rate)
          - Established school (> 10 years) → small boost
          - High poverty (FRL > 80%) → small penalty
          - Good LEA accountability score → small boost
          - Already-closed schools → fixed low score
        """
        ref_year  = pd.to_numeric(df.get("data_year", 2024), errors="coerce").fillna(2024)
        year_open = pd.to_numeric(df.get("year_opened", np.nan), errors="coerce")
        years_open_series = (ref_year - year_open).clip(0, 40)

        scores = []
        for i, (_, row) in enumerate(df.iterrows()):
            if str(row.get("school_status", "")).strip() == "Closed":
                scores.append(0.15)
                continue

            score = 0.60

            enrollment = float(row.get("enrollment") or 0)
            if enrollment > 500:
                score += 0.08
            elif enrollment > 200:
                score += 0.04
            elif enrollment < 100:
                score -= 0.05

            years_open = years_open_series.iloc[i] if i < len(years_open_series) else np.nan
            if pd.notna(years_open):
                if years_open < 3:
                    score -= 0.10
                elif years_open > 10:
                    score += 0.06

            frl = float(row.get("pct_free_reduced_lunch") or 0)
            if frl > 80:
                score -= 0.05

            lea_score = float(row.get("accountability_score") or 70)
            if lea_score > 80:
                score += 0.04
            elif lea_score < 50:
                score -= 0.05

            scores.append(round(float(np.clip(score, 0.05, 0.95)), 3))

        return np.array(scores)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _score_to_tier(score: float) -> str:
        """Convert a 0–1 survival probability to a risk tier label."""
        if score is None or (isinstance(score, float) and np.isnan(score)):
            return "Unknown"
        if score >= 0.65:
            return "Low"
        elif score >= 0.40:
            return "Medium"
        else:
            return "High"
