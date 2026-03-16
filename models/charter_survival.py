"""
models/charter_survival.py — Charter school survival prediction model.

This model estimates the probability that a charter school remains open
(doesn't close) over the next 3 years.

Features used:
  - enrollment (size)
  - years in operation
  - % free/reduced price lunch (poverty proxy)
  - % ELL, % SPED
  - LEA accountability score
  - state (via dummy encoding)

The model can run in two modes:
  1. Trained mode: a sklearn RandomForestClassifier trained on historical data.
     Load with model.load('models/charter_survival.pkl').
  2. Heuristic mode (default): a rule-based score when no trained model exists.
     Useful for development before you have real training data.

To train on real data, see train() below.
"""

import os
import pickle
import numpy as np
import pandas as pd


class CharterSurvivalModel:
    """
    Wrapper around a survival (open/close) classification model for charter schools.

    Attributes:
        model: trained sklearn estimator (None if using heuristics)
        feature_cols: list of feature column names the model expects
    """

    # Features the sklearn model expects (must match training)
    FEATURE_COLS = [
        "enrollment",
        "years_open",
        "pct_free_reduced_lunch",
        "pct_ell",
        "pct_sped",
        "accountability_score",
    ]

    def __init__(self):
        self.model = None  # Will be set after loading or training

    def load(self, path: str):
        """Load a previously trained model from a pickle file."""
        with open(path, "rb") as f:
            self.model = pickle.load(f)

    def save(self, path: str):
        """Save the trained model to a pickle file."""
        with open(path, "wb") as f:
            pickle.dump(self.model, f)

    def train(self, df: pd.DataFrame):
        """
        Train the survival model on historical charter school data.

        The training data should include schools that have since closed
        (school_status='Closed') as negative examples and open schools
        as positive examples.

        Args:
            df: DataFrame with columns matching FEATURE_COLS plus 'school_status'
        """
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.preprocessing import LabelEncoder

        df = df.copy()

        # Target: 1 = still open, 0 = closed
        df["target"] = (df["school_status"] == "Open").astype(int)

        # Add derived features
        df = self._add_derived_features(df)

        # Build feature matrix (drop rows with too many NaN features)
        X = df[self.FEATURE_COLS].fillna(df[self.FEATURE_COLS].median())
        y = df["target"]

        self.model = RandomForestClassifier(
            n_estimators=200,
            max_depth=8,
            min_samples_leaf=10,
            random_state=42,
            class_weight="balanced",  # handle imbalanced open/closed
        )
        self.model.fit(X, y)

    def predict_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Score a batch of charter schools.

        Returns a DataFrame with two columns:
          - survival_score: float 0–1 (probability of staying open)
          - survival_risk_tier: 'Low', 'Medium', or 'High'
        """
        df = df.copy()
        df = self._add_derived_features(df)

        if self.model is not None:
            scores = self._predict_with_model(df)
        else:
            scores = self._predict_heuristic(df)

        result = pd.DataFrame({
            "survival_score": scores,
            "survival_risk_tier": [self._score_to_tier(s) for s in scores],
        })
        return result

    def predict_one(self, school: dict) -> dict:
        """Score a single school (passed as a dict). Returns score and tier."""
        df = pd.DataFrame([school])
        result = self.predict_batch(df)
        return {
            "survival_score": result["survival_score"].iloc[0],
            "survival_risk_tier": result["survival_risk_tier"].iloc[0],
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add computed features that the model needs."""
        # Years the school has been operating (as of data_year or current year)
        ref_year = df.get("data_year", pd.Series([2023])).fillna(2023).astype(int)
        year_opened = pd.to_numeric(df.get("year_opened", pd.Series([np.nan])), errors="coerce")
        df["years_open"] = ref_year - year_opened

        # Clamp to reasonable range (negative means bad data)
        df["years_open"] = df["years_open"].clip(0, 50)

        return df

    def _predict_with_model(self, df: pd.DataFrame) -> np.ndarray:
        """Use the trained sklearn model to score schools."""
        X = df[self.FEATURE_COLS].fillna(df[self.FEATURE_COLS].median())
        # predict_proba returns [prob_closed, prob_open]; we want prob_open
        probs = self.model.predict_proba(X)[:, 1]
        return np.round(probs, 3)

    def _predict_heuristic(self, df: pd.DataFrame) -> np.ndarray:
        """
        Rule-based survival score when no trained model is available.

        Logic:
          - Start at 0.6 (base probability)
          - Larger schools get a small boost (size = stability)
          - Newer schools (< 3 years) get a penalty (high failure rate early on)
          - Very old schools (> 10 years) get a small boost
          - High poverty (FRL > 80%) gets a small penalty
          - Good LEA accountability score gets a small boost
          - Closed schools get a low score regardless

        This is intentionally simple — it's a placeholder until real data exists.
        """
        scores = []

        for _, row in df.iterrows():
            # Schools already marked closed get a low fixed score
            if str(row.get("school_status", "")).strip() == "Closed":
                scores.append(0.15)
                continue

            score = 0.60  # base

            # Enrollment: larger = more stable
            enrollment = row.get("enrollment") or 0
            if enrollment > 500:
                score += 0.08
            elif enrollment > 200:
                score += 0.04
            elif enrollment < 100:
                score -= 0.05

            # Years open
            years_open = row.get("years_open") or 0
            if years_open < 3:
                score -= 0.10  # young schools fail more often
            elif years_open > 10:
                score += 0.06

            # Free/reduced lunch (poverty proxy)
            frl = row.get("pct_free_reduced_lunch") or 0
            if frl > 80:
                score -= 0.05

            # LEA accountability score
            lea_score = row.get("accountability_score") or 70
            if lea_score > 80:
                score += 0.04
            elif lea_score < 50:
                score -= 0.05

            # Clamp to [0.05, 0.95]
            score = round(float(np.clip(score, 0.05, 0.95)), 3)
            scores.append(score)

        return np.array(scores)

    @staticmethod
    def _score_to_tier(score: float) -> str:
        """Convert a 0–1 survival score to a risk tier label."""
        if score is None or (isinstance(score, float) and np.isnan(score)):
            return "Unknown"
        if score >= 0.65:
            return "Low"
        elif score >= 0.40:
            return "Medium"
        else:
            return "High"
