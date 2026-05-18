"""TN-specific validator registry.

Validators run inside the load transaction, AFTER fact rows are INSERTed but
BEFORE COMMIT. They catch semantic issues that schema-level constraints can't:
- Cross-row consistency (e.g. all subgroups for one school must agree)
- Convention enforcement (e.g. ACT rows must have grade='Composite' per our
  coalesce contract — schema only says grade NOT NULL, doesn't say which
  string is the right one)
- Historical-artifact regression catchers (e.g. eligible-but-no-scores)

Each validator returns a list[ValidationFailure]. The runner concatenates,
and any non-empty list raises ValidationError → ROLLBACK.
"""
from __future__ import annotations

from etl.state_accountability.types import ValidationFailure
from etl.state_accountability.validators import Validator


# ---------------------------------------------------------------------
# tvaas_school_subject validators
# ---------------------------------------------------------------------

def validate_grade_test_consistency(cur, load_id: int) -> list[ValidationFailure]:
    """Every (test, grade) pair must match the coalesce convention:
      test=ACT        → grade='Composite'
      test=EOC        → grade='Course'
      test=Grades 3-8 → grade in {'3','4','5','6','7','8','Cumulative Grades'}

    Schema enforces grade NOT NULL via the PK, and test via CHECK. But neither
    enforces the per-test grade vocabulary. This validator does. If a future
    coalesce-logic bug emits the wrong sentinel, this fails the load before
    bad data lands in the table.
    """
    cur.execute(
        """
        SELECT tdoe_system_id, tdoe_school_id, year, test, subject, grade
        FROM tn_tvaas_school_subject
        WHERE source_load_id = %s
          AND (
                (test = 'ACT'        AND grade <> 'Composite')
             OR (test = 'EOC'        AND grade <> 'Course')
             OR (test = 'Grades 3-8' AND grade NOT IN ('3','4','5','6','7','8','Cumulative Grades'))
          )
        """,
        (load_id,),
    )
    return [
        ValidationFailure(
            rule="grade_test_consistency",
            message=(
                f"row {r[0]}/{r[1]} year={r[2]} test={r[3]!r} subject={r[4]!r} "
                f"grade={r[5]!r}: grade value violates the per-test sentinel convention"
            ),
            context={"system": r[0], "school": r[1], "year": r[2], "test": r[3], "subject": r[4], "grade": r[5]},
        )
        for r in cur.fetchall()
    ]


# ---------------------------------------------------------------------
# letter_grade validators
# ---------------------------------------------------------------------

def validate_eligibility_score_consistency(cur, load_id: int) -> list[ValidationFailure]:
    """WARNING severity. Schools marked lg_ineligible=false should have at least
    one component score populated. Historical artifact: 3 rows in 2022-23, 3 in
    2023-24, 0 in 2024-25 — TDOE has been fixing these. This validator catches
    any regression but doesn't block the load.

    Three plausible explanations for the warning when it fires:
      1. TDOE re-introduced a data-publishing bug (most likely)
      2. Eligibility rules changed (worth checking TDOE accountability docs)
      3. The 2024-25-style file is incomplete (verify row count vs prior years)
    """
    cur.execute(
        """
        SELECT tdoe_system_id, tdoe_school_id, year, school_name
        FROM tn_letter_grade
        WHERE source_load_id = %s
          AND lg_ineligible = false
          AND ach_score      IS NULL
          AND growth_score   IS NULL
          AND growth25_score IS NULL
          AND ccr_score      IS NULL
        """,
        (load_id,),
    )
    return [
        ValidationFailure(
            rule="eligibility_score_consistency",
            severity="warning",
            message=(
                f"school {r[0]}/{r[1]} year={r[2]} ({r[3]!r}): "
                f"lg_ineligible=false but all four component scores are NULL"
            ),
            context={"system": r[0], "school": r[1], "year": r[2]},
        )
        for r in cur.fetchall()
    ]


def validate_no_negative_weights(cur, load_id: int) -> list[ValidationFailure]:
    """ERROR severity. Schema CHECK requires the four weights to sum to 1.0
    within tolerance, but allows individual weights to be negative as long as
    they sum to 1. Negative weights are nonsensical for an accountability
    weighting scheme. This catches them explicitly with a friendlier message
    than the CHECK would give.
    """
    cur.execute(
        """
        SELECT tdoe_system_id, tdoe_school_id, year, school_name,
               ach_weight, growth_weight, growth25_weight, ccr_weight
        FROM tn_letter_grade
        WHERE source_load_id = %s
          AND lg_ineligible = false
          AND (ach_weight < 0 OR growth_weight < 0
               OR growth25_weight < 0 OR ccr_weight < 0)
        """,
        (load_id,),
    )
    return [
        ValidationFailure(
            rule="no_negative_weights",
            severity="error",
            message=(
                f"school {r[0]}/{r[1]} year={r[2]} ({r[3]!r}): "
                f"negative weight detected (ach={r[4]}, growth={r[5]}, "
                f"growth25={r[6]}, ccr={r[7]})"
            ),
            context={"system": r[0], "school": r[1], "year": r[2]},
        )
        for r in cur.fetchall()
    ]


def validate_metric_value_range(cur, load_id: int) -> list[ValidationFailure]:
    """ERROR severity. Schema CHECK enforces pct in [0,100] and score_1_5 in
    [1.0,5.0]. This duplicates the check pre-COMMIT for a friendlier error,
    but mainly catches edge cases (e.g. metric_value = NaN, which the CHECK
    treats as NULL but is suspicious if suppressed=false).
    """
    cur.execute(
        """
        SELECT tdoe_system_id, tdoe_school_id, year, component, subgroup,
               subject, grade_band, ccr_component, unit, metric_value
        FROM tn_letter_grade_metric
        WHERE source_load_id = %s
          AND suppressed = false
          AND (
                (unit = 'pct'       AND (metric_value < 0   OR metric_value > 100))
             OR (unit = 'score_1_5' AND (metric_value < 1.0 OR metric_value > 5.0))
          )
        """,
        (load_id,),
    )
    return [
        ValidationFailure(
            rule="metric_value_range",
            severity="error",
            message=(
                f"school {r[0]}/{r[1]} year={r[2]} "
                f"({r[3]}/{r[4]}/{r[5]}/{r[6]}/{r[7]}): "
                f"metric_value={r[9]} out of range for unit={r[8]!r}"
            ),
        )
        for r in cur.fetchall()
    ]


VALIDATORS_BY_KIND: dict[str, list[Validator]] = {
    "tvaas_school_composite":   [],
    "tvaas_school_subject":     [validate_grade_test_consistency],
    "tvaas_district_composite": [],
    "tvaas_district_subject":   [],
    "letter_grade": [
        validate_eligibility_score_consistency,  # warning
        validate_no_negative_weights,            # error
        validate_metric_value_range,             # error
    ],
}
