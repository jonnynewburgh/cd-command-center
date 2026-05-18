"""Pure transform tests for TN handlers.

No DB. Each test builds a small DataFrame matching a real source-file column
regime, runs transform(), asserts on the output dicts.

Covered handlers:
    - tvaas_school_composite (modern 2017-2025 + 2016 legacy + 2015 legacy)
"""
from __future__ import annotations

import math

import pandas as pd
import pytest

from etl.state_accountability.tn.handlers import (
    COLUMN_TO_METRIC_TUPLE,
    _af_cell_state,
    _coalesce_grade,
    _normalize_level,
    transform_letter_grade,
    transform_tvaas_school_composite,
    transform_tvaas_school_subject,
)


# ---------------------------------------------------------------------
# tvaas_school_composite
# ---------------------------------------------------------------------

def _modern_df():
    return pd.DataFrame([
        {
            "District Number": 10, "School Number": 2,
            "District Name": "Anderson County", "School Name": "Anderson County High School",
            "Overall Composite": 1, "Literacy Composite": 1, "Numeracy Composite": 3,
            "Literacy and Numeracy Composite": 1,
            "Science Composite": 3, "Social Studies Composite": float("nan"),
        },
        {
            "District Number": 10, "School Number": 10,
            "District Name": "Anderson County", "School Name": "Briceville Elementary",
            "Overall Composite": 2, "Literacy Composite": 3, "Numeracy Composite": 1,
            "Literacy and Numeracy Composite": 1,
            "Science Composite": 3, "Social Studies Composite": float("nan"),
        },
    ])


def test_modern_format_transforms_to_canonical_columns():
    out = transform_tvaas_school_composite(_modern_df(), year=2024)
    assert set(out.keys()) == {"tn_tvaas_school_composite"}
    rows = out["tn_tvaas_school_composite"]
    assert len(rows) == 2

    row = rows[0]
    assert row["tdoe_system_id"] == 10
    assert row["tdoe_school_id"] == 2
    assert row["year"] == 2024
    assert row["district_name"] == "Anderson County"
    assert row["school_name"] == "Anderson County High School"
    assert row["overall_composite"] == 1
    assert row["literacy_composite"] == 1
    assert row["numeracy_composite"] == 3
    assert row["literacy_numeracy_composite"] == 1
    assert row["science_composite"] == 3
    assert row["social_studies_composite"] is None  # NaN → None

    # Confirm no source_load_id is present — runner injects it.
    assert "source_load_id" not in row


def test_2016_legacy_format_transforms_to_canonical_columns():
    df = pd.DataFrame([{
        "District Number": 10, "School Number": 2,
        "District Name": "Anderson County", "School Name": "Anderson County HS",
        "School-Wide: Composite": 4, "School-Wide: Literacy": 4,
        "School-Wide: Numeracy": 5, "School-Wide: Literacy and Numeracy": 4,
        "School-Wide: Science": 5, "School-Wide: Social Studies": 5,
    }])
    out = transform_tvaas_school_composite(df, year=2016)
    rows = out["tn_tvaas_school_composite"]
    assert len(rows) == 1
    assert rows[0]["overall_composite"] == 4
    assert rows[0]["literacy_composite"] == 4
    assert rows[0]["numeracy_composite"] == 5
    assert rows[0]["literacy_numeracy_composite"] == 4
    assert rows[0]["science_composite"] == 5
    assert rows[0]["social_studies_composite"] == 5
    assert rows[0]["year"] == 2016


def test_2015_legacy_format_missing_social_studies_becomes_null():
    df = pd.DataFrame([{
        "District Number": 10, "School Number": 2,
        "District Name": "Anderson County", "School Name": "Anderson County HS",
        "School-Wide: Composite": 3, "School-Wide: Literacy": 1,
        "School-Wide: Numeracy": 5, "School-Wide: Literacy and Numeracy": 3,
        "School-Wide: Science": 3,
        # No "School-Wide: Social Studies" column at all in 2015
    }])
    out = transform_tvaas_school_composite(df, year=2015)
    rows = out["tn_tvaas_school_composite"]
    assert len(rows) == 1
    assert rows[0]["social_studies_composite"] is None
    assert rows[0]["science_composite"] == 3


def test_unrecognized_column_shape_raises():
    df = pd.DataFrame([{"foo": 1, "bar": 2}])
    with pytest.raises(ValueError, match="Unrecognized TVAAS school composite column shape"):
        transform_tvaas_school_composite(df, year=2024)


def test_composite_value_out_of_range_raises():
    df = _modern_df()
    df.loc[0, "Overall Composite"] = 7  # invalid: levels are 1-5
    with pytest.raises(ValueError, match="composite value out of 1-5 range"):
        transform_tvaas_school_composite(df, year=2024)


def test_runner_injects_source_load_id_not_handler():
    """Smoke test that the handler does NOT emit source_load_id — the runner
    rejects rows with that key. This is enforced in runner._insert_rows."""
    out = transform_tvaas_school_composite(_modern_df(), year=2024)
    for row in out["tn_tvaas_school_composite"]:
        assert "source_load_id" not in row


# ---------------------------------------------------------------------
# tvaas_school_subject — helpers
# ---------------------------------------------------------------------

def test_coalesce_grade_act_null_to_composite():
    assert _coalesce_grade("ACT", float("nan")) == "Composite"
    assert _coalesce_grade("ACT", None) == "Composite"
    assert _coalesce_grade("ACT", "") == "Composite"


def test_coalesce_grade_eoc_null_to_course():
    assert _coalesce_grade("EOC", float("nan")) == "Course"


def test_coalesce_grade_grades_3_8_passthrough():
    assert _coalesce_grade("Grades 3-8", 3) == "3"
    assert _coalesce_grade("Grades 3-8", 8.0) == "8"
    assert _coalesce_grade("Grades 3-8", "Cumulative Grades") == "Cumulative Grades"


def test_coalesce_grade_null_for_grades_3_8_raises():
    """A null grade for the per-grade test type is a real data integrity
    issue — fail loudly at transform, not silently coalesce."""
    with pytest.raises(ValueError, match="NULL grade for test='Grades 3-8'"):
        _coalesce_grade("Grades 3-8", float("nan"))


def test_normalize_level_text_to_int():
    assert _normalize_level("Level 1") == 1
    assert _normalize_level("Level 5") == 5
    assert _normalize_level("Level 3") == 3


def test_normalize_level_handles_null_and_blank():
    assert _normalize_level(float("nan")) is None
    assert _normalize_level("") is None


def test_normalize_level_out_of_range_raises():
    with pytest.raises(ValueError, match="level out of 1-5 range"):
        _normalize_level("Level 7")


# ---------------------------------------------------------------------
# tvaas_school_subject — full transform
# ---------------------------------------------------------------------

def test_subject_transform_grades_3_8_row():
    df = pd.DataFrame([{
        "District Number": 10, "School Number": 10,
        "District": "Anderson County", "School": "Briceville Elementary",
        "Test": "Grades 3-8", "Subject": "English Language Arts",
        "Grade": 4, "Year": 2024,
        "Growth Measure": -4.9, "Standard Error": 3.7, "Index": -1.31,
        "Level": "Level 2", "Number of Students": 11,
    }])
    rows = transform_tvaas_school_subject(df, year=2024)["tn_tvaas_school_subject"]
    assert len(rows) == 1
    r = rows[0]
    assert r["tdoe_system_id"] == 10
    assert r["tdoe_school_id"] == 10
    assert r["year"] == 2024
    assert r["test"] == "Grades 3-8"
    assert r["subject"] == "English Language Arts"
    assert r["grade"] == "4"
    assert r["growth_measure"] == -4.9
    assert r["standard_error"] == 3.7
    assert r["tvaas_index"] == -1.31
    assert r["level"] == 2
    assert r["n_students"] == 11


def test_subject_transform_act_null_grade_coalesced():
    df = pd.DataFrame([{
        "District Number": 10, "School Number": 2,
        "District": "Anderson County", "School": "Anderson Co HS",
        "Test": "ACT", "Subject": "ACT Composite",
        "Grade": float("nan"), "Year": 2024,
        "Growth Measure": 0.5, "Standard Error": 1.2, "Index": 0.42,
        "Level": "Level 3", "Number of Students": 200,
    }])
    rows = transform_tvaas_school_subject(df, year=2024)["tn_tvaas_school_subject"]
    assert rows[0]["grade"] == "Composite"
    assert rows[0]["test"] == "ACT"


def test_subject_transform_eoc_null_grade_coalesced():
    df = pd.DataFrame([{
        "District Number": 10, "School Number": 2,
        "District": "Anderson County", "School": "Anderson Co HS",
        "Test": "EOC", "Subject": "Algebra I",
        "Grade": float("nan"), "Year": 2024,
        "Growth Measure": 1.1, "Standard Error": 2.0, "Index": 0.55,
        "Level": "Level 4", "Number of Students": 85,
    }])
    rows = transform_tvaas_school_subject(df, year=2024)["tn_tvaas_school_subject"]
    assert rows[0]["grade"] == "Course"
    assert rows[0]["test"] == "EOC"


def test_subject_transform_missing_required_columns_raises():
    df = pd.DataFrame([{"foo": 1}])
    with pytest.raises(ValueError, match="missing required columns"):
        transform_tvaas_school_subject(df, year=2024)


# ---------------------------------------------------------------------
# A-F letter grade — COLUMN_TO_METRIC_TUPLE map
# ---------------------------------------------------------------------

def test_column_to_metric_tuple_has_50_entries():
    """1 overall + 9 subgroup + 11 grade-band×subject + 4 growth-subject
    + 11 growth-subgroup + 1 ccr + 4 ccr-component + 9 ccr-subgroup = 50."""
    assert len(COLUMN_TO_METRIC_TUPLE) == 50


def test_column_to_metric_tuple_dimensions_pass_dim_consistency():
    """Every emitted tuple must satisfy the schema's lgm_component_dim_consistency
    CHECK: achievement→ccr=all; growth→ccr=all AND grade_band=all; ccr→subject=all
    AND grade_band=all."""
    for col, (component, sg, subj, gb, ccr_c, unit) in COLUMN_TO_METRIC_TUPLE.items():
        if component == "achievement":
            assert ccr_c == "all", f"{col}: achievement must have ccr_component=all"
        elif component == "growth":
            assert ccr_c == "all" and gb == "all", f"{col}: growth must have ccr_component=all AND grade_band=all"
        elif component == "ccr":
            assert subj == "all" and gb == "all", f"{col}: ccr must have subject=all AND grade_band=all"
        else:
            pytest.fail(f"{col}: unexpected component {component}")


def test_column_to_metric_tuple_unit_matches_component():
    """Every emitted tuple must satisfy lgm_unit_matches_component."""
    for col, (component, _, _, _, _, unit) in COLUMN_TO_METRIC_TUPLE.items():
        if component in ("achievement", "ccr"):
            assert unit == "pct", f"{col}: {component} must use unit=pct"
        elif component == "growth":
            assert unit == "score_1_5", f"{col}: growth must use unit=score_1_5"


def test_column_to_metric_tuple_specific_known_mappings():
    """Spot-check the mappings we hand-verified against the source file."""
    expected = {
        "overall_success_rate_all_students":  ("achievement", "all", "all", "all", "all", "pct"),
        "overall_success_rate_ed":            ("achievement", "ed",  "all", "all", "all", "pct"),
        "success_rate_g3-5_math":             ("achievement", "all", "math", "g3_5", "all", "pct"),
        "growth_numeracy_score":              ("growth", "all", "numeracy", "all", "all", "score_1_5"),
        "growth_ela_math_score_ed":           ("growth", "ed",  "ela_math", "all", "all", "score_1_5"),
        "growth_ela_math_score_super_subgroup": ("growth", "super_subgroup", "ela_math", "all", "all", "score_1_5"),
        "ccr_act_rate":                       ("ccr", "all", "all", "all", "act", "pct"),
        "ccr_rate":                           ("ccr", "all", "all", "all", "all", "pct"),
        "ccr_rate_ed":                        ("ccr", "ed",  "all", "all", "all", "pct"),
    }
    for col, tup in expected.items():
        assert COLUMN_TO_METRIC_TUPLE[col] == tup


# ---------------------------------------------------------------------
# A-F cell-state classifier
# ---------------------------------------------------------------------

def test_af_cell_state_numeric():
    assert _af_cell_state(3.5) == ("numeric", 3.5)
    assert _af_cell_state(0) == ("numeric", 0.0)
    assert _af_cell_state("85") == ("numeric", 85.0)  # Excel-string-as-numeric


def test_af_cell_state_sentinels():
    assert _af_cell_state("Insufficient N Count") == ("suppressed", None)
    assert _af_cell_state("Not a High School") == ("not_applicable", None)
    assert _af_cell_state("Not Eligible for a Letter Grade") == ("ineligible", None)


def test_af_cell_state_privacy_buckets():
    """TDOE caps rates at <5% / >95% for privacy. Collapse both to suppressed."""
    assert _af_cell_state("<5%") == ("suppressed", None)
    assert _af_cell_state(">95%") == ("suppressed", None)


def test_af_cell_state_blank_and_nan():
    assert _af_cell_state(float("nan")) == ("not_applicable", None)
    assert _af_cell_state("") == ("not_applicable", None)


def test_af_cell_state_unknown_text_raises():
    with pytest.raises(ValueError, match="unrecognized A-F cell value"):
        _af_cell_state("Some New Sentinel TDOE Just Invented")


# ---------------------------------------------------------------------
# A-F transform
# ---------------------------------------------------------------------

def _minimal_af_df():
    """Build a 2-row DataFrame with all expected columns. One eligible HS, one
    eligible K8. Adequate for shape + sentinel-handling tests."""
    base_row = {
        "year": 2025,
        "system": 10, "system_name": "Anderson County",
        "school": 2, "school_name": "Anderson County HS",
        "school_pool": "HS",
        "lg_ineligible": 0,
        "grade_band_3-5": "N", "grade_band_6-8": "N", "grade_band_9-12": "Y",
        "ach_score": 3.0, "growth_score": 4.0, "growth25_score": 2.0, "ccr_score": 4.0,
        "ach_grade": "C", "growth_grade": "B", "growth25_grade": "D", "ccr_grade": "B",
        "ach_score_weighted": 1.5, "growth_score_weighted": 1.2,
        "growth25_score_weighted": 0.2, "ccr_score_weighted": 0.4,
        "ach_weight": 0.5, "growth_weight": 0.3, "growth25_weight": 0.1, "ccr_weight": 0.1,
        "lg_score": 3.3, "lg_grade": "C",
    }
    # Add all 50 metric columns with sample values.
    metric_values = {col: 50.0 if "growth" not in col else 3.0 for col in COLUMN_TO_METRIC_TUPLE}
    base_row.update(metric_values)
    hs_row = dict(base_row)
    k8_row = dict(base_row)
    k8_row.update({
        "school": 10, "school_name": "Briceville Elementary", "school_pool": "K8",
        "ccr_score": "Not a High School", "ccr_grade": "Not a High School",
        "ccr_score_weighted": "Not a High School", "ccr_weight": "Not a High School",
        "grade_band_9-12": "N", "grade_band_3-5": "Y",
    })
    # K8 schools' CCR breakdown columns are also Not a High School
    for col, (comp, *_) in COLUMN_TO_METRIC_TUPLE.items():
        if comp == "ccr":
            k8_row[col] = "Not a High School"
    return pd.DataFrame([hs_row, k8_row])


def test_af_transform_produces_two_tables():
    df = _minimal_af_df()
    out = transform_letter_grade(df, year=2025)
    assert set(out.keys()) == {"tn_letter_grade", "tn_letter_grade_metric"}


def test_af_transform_wide_row_has_correct_shape():
    df = _minimal_af_df()
    wide = transform_letter_grade(df, year=2025)["tn_letter_grade"]
    assert len(wide) == 2  # one per school
    hs = wide[0]
    assert hs["tdoe_system_id"] == 10
    assert hs["tdoe_school_id"] == 2
    assert hs["year"] == 2025
    assert hs["school_pool"] == "HS"
    assert hs["lg_ineligible"] is False
    assert hs["lg_grade"] == "C"
    assert hs["ach_weight"] == 0.5
    assert hs["ccr_weight"] == 0.1


def test_af_transform_k8_ccr_weight_coerced_to_zero():
    """K8 schools: source says 'Not a High School' for ccr_weight, but schema
    CHECK requires it NOT NULL and weights to sum to 1.0. Coerce to 0.0."""
    df = _minimal_af_df()
    wide = transform_letter_grade(df, year=2025)["tn_letter_grade"]
    k8 = wide[1]
    assert k8["school_pool"] == "K8"
    assert k8["ccr_weight"] == 0.0
    assert k8["ccr_score"] is None     # score stays NULL (no CCR for K8)
    assert k8["ccr_grade"] is None


def test_af_transform_partial_weight_pattern_coerced():
    """K8 row with (ach=0.5, growth=0.5, growth25=NaN, ccr=NaN) should coerce
    the two NaN values to 0.0, preserving the sum=1.0 constraint."""
    df = _minimal_af_df()
    df["growth25_weight"] = df["growth25_weight"].astype(object)
    df.loc[1, "growth25_weight"] = "Insufficient N Count"
    df.loc[1, "growth_weight"] = 0.5
    df.loc[1, "ach_weight"] = 0.5
    wide = transform_letter_grade(df, year=2025)["tn_letter_grade"]
    k8 = wide[1]
    assert k8["ach_weight"] == 0.5
    assert k8["growth_weight"] == 0.5
    assert k8["growth25_weight"] == 0.0
    assert k8["ccr_weight"] == 0.0


def test_af_transform_eligibility_anomaly_keeps_weights_null():
    """Eligible row with all 4 weights AND all 4 scores NULL is the
    eligibility-anomaly state (6 lifetime rows across years). Transform
    leaves all four weights NULL; schema CHECK admits via the all-NULL
    branch; validator emits WARNING."""
    df = _minimal_af_df()
    cols = ("ach_weight", "growth_weight", "growth25_weight", "ccr_weight",
            "ach_score", "growth_score", "growth25_score", "ccr_score")
    for col in cols:
        df[col] = df[col].astype(object)
        df.loc[1, col] = "Insufficient N Count"
    wide = transform_letter_grade(df, year=2025)["tn_letter_grade"]
    k8 = wide[1]
    assert k8["lg_ineligible"] is False
    assert all(k8[c] is None for c in ("ach_weight", "growth_weight", "growth25_weight", "ccr_weight"))
    assert all(k8[c] is None for c in ("ach_score", "growth_score", "growth25_score", "ccr_score"))


def test_af_transform_k8_ccr_metric_rows_skipped():
    """K8 schools should not produce ccr-component metric rows (those source
    cells are 'Not a High School' → skip)."""
    df = _minimal_af_df()
    metric = transform_letter_grade(df, year=2025)["tn_letter_grade_metric"]
    k8_ccr = [m for m in metric if m["tdoe_school_id"] == 10 and m["component"] == "ccr"]
    assert k8_ccr == []


def test_af_transform_2022_format_missing_grade_cols():
    """2022-23 didn't publish the four *_grade columns. parse_letter_grade
    pads them as NaN. The transform should accept that and emit NULL."""
    from etl.state_accountability.tn.handlers import parse_letter_grade
    df = _minimal_af_df()
    # Simulate the 2022-23 shape by dropping the 4 grade columns first.
    df = df.drop(columns=["ach_grade", "growth_grade", "growth25_grade", "ccr_grade"])
    # parse_letter_grade pads them back; emulate that step.
    for col in ("ach_grade", "growth_grade", "growth25_grade", "ccr_grade"):
        df[col] = pd.NA
    wide = transform_letter_grade(df, year=2023)["tn_letter_grade"]
    assert wide[0]["ach_grade"] is None
    assert wide[0]["growth_grade"] is None


def test_af_transform_keys_uniform_across_rows():
    """Both wide and metric row sets must have uniform key sets per
    runner._insert_rows."""
    df = _minimal_af_df()
    out = transform_letter_grade(df, year=2025)
    for table, rows in out.items():
        if not rows:
            continue
        expected = set(rows[0].keys())
        for r in rows[1:]:
            assert set(r.keys()) == expected, f"{table}: row key mismatch"


def test_subject_transform_keys_uniform_across_rows():
    """Every row must have the same key set — runner._insert_rows enforces this
    and will raise ValueError if violated. Verify the transform produces uniform
    keys across heterogeneous source rows (mixed Test types)."""
    df = pd.DataFrame([
        {"District Number": 10, "School Number": 10, "Test": "Grades 3-8", "Subject": "ELA", "Grade": 3,
         "Year": 2024, "Growth Measure": 1.0, "Standard Error": 1.0, "Index": 1.0, "Level": "Level 3", "Number of Students": 50},
        {"District Number": 10, "School Number": 2,  "Test": "ACT",         "Subject": "Composite", "Grade": float("nan"),
         "Year": 2024, "Growth Measure": 0.5, "Standard Error": 1.2, "Index": 0.42, "Level": "Level 3", "Number of Students": 200},
        {"District Number": 10, "School Number": 2,  "Test": "EOC",         "Subject": "Algebra I", "Grade": float("nan"),
         "Year": 2024, "Growth Measure": 1.1, "Standard Error": 2.0, "Index": 0.55, "Level": "Level 4", "Number of Students": 85},
    ])
    rows = transform_tvaas_school_subject(df, year=2024)["tn_tvaas_school_subject"]
    expected_keys = set(rows[0].keys())
    for r in rows:
        assert set(r.keys()) == expected_keys
