"""TN handler registry.

One entry per FileType in etl.tn_filename_parser. SkipHandler entries explicitly
record that we recognize a file type but choose not to load it (e.g. district
TVAAS data — we don't model districts as fact tables).

Each FileHandler is the pair (parse, transform). parse reads xlsx/csv into a
DataFrame with no transformation; transform converts the DataFrame into
{table_name: list[dict]} ready for INSERT. The runner injects source_load_id
on each row and handles the INSERT itself.

Transform functions are pure (no DB, no I/O). Unit-tested in
tests/test_tn_handlers.py without a database.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from etl.state_accountability.types import FileHandler, SkipHandler

# ---------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------

def _maybe_int(v) -> int | None:
    if pd.isna(v):
        return None
    return int(v)


def _maybe_float(v) -> float | None:
    if pd.isna(v):
        return None
    return float(v)


def _maybe_smallint_1_5(v) -> int | None:
    """Coerce a composite value to a 1-5 smallint. Returns None for NaN/blank."""
    if pd.isna(v) or v == "":
        return None
    iv = int(v)
    if iv < 1 or iv > 5:
        raise ValueError(f"composite value out of 1-5 range: {v!r}")
    return iv


def _normalize_level(v) -> int | None:
    """Normalize TDOE Level text ('Level 1'..'Level 5') to smallint 1..5.

    Source files write the level as text ("Level 3"). Schema stores it as
    smallint with CHECK (level BETWEEN 1 AND 5). This helper converts.
    """
    if pd.isna(v) or v == "":
        return None
    if isinstance(v, (int, float)):
        iv = int(v)
    else:
        s = str(v).strip()
        if s.startswith("Level "):
            iv = int(s[len("Level "):])
        else:
            iv = int(s)
    if iv < 1 or iv > 5:
        raise ValueError(f"level out of 1-5 range: {v!r}")
    return iv


def _coalesce_grade(test: str, grade) -> str:
    """Per the schema convention: ACT rows store 'Composite' as grade (ACT
    reports a single English/Math/Reading/Science composite, not per-grade);
    EOC rows store 'Course' (EOCs are tied to courses like Algebra I, not
    grades). Source files have NULL Grade for these rows — we coalesce at
    load time to keep the PK NOT NULL contract."""
    if pd.isna(grade) or (isinstance(grade, str) and grade.strip() == ""):
        if test == "ACT":
            return "Composite"
        if test == "EOC":
            return "Course"
        raise ValueError(f"NULL grade for test={test!r} — coalesce only defined for ACT/EOC")
    # Grades 3-8: source may give an int or 'Cumulative Grades' text.
    if isinstance(grade, (int, float)) and not pd.isna(grade):
        return str(int(grade))
    return str(grade).strip()


# ---------------------------------------------------------------------
# tvaas_school_composite
# ---------------------------------------------------------------------
# Three column-name regimes across years:
#   - Modern (2017-2025): "Overall Composite", "Literacy Composite", "Numeracy Composite",
#       "Literacy and Numeracy Composite", "Science Composite", "Social Studies Composite"
#   - 2016 legacy: "School-Wide: Composite", "School-Wide: Literacy", "School-Wide: Numeracy",
#       "School-Wide: Literacy and Numeracy", "School-Wide: Science", "School-Wide: Social Studies"
#   - 2015 legacy: same as 2016 minus "School-Wide: Social Studies" (introduced in 2016)
#
# All three normalize to the same target columns. _normalize_composite_columns
# returns a DataFrame with canonical names; missing columns become NaN.

_COMPOSITE_COLUMN_MAP_MODERN = {
    "District Number":                 "tdoe_system_id",
    "School Number":                   "tdoe_school_id",
    "District Name":                   "district_name",
    "School Name":                     "school_name",
    "Overall Composite":               "overall_composite",
    "Literacy Composite":              "literacy_composite",
    "Numeracy Composite":              "numeracy_composite",
    "Literacy and Numeracy Composite": "literacy_numeracy_composite",
    "Science Composite":               "science_composite",
    "Social Studies Composite":        "social_studies_composite",
}

_COMPOSITE_COLUMN_MAP_LEGACY = {
    "District Number":                    "tdoe_system_id",
    "School Number":                      "tdoe_school_id",
    "District Name":                      "district_name",
    "School Name":                        "school_name",
    "School-Wide: Composite":             "overall_composite",
    "School-Wide: Literacy":              "literacy_composite",
    "School-Wide: Numeracy":              "numeracy_composite",
    "School-Wide: Literacy and Numeracy": "literacy_numeracy_composite",
    "School-Wide: Science":               "science_composite",
    "School-Wide: Social Studies":        "social_studies_composite",
}

_CANONICAL_COMPOSITE_COLUMNS = [
    "tdoe_system_id", "tdoe_school_id", "year",
    "district_name", "school_name",
    "overall_composite", "literacy_composite", "numeracy_composite",
    "literacy_numeracy_composite", "science_composite", "social_studies_composite",
]


def _normalize_composite_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map any of the three column-name regimes to canonical names. Add missing
    columns as NaN (e.g. social_studies for 2015)."""
    if "Overall Composite" in df.columns:
        mapping = _COMPOSITE_COLUMN_MAP_MODERN
    elif "School-Wide: Composite" in df.columns:
        mapping = _COMPOSITE_COLUMN_MAP_LEGACY
    else:
        raise ValueError(
            f"Unrecognized TVAAS school composite column shape. Columns: {list(df.columns)}"
        )
    renamed = df.rename(columns=mapping)
    # Add any missing canonical columns as NaN so downstream code can assume presence.
    for col in _CANONICAL_COMPOSITE_COLUMNS:
        if col != "year" and col not in renamed.columns:
            renamed[col] = pd.NA
    return renamed


def parse_tvaas_school_composite(filepath: Path) -> pd.DataFrame:
    if filepath.suffix.lower() == ".csv":
        return pd.read_csv(filepath)
    return pd.read_excel(filepath)


def transform_tvaas_school_composite(df: pd.DataFrame, year: int) -> dict[str, list[dict]]:
    df = _normalize_composite_columns(df)
    rows: list[dict] = []
    for _, r in df.iterrows():
        rows.append({
            "tdoe_system_id":              int(r["tdoe_system_id"]),
            "tdoe_school_id":              int(r["tdoe_school_id"]),
            "year":                        year,
            "district_name":               None if pd.isna(r["district_name"]) else str(r["district_name"]),
            "school_name":                 None if pd.isna(r["school_name"])   else str(r["school_name"]),
            "overall_composite":           _maybe_smallint_1_5(r["overall_composite"]),
            "literacy_composite":          _maybe_smallint_1_5(r["literacy_composite"]),
            "numeracy_composite":          _maybe_smallint_1_5(r["numeracy_composite"]),
            "literacy_numeracy_composite": _maybe_smallint_1_5(r["literacy_numeracy_composite"]),
            "science_composite":           _maybe_smallint_1_5(r["science_composite"]),
            "social_studies_composite":    _maybe_smallint_1_5(r["social_studies_composite"]),
        })
    return {"tn_tvaas_school_composite": rows}


# ---------------------------------------------------------------------
# tvaas_school_subject
# ---------------------------------------------------------------------
# Source columns (modern format, observed 2022-2025):
#   District, District Number, School, School Number, Test, Subject, Grade,
#   Year, Growth Measure, Standard Error, Index, Level, Number of Students
#
# Grade is NULL in source for ACT and EOC rows — coalesce at load time.
# Level is text ("Level 3") — normalize to smallint.
# 2017/2018/2019 legacy files use the same column shape.

def parse_tvaas_school_subject(filepath: Path) -> pd.DataFrame:
    if filepath.suffix.lower() == ".csv":
        return pd.read_csv(filepath)
    return pd.read_excel(filepath)


def transform_tvaas_school_subject(df: pd.DataFrame, year: int) -> dict[str, list[dict]]:
    required = ["District Number", "School Number", "Test", "Subject"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"TVAAS subject file missing required columns: {missing}. Got: {list(df.columns)}")

    rows: list[dict] = []
    for _, r in df.iterrows():
        test = str(r["Test"]).strip()
        grade = _coalesce_grade(test, r.get("Grade"))
        rows.append({
            "tdoe_system_id": int(r["District Number"]),
            "tdoe_school_id": int(r["School Number"]),
            "year":           year,
            "test":           test,
            "subject":        str(r["Subject"]).strip(),
            "grade":          grade,
            "growth_measure": _maybe_float(r.get("Growth Measure")),
            "standard_error": _maybe_float(r.get("Standard Error")),
            "tvaas_index":    _maybe_float(r.get("Index")),
            "level":          _normalize_level(r.get("Level")),
            "n_students":     _maybe_int(r.get("Number of Students")),
        })
    return {"tn_tvaas_school_subject": rows}


# ---------------------------------------------------------------------
# letter_grade (A-F)
# ---------------------------------------------------------------------
# Source file shape (verified across 2022-23, 2023-24, 2024-25):
#   - 74 cols in 2022-23, 78 cols in 2023-24/2024-25
#   - Only delta is 4 *_grade columns added in 2023-24
#   - No renames, no removed columns
#
# Handler produces rows for two tables:
#   tn_letter_grade        — one row per school (wide, headline metrics)
#   tn_letter_grade_metric — one row per (school, breakdown column), long form
#
# Sentinel handling in source cells:
#   'Insufficient N Count'            → metric: suppressed=true, value=NULL
#                                        wide: NULL
#   'Not a High School'                → metric: skip row (measure N/A)
#                                        wide: NULL (or 0.0 for ccr_weight on K8 schools)
#   'Not Eligible for a Letter Grade' → only in lg_score/lg_grade; convert to NULL
#                                        (lg_ineligible boolean signals the state)
#   NaN / blank                        → metric: skip row; wide: NULL
#   any other text                     → raise (force loud failure if TDOE adds a new sentinel)

_SUBGROUPS_OVERALL = ["ed", "el", "swd", "aian", "asian", "black", "hispanic", "nhpi", "white"]
_SUBGROUPS_GROWTH = _SUBGROUPS_OVERALL + ["bhn", "super_subgroup"]
_CCR_SUBCOMPONENTS = ["act", "postsec", "ic", "asvab"]


def _build_column_to_metric_tuple() -> dict[str, tuple[str, str, str, str, str, str]]:
    """Map source column name → (component, subgroup, subject, grade_band, ccr_component, unit).

    Built via loops over explicit subgroup/subject lists rather than hand-typed
    so the structure is verifiable by inspection. Exactly 50 entries:
      1   overall_success_rate_all_students
      9   overall_success_rate_{subgroup}
      11  success_rate_g{band}_{subject}   (3 for g3_5, 4 each for g6_8 and g9_12)
      4   growth_{subject}_score           (numeracy, literacy, science, social_studies)
      11  growth_ela_math_score_{subgroup} (incl. bhn, super_subgroup)
      1   ccr_rate
      4   ccr_{sub}_rate                   (act, postsec, ic, asvab)
      9   ccr_rate_{subgroup}
    """
    m: dict[str, tuple[str, str, str, str, str, str]] = {}
    # Achievement: overall + by subgroup
    m["overall_success_rate_all_students"] = ("achievement", "all", "all", "all", "all", "pct")
    for sg in _SUBGROUPS_OVERALL:
        m[f"overall_success_rate_{sg}"] = ("achievement", sg, "all", "all", "all", "pct")
    # Achievement: by grade-band × subject. Source uses 'g3-5' (hyphen); schema uses 'g3_5' (underscore).
    for src_band, schema_band, subjects in [
        ("g3-5",  "g3_5",  ["ela", "math", "science"]),                          # no social_studies in g3-5
        ("g6-8",  "g6_8",  ["ela", "math", "science", "social_studies"]),
        ("g9-12", "g9_12", ["ela", "math", "science", "social_studies"]),
    ]:
        for subj in subjects:
            m[f"success_rate_{src_band}_{subj}"] = ("achievement", "all", subj, schema_band, "all", "pct")
    # Growth: by subject
    for subj in ["numeracy", "literacy", "science", "social_studies"]:
        m[f"growth_{subj}_score"] = ("growth", "all", subj, "all", "all", "score_1_5")
    # Growth: ELA+Math composite by subgroup
    for sg in _SUBGROUPS_GROWTH:
        m[f"growth_ela_math_score_{sg}"] = ("growth", sg, "ela_math", "all", "all", "score_1_5")
    # CCR: overall + sub-components + by subgroup
    m["ccr_rate"] = ("ccr", "all", "all", "all", "all", "pct")
    for sub in _CCR_SUBCOMPONENTS:
        m[f"ccr_{sub}_rate"] = ("ccr", "all", "all", "all", sub, "pct")
    for sg in _SUBGROUPS_OVERALL:
        m[f"ccr_rate_{sg}"] = ("ccr", sg, "all", "all", "all", "pct")
    return m


COLUMN_TO_METRIC_TUPLE = _build_column_to_metric_tuple()


# Cell-state classifier. Returns a state token plus an optional numeric value.
# Single source of truth for "how to interpret a value in the A-F file".
_SENTINEL_INSUFFICIENT = "Insufficient N Count"
_SENTINEL_NOT_HS       = "Not a High School"
_SENTINEL_INELIGIBLE   = "Not Eligible for a Letter Grade"
# TDOE privacy buckets — exact rate suppressed but bound is known.
# Currently collapsed to 'suppressed' (metric_value=NULL), losing the directional
# info. If analysts need the lower/upper distinction later, add a suppression_bucket
# text column to tn_letter_grade_metric and stop collapsing.
_SENTINEL_LT_5  = "<5%"
_SENTINEL_GT_95 = ">95%"


def _af_cell_state(v) -> tuple[str, float | None]:
    """Returns ('numeric', val) | ('suppressed', None) | ('not_applicable', None) | ('ineligible', None).
    Raises ValueError on unrecognized text — forces loud failure if TDOE adds a new sentinel."""
    if pd.isna(v):
        return ("not_applicable", None)
    if isinstance(v, (int, float)):
        return ("numeric", float(v))
    s = str(v).strip()
    if s == "":
        return ("not_applicable", None)
    if s == _SENTINEL_INSUFFICIENT:
        return ("suppressed", None)
    if s in (_SENTINEL_LT_5, _SENTINEL_GT_95):
        return ("suppressed", None)
    if s == _SENTINEL_NOT_HS:
        return ("not_applicable", None)
    if s == _SENTINEL_INELIGIBLE:
        return ("ineligible", None)
    # Excel sometimes stores numerics as text strings.
    try:
        return ("numeric", float(s))
    except ValueError:
        raise ValueError(f"unrecognized A-F cell value: {v!r}")


def _wide_cell(v) -> float | None:
    """Generic wide-table conversion: any non-numeric state → NULL."""
    state, val = _af_cell_state(v)
    return val if state == "numeric" else None


def _wide_grade(v) -> str | None:
    """Wide-table letter-grade column: A/B/C/D/F → keep; sentinels/NaN → NULL."""
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s in ("A", "B", "C", "D", "F"):
        return s
    return None  # any sentinel string


_AF_HEADLINE_ONLY_COLUMNS_NULLABLE_IN_2022 = (
    "ach_grade", "growth_grade", "growth25_grade", "ccr_grade",
)


def parse_letter_grade(filepath: Path) -> pd.DataFrame:
    df = pd.read_excel(filepath)
    # 2022-23 didn't publish the four per-component letter grade columns.
    # Add them as NaN so downstream transform code can use df[col] uniformly.
    for col in _AF_HEADLINE_ONLY_COLUMNS_NULLABLE_IN_2022:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def transform_letter_grade(df: pd.DataFrame, year: int) -> dict[str, list[dict]]:
    wide_rows: list[dict] = []
    metric_rows: list[dict] = []

    metric_cols_present = [c for c in COLUMN_TO_METRIC_TUPLE if c in df.columns]
    metric_cols_missing = [c for c in COLUMN_TO_METRIC_TUPLE if c not in df.columns]
    if metric_cols_missing:
        # All 50 should always be present (verified across 3 years). Raise if not.
        raise ValueError(
            f"A-F file missing expected breakdown columns: {metric_cols_missing}. "
            f"Add to COLUMN_TO_METRIC_TUPLE or update the parser if TDOE renamed them."
        )

    # Detect unknown breakdown columns — surface as a loader bug.
    known_non_metric = {
        "year", "system", "system_name", "school", "school_name",
        "lg_ineligible", "school_pool",
        "grade_band_3-5", "grade_band_6-8", "grade_band_9-12",
        "ach_score", "growth_score", "growth25_score", "ccr_score",
        "ach_grade", "growth_grade", "growth25_grade", "ccr_grade",
        "ach_score_weighted", "growth_score_weighted",
        "growth25_score_weighted", "ccr_score_weighted",
        "ach_weight", "growth_weight", "growth25_weight", "ccr_weight",
        "lg_score", "lg_grade",
    }
    unknown = set(df.columns) - known_non_metric - set(COLUMN_TO_METRIC_TUPLE)
    if unknown:
        raise ValueError(
            f"A-F file has columns not classified as headline OR breakdown: {sorted(unknown)}. "
            f"Add to known_non_metric (if headline) or COLUMN_TO_METRIC_TUPLE (if breakdown)."
        )

    for _, r in df.iterrows():
        sys_id = int(r["system"])
        sch_id = int(r["school"])
        pool = str(r["school_pool"]).strip()
        if pool not in ("HS", "K8"):
            raise ValueError(f"unexpected school_pool {pool!r} for school {sys_id}/{sch_id}")
        ineligible = bool(int(r["lg_ineligible"]))

        # Wide-table row.
        ach_w = _wide_cell(r["ach_weight"])
        growth_w = _wide_cell(r["growth_weight"])
        growth25_w = _wide_cell(r["growth25_weight"])
        ccr_w = _wide_cell(r["ccr_weight"])
        # For eligible rows with PARTIAL weight coverage (at least one weight
        # present, but others missing), coerce missing → 0.0. Patterns we see:
        #   K8 standard:        (0.5, 0.4, 0.1, NaN)        — ccr absent
        #   K8 without growth25: (0.5, 0.5, NaN, NaN)       — growth25+ccr absent
        #   HS without growth25: (0.5, 0.4, NaN, 0.1)       — growth25 absent
        # For ALL-NULL weights with all-NULL scores (the eligibility anomaly),
        # leave all four NULL — the schema CHECK admits this branch and the
        # validator emits a WARNING.
        if not ineligible:
            present = [w for w in (ach_w, growth_w, growth25_w, ccr_w) if w is not None]
            if len(present) > 0 and len(present) < 4:
                ach_w     = 0.0 if ach_w     is None else ach_w
                growth_w  = 0.0 if growth_w  is None else growth_w
                growth25_w = 0.0 if growth25_w is None else growth25_w
                ccr_w     = 0.0 if ccr_w     is None else ccr_w

        wide_rows.append({
            "tdoe_system_id": sys_id,
            "tdoe_school_id": sch_id,
            "year":           year,
            "system_name":    None if pd.isna(r["system_name"]) else str(r["system_name"]),
            "school_name":    None if pd.isna(r["school_name"]) else str(r["school_name"]),
            "school_pool":    pool,
            "lg_ineligible":  ineligible,
            "lg_score":       _wide_cell(r["lg_score"]),
            "lg_grade":       _wide_grade(r["lg_grade"]),
            "ach_score":      _wide_cell(r["ach_score"]),
            "growth_score":   _wide_cell(r["growth_score"]),
            "growth25_score": _wide_cell(r["growth25_score"]),
            "ccr_score":      _wide_cell(r["ccr_score"]),
            "ach_grade":      _wide_grade(r["ach_grade"]),
            "growth_grade":   _wide_grade(r["growth_grade"]),
            "growth25_grade": _wide_grade(r["growth25_grade"]),
            "ccr_grade":      _wide_grade(r["ccr_grade"]),
            "ach_weight":     ach_w,
            "growth_weight":  growth_w,
            "growth25_weight": growth25_w,
            "ccr_weight":     ccr_w,
        })

        # Metric-table rows.
        for col in metric_cols_present:
            component, subgroup, subject, grade_band, ccr_component, unit = COLUMN_TO_METRIC_TUPLE[col]
            state, val = _af_cell_state(r[col])
            if state == "not_applicable":
                continue   # skip — measure doesn't apply to this school
            if state == "ineligible":
                # Shouldn't appear in metric-feeding columns (only in lg_*).
                raise ValueError(
                    f"unexpected 'Not Eligible' value in metric column {col!r} for school {sys_id}/{sch_id}"
                )
            metric_rows.append({
                "tdoe_system_id": sys_id,
                "tdoe_school_id": sch_id,
                "year":           year,
                "component":      component,
                "subgroup":       subgroup,
                "subject":        subject,
                "grade_band":     grade_band,
                "ccr_component":  ccr_component,
                "unit":           unit,
                "metric_value":   val,
                "suppressed":     state == "suppressed",
            })

    return {
        "tn_letter_grade":        wide_rows,
        "tn_letter_grade_metric": metric_rows,
    }


# ---------------------------------------------------------------------
# HANDLERS registry
# ---------------------------------------------------------------------

HANDLERS = {
    "tvaas_school_composite": FileHandler(
        parse=parse_tvaas_school_composite,
        transform=transform_tvaas_school_composite,
        target_tables=("tn_tvaas_school_composite",),
    ),
    "tvaas_school_subject": FileHandler(
        parse=parse_tvaas_school_subject,
        transform=transform_tvaas_school_subject,
        target_tables=("tn_tvaas_school_subject",),
    ),
    "letter_grade": FileHandler(
        parse=parse_letter_grade,
        transform=transform_letter_grade,
        target_tables=("tn_letter_grade", "tn_letter_grade_metric"),
    ),
    "tvaas_district_composite": SkipHandler(
        reason="no destination table — district-level data not modeled in current schema",
    ),
    "tvaas_district_subject": SkipHandler(
        reason="no destination table — district-level data not modeled in current schema",
    ),
}
