"""Single point that resolves a TN raw filename to (year, file_type).

Loaders should never parse filenames themselves; they take (year, file_type, dataframe)
and don't care about source naming conventions. New filename patterns get added here
with a matching test in tests/test_tn_filename_parser.py.

file_type enum values:
    tvaas_school_composite
    tvaas_school_subject
    tvaas_district_composite
    tvaas_district_subject
    letter_grade

For school-year-spanning files (2020-21, 2022-23, etc.), `year` is the END year
(2021, 2023). This matches TDOE's convention for reporting cohorts.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

FileType = Literal[
    "tvaas_school_composite",
    "tvaas_school_subject",
    "tvaas_district_composite",
    "tvaas_district_subject",
    "letter_grade",
]


@dataclass(frozen=True)
class ParsedFilename:
    year: int
    file_type: FileType


class UnknownFilenameError(ValueError):
    """Raised when a filename doesn't match any known TN pattern."""


# Pattern definitions: (compiled regex, year_resolver, file_type_resolver).
# Order matters — more specific patterns first.
_PATTERNS: list[tuple[re.Pattern[str], object, object]] = [
    # A-F letter grade: 2022-23_A-F_Letter_Grade_File.xlsx (year = end year)
    (
        re.compile(r"^(?P<start>\d{4})-(?P<end>\d{2})_A-F_Letter_Grade_File\.xlsx$"),
        lambda m: int(m["start"][:2] + m["end"]),
        lambda m: "letter_grade",
    ),
    # Modern TVAAS: 2024_tvaas_school_composite.xlsx, 2019_tvaas_district_subject_level.csv
    (
        re.compile(
            r"^(?P<year>\d{4})_tvaas_"
            r"(?P<scope>school|district)_"
            r"(?P<kind>composite|subject_level)"
            r"\.(xlsx|csv)$"
        ),
        lambda m: int(m["year"]),
        lambda m: _tvaas_type(m["scope"], m["kind"]),
    ),
    # Cross-year TVAAS: 2020-21_tvaas_school_composite.csv (year = end year)
    (
        re.compile(
            r"^(?P<start>\d{4})-(?P<end>\d{2})_tvaas_"
            r"(?P<scope>school|district)_"
            r"(?P<kind>composite|subject_level)"
            r"\.(xlsx|csv)$"
        ),
        lambda m: int(m["start"][:2] + m["end"]),
        lambda m: _tvaas_type(m["scope"], m["kind"]),
    ),
    # 2017 legacy: TVAAS_School_Composites_20171.xlsx
    # The trailing "1" appears to be a TDOE internal version suffix; year is the leading 4.
    (
        re.compile(
            r"^TVAAS_"
            r"(?P<scope>School|District)_"
            r"(?P<kind>Composites|Subject_Level)_"
            r"(?P<year>\d{4})\d*\.xlsx$"
        ),
        lambda m: int(m["year"]),
        lambda m: _tvaas_type(m["scope"].lower(), _normalize_kind_2017(m["kind"])),
    ),
    # 2018 legacy: data_2018_TVAAS_School_Composite.xlsx
    (
        re.compile(
            r"^data_(?P<year>\d{4})_TVAAS_"
            r"(?P<scope>School|District)_"
            r"(?P<kind>Composite|Subject_Level)"
            r"\.xlsx$"
        ),
        lambda m: int(m["year"]),
        lambda m: _tvaas_type(m["scope"].lower(), _normalize_kind_2018(m["kind"])),
    ),
    # 2015/2016 legacy: data_school_wide_tvaas_2015.xlsx, data_district_wide_tvaas_2016.xlsx
    # These files have only overall+subject composites at school/district level (no per-subject
    # × per-grade subject-level breakdown). Mapped to *_composite types.
    (
        re.compile(
            r"^data_(?P<scope>school|district)_wide_tvaas_(?P<year>\d{4})\.xlsx$"
        ),
        lambda m: int(m["year"]),
        lambda m: _tvaas_type(m["scope"], "composite"),
    ),
]


def _tvaas_type(scope: str, kind: str) -> FileType:
    if scope == "school" and kind == "composite":
        return "tvaas_school_composite"
    if scope == "school" and kind == "subject_level":
        return "tvaas_school_subject"
    if scope == "district" and kind == "composite":
        return "tvaas_district_composite"
    if scope == "district" and kind == "subject_level":
        return "tvaas_district_subject"
    raise ValueError(f"unknown TVAAS combo: scope={scope!r} kind={kind!r}")


def _normalize_kind_2017(kind: str) -> str:
    # 2017 used "Composites" (plural) and "Subject_Level"
    return "composite" if kind.lower().startswith("composite") else "subject_level"


def _normalize_kind_2018(kind: str) -> str:
    # 2018 used "Composite" (singular) and "Subject_Level"
    return "composite" if kind.lower() == "composite" else "subject_level"


def parse_filename(filename: str) -> ParsedFilename:
    """Resolve a TN raw filename to (year, file_type).

    Args:
        filename: bare filename (no directory), e.g. "2024_tvaas_school_composite.xlsx"

    Returns:
        ParsedFilename with year (int) and file_type (FileType).

    Raises:
        UnknownFilenameError: filename doesn't match any known pattern.
    """
    for pattern, year_fn, type_fn in _PATTERNS:
        m = pattern.match(filename)
        if m:
            return ParsedFilename(year=year_fn(m), file_type=type_fn(m))
    raise UnknownFilenameError(f"no TN filename pattern matched: {filename!r}")
