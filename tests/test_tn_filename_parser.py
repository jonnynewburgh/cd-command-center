"""Tests for etl.tn_filename_parser.

The EXPECTED_MAPPINGS list mirrors every filename currently present in
data/raw/charter accountability/TN/ (post-cleanup, 2026-05-17). If TDOE
publishes a new pattern, add a fixture row here first, then make the parser
satisfy it.
"""
from __future__ import annotations

import pytest

from etl.tn_filename_parser import (
    ParsedFilename,
    UnknownFilenameError,
    parse_filename,
)

# (filename, expected_year, expected_file_type)
EXPECTED_MAPPINGS: list[tuple[str, int, str]] = [
    # Modern (2019-2025) TVAAS
    ("2019_tvaas_district_composite.csv",      2019, "tvaas_district_composite"),
    ("2019_tvaas_district_subject_level.csv",  2019, "tvaas_district_subject"),
    ("2019_tvaas_school_composite.csv",        2019, "tvaas_school_composite"),
    ("2019_tvaas_school_subject_level.csv",    2019, "tvaas_school_subject"),
    ("2022_tvaas_district_composite.xlsx",     2022, "tvaas_district_composite"),
    ("2022_tvaas_district_subject_level.xlsx", 2022, "tvaas_district_subject"),
    ("2022_tvaas_school_composite.xlsx",       2022, "tvaas_school_composite"),
    ("2022_tvaas_school_subject_level.xlsx",   2022, "tvaas_school_subject"),
    ("2023_tvaas_district_composite.xlsx",     2023, "tvaas_district_composite"),
    ("2023_tvaas_district_subject_level.xlsx", 2023, "tvaas_district_subject"),
    ("2023_tvaas_school_composite.xlsx",       2023, "tvaas_school_composite"),
    ("2023_tvaas_school_subject_level.xlsx",   2023, "tvaas_school_subject"),
    ("2024_tvaas_district_composite.xlsx",     2024, "tvaas_district_composite"),
    ("2024_tvaas_district_subject_level.xlsx", 2024, "tvaas_district_subject"),
    ("2024_tvaas_school_composite.xlsx",       2024, "tvaas_school_composite"),
    ("2024_tvaas_school_subject_level.xlsx",   2024, "tvaas_school_subject"),
    ("2025_tvaas_district_composite.xlsx",     2025, "tvaas_district_composite"),
    ("2025_tvaas_district_subject_level.xlsx", 2025, "tvaas_district_subject"),
    ("2025_tvaas_school_composite.xlsx",       2025, "tvaas_school_composite"),
    ("2025_tvaas_school_subject_level.xlsx",   2025, "tvaas_school_subject"),
    # Cross-year TVAAS (year = end of SY)
    ("2020-21_tvaas_district_composite.csv",      2021, "tvaas_district_composite"),
    ("2020-21_tvaas_district_subject_level.csv",  2021, "tvaas_district_subject"),
    ("2020-21_tvaas_school_composite.csv",        2021, "tvaas_school_composite"),
    ("2020-21_tvaas_school_subject_level.csv",    2021, "tvaas_school_subject"),
    # A-F letter grade (year = end of SY)
    ("2022-23_A-F_Letter_Grade_File.xlsx", 2023, "letter_grade"),
    ("2023-24_A-F_Letter_Grade_File.xlsx", 2024, "letter_grade"),
    ("2024-25_A-F_Letter_Grade_File.xlsx", 2025, "letter_grade"),
    # 2017 legacy
    ("TVAAS_District_Composites_20171.xlsx",    2017, "tvaas_district_composite"),
    ("TVAAS_District_Subject_Level_20171.xlsx", 2017, "tvaas_district_subject"),
    ("TVAAS_School_Composites_20171.xlsx",      2017, "tvaas_school_composite"),
    ("TVAAS_School_Subject_Level_20171.xlsx",   2017, "tvaas_school_subject"),
    # 2018 legacy
    ("data_2018_TVAAS_District_Composite.xlsx",    2018, "tvaas_district_composite"),
    ("data_2018_TVAAS_District_Subject_Level.xlsx", 2018, "tvaas_district_subject"),
    ("data_2018_TVAAS_School_Composite.xlsx",       2018, "tvaas_school_composite"),
    ("data_2018_TVAAS_School_Subject_Level.xlsx",   2018, "tvaas_school_subject"),
    # 2015/2016 legacy (only district+school composite; no subject-level files)
    ("data_district_wide_tvaas_2015.xlsx", 2015, "tvaas_district_composite"),
    ("data_district_wide_tvaas_2016.xlsx", 2016, "tvaas_district_composite"),
    ("data_school_wide_tvaas_2015.xlsx",   2015, "tvaas_school_composite"),
    ("data_school_wide_tvaas_2016.xlsx",   2016, "tvaas_school_composite"),
]


@pytest.mark.parametrize("filename,year,file_type", EXPECTED_MAPPINGS)
def test_parse_known_filenames(filename: str, year: int, file_type: str) -> None:
    result = parse_filename(filename)
    assert result == ParsedFilename(year=year, file_type=file_type)


def test_total_fixture_count_matches_raw_dir() -> None:
    """If a new file lands in raw/ that doesn't appear in EXPECTED_MAPPINGS,
    this test will fail and force us to add a fixture row (or recognize that
    the parser needs a new pattern)."""
    from pathlib import Path

    raw_dir = Path(__file__).parent.parent / "data" / "raw" / "charter accountability" / "TN"
    if not raw_dir.exists():
        pytest.skip(f"raw dir not present: {raw_dir}")

    actual = {
        p.name for p in raw_dir.iterdir()
        if p.is_file() and p.suffix in (".csv", ".xlsx")
    }
    expected = {row[0] for row in EXPECTED_MAPPINGS}

    missing_from_fixtures = actual - expected
    missing_from_dir = expected - actual
    assert not missing_from_fixtures, (
        f"raw/ has files not in EXPECTED_MAPPINGS: {sorted(missing_from_fixtures)}"
    )
    assert not missing_from_dir, (
        f"EXPECTED_MAPPINGS references files not in raw/: {sorted(missing_from_dir)}"
    )


def test_unknown_filename_raises() -> None:
    with pytest.raises(UnknownFilenameError):
        parse_filename("random_unknown_file.xlsx")


def test_unknown_filename_message_includes_filename() -> None:
    try:
        parse_filename("nonsense.csv")
    except UnknownFilenameError as e:
        assert "nonsense.csv" in str(e)
    else:
        pytest.fail("expected UnknownFilenameError")
