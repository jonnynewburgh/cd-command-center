# Canonical TN Accountability Load Order

Deterministic file order for the canonical wipe + reload of TN charter accountability data.
Expected row counts are populated from development load_log rows for the files that have
been loaded; remaining rows show `?` and will be filled in post-load.

## Wipe protocol

Before reloading, wipe in this order:

```sql
TRUNCATE tn_letter_grade_metric CASCADE;
TRUNCATE tn_letter_grade CASCADE;
TRUNCATE tn_tvaas_school_subject CASCADE;
TRUNCATE tn_tvaas_school_composite CASCADE;
TRUNCATE etl_load_log RESTART IDENTITY CASCADE;
```

`RESTART IDENTITY` resets the bigserial `load_id` sequence so the canonical reload produces
load_ids 1, 2, 3, ... matching this document's order.

DO NOT wipe `tn_school_crosswalk` here — it's loaded by its own pipeline.

## Canonical order

The order is: oldest year first; within each year, school composite → school subject;
A-F files slot into their end-year cohort. District files are skipped by handler but
still get a load_log row recording the skip.

| load_id | filename | file_type | year | expected row_count(s) |
|---|---|---|---|---|
| 1 | `data_district_wide_tvaas_2015.xlsx` | tvaas_district_composite | 2015 | skipped |
| 2 | `data_school_wide_tvaas_2015.xlsx` | tvaas_school_composite | 2015 | tn_tvaas_school_composite: ? |
| 3 | `data_district_wide_tvaas_2016.xlsx` | tvaas_district_composite | 2016 | skipped |
| 4 | `data_school_wide_tvaas_2016.xlsx` | tvaas_school_composite | 2016 | tn_tvaas_school_composite: ? |
| 5 | `TVAAS_District_Composites_20171.xlsx` | tvaas_district_composite | 2017 | skipped |
| 6 | `TVAAS_District_Subject_Level_20171.xlsx` | tvaas_district_subject | 2017 | skipped |
| 7 | `TVAAS_School_Composites_20171.xlsx` | tvaas_school_composite | 2017 | tn_tvaas_school_composite: ? |
| 8 | `TVAAS_School_Subject_Level_20171.xlsx` | tvaas_school_subject | 2017 | tn_tvaas_school_subject: ? |
| 9 | `data_2018_TVAAS_District_Composite.xlsx` | tvaas_district_composite | 2018 | skipped |
| 10 | `data_2018_TVAAS_District_Subject_Level.xlsx` | tvaas_district_subject | 2018 | skipped |
| 11 | `data_2018_TVAAS_School_Composite.xlsx` | tvaas_school_composite | 2018 | tn_tvaas_school_composite: ? |
| 12 | `data_2018_TVAAS_School_Subject_Level.xlsx` | tvaas_school_subject | 2018 | tn_tvaas_school_subject: ? |
| 13 | `2019_tvaas_district_composite.csv` | tvaas_district_composite | 2019 | skipped |
| 14 | `2019_tvaas_district_subject_level.csv` | tvaas_district_subject | 2019 | skipped |
| 15 | `2019_tvaas_school_composite.csv` | tvaas_school_composite | 2019 | tn_tvaas_school_composite: 1710 (line count) |
| 16 | `2019_tvaas_school_subject_level.csv` | tvaas_school_subject | 2019 | tn_tvaas_school_subject: 17240 (line count) |
| 17 | `2020-21_tvaas_district_composite.csv` | tvaas_district_composite | 2021 | skipped |
| 18 | `2020-21_tvaas_district_subject_level.csv` | tvaas_district_subject | 2021 | skipped |
| 19 | `2020-21_tvaas_school_composite.csv` | tvaas_school_composite | 2021 | tn_tvaas_school_composite: ~1544 |
| 20 | `2020-21_tvaas_school_subject_level.csv` | tvaas_school_subject | 2021 | tn_tvaas_school_subject: ~16391 |
| 21 | `2022_tvaas_district_composite.xlsx` | tvaas_district_composite | 2022 | skipped |
| 22 | `2022_tvaas_district_subject_level.xlsx` | tvaas_district_subject | 2022 | skipped |
| 23 | `2022_tvaas_school_composite.xlsx` | tvaas_school_composite | 2022 | tn_tvaas_school_composite: ? |
| 24 | `2022_tvaas_school_subject_level.xlsx` | tvaas_school_subject | 2022 | tn_tvaas_school_subject: ? |
| 25 | `2022-23_A-F_Letter_Grade_File.xlsx` | letter_grade | 2023 | **tn_letter_grade: 1900, tn_letter_grade_metric: 62173** ✓ |
| 26 | `2023_tvaas_district_composite.xlsx` | tvaas_district_composite | 2023 | skipped |
| 27 | `2023_tvaas_district_subject_level.xlsx` | tvaas_district_subject | 2023 | skipped |
| 28 | `2023_tvaas_school_composite.xlsx` | tvaas_school_composite | 2023 | tn_tvaas_school_composite: ? |
| 29 | `2023_tvaas_school_subject_level.xlsx` | tvaas_school_subject | 2023 | tn_tvaas_school_subject: ? |
| 30 | `2023-24_A-F_Letter_Grade_File.xlsx` | letter_grade | 2024 | **tn_letter_grade: 1905, tn_letter_grade_metric: 62290** ✓ |
| 31 | `2024_tvaas_district_composite.xlsx` | tvaas_district_composite | 2024 | skipped |
| 32 | `2024_tvaas_district_subject_level.xlsx` | tvaas_district_subject | 2024 | skipped |
| 33 | `2024_tvaas_school_composite.xlsx` | tvaas_school_composite | 2024 | **tn_tvaas_school_composite: 1728** ✓ |
| 34 | `2024_tvaas_school_subject_level.xlsx` | tvaas_school_subject | 2024 | **tn_tvaas_school_subject: 20422** ✓ |
| 35 | `2024-25_A-F_Letter_Grade_File.xlsx` | letter_grade | 2025 | **tn_letter_grade: 1905, tn_letter_grade_metric: 62269** ✓ |
| 36 | `2025_tvaas_district_composite.xlsx` | tvaas_district_composite | 2025 | skipped |
| 37 | `2025_tvaas_district_subject_level.xlsx` | tvaas_district_subject | 2025 | skipped |
| 38 | `2025_tvaas_school_composite.xlsx` | tvaas_school_composite | 2025 | tn_tvaas_school_composite: ? |
| 39 | `2025_tvaas_school_subject_level.xlsx` | tvaas_school_subject | 2025 | tn_tvaas_school_subject: ? |

**Bold ✓ entries** were loaded during development and the row counts are verified.
**Plain `?` entries** will be filled in after the canonical reload.
**Line-count estimates** for 2019/2020-21 CSV files are pre-load row counts from `wc -l` (header included).

Expected eligibility-anomaly warnings during the load:
- load_id 25 (2022-23 A-F): 3 warnings (Crestview Middle 840/13, Crestview Elementary 840/17, Martin Elementary 920/35)
- load_id 30 (2023-24 A-F): 3 warnings (same three schools)
- load_id 35 (2024-25 A-F): 0 warnings

## Reload command

The loader iterates `raw/` in filesystem order, which by lucky filename design produces
something close to chronological order. For strict canonical order, run files one at a time:

```bash
cd /c/Users/jonny/Documents/GitHub/cd-command-center

# Then for each filename in the table above, in order:
python -m etl.load_tn_accountability --file <filename>
```

Or, accepting filesystem order (which differs from canonical order in legacy-prefix
filenames like `TVAAS_*_20171.xlsx` and `data_*` files): just run without `--file`:

```bash
python -m etl.load_tn_accountability
```

If the canonical load_ids matter for downstream documentation, use the one-at-a-time
form in the order listed above.
