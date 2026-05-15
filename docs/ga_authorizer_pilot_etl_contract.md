# GA Authorizer Pilot ETL Contract

This defines the first end-to-end state pipeline for charter authorizer accountability joins.

## Goal

Populate:
- `authorizers` (named entities)
- `school_authorizer` (school-year links)

for Georgia, using NCES IDs as the stable join key to `schools` and EDFacts-derived data.

## Inputs

### 1) Authorizers file (`--authorizers-file`)

Required column:
- `authorizer_name`

Optional columns:
- `authorizer_kind` (expected values: `SEA`, `LEA`, `ICB`, `HEI`, `NEG`, `NPO`)
- `nces_lea_id`
- `state_authorizer_id`
- `source_system`
- `source_url`
- `notes`
- `is_active` (`1`/`0`, `true`/`false`)

### 2) School-authorizer links file (`--links-file`)

Required columns:
- `nces_school_id` (NCES school ID; joins `schools.nces_id`)
- `authorizer_name` (must match one in authorizers file)
- `school_year` (e.g. `2023-24`)

Optional columns:
- `relationship` (default `authorizer`)
- `source_system`

## Loader

Script:
- `etl/load_ga_authorizers.py`

Command:

```bash
python etl/load_ga_authorizers.py \
  --authorizers-file "data/raw/charter accountability/GA/ga_authorizers.csv" \
  --links-file "data/raw/charter accountability/GA/ga_school_authorizer_links.csv"
```

Dry run:

```bash
python etl/load_ga_authorizers.py --authorizers-file ... --links-file ... --dry-run
```

## Mapping Rules

- `authorizers.state` is always `GA` for this pilot loader.
- `authorizers` upsert key: `(state, name)`.
- `school_authorizer` upsert key: `(nces_school_id, authorizer_id, school_year)`.
- Links where `authorizer_name` does not map to a loaded authorizer are skipped and counted.

## Validation Queries (post-load)

1) Links without matching school:

```sql
SELECT COUNT(*)
FROM school_authorizer sa
LEFT JOIN schools s ON s.nces_id = sa.nces_school_id
WHERE s.nces_id IS NULL;
```

2) School-years with multiple authorizers (review expectedness):

```sql
SELECT nces_school_id, school_year, COUNT(*) AS n
FROM school_authorizer
GROUP BY nces_school_id, school_year
HAVING COUNT(*) > 1;
```

3) Coverage snapshot:

```sql
SELECT
  (SELECT COUNT(*) FROM authorizers WHERE state = 'GA') AS ga_authorizers,
  (SELECT COUNT(*) FROM school_authorizer sa
     JOIN authorizers a ON a.id = sa.authorizer_id
    WHERE a.state = 'GA') AS ga_school_links;
```

## Next Iteration After GA

- Add a state plug-in pattern (`etl/load_state_authorizers.py --state XX`) that reuses the same target schema.
- Add `data_year`/`effective_date` support if a state publishes mid-year authorizer changes.
- Add a QA report artifact in `analyses/` for unmatched `nces_school_id` and unknown `authorizer_name`.

