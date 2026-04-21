# Data Refresh Schedule

How and when each data source in CD Command Center gets updated.

## Automated Schedules (Claude Code Triggers)

### Daily — Market Rates

**Cron:** `0 7 * * *` (7:00 AM ET daily)

| Source | Script | Notes |
|--------|--------|-------|
| FRED Market Rates | `fetch_fred_rates.py --latest` | SOFR, 5/10/30yr Treasuries, Fed Funds. Moves daily; critical for deal pricing. |

### Monthly — Rolling Federal Data

**Cron:** `0 7 1 * *` (1st of each month, 7:00 AM ET)

| Source | Script | Notes |
|--------|--------|-------|
| BLS Unemployment | `fetch_bls_unemployment.py` | State/county rates released ~1st Friday each month. |
| FAC Single Audits | `fetch_fac.py --all-states` | Audits submitted on a rolling basis. Monthly catches new filings. |
| IRS 990 | `fetch_990_data.py --schools --fqhc --years 5` | ProPublica index updates as IRS processes returns. |

### Quarterly — Lending & Employment

**Cron:** `0 7 1 1,4,7,10 *` (1st of Jan, Apr, Jul, Oct — 7:00 AM ET)

| Source | Script | Notes |
|--------|--------|-------|
| SBA 7(a) Loans | `fetch_sba_loans.py --auto --program 7a` | data.sba.gov updates throughout the fiscal year. |
| SBA 504 Loans | `fetch_sba_loans.py --auto --program 504` | Same as above. |
| HMDA | `fetch_hmda.py --all` | CFPB releases annual snapshot in spring; dynamic data quarterly. |
| CRA Data | `fetch_cra_data.py --auto` | FFIEC releases prior-year data ~September. |
| BLS QCEW | `fetch_bls_qcew.py` | Quarterly with ~6-month lag. |

### Annual — Full Pipeline

**Cron:** `0 7 15 12 *` (December 15, 7:00 AM ET)

Runs the full auto-downloadable pipeline to pick up all annual releases:

| Source | Script | Typical Release Window |
|--------|--------|----------------------|
| NCES Schools | `fetch_nces_schools.py --demographics` | Oct-Dec |
| Enrollment Trends | `fetch_enrollment_trends.py --years 5` | Oct-Dec (with NCES) |
| EDFacts | `fetch_edfacts_auto.py` | Oct-Dec (with NCES) |
| Census Tracts (ACS) | `load_census_tracts.py --all --historical` | Dec (5-year ACS) |
| HUD AMI | `fetch_hud_ami.py` | Mar-Apr |
| HUD FMR | `fetch_hud_fmr.py` | Sep-Oct |
| CDFI Awards | `fetch_cdfi_awards.py --auto` | Q1 |
| CDFI Directory | `load_cdfi_directory.py --auto` | Anytime |
| FQHC | `fetch_fqhc.py` | Spring (HRSA UDS) |

The annual run also re-runs all daily/monthly/quarterly sources to ensure nothing was missed.

**Command:** `python etl/run_pipeline.py --continue-on-error`

## Manual Refresh (Download Required)

These sources require manual file downloads before running the ETL script. Check for new data at the listed frequency.

| Source | Script | Check Frequency | Where to Download |
|--------|--------|-----------------|-------------------|
| NMTC Project Data | `load_nmtc_data.py --file ...` | Annually | CDFI Fund Excel release |
| NMTC Coalition | `load_nmtc_coalition.py --file ...` | Annually | nmtccoalition.org |
| ECE/Child Care | `load_ece_data.py --file ... --state XX` | Quarterly | State licensing agency (varies by state) |
| Head Start PIR | `load_headstart_pir.py --dir data/raw/childcare` | Annually (spring) | HSES portal (requires login) |
| SCSC CPF | `load_scsc_cpf.py` | Annually (fall) | charters repo → `cpf_all_years.csv` |
| EJScreen | `load_ejscreen.py --file ...` | Annually (summer) | EPA FTP |
| Opportunity Zones | `load_opportunity_zones.py --file ...` | Rarely | IRS (one-time designation, rarely changes) |
| State Programs | `load_state_programs.py` | As needed | Manual seed file |

## Environment Variables Required

All scheduled triggers need these set in `~/.bashrc`:

```bash
export DATABASE_URL="postgresql://postgres:...@localhost:5432/cd_command_center"
export FRED_API_KEY="..."
export FAC_API_KEY="..."
export HSES_USERNAME="..."
export HSES_PASSWORD="..."
```

## Running Manually

```bash
# Full pipeline (all auto-downloadable stages)
python etl/run_pipeline.py --continue-on-error

# State-filtered run
python etl/run_pipeline.py --states GA TX --continue-on-error

# Specific stages only
python etl/run_pipeline.py --only fred 990 fac

# Dry run (preview)
python etl/run_pipeline.py --dry-run
```

## Monitoring

Each scheduled trigger logs its output. Check trigger history with:

```bash
claude schedule list
```

If a stage fails, the pipeline's backup/restore guard rolls back that stage's tables to their pre-run state — no data is lost.
