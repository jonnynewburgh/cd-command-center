"""
etl/run_pipeline.py — Master ETL pipeline runner for CD Command Center.

Runs all data ingestion scripts in dependency order. Scripts that can
auto-download their source data do so automatically. Scripts that require
manually downloaded files are skipped with a clear note.

Usage:
    python etl/run_pipeline.py                    # run everything auto-downloadable
    python etl/run_pipeline.py --skip 990 sba     # skip specific stages
    python etl/run_pipeline.py --only schools fqhc # run only specific stages
    python etl/run_pipeline.py --states GA TX      # pass state filter where supported
    python etl/run_pipeline.py --dry-run           # print what would run, don't execute

Stages (in order):
    schools         NCES public school data (Urban Institute API)
    tracts          Census tract ACS demographics
    fqhc            HRSA Federally Qualified Health Centers
    edfacts         EDFacts LEA accountability data (Ed.gov, auto)
    enrollment      NCES enrollment history (Urban Institute API)
    990             IRS 990 financial data (ProPublica API)
    hmda            HMDA mortgage activity (CFPB API)
    hud-ami         HUD Area Median Income limits (HUD API)
    hud-fmr         HUD Fair Market Rents (HUD API)
    bls-unemp       BLS unemployment by county/MSA (FRED API — needs FRED_API_KEY)
    bls-qcew        BLS quarterly employment (BLS API)
    sba             SBA 7(a) and 504 loan data (data.sba.gov, auto)
    cra             FFIEC CRA institution/assessment area data (ffiec.gov, auto)
    cdfi-awards     CDFI Fund award data (data.gov, auto)
    cdfi-dir        CDFI certified institution directory (data.gov, auto)
    scsc            SCSC CPF charter accountability scores (charters repo)
    fred            FRED market rates — SOFR, Treasuries, etc. (needs FRED_API_KEY)
    fac             Federal Audit Clearinghouse Single Audits (needs FAC_API_KEY)
    headstart       Head Start PIR program data (from HSES Excel exports in data/raw/childcare)

Manual-only stages (require --file):
    nmtc-data       NMTC project data (CDFI Fund Excel — download manually)
    nmtc-coalition  NMTC Coalition transaction report (nmtccoalition.org — download manually)
    ece             ECE/child care facility data (state licensing files — download manually)
    ejscreen        EPA EJScreen indicators (EPA FTP — download manually)
    oz              Opportunity Zone designations (IRS Excel — download manually)
"""

import argparse
import datetime
import os
import subprocess
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ETL_DIR   = os.path.join(REPO_ROOT, "etl")
DB_PATH   = os.path.join(REPO_ROOT, "data", "cd_command_center.sqlite")

# Add repo root to path so utils/ is importable
sys.path.insert(0, REPO_ROOT)
from utils.db_backup import backup_tables, validate_and_finalize  # noqa: E402

# ---------------------------------------------------------------------------
# Tables each stage writes to — used for backup/restore guard.
# ---------------------------------------------------------------------------
STAGE_TABLES = {
    "schools":     ["schools", "charter_schools"],
    "tracts":      ["census_tracts"],
    "fqhc":        ["fqhc"],
    "edfacts":     ["lea_accountability"],
    "enrollment":  ["enrollment_history"],
    "990":         ["irs_990", "irs_990_history", "financial_ratios"],
    "hmda":        ["hmda_activity"],
    "hud-ami":     ["hud_ami"],
    "hud-fmr":     ["hud_fmr"],
    "bls-unemp":   ["bls_unemployment"],
    "bls-qcew":    ["bls_qcew"],
    "sba-7a":      ["sba_loans"],
    "sba-504":     ["sba_loans"],
    "cra":         ["cra_institutions", "cra_assessment_areas"],
    "cdfi-awards": ["cdfi_awards"],
    "cdfi-dir":    ["cdfi_directory"],
    "scsc":        ["scsc_cpf"],
    "fred":        ["market_rates"],
    "fac":         ["federal_audits", "federal_audit_programs"],
    "headstart":   ["headstart_programs"],
}

# ---------------------------------------------------------------------------
# Stage definitions
# Each entry: (stage_name, script, args_template, needs_fred_key, auto_capable)
# ---------------------------------------------------------------------------

def build_stages(args):
    """Build the ordered list of (name, command_list) tuples to run."""
    states_flag = (["--states"] + args.states) if args.states else []
    fred_key    = os.environ.get("FRED_API_KEY", "")
    year        = str(args.year) if args.year else str(datetime.datetime.now().year - 1)

    stages = []

    # ── NCES schools ──────────────────────────────────────────────────────────
    stages.append(("schools", [
        sys.executable, os.path.join(ETL_DIR, "fetch_nces_schools.py"),
        "--demographics",
    ] + states_flag))

    # ── Census tracts ─────────────────────────────────────────────────────────
    if args.states:
        stages.append(("tracts", [
            sys.executable, os.path.join(ETL_DIR, "load_census_tracts.py"),
            "--historical",
        ] + states_flag))
    else:
        stages.append(("tracts", [
            sys.executable, os.path.join(ETL_DIR, "load_census_tracts.py"),
            "--all", "--historical",
        ]))

    # ── FQHC ─────────────────────────────────────────────────────────────────
    stages.append(("fqhc", [
        sys.executable, os.path.join(ETL_DIR, "fetch_fqhc.py"),
    ] + states_flag))

    # ── EDFacts ───────────────────────────────────────────────────────────────
    edfacts_cmd = [
        sys.executable, os.path.join(ETL_DIR, "fetch_edfacts_auto.py"),
        "--year", year,
    ]
    if args.states:
        edfacts_cmd += ["--states"] + args.states
    stages.append(("edfacts", edfacts_cmd))

    # ── Enrollment history ────────────────────────────────────────────────────
    stages.append(("enrollment", [
        sys.executable, os.path.join(ETL_DIR, "fetch_enrollment_trends.py"),
        "--years", "5",
    ] + states_flag))

    # ── IRS 990 data ──────────────────────────────────────────────────────────
    stages.append(("990", [
        sys.executable, os.path.join(ETL_DIR, "fetch_990_data.py"),
        "--schools", "--fqhc", "--years", "5",
    ] + states_flag))

    # ── HMDA ─────────────────────────────────────────────────────────────────
    if args.states:
        stages.append(("hmda", [
            sys.executable, os.path.join(ETL_DIR, "fetch_hmda.py"),
            "--year", year,
        ] + states_flag))
    else:
        stages.append(("hmda", [
            sys.executable, os.path.join(ETL_DIR, "fetch_hmda.py"),
            "--year", year, "--all",
        ]))

    # ── HUD AMI ───────────────────────────────────────────────────────────────
    stages.append(("hud-ami", [
        sys.executable, os.path.join(ETL_DIR, "fetch_hud_ami.py"),
    ] + states_flag))

    # ── HUD FMR ───────────────────────────────────────────────────────────────
    stages.append(("hud-fmr", [
        sys.executable, os.path.join(ETL_DIR, "fetch_hud_fmr.py"),
    ] + states_flag))

    # ── BLS unemployment (FRED) ───────────────────────────────────────────────
    if fred_key:
        bls_unemp_cmd = [
            sys.executable, os.path.join(ETL_DIR, "fetch_bls_unemployment.py"),
            "--mode", "fred-states", "--api-key", fred_key, "--months", "36",
        ]
        if args.states:
            bls_unemp_cmd += ["--states"] + args.states
        stages.append(("bls-unemp", bls_unemp_cmd))
    else:
        stages.append(("bls-unemp", None))  # None = skip with note

    # ── BLS QCEW ──────────────────────────────────────────────────────────────
    # QCEW API works best with specific FIPS; without states filter, skip
    # (bulk file is too large to auto-download reliably)
    stages.append(("bls-qcew", None))   # requires FIPS or bulk file

    # ── SBA loans ─────────────────────────────────────────────────────────────
    stages.append(("sba-7a", [
        sys.executable, os.path.join(ETL_DIR, "fetch_sba_loans.py"),
        "--auto", "--program", "7a", "--fiscal-year", year,
    ] + states_flag))
    stages.append(("sba-504", [
        sys.executable, os.path.join(ETL_DIR, "fetch_sba_loans.py"),
        "--auto", "--program", "504", "--fiscal-year", year,
    ] + states_flag))

    # ── CRA data ──────────────────────────────────────────────────────────────
    stages.append(("cra", [
        sys.executable, os.path.join(ETL_DIR, "fetch_cra_data.py"),
        "--auto", "--year", year,
    ] + states_flag))

    # ── CDFI awards ───────────────────────────────────────────────────────────
    stages.append(("cdfi-awards", [
        sys.executable, os.path.join(ETL_DIR, "fetch_cdfi_awards.py"),
        "--auto",
    ] + (["--states"] + args.states if args.states else [])))

    # ── CDFI directory ────────────────────────────────────────────────────────
    stages.append(("cdfi-dir", [
        sys.executable, os.path.join(ETL_DIR, "load_cdfi_directory.py"),
        "--auto",
    ] + (["--states"] + args.states if args.states else [])))

    # ── SCSC CPF ──────────────────────────────────────────────────────────────
    scsc_file = os.path.join(REPO_ROOT, "..", "charters", "data", "cpf_all_years.csv")
    scsc_file = os.path.normpath(scsc_file)
    if os.path.exists(scsc_file):
        stages.append(("scsc", [
            sys.executable, os.path.join(ETL_DIR, "load_scsc_cpf.py"),
            "--file", scsc_file,
        ]))
    else:
        stages.append(("scsc", None))   # charters repo not present

    # ── FRED market rates ────────────────────────────────────────────────────
    if fred_key:
        stages.append(("fred", [
            sys.executable, os.path.join(ETL_DIR, "fetch_fred_rates.py"),
            "--api-key", fred_key, "--latest",
        ]))
    else:
        stages.append(("fred", None))  # needs FRED_API_KEY

    # ── Federal Audit Clearinghouse ──────────────────────────────────────────
    fac_key = os.environ.get("FAC_API_KEY", "")
    if fac_key:
        if args.states:
            for st in args.states:
                stages.append(("fac", [
                    sys.executable, os.path.join(ETL_DIR, "fetch_fac.py"),
                    "--state", st, "--year", year,
                ]))
        else:
            stages.append(("fac", [
                sys.executable, os.path.join(ETL_DIR, "fetch_fac.py"),
                "--all-states", "--year", year,
            ]))
    else:
        stages.append(("fac", None))  # needs FAC_API_KEY

    # ── Head Start PIR ───────────────────────────────────────────────────────
    pir_dir = os.path.join(REPO_ROOT, "data", "raw", "childcare")
    if os.path.isdir(pir_dir):
        hs_cmd = [
            sys.executable, os.path.join(ETL_DIR, "load_headstart_pir.py"),
            "--dir", pir_dir,
        ]
        if args.states:
            hs_cmd += ["--states"] + args.states
        stages.append(("headstart", hs_cmd))
    else:
        stages.append(("headstart", None))

    return stages


MANUAL_STAGES = {
    "nmtc-data":      "python etl/load_nmtc_data.py --file data/raw/nmtc_public_data_2024.xlsx",
    "nmtc-coalition": "python etl/load_nmtc_coalition.py --file data/raw/nmtc_transaction_report_2024.xlsx",
    "ece":            "python etl/load_ece_data.py --file data/raw/<state>_childcare.csv --state <ST>",
    "ejscreen":       "python etl/load_ejscreen.py --file data/raw/EJSCREEN_2023_Tracts.csv",
    "oz":             "python etl/load_opportunity_zones.py --file data/raw/opportunity_zones.csv",
}


def run_stage(name: str, cmd: list[str], dry_run: bool) -> bool:
    """
    Run a single stage with backup/restore guard.

    Before running: snapshot all tables this stage writes to.
    After running:
      - If subprocess failed OR row counts dropped >10%: restore backups.
      - Otherwise: drop backups (keep new data).

    Returns True if the new data was kept, False if backups were restored
    (or if this was a dry run).
    """
    print(f"\n{'='*60}")
    print(f"  STAGE: {name}")
    print(f"{'='*60}")
    print(f"  Command: {' '.join(cmd)}")

    if dry_run:
        print("  (dry-run — skipping)")
        return True

    tables = STAGE_TABLES.get(name, [])
    backups = []
    if tables and os.path.exists(DB_PATH):
        print(f"  Backing up: {', '.join(tables)}")
        backups = backup_tables(DB_PATH, tables)

    start = time.time()
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    elapsed = time.time() - start
    load_ok = result.returncode == 0

    if load_ok:
        print(f"\n  Stage completed in {elapsed:.0f}s — validating data...")
    else:
        print(f"\n  Stage FAILED (exit {result.returncode}) after {elapsed:.0f}s — restoring...")

    kept = validate_and_finalize(DB_PATH, backups, load_succeeded=load_ok)

    if kept:
        print(f"  [OK] {name} complete")
    else:
        print(f"  [ROLLED BACK] {name}")
    return kept


def main():
    parser = argparse.ArgumentParser(
        description="CD Command Center master ETL pipeline"
    )
    parser.add_argument(
        "--skip",
        nargs="+",
        metavar="STAGE",
        default=[],
        help="Stage name(s) to skip.",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        metavar="STAGE",
        default=[],
        help="Run only these stage(s) (ignores all others).",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        default=[],
        help="State abbreviations to filter where supported (e.g. GA TX CA).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Data year for downloads (default: current year minus 1).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would run without actually running it.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Keep running even if a stage fails.",
    )
    args = parser.parse_args()

    stages = build_stages(args)

    print("CD Command Center — ETL Pipeline")
    print(f"  Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if args.states:
        print(f"  States: {', '.join(args.states)}")
    if args.dry_run:
        print("  Mode: DRY RUN")
    print()

    # Print manual-only stages so user knows what to do after
    print("Manual-download stages (not auto-run):")
    for name, hint in MANUAL_STAGES.items():
        print(f"  {name:20s}  {hint}")
    print()

    skipped = []
    failed  = []
    success = []

    for name, cmd in stages:
        # Filter by --only
        if args.only and name not in args.only:
            continue
        # Filter by --skip
        if name in args.skip:
            print(f"  Skipping: {name}")
            skipped.append(name)
            continue
        # Stage has no command (requires manual setup)
        if cmd is None:
            skip_reason = {
                "bls-unemp":  "Set FRED_API_KEY env var to enable BLS unemployment download",
                "bls-qcew":   "Provide --fips args or bulk file; run fetch_bls_qcew.py manually",
                "scsc":       "charters repo not found at ../charters/data/cpf_all_years.csv",
                "fred":       "Set FRED_API_KEY env var to enable market rates download",
                "fac":        "Set FAC_API_KEY env var to enable Federal Audit Clearinghouse download",
                "headstart":  "No PIR files found in data/raw/childcare/",
            }.get(name, "manual setup required")
            print(f"\n  [SKIP] {name}: {skip_reason}")
            skipped.append(name)
            continue

        ok = run_stage(name, cmd, args.dry_run)
        if ok:
            success.append(name)
        else:
            failed.append(name)
            if not args.continue_on_error:
                print(f"\nPipeline stopped at '{name}'. Use --continue-on-error to keep going.")
                break

    # Summary
    print(f"\n{'='*60}")
    print("PIPELINE SUMMARY")
    print(f"{'='*60}")
    print(f"  Completed:  {len(success)}  ({', '.join(success) or 'none'})")
    print(f"  Failed:     {len(failed)}   ({', '.join(failed) or 'none'})")
    print(f"  Skipped:    {len(skipped)}  ({', '.join(skipped) or 'none'})")

    if MANUAL_STAGES:
        print()
        print("Next: run manual stages if needed:")
        for name, hint in MANUAL_STAGES.items():
            print(f"  {hint}")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
