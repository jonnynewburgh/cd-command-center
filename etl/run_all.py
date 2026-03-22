"""
etl/run_all.py — Master zero-touch ETL orchestrator for CD Command Center.

Runs all ETL scripts in the correct dependency order to build a complete
database with no manual file downloads or uploads required.

Every data source is fetched automatically from its official public URL.
The only requirement is an internet connection.

Usage:
    # Full build — all states, all sources:
    python etl/run_all.py

    # Specific states only (faster for development):
    python etl/run_all.py --states CA TX NY IL

    # Skip slow steps (e.g. the 5 GB EJScreen download):
    python etl/run_all.py --skip ejscreen

    # Skip multiple steps:
    python etl/run_all.py --skip ejscreen edfacts

    # See what steps would run without executing them:
    python etl/run_all.py --dry-run

    # Re-download all files even if recent cached copies exist:
    python etl/run_all.py --force-download

    # Stop on first failure instead of continuing:
    python etl/run_all.py --fail-fast

    # Use a specific accountability data year:
    python etl/run_all.py --accountability-year 2023

Available step names (for --skip):
    schools         NCES public school data (charter + traditional)
    census          Census ACS demographic data
    geocode         Census tract assignment for all facilities
    nmtc            NMTC project and CDE allocation data (CDFI Fund)
    fqhc            HRSA Federally Qualified Health Centers
    ece             State ECE / child care facility data (all states)
    oz              Opportunity Zone census tract designations (IRS)
    ejscreen        EPA EJScreen environmental justice indicators (~5 GB download)
    cdfi_dir        CDFI Fund certified CDFI directory
    cdfi_awards     CDFI Fund award data (FA/TA/BEA/CMF/Bond)
    state_programs  State incentive programs (seed data — already in repo)
    990             IRS 990 nonprofit financial data (ProPublica API)
    enrollment      NCES historical enrollment trends
    edfacts         EDFacts federal LEA accountability (all 50 states)
    accountability  State-specific accountability data (TX, CA, NY, FL, etc.)
    survival        Charter school survival model training
"""

import argparse
import subprocess
import sys
import os
import time
import datetime

# ---------------------------------------------------------------------------
# Step definitions
# ---------------------------------------------------------------------------
# Each step is: {"name": ..., "script": ..., "args": [...], "description": ...}
# Steps run in order. Dependencies (e.g. census before geocode) are enforced
# by order alone.
#
# {states} in args is replaced at runtime with the actual state list.
# {accountability_year} is replaced with the year argument.

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

STEPS = [
    {
        "name": "schools",
        "script": "etl/fetch_nces_schools.py",
        "args": ["--demographics"],
        "states_arg": "--states",
        "description": "Fetch all public schools from NCES via Urban Institute API",
    },
    {
        "name": "census",
        "script": "etl/load_census_tracts.py",
        "args": [],
        "states_arg": "--states",
        "description": "Load census tract demographics from ACS API",
    },
    {
        "name": "geocode",
        "script": "etl/assign_census_tracts.py",
        "args": ["--schools"],
        "description": "Assign census tracts to schools (batch geocoding)",
    },
    {
        "name": "nmtc",
        "script": "etl/load_nmtc_data.py",
        "args": [],   # auto-downloads
        "description": "Load NMTC project and CDE data from CDFI Fund (auto-download)",
    },
    {
        "name": "nmtc_geocode",
        "script": "etl/geocode_nmtc.py",
        "args": [],
        "description": "Geocode NMTC projects using ZIP code lookup",
        "group": "nmtc",  # skipped when "nmtc" is in --skip
    },
    {
        "name": "fqhc",
        "script": "etl/fetch_fqhc.py",
        "args": [],
        "states_arg": "--states",
        "description": "Load HRSA health center data (auto-downloads)",
    },
    {
        "name": "fqhc_geocode",
        "script": "etl/assign_census_tracts.py",
        "args": ["--fqhc"],
        "description": "Assign census tracts to FQHCs",
        "group": "geocode",
    },
    {
        "name": "ece",
        "script": "etl/load_ece_data.py",
        "args": ["--all-states"],
        "description": "Load ECE facility data for all supported states (auto-downloads)",
    },
    {
        "name": "oz",
        "script": "etl/load_opportunity_zones.py",
        "args": [],   # auto-downloads
        "description": "Load Opportunity Zone designations from IRS (auto-download)",
    },
    {
        "name": "ejscreen",
        "script": "etl/load_ejscreen.py",
        "args": [],   # auto-downloads; filter to states if provided
        "states_arg": "--states",
        "description": "Load EPA EJScreen EJ indicators (auto-downloads ~5 GB zip)",
    },
    {
        "name": "cdfi_dir",
        "script": "etl/load_cdfi_directory.py",
        "args": [],   # auto-downloads
        "description": "Load certified CDFI directory from CDFI Fund (auto-download)",
    },
    {
        "name": "cdfi_awards",
        "script": "etl/fetch_cdfi_awards.py",
        "args": [],   # auto-downloads
        "description": "Load CDFI Fund award data (FA/TA/BEA/CMF/Bond) (auto-download)",
    },
    {
        "name": "state_programs",
        "script": "etl/load_state_programs.py",
        "args": [],
        "description": "Load state incentive programs from seed file (no download needed)",
    },
    {
        "name": "990",
        "script": "etl/fetch_990_data.py",
        "args": ["--schools"],
        "states_arg": "--states",
        "description": "Load 990 data for charter schools from ProPublica API",
    },
    {
        "name": "990_fqhc",
        "script": "etl/fetch_990_data.py",
        "args": ["--fqhc"],
        "states_arg": "--states",
        "description": "Load 990 data for FQHCs from ProPublica API",
        "group": "990",
    },
    {
        "name": "enrollment",
        "script": "etl/fetch_enrollment_trends.py",
        "args": [],
        "states_arg": "--states",
        "description": "Load historical enrollment trends from NCES API",
    },
    {
        "name": "census_historical",
        "script": "etl/load_census_tracts.py",
        "args": ["--historical"],
        "states_arg": "--states",
        "description": "Load 5-year historical census data for tract change analysis",
        "group": "census",
    },
    {
        "name": "edfacts",
        "script": "etl/fetch_edfacts.py",
        "args": [],          # year inserted at runtime
        "year_arg": "--year",
        "description": "Load EDFacts federal LEA accountability data (all 50 states, auto-download)",
    },
    {
        "name": "accountability",
        "script": "etl/fetch_state_accountability.py",
        "args": ["--all-states"],   # year inserted at runtime
        "year_arg": "--year",
        "description": "Load state-specific accountability data (TX, CA, NY, FL, etc.)",
    },
    {
        "name": "survival",
        "script": "etl/train_survival_model.py",
        "args": [],
        "description": "Train charter school survival model on loaded school data",
    },
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_step(step: dict, states: list, accountability_year: int,
             force_download: bool, dry_run: bool) -> bool:
    """
    Run a single ETL step as a subprocess.
    Returns True on success, False on failure.
    """
    script = step["script"]
    args = list(step.get("args", []))

    # Inject --states if this step supports it and states were specified
    states_arg = step.get("states_arg")
    if states_arg and states:
        args += [states_arg] + states

    # Inject --year for accountability steps
    year_arg = step.get("year_arg")
    if year_arg:
        args += [year_arg, str(accountability_year)]

    # Inject --force-download if applicable
    if force_download and "--force-download" not in args:
        # Only inject for scripts that accept this flag
        scripts_with_force = {
            "etl/load_nmtc_data.py", "etl/load_opportunity_zones.py",
            "etl/load_ejscreen.py", "etl/load_cdfi_directory.py",
            "etl/fetch_cdfi_awards.py", "etl/load_ece_data.py",
            "etl/fetch_state_accountability.py", "etl/fetch_edfacts.py",
        }
        if script in scripts_with_force:
            args.append("--force-download")

    cmd = [sys.executable, os.path.join(BASE_DIR, script)] + args

    print(f"\n{'─' * 60}")
    print(f"  {step['description']}")
    print(f"  Command: {' '.join(cmd)}")
    print(f"{'─' * 60}")

    if dry_run:
        print("  [dry-run] Would execute the above command.")
        return True

    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=BASE_DIR,
            check=False,  # don't raise on non-zero exit; we handle it below
        )
        elapsed = time.time() - start
        if result.returncode == 0:
            print(f"\n  ✓ Completed in {elapsed:.0f}s")
            return True
        else:
            print(f"\n  ✗ Failed (exit code {result.returncode}) after {elapsed:.0f}s")
            return False
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n  ✗ Error running step: {e} (after {elapsed:.0f}s)")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="CD Command Center — Zero-touch full ETL pipeline"
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help=(
            "Limit data loading to these 2-letter state codes (e.g. CA TX NY). "
            "Applies to all scripts that support state filtering. "
            "If omitted, loads all states (may take hours for a full national build)."
        ),
    )
    parser.add_argument(
        "--skip",
        nargs="+",
        metavar="STEP",
        default=[],
        help=(
            "Step names to skip (e.g. --skip ejscreen). "
            "Use 'ejscreen' to skip the large 5 GB download. "
            "Use 'edfacts' to skip the federal assessment download. "
            "A group name (e.g. 'nmtc') also skips child steps with that group."
        ),
    )
    parser.add_argument(
        "--only",
        nargs="+",
        metavar="STEP",
        help="Run only these step names (e.g. --only schools census).",
    )
    parser.add_argument(
        "--accountability-year",
        type=int,
        default=datetime.date.today().year - 1,
        help=(
            "School year for EDFacts and state accountability data "
            "(e.g. 2023 = the 2022-23 school year). Defaults to last year."
        ),
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download all files even if recent cached copies exist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the commands that would run without executing them.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately on the first step failure (default: continue).",
    )
    parser.add_argument(
        "--list-steps",
        action="store_true",
        help="Print all step names and descriptions, then exit.",
    )
    args = parser.parse_args()

    if args.list_steps:
        print("CD Command Center ETL Steps:")
        print()
        for step in STEPS:
            group = f" (group: {step['group']})" if step.get("group") else ""
            print(f"  {step['name']:20s}  {step['description']}{group}")
        return

    print("=" * 60)
    print("  CD Command Center — Full ETL Pipeline")
    print(f"  Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if args.states:
        print(f"  States: {', '.join(args.states)}")
    else:
        print("  States: ALL (national build)")
    if args.skip:
        print(f"  Skipping: {', '.join(args.skip)}")
    if args.only:
        print(f"  Only running: {', '.join(args.only)}")
    print(f"  Accountability year: {args.accountability_year}")
    if args.dry_run:
        print("  MODE: DRY RUN — no commands will be executed")
    print("=" * 60)

    # Build the active step list
    skip_set = set(args.skip)
    only_set = set(args.only) if args.only else None

    active_steps = []
    for step in STEPS:
        name = step["name"]
        group = step.get("group", name)

        # Skip if in --skip (by name or group)
        if name in skip_set or group in skip_set:
            continue

        # Only run steps in --only (if specified)
        if only_set and name not in only_set:
            continue

        active_steps.append(step)

    print(f"\nSteps to run ({len(active_steps)}):")
    for i, step in enumerate(active_steps, 1):
        print(f"  {i:2d}. {step['name']:20s}  {step['description']}")

    if not active_steps:
        print("\nNo steps to run. Check --skip and --only arguments.")
        return

    if not args.dry_run:
        print("\nStarting in 3 seconds... (Ctrl+C to abort)")
        time.sleep(3)

    # Execute steps
    results = []
    pipeline_start = time.time()

    for i, step in enumerate(active_steps, 1):
        print(f"\n[{i}/{len(active_steps)}] {step['name']}")
        success = run_step(
            step=step,
            states=args.states,
            accountability_year=args.accountability_year,
            force_download=args.force_download,
            dry_run=args.dry_run,
        )
        results.append((step["name"], success))

        if not success and args.fail_fast:
            print(f"\n[FAIL-FAST] Stopping after failure in step '{step['name']}'.")
            break

    # Summary
    elapsed = time.time() - pipeline_start
    print(f"\n{'=' * 60}")
    print(f"  ETL Pipeline Complete — {elapsed / 60:.1f} minutes")
    print(f"{'=' * 60}")

    succeeded = [name for name, ok in results if ok]
    failed = [name for name, ok in results if not ok]
    skipped_count = len(active_steps) - len(results)

    print(f"  Steps succeeded: {len(succeeded)}")
    print(f"  Steps failed:    {len(failed)}")
    if skipped_count > 0:
        print(f"  Steps skipped (fail-fast): {skipped_count}")

    if failed:
        print(f"\n  FAILED STEPS:")
        for name in failed:
            print(f"    - {name}")
        print(f"\n  Re-run failed steps with: python etl/run_all.py --only {' '.join(failed)}")

    if not args.dry_run and not failed:
        print(f"\n  All steps succeeded! Launch the app with:")
        print(f"    streamlit run app.py")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
