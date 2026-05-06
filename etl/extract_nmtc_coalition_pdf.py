"""
etl/extract_nmtc_coalition_pdf.py — Extract project case-study profiles from
the NMTC Coalition's annual Progress Report PDF.

Source PDF: data/raw/2025-NMTC-Progress-Report-full.pdf (or any year's report)

The Coalition Progress Report is a narrative document, not a structured
database — it features ~30-50 case-study profiles per year, even though the
report says "350 projects financed." This extractor pulls out the named
case-studies; the unnamed remainder cannot be recovered from this source.

Each case-study block follows a recognizable shape:

    PROJECT NAME (ALL CAPS, sometimes followed by parenthetical)
    City, ST                            <-- two-letter state, anchor pattern
    CDES: NAME1, NAME2[. INVESTOR: ...] <-- "CDE:" or "CDES:" prefix
    [N PERMANENT JOBS[, M CONSTRUCTION JOBS]]
    [Description paragraph]

Anchor rule: a line matching `^[A-Za-z][A-Za-z\s.,]+,\s*[A-Z]{2}$` (City, ST)
immediately followed (within 3 lines) by a line starting with `CDE:` or `CDES:`
is a project block. The line immediately above the City line is the project
name (skipping section-header noise like "SPECIAL REPORT" or "RESTORING
AMERICAN MANUFACTURING").

Output: writes to nmtc_coalition_named_projects table (created on first run).

Usage:
    python etl/extract_nmtc_coalition_pdf.py --file data/raw/2025-NMTC-Progress-Report-full.pdf
    python etl/extract_nmtc_coalition_pdf.py --file ... --report-year 2025
    python etl/extract_nmtc_coalition_pdf.py --file ... --dry-run
"""

import argparse
import os
import re
import sys

import pdfplumber

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db


# ---------------------------------------------------------------------------
# Regex anchors
# ---------------------------------------------------------------------------

CITY_STATE_RE = re.compile(r"^([A-Za-z][A-Za-z .'\-]+?),\s*([A-Z]{2})$")
CDE_LINE_RE   = re.compile(
    r"^(?:CDES?|(?:PROJECT\s+)?SUBMITTED\s+BY)\s*:\s*(.+?)(?:\.\s*INVESTOR\s*:\s*(.+?))?\.?$",
    re.IGNORECASE,
)
JOBS_RE       = re.compile(
    r"(\d+)\s+PERMANENT\s+JOBS(?:[\s,]+(\d+)\s+CONSTRUCTION\s+JOBS)?",
    re.IGNORECASE,
)

# Section-header noise to skip when looking backward for a project name.
SECTION_NOISE = {
    "SPECIAL REPORT",
    "THE NMTC: RESTORING AMERICAN MANUFACTURING",
    "RESTORING AMERICAN MANUFACTURING",
    "AMERICAN MANUFACTURING",
    "TRENDS",
    "RURAL MANUFACTURING",
    "TRIBES",
    "RURAL TARGETING",
    "COMMUNITY CHARACTERISTICS",
    "PROJECT TYPES",
    "Coming Soon",
    "2025 PIPELINE",
    "2024 PIPELINE",
    "BY THE NUMBERS",
}


def _is_section_noise(line: str) -> bool:
    s = line.strip()
    if not s:
        return True
    if s in SECTION_NOISE:
        return True
    if re.match(r"^\d+\s+\d{4}\s+NMTC Progress Report$", s):
        return True
    if re.match(r"^\d{4}\s+NMTC Progress Report\s+\d+$", s):
        return True
    if re.match(r"^\d+\s+SPECIAL REPORT$", s):
        return True
    return False


def _is_plausible_project_name(line: str) -> bool:
    s = line.strip()
    if not s or len(s) < 3 or len(s) > 120:
        return False
    if _is_section_noise(s):
        return False
    # Reject lines that look like prose (lots of lowercase words)
    letters = [c for c in s if c.isalpha()]
    if not letters:
        return False
    upper_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
    if upper_ratio < 0.5:  # require mostly upper (allows mixed-case names like "Project H.O.O.D.")
        return False
    return True


def _extract_blocks(lines: list[str]) -> list[dict]:
    """Walk lines top-to-bottom, emit one record per City-then-CDE anchor."""
    out = []
    n = len(lines)
    for i, line in enumerate(lines):
        m = CITY_STATE_RE.match(line.strip())
        if not m:
            continue
        city, state = m.group(1).strip(), m.group(2).strip()

        # Look ahead 1-3 lines for a CDE marker
        cde_idx = None
        for j in range(i + 1, min(i + 4, n)):
            if CDE_LINE_RE.match(lines[j].strip()):
                cde_idx = j
                break
        if cde_idx is None:
            continue

        cde_match = CDE_LINE_RE.match(lines[cde_idx].strip())
        cde_list  = [c.strip() for c in cde_match.group(1).split(",")] if cde_match else []
        investor  = cde_match.group(2).strip() if cde_match and cde_match.group(2) else None

        # Walk backward from City line to find the project name (skip noise)
        name = None
        for k in range(i - 1, max(i - 6, -1), -1):
            cand = lines[k].strip()
            if _is_plausible_project_name(cand):
                name = cand
                break

        if not name:
            continue

        # Look ahead a few lines for jobs
        jobs_perm, jobs_const = None, None
        for k in range(cde_idx + 1, min(cde_idx + 5, n)):
            jm = JOBS_RE.search(lines[k])
            if jm:
                jobs_perm = int(jm.group(1))
                jobs_const = int(jm.group(2)) if jm.group(2) else None
                break

        out.append({
            "project_name":      name,
            "city":              city,
            "state":             state,
            "cde_names":         "; ".join(cde_list),
            "investor":          investor,
            "jobs_permanent":    jobs_perm,
            "jobs_construction": jobs_const,
        })
    return out


def extract_pdf(path: str) -> list[dict]:
    """Walk every page and return all extracted project records."""
    all_records = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = [l for l in text.split("\n") if l.strip()]
            all_records.extend(_extract_blocks(lines))
    return all_records


def ensure_table():
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nmtc_coalition_named_projects (
            id SERIAL PRIMARY KEY,
            report_year INTEGER NOT NULL,
            project_name TEXT NOT NULL,
            city TEXT,
            state CHAR(2),
            cde_names TEXT,
            investor TEXT,
            jobs_permanent INTEGER,
            jobs_construction INTEGER,
            ein VARCHAR(9),
            nmtc_project_id INTEGER,
            match_confidence REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (report_year, project_name, state)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ncnp_state ON nmtc_coalition_named_projects(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ncnp_name  ON nmtc_coalition_named_projects(project_name)")
    conn.commit()
    conn.close()


def upsert(records: list[dict], report_year: int):
    conn = db.get_connection()
    cur = conn.cursor()
    inserted = 0
    for r in records:
        cur.execute("""
            INSERT INTO nmtc_coalition_named_projects
              (report_year, project_name, city, state, cde_names, investor,
               jobs_permanent, jobs_construction)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_year, project_name, state) DO UPDATE SET
              city              = EXCLUDED.city,
              cde_names         = EXCLUDED.cde_names,
              investor          = EXCLUDED.investor,
              jobs_permanent    = EXCLUDED.jobs_permanent,
              jobs_construction = EXCLUDED.jobs_construction
        """, (report_year, r["project_name"], r["city"], r["state"],
              r["cde_names"], r["investor"], r["jobs_permanent"], r["jobs_construction"]))
        inserted += 1
    conn.commit()
    conn.close()
    return inserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to NMTC Coalition Progress Report PDF")
    parser.add_argument("--report-year", type=int, help="Report year (e.g. 2025); inferred from filename if omitted")
    parser.add_argument("--dry-run", action="store_true", help="Show extracted records without writing to DB")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        sys.exit(f"ERROR: file not found: {args.file}")

    report_year = args.report_year
    if not report_year:
        m = re.search(r"(20\d{2})", os.path.basename(args.file))
        report_year = int(m.group(1)) if m else None
    if not report_year:
        sys.exit("ERROR: could not infer report year; pass --report-year")

    print(f"Extracting from: {args.file}")
    print(f"Report year:     {report_year}")
    print()

    records = extract_pdf(args.file)
    print(f"Extracted {len(records):,} project case-studies")
    print()

    if args.dry_run or len(records) <= 60:
        print(f"{'PROJECT':<55} {'CITY':<25} ST  {'CDEs':<60} JOBS")
        print("-" * 160)
        for r in records:
            jobs = ""
            if r["jobs_permanent"] is not None:
                jobs = f"{r['jobs_permanent']:,}p"
                if r["jobs_construction"] is not None:
                    jobs += f"+{r['jobs_construction']:,}c"
            print(f"{r['project_name'][:55]:<55} {r['city'][:25]:<25} {r['state']}  "
                  f"{r['cde_names'][:60]:<60} {jobs}")

    if not args.dry_run:
        ensure_table()
        n = upsert(records, report_year)
        print(f"\nUpserted {n:,} rows into nmtc_coalition_named_projects "
              f"(report_year={report_year})")


if __name__ == "__main__":
    main()
