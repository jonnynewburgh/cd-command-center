"""
etl/fetch_nmtc_award_books.py — Parse CDFI Fund NMTC Award Book PDFs to extract
CDE allocation data and load into cde_allocations table.

The CDFI Fund publishes annual Award Books as PDFs. Each lists the allocatees
(Community Development Entities) that received NMTC allocation authority that year,
along with their city, state, service area, and allocated amount.

This script downloads and parses available Award Books, then upserts the results
into cde_allocations, updating allocation_amount and allocation_year for each CDE.

Usage:
    python etl/fetch_nmtc_award_books.py
    python etl/fetch_nmtc_award_books.py --dry-run   # print extracted rows, no DB write
"""

import argparse
import re
import io
import sys
import os

import requests
import pdfplumber

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

# ---------------------------------------------------------------------------
# Award Book PDF URLs — add newer years here as CDFI Fund publishes them
# ---------------------------------------------------------------------------

AWARD_BOOKS = [
    # (award_year, url)
    # CY 2022 uses a single-column table layout (cleanest)
    (2022, "https://www.cdfifund.gov/system/files/2023-09/CY_2022_NMTC_Program_Award_Book_FINAL.pdf"),
    # CY 2023 uses a two-column newspaper layout; needs text fallback
    (2023, "https://www.cdfifund.gov/system/files/2024-09/NMTC_CY2023_Award_Book_Final_Approval_Copy.pdf"),
    # CY 2024-2025 is a combined round
    (2025, "https://www.cdfifund.gov/system/files/2025-12/NMTC_CY24_25_Award_Book_Final.pdf"),
]

DOWNLOAD_TIMEOUT = 30


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

_DOLLAR_RE = re.compile(r"\$[\d,]+")
_HEADER_WORDS = {"ALLOCATEE", "NAME OF ALLOCATEE", "CITY", "STATE", "SERVICE AREA", "AMOUNT"}

# Words that indicate an org name (vs. a bare city name or PDF artifact)
_ORG_WORDS = {
    "LLC", "L.L.C", "INC", "CORP", "FUND", "TRUST", "CAPITAL", "DEVELOPMENT",
    "FINANCIAL", "INVESTMENT", "ENTERPRISE", "PARTNERS", "ALLIANCE", "VENTURES",
    "INITIATIVE", "SERVICES", "FOUNDATION", "ASSOCIATES", "GROUP", "CENTER",
    "NETWORK", "PROGRAM", "COMMUNITY", "CDE", "BANK", "LENDING", "CREDIT",
    "EQUITY", "MANAGEMENT", "MGMT", "AGENCY", "AUTHORITY", "COUNCIL", "INSTITUTE",
    "CORPORATION", "COOPERATIVE", "ASSOCIATION", "COMPANY",
}


_JUNK_PREFIXES = ("TERRITORY-WIDE)", "URBAN VS.", "TOTAL", "(OR TERRITORY", "FUND,", "ENDEAVOR")
_JUNK_SUBSTRINGS = ("TERRITORY-WIDE)", "APPROXIMATELY APP", "URBAN VS. RURAL")

def _looks_like_cde(name: str) -> bool:
    """Return True if name looks like a real CDE organization, not a city or PDF artifact."""
    if len(name) < 6:
        return False
    # Dollar signs in the name means it's a totals row or PDF artifact
    if "$" in name:
        return False
    if name.startswith("("):
        return False
    name_upper = name.upper()
    if any(name_upper.startswith(p) for p in _JUNK_PREFIXES):
        return False
    if any(s in name_upper for s in _JUNK_SUBSTRINGS):
        return False
    words = set(re.sub(r"[,.]", "", name).upper().split())
    return bool(words & _ORG_WORDS)


def _parse_amount(s: str) -> float | None:
    """Convert '$50,000,000' → 50000000.0"""
    s = str(s or "").replace(",", "").replace("$", "").strip()
    m = re.search(r"(\d+)", s)
    return float(m.group(1)) if m else None


def _is_header(cells: list[str]) -> bool:
    """Return True if this row looks like a column header."""
    joined = " ".join(cells).upper()
    return any(w in joined for w in _HEADER_WORDS) and not _DOLLAR_RE.search(joined)


def _row_to_record(cells: list[str], year: int) -> dict | None:
    """
    Convert a list of cell strings to a cde_allocations record dict.
    Returns None if the row doesn't look like a valid allocatee row.
    """
    if len(cells) < 3:
        return None

    # Normalize: strip newlines from all cells
    cells = [str(c or "").strip().replace("\n", " ") for c in cells]

    # Must have a dollar amount somewhere
    amount_str = next((c for c in cells if _DOLLAR_RE.search(c)), None)
    if not amount_str:
        return None

    name = cells[0].strip()
    if not name or name.upper() in _HEADER_WORDS:
        return None
    if not _looks_like_cde(name):
        return None

    return {
        "cde_name":         name[:200],
        "city":             cells[1].strip() if len(cells) > 1 else None,
        "state":            cells[2].strip() if len(cells) > 2 else None,
        "service_areas":    cells[3].strip() if len(cells) > 3 else None,
        "allocation_amount": _parse_amount(amount_str),
        "allocation_year":  year,
    }


def extract_from_tables(pdf, year: int) -> list[dict]:
    """Primary extraction: pdfplumber table parsing. Works well for CY 2022."""
    records = []
    seen = set()

    for page in pdf.pages:
        for table in (page.extract_tables() or []):
            for row in table:
                if not row:
                    continue
                rec = _row_to_record(row, year)
                if rec and rec["cde_name"] not in seen:
                    seen.add(rec["cde_name"])
                    records.append(rec)
    return records


def extract_from_text(pdf, year: int, existing_names: set) -> list[dict]:
    """
    Text-fallback extraction for two-column layouts (CY 2023, CY 2025).
    Scans each line for a dollar amount and tries to split the rest into fields.
    Only captures names not already found by table extraction.
    """
    records = []
    seen = set(existing_names)

    for page in pdf.pages:
        text = page.extract_text() or ""
        for line in text.split("\n"):
            if not _DOLLAR_RE.search(line):
                continue
            # Find the amount position and split the preceding text into fields
            m = _DOLLAR_RE.search(line)
            amount_str = m.group(0)
            before = line[:m.start()].strip()
            # Split on 2+ spaces (column separator in PDF text)
            parts = re.split(r"\s{2,}", before)
            if len(parts) < 2:
                # Try splitting on known state abbreviation pattern
                sm = re.search(r"\b([A-Z]{2})\b", before)
                if sm:
                    state = sm.group(1)
                    idx = sm.start()
                    name = before[:idx].strip()
                    parts = [name, "", state]
                else:
                    continue

            name = parts[0].strip()
            if not name or name.upper() in _HEADER_WORDS or name in seen:
                continue
            # Skip bare city names, PDF artifacts, and short fragments
            if not _looks_like_cde(name):
                continue
            # Skip if this name is just an existing name with a city appended
            # e.g. "Atlanta Emerging Markets, Inc. Atlanta" when "Atlanta Emerging Markets, Inc." is seen
            if any(name.startswith(existing + " ") for existing in seen):
                continue

            rec = {
                "cde_name":          name[:200],
                "city":              parts[1].strip() if len(parts) > 1 else None,
                "state":             parts[2].strip() if len(parts) > 2 else None,
                "service_areas":     parts[3].strip() if len(parts) > 3 else None,
                "allocation_amount": _parse_amount(amount_str),
                "allocation_year":   year,
            }
            seen.add(name)
            records.append(rec)

    return records


def parse_award_book(pdf_bytes: bytes, year: int) -> list[dict]:
    """
    Extract allocatee records from an NMTC Award Book PDF.
    Uses table extraction first, then text fallback for any missed rows.
    """
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        table_records = extract_from_tables(pdf, year)
        table_names = {r["cde_name"] for r in table_records}
        text_records = extract_from_text(pdf, year, table_names)

    all_records = table_records + text_records

    # Deduplicate by name within this year (keep highest amount if dupes)
    by_name = {}
    for rec in all_records:
        name = rec["cde_name"]
        if name not in by_name or (rec["allocation_amount"] or 0) > (by_name[name]["allocation_amount"] or 0):
            by_name[name] = rec

    # Remove entries whose name is a strict prefix of another entry's name
    # e.g. "COMMUNITY LOAN FUND OF" when "COMMUNITY LOAN FUND OF NEW JERSEY, INC." also exists
    all_names = set(by_name.keys())
    final = {
        name: rec
        for name, rec in by_name.items()
        if not any(
            other != name and other.startswith(name + " ")
            for other in all_names
        )
    }

    return list(final.values())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Parse CDFI Fund NMTC Award Book PDFs and load CDE allocation data"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print extracted rows without writing to database"
    )
    args = parser.parse_args()

    db.init_db()

    total_loaded = 0

    for year, url in AWARD_BOOKS:
        print(f"\nCY {year}: {url}")
        try:
            resp = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
            resp.raise_for_status()
            print(f"  Downloaded {len(resp.content) // 1024}KB")
        except Exception as e:
            print(f"  SKIP: download failed — {e}")
            continue

        records = parse_award_book(resp.content, year)
        print(f"  Extracted {len(records)} allocatees")

        if not records:
            print("  WARNING: 0 rows extracted — PDF layout may have changed")
            continue

        if args.dry_run:
            for rec in sorted(records, key=lambda r: r["cde_name"]):
                amt = f"${rec['allocation_amount']:,.0f}" if rec["allocation_amount"] else "?"
                print(f"    {rec['cde_name'][:50]:<50}  {rec['state'] or '?':2}  {amt}")
            continue

        loaded = 0
        for rec in records:
            try:
                # Upsert: update allocation_amount and service_areas for existing CDEs;
                # insert new rows for CDEs not yet seen.
                conn = db.get_connection()
                cur = conn.cursor()

                # Check if CDE already exists (from project-data derivation, year=0)
                cur.execute(
                    db.adapt_sql("SELECT id FROM cde_allocations WHERE cde_name = ? AND allocation_year = ?"),
                    (rec["cde_name"], year)
                )
                existing = cur.fetchone()

                if existing:
                    cur.execute(
                        db.adapt_sql(
                            "UPDATE cde_allocations SET allocation_amount=?, city=?, state=?, "
                            "service_areas=? WHERE cde_name=? AND allocation_year=?"
                        ),
                        (rec["allocation_amount"], rec["city"], rec["state"],
                         rec["service_areas"], rec["cde_name"], year)
                    )
                else:
                    db.upsert_cde_allocation(rec)

                conn.commit()
                conn.close()
                loaded += 1
            except Exception as e:
                print(f"    DB error for {rec['cde_name']}: {e}")

        print(f"  Loaded: {loaded}")
        total_loaded += loaded

    print(f"\nDone. Total CDE allocation records loaded/updated: {total_loaded}")

    # Summary
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM cde_allocations")
    n = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM cde_allocations WHERE allocation_amount IS NOT NULL AND allocation_year > 0"
    )
    n_with_amt = cur.fetchone()[0]
    conn.close()
    print(f"cde_allocations total rows:        {n:,}")
    print(f"With official allocation amounts:  {n_with_amt:,}")


if __name__ == "__main__":
    main()
