"""
etl/fetch_990_irs.py — Fetch IRS Form 990 financial data from the primary source.

DATA SOURCE: IRS electronically-filed 990 data
  Index: https://apps.irs.gov/pub/epostcard/990/xml/{year}/index_{year}.csv
  XMLs:  https://apps.irs.gov/pub/epostcard/990/xml/{year}/{year}_TEOS_XML_{MM}{S}.zip

  US federal government data — public domain (17 U.S.C. § 105), no commercial
  use restrictions regardless of which tool is used to access it.

HOW IT AVOIDS DOWNLOADING FULL ZIPs:
  ZIP files for each month can be 100 MB – 1.2 GB. This script uses HTTP
  range requests to read only what it needs:
    1. The ZIP's central directory  (~1-5 MB per ZIP) to build an
       object_id → member_name index.
    2. Individual compressed XML entries (a few KB each) on demand.
  A full run makes ~15 range requests per ZIP (one for the central directory)
  plus one range request per EIN whose XML is in that ZIP. No ZIP is ever
  fully downloaded or stored on disk.

APPROACH:
  1. Collect all EINs already in our database (irs_990 + schools + fqhc).
  2. Download IRS index CSVs for recent years (~10-50 MB each, cached locally)
     to build a per-EIN map to the most recent filing's OBJECT_ID.
  3. Enumerate all ZIP files for each required year. Read each ZIP's central
     directory via range requests to build an OBJECT_ID → ZIP location map.
  4. For each EIN, stream-read the XML from the appropriate ZIP, parse it, and
     upsert the financial fields into irs_990 with data_source='IRS'.

  Does NOT delete existing records. Records are updated in place when a
  matching IRS filing is found; unmatched records are left unchanged.

CACHE:
  Index CSVs are cached in data/raw/irs990/. They are small (~10-50 MB each)
  and reused across runs. Use --refresh-index to re-download them.

RUNNING TIME:
  ~30-60 min for 4,000 EINs on first run (enumerating ZIP central directories
  takes most of the time). Subsequent runs with cached indexes are faster.

Usage:
    python etl/fetch_990_irs.py                     # all EINs in DB
    python etl/fetch_990_irs.py --years 2023 2024   # specific index years
    python etl/fetch_990_irs.py --limit 50          # test on 50 EINs
    python etl/fetch_990_irs.py --dry-run           # parse only, no DB writes
    python etl/fetch_990_irs.py --refresh-index     # re-download cached indexes
"""

import argparse
import csv
import io
import os
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

IRS_BASE       = "https://apps.irs.gov/pub/epostcard/990/xml"
CACHE_DIR      = Path("data/raw/irs990")
DEFAULT_YEARS  = [2022, 2023, 2024]
API_SLEEP      = 0.1   # seconds between XML range requests

# Form types we care about (990T = unrelated business income, skip it)
SUPPORTED_FORMS = {"990", "990EZ", "990PF", "990-EZ", "990-PF"}


# ---------------------------------------------------------------------------
# HTTP range-request file — lets zipfile read remote ZIPs without downloading
# ---------------------------------------------------------------------------

class RangeHTTPFile:
    """
    Read-only file-like object backed by HTTP range requests.
    Allows zipfile.ZipFile to open a remote ZIP and read individual entries
    without downloading the entire archive.

    Each read() call issues one HTTP GET with a Range header. The zipfile
    module makes a small number of seeks (for the central directory) plus
    one read per entry accessed — so total bandwidth is proportional to
    the files actually opened, not the ZIP size.
    """

    def __init__(self, url: str, session: requests.Session | None = None):
        self.url     = url
        self._sess   = session or requests.Session()
        self._pos    = 0
        self._size: int | None = None

    def _total(self) -> int:
        if self._size is None:
            r = self._sess.head(self.url, timeout=30, allow_redirects=True)
            r.raise_for_status()
            self._size = int(r.headers["content-length"])
        return self._size

    def seek(self, offset: int, whence: int = 0) -> int:
        if   whence == 0: self._pos  = offset
        elif whence == 1: self._pos += offset
        elif whence == 2: self._pos  = self._total() + offset
        return self._pos

    def tell(self) -> int:
        return self._pos

    def read(self, size: int = -1) -> bytes:
        total = self._total()
        if size < 0:
            size = total - self._pos
        if size == 0 or self._pos >= total:
            return b""
        end = min(self._pos + size - 1, total - 1)
        r   = self._sess.get(self.url,
                             headers={"Range": f"bytes={self._pos}-{end}"},
                             timeout=60)
        r.raise_for_status()
        data = r.content
        self._pos += len(data)
        return data

    def seekable(self) -> bool: return True
    def readable(self) -> bool: return True
    def writable(self) -> bool: return False


# ---------------------------------------------------------------------------
# Index CSV management
# ---------------------------------------------------------------------------

def _load_index_csv(year: int, refresh: bool = False) -> list[dict]:
    """
    Download (and cache) the IRS index CSV for a given submission year.
    Returns a list of dicts with keys: EIN, TAX_PERIOD, RETURN_TYPE, OBJECT_ID.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"index_{year}.csv"

    if cache_path.exists() and not refresh:
        with open(cache_path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    url = f"{IRS_BASE}/{year}/index_{year}.csv"
    print(f"  Downloading IRS index {year}...", flush=True)
    try:
        r = requests.get(url, timeout=180)
        r.raise_for_status()
    except Exception as e:
        print(f"    Warning: failed to download index {year} — {e}")
        return []

    with open(cache_path, "wb") as f:
        f.write(r.content)

    with open(cache_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_ein_map(years: list[int], refresh: bool = False) -> dict[str, dict]:
    """
    Return {ein: filing_info} keeping only the most recent filing per EIN
    across all given years. Only includes 990 / 990EZ / 990PF form types.
    """
    ein_map: dict[str, dict] = {}

    for year in years:
        rows = _load_index_csv(year, refresh=refresh)
        print(f"    {year}: {len(rows):,} index rows loaded")
        for row in rows:
            form = row.get("RETURN_TYPE", "").strip()
            if form not in SUPPORTED_FORMS:
                continue
            ein = row.get("EIN", "").strip().zfill(9)
            if not ein or ein == "000000000":
                continue
            tax_period = row.get("TAX_PERIOD", "").strip()
            existing   = ein_map.get(ein)
            if existing is None or tax_period > existing.get("TAX_PERIOD", ""):
                ein_map[ein] = {
                    "EIN":        ein,
                    "OBJECT_ID":  row.get("OBJECT_ID", "").strip(),
                    "RETURN_TYPE": form,
                    "TAX_PERIOD": tax_period,
                    "year":       year,
                }

    return ein_map


# ---------------------------------------------------------------------------
# ZIP enumeration — build object_id → (zip_url, member_name) map
# ---------------------------------------------------------------------------

def _enumerate_zips(year: int, session: requests.Session) -> list[str]:
    """Return all existing ZIP URLs for a given submission year."""
    urls = []
    for month in range(1, 13):
        for suffix in "ABCDE":
            url = f"{IRS_BASE}/{year}/{year}_TEOS_XML_{month:02d}{suffix}.zip"
            try:
                r = session.head(url, timeout=15, allow_redirects=True)
                if r.status_code == 200:
                    urls.append(url)
                elif r.status_code == 404:
                    break   # no more suffixes for this month
            except Exception:
                break
    return urls


def build_object_map(
    years: list[int],
    target_object_ids: set[str],
    session: requests.Session,
) -> dict[str, tuple[str, str]]:
    """
    Build {object_id: (zip_url, member_name)} for all object IDs that exist
    in the target set. Reads only ZIP central directories (via range requests)
    — no full ZIP downloads.

    Returns a dict mapping each found object_id to the ZIP URL and filename
    inside the ZIP.
    """
    result: dict[str, tuple[str, str]] = {}

    for year in years:
        zip_urls = _enumerate_zips(year, session)
        print(f"    {year}: {len(zip_urls)} ZIP files to scan", flush=True)

        for zip_url in zip_urls:
            try:
                zf = zipfile.ZipFile(RangeHTTPFile(zip_url, session=session))
            except Exception as e:
                print(f"      Warning: could not open {zip_url.split('/')[-1]} — {e}")
                continue

            matched = 0
            for name in zf.namelist():
                # member name format: '{zip_base}/{object_id}_public.xml'
                obj_id = name.split("/")[-1].replace("_public.xml", "")
                if obj_id in target_object_ids:
                    result[obj_id] = (zip_url, name)
                    matched += 1

            zf.fp.seek(0)   # reset to allow GC; the RangeHTTPFile is ephemeral
            zip_base = zip_url.split("/")[-1]
            if matched:
                print(f"      {zip_base}: {matched} of our EINs found")

    return result


# ---------------------------------------------------------------------------
# XML parsing
# ---------------------------------------------------------------------------

def _tag(elem: ET.Element) -> str:
    t = elem.tag
    return t.split("}")[-1] if "}" in t else t


def _find(parent: ET.Element, *local_names: str) -> ET.Element | None:
    for child in parent:
        if _tag(child) in local_names:
            return child
    return None


def _text(parent: ET.Element, path: str) -> str | None:
    current = parent
    for part in path.split("/"):
        nxt = _find(current, part)
        if nxt is None:
            return None
        current = nxt
    return current.text.strip() if (current is not None and current.text) else None


def _amt(parent: ET.Element, *paths: str) -> float | None:
    for path in paths:
        val = _text(parent, path)
        if val is not None:
            try:
                return float(val)
            except ValueError:
                pass
    return None


def parse_990_xml(xml_bytes: bytes) -> dict | None:
    """
    Parse a 990 XML filing. Returns a dict of financial fields for irs_990,
    or None if parsing fails. Handles Form 990, 990-EZ, and 990-PF.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None

    header = _find(root, "ReturnHeader")
    if header is None:
        return None

    ein = _text(header, "Filer/EIN")
    if not ein:
        return None
    ein = ein.zfill(9)

    tax_year_raw = _text(header, "TaxYr") or _text(header, "TaxYear")
    tax_year = int(tax_year_raw) if tax_year_raw and tax_year_raw.isdigit() else None

    org_name = (
        _text(header, "Filer/BusinessName/BusinessNameLine1Txt")
        or _text(header, "Filer/BusinessNameLine1Txt")
        or _text(header, "Filer/Name/BusinessNameLine1")
    )
    city  = _text(header, "Filer/USAddress/CityNm") or _text(header, "Filer/USAddress/City")
    state = (
        _text(header, "Filer/USAddress/StateAbbreviationCd")
        or _text(header, "Filer/USAddress/State")
    )

    return_data = _find(root, "ReturnData")
    if return_data is None:
        return None

    form = _find(return_data, "IRS990", "IRS990EZ", "IRS990PF")
    if form is None:
        return None
    form_type = _tag(form)

    if form_type == "IRS990":
        total_revenue  = _amt(form, "CYTotalRevenueAmt", "TotalRevenue")
        total_expenses = _amt(form, "CYTotalExpensesAmt",
                              "TotalFunctionalExpenses/TotalAmt")
        total_assets   = _amt(form, "TotalAssetsEOYAmt")
        total_liab     = _amt(form, "TotalLiabilitiesEOYAmt")
        ps_revenue     = _amt(form, "CYProgramServiceRevenueAmt")
        ps_expenses    = _amt(form, "CYProgramServiceExpensesAmt")
        officer_comp   = _amt(form, "CYSalariesCompEmpBnftPaidAmt",
                              "CompCurrentOfcrDirectorsTrustAmt")
        cash           = _amt(form, "CashNonInterestBearingGrp/EOYAmt",
                              "CashSavingsEOYAmt")
        unrestricted   = _amt(form, "UnrestrictedNetAssetsGrp/EOYAmt",
                              "NetAssetsFundBalanceEOYAmt")
        accts_payable  = _amt(form, "AccountsPayableAccruedExpensesGrp/EOYAmt",
                              "AccountsPayableEOYAmt")
        notes_payable  = _amt(form, "MortgageNotesBondsPayableLessGrp/EOYAmt",
                              "BondsPayableGrp/EOYAmt")

    elif form_type == "IRS990EZ":
        total_revenue  = _amt(form, "TotalRevenueAmt")
        total_expenses = _amt(form, "TotalExpensesAmt")
        total_assets   = _amt(form, "Form990TotalAssetsGrp/EOYAmt",
                              "TotalAssetsEOYAmt")
        total_liab     = _amt(form, "SumOfTotalLiabilitiesGrp/EOYAmt",
                              "TotalLiabilitiesEOYAmt")
        ps_revenue     = _amt(form, "ProgramServiceRevenueAmt")
        ps_expenses    = _amt(form, "ProgramServiceExpensesAmt")
        officer_comp   = _amt(form, "SalariesOtherCompEmplBnftAmt")
        cash           = _amt(form, "CashSavingsAndInvestmentsGrp/EOYAmt")
        unrestricted   = _amt(form, "NetAssetsOrFundBalancesEOYAmt")
        accts_payable  = None
        notes_payable  = _amt(form, "LoansFromOfficersDirectorsEtcGrp/EOYAmt")

    elif form_type == "IRS990PF":
        total_revenue  = _amt(form, "TotalRevAndExpnssAmt", "TotalRevAndExpenses")
        total_expenses = _amt(form, "TotalExpensesAndDisbursementsAmt")
        total_assets   = _amt(form, "FairMktValueAssetsEOYAmt",
                              "TotalAssetsEOYFMVAmt")
        total_liab     = _amt(form, "TotalLiabilitiesEOYAmt")
        ps_revenue     = None
        ps_expenses    = _amt(form, "TotalOperatingAndAdminExpensesAmt")
        officer_comp   = None
        cash           = _amt(form, "CashAndCashEquivalentsEOYAmt")
        unrestricted   = _amt(form, "NetAssetsOrFundBalancesEOYAmt")
        accts_payable  = None
        notes_payable  = None

    else:
        return None

    net_income = None
    if total_revenue is not None and total_expenses is not None:
        net_income = total_revenue - total_expenses

    return {
        "ein":                      ein,
        "org_name":                 org_name,
        "city":                     city,
        "state":                    state,
        "tax_year":                 tax_year,
        "total_revenue":            total_revenue,
        "total_expenses":           total_expenses,
        "total_assets":             total_assets,
        "total_liabilities":        total_liab,
        "net_income":               net_income,
        "program_service_revenue":  ps_revenue,
        "program_service_expenses": ps_expenses,
        "officer_compensation":     officer_comp,
        "cash_savings":             cash,
        "unrestricted_net_assets":  unrestricted,
        "accounts_payable":         accts_payable,
        "notes_payable":            notes_payable,
        "data_source":              "IRS",
        # IRS XML doesn't have a PDF URL. We leave filing_pdf_url untouched
        # in the upsert so any existing ProPublica URL is preserved.
    }


# ---------------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------------

def _upsert_irs_record(record: dict):
    """
    Upsert an IRS-sourced record. Only updates non-null financial fields.
    Preserves existing filing_pdf_url, ntee_code, and subsection_code —
    those come from ProPublica / IRS BMF and are not in the 990 XML.
    """
    conn = db.get_connection()
    cur  = conn.cursor()

    preserve = {"filing_pdf_url", "ntee_code", "subsection_code"}
    cols = [k for k, v in record.items() if v is not None]
    vals = [record[k] for k in cols]
    placeholders = ",".join("?" * len(cols))
    update = ",".join(
        f"{c}=excluded.{c}"
        for c in cols
        if c not in {"ein"} | preserve
    )
    cur.execute(
        db.adapt_sql(
            f"INSERT INTO irs_990 ({','.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT(ein) DO UPDATE SET {update}, updated_at=CURRENT_TIMESTAMP"
        ),
        vals,
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Main ETL
# ---------------------------------------------------------------------------

def fetch_990_from_irs(
    index_years: list[int] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    refresh_index: bool = False,
) -> tuple[int, int, int]:

    if index_years is None:
        index_years = DEFAULT_YEARS

    session = requests.Session()

    # ---- Collect target EINs -----------------------------------------------
    conn = db.get_connection()
    cur  = conn.cursor()

    target_eins: set[str] = set()
    cur.execute("SELECT ein FROM irs_990 WHERE ein IS NOT NULL")
    for (e,) in cur.fetchall():
        target_eins.add(str(e).zfill(9))

    cur.execute("SELECT ein FROM schools WHERE ein IS NOT NULL AND ein != ''")
    for (e,) in cur.fetchall():
        target_eins.add(str(e).zfill(9))

    try:
        cur.execute("SELECT ein FROM fqhc WHERE ein IS NOT NULL AND ein != ''")
        for (e,) in cur.fetchall():
            target_eins.add(str(e).zfill(9))
    except Exception:
        pass

    conn.close()

    print(f"  EINs to look up:   {len(target_eins):,}")
    print(f"  Index years:       {index_years}")
    print()

    # ---- Build EIN map from index CSVs -------------------------------------
    print("Step 1: Building EIN map from IRS index CSVs...")
    ein_map = build_ein_map(index_years, refresh=refresh_index)
    matched_eins = target_eins & ein_map.keys()
    print(f"  {len(ein_map):,} total EINs in IRS index across {index_years}")
    print(f"  {len(matched_eins):,} of our EINs found in index")

    targets = list(matched_eins)
    if limit:
        targets = targets[:limit]

    if not targets:
        print("  Nothing to fetch.")
        return 0, 0, 0

    # ---- Build object_id → ZIP location map --------------------------------
    target_object_ids = {ein_map[e]["OBJECT_ID"] for e in targets
                         if ein_map[e].get("OBJECT_ID")}

    # Only scan years that have at least one target filing
    years_needed = sorted({ein_map[e]["year"] for e in targets
                           if ein_map[e].get("OBJECT_ID")})

    print()
    print(f"Step 2: Scanning ZIP central directories for {len(target_object_ids):,} "
          f"object IDs across years {years_needed}...")
    object_map = build_object_map(years_needed, target_object_ids, session)
    print(f"  Located {len(object_map):,} object IDs in ZIPs")

    # ---- Fetch and parse XMLs ----------------------------------------------
    print()
    print(f"Step 3: Reading {len(targets):,} XMLs from IRS ZIPs...")

    # Group targets by ZIP URL so we open each ZipFile once
    zip_groups: dict[str, list[tuple[str, str, str]]] = {}   # zip_url → [(ein, obj_id, member)]
    missing = []
    for ein in targets:
        filing = ein_map[ein]
        obj_id = filing.get("OBJECT_ID", "")
        if not obj_id or obj_id not in object_map:
            missing.append(ein)
            continue
        zip_url, member = object_map[obj_id]
        zip_groups.setdefault(zip_url, []).append((ein, obj_id, member))

    upserted = 0
    failed   = 0

    processed = 0
    total = len(targets) - len(missing)

    for zip_url, entries in zip_groups.items():
        zip_name = zip_url.split("/")[-1]
        try:
            zf = zipfile.ZipFile(RangeHTTPFile(zip_url, session=session))
        except Exception as e:
            print(f"  Warning: could not open {zip_name} — {e}")
            failed += len(entries)
            continue

        for ein, obj_id, member in entries:
            try:
                xml_bytes = zf.read(member)
                time.sleep(API_SLEEP)
            except Exception as e:
                failed += 1
                processed += 1
                continue

            record = parse_990_xml(xml_bytes)
            if record is None:
                failed += 1
                processed += 1
                continue

            if dry_run:
                rev = f"${record['total_revenue']:>12,.0f}" if record.get("total_revenue") else "           ?"
                ast = f"${record['total_assets']:>12,.0f}" if record.get("total_assets") else "           ?"
                print(f"  {ein}  {(record.get('org_name') or '')[:45]:<45}  "
                      f"TY={record.get('tax_year')}  rev={rev}  assets={ast}")
            else:
                _upsert_irs_record(record)

            upserted  += 1
            processed += 1

            if processed % 100 == 0 or processed == total:
                print(f"  [{processed:,}/{total:,}]  upserted={upserted:,}  failed={failed}")

    skipped = len(missing)
    return upserted, failed, skipped


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch IRS 990 data directly from IRS (primary source, public domain)"
    )
    parser.add_argument(
        "--years", nargs="+", type=int, default=DEFAULT_YEARS, metavar="YEAR",
        help=f"IRS submission years to search (default: {DEFAULT_YEARS})",
    )
    parser.add_argument(
        "--limit", type=int,
        help="Process only this many EINs (for testing)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse XMLs and print results without writing to DB",
    )
    parser.add_argument(
        "--refresh-index", action="store_true",
        help="Re-download cached index CSV files",
    )
    args = parser.parse_args()

    db.init_db()

    print("CD Command Center — IRS 990 Primary Source Fetch")
    print(f"  Source:  IRS TEOS XML (apps.irs.gov/pub/epostcard/990/xml)")
    print(f"  Method:  HTTP range requests — no full ZIP downloads")
    if args.dry_run:
        print("  Mode:    DRY RUN (no DB writes)")
    print()

    upserted, failed, skipped = fetch_990_from_irs(
        index_years=args.years,
        limit=args.limit,
        dry_run=args.dry_run,
        refresh_index=args.refresh_index,
    )

    print()
    if args.dry_run:
        print(f"DRY RUN — {upserted:,} records would be written")
        return

    conn = db.get_connection()
    cur  = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM irs_990")
    total_rows = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM irs_990 WHERE data_source = 'IRS'")
    from_irs   = cur.fetchone()[0]
    conn.close()

    print(f"Done.")
    print(f"  Upserted this run:              {upserted:,}")
    print(f"  Failed / unreadable:            {failed:,}")
    print(f"  Not found in IRS index:         {skipped:,}")
    print(f"  Total irs_990 rows:             {total_rows:,}")
    print(f"  Sourced from IRS directly:      {from_irs:,}")


if __name__ == "__main__":
    main()
