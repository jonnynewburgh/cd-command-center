"""
etl/load_nmtc_awards_2024_2025.py — Load CY 2024-2025 NMTC allocatees into cdfi_awards.

The CY 2024-2025 round was a combined two-year round: $10 billion in allocation
authority awarded to 142 CDEs, announced in December 2025. The CDFI Fund's
public Awards Database has not yet been updated with these entries, so we parse
the official Award Book PDF directly.

Source PDF (CY 2024-2025 Award Book):
  https://www.cdfifund.gov/system/files/2025-12/NMTC_CY24_25_Award_Book_Final.pdf

Loaded rows go into cdfi_awards with program='NMTC' and award_year=2025
(the round's announcement year — same convention used elsewhere in the repo
for combined rounds; see fetch_nmtc_award_books.AWARD_BOOKS).

Usage:
    python etl/load_nmtc_awards_2024_2025.py
    python etl/load_nmtc_awards_2024_2025.py --dry-run
"""

import argparse
import io
import os
import re
import sys

import pdfplumber
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

PDF_URL = "https://www.cdfifund.gov/system/files/2025-12/NMTC_CY24_25_Award_Book_Final.pdf"
AWARD_YEAR = 2025
ALLOCATEE_PAGE_RANGE = range(4, 7)  # 0-indexed; pages 5-7 in 1-indexed PDF

DOLLAR_RE = re.compile(r"\$([\d,]+)")
SA_RE = re.compile(
    r"(?:STATEWIDE\s*\(?OR?\s*TERRITORY-WIDE\)?|MULTI-STATE|MULTISTATE|NATIONAL|LOCAL|STATEWIDE)"
)
LEAD_SA_FRAGMENT = re.compile(
    r"^(?:TERRITORY-WIDE\)|STATEWIDE\s*\(OR|MULTI-STATE|NATIONAL|LOCAL)\s+"
)

# Tokens that mark "this token is part of an org name, not a city" — used to
# stop the city-extraction backtrack so we don't eat into the org name.
CORP_TOKENS = {
    "LLC", "L.L.C.", "INC", "INC.", "CORP", "CORPORATION", "COMPANY",
    "L.P.", "LP", "LLP", "PARTNERS", "FUND", "FOUNDATION", "TRUST",
}

# US cities whose names span multiple tokens. Without this list the parser
# would split "NEW YORK" into name=...NEW + city=YORK.
MULTI_WORD_CITY_HEADS = {
    "NEW", "LOS", "SAN", "ST.", "ST", "SAINT", "SALT", "KANSAS", "OKLAHOMA",
    "LITTLE", "BATON", "HUNT", "FORT", "FT", "FT.", "PORT", "WEST", "EAST",
    "NORTH", "SOUTH", "PALM", "WINSTON", "GRAND", "MOUNT", "MT", "MT.",
    "JERSEY", "VIRGINIA",
}
THREE_WORD_CITY_HEADS = {"SALT"}  # "SALT LAKE CITY"


def _extract_records_from_page(page) -> list[dict]:
    """
    Parse one Award Book page. The page renders allocatees in two side-by-side
    columns; pdfplumber's default text extraction interleaves them, which makes
    a flat regex parse unreliable. Instead we use word-level positions:
    group words by visual row (y), split each row into left/right by x, then
    walk each column's word stream and emit one record per $amount.
    """
    words = page.extract_words(use_text_flow=False)
    if not words:
        return []
    words.sort(key=lambda w: (w["top"], w["x0"]))

    # Group words into visual rows (within ~4px vertically).
    rows: list[list[dict]] = []
    cur: list[dict] = []
    cur_top: float | None = None
    for w in words:
        if cur_top is None or abs(w["top"] - cur_top) <= 4:
            cur.append(w)
            if cur_top is None:
                cur_top = w["top"]
        else:
            rows.append(cur)
            cur = [w]
            cur_top = w["top"]
    if cur:
        rows.append(cur)

    mid_x = page.width / 2

    records: list[dict] = []
    for col_idx in (0, 1):
        col_lines: list[str] = []
        for row in rows:
            col_words = [
                w for w in row
                if (w["x0"] < mid_x) == (col_idx == 0)
            ]
            col_words.sort(key=lambda w: w["x0"])
            if col_words:
                col_lines.append(" ".join(w["text"] for w in col_words))

        # Walk this column's lines; emit a record at each $amount boundary.
        buf: list[str] = []
        for line in col_lines:
            if not line.strip():
                continue
            buf.extend(line.split())
            joined = " ".join(buf)
            m = DOLLAR_RE.search(joined)
            if not m:
                continue
            amount = int(m.group(1).replace(",", ""))
            buf = []
            if amount < 1_000_000:
                continue
            before = joined[:m.start()].strip()
            # Strip leftover service-area fragment that wrapped from prior record
            before = LEAD_SA_FRAGMENT.sub("", before).strip()

            # Pull off the trailing service-area phrase
            sa_matches = list(SA_RE.finditer(before))
            if sa_matches:
                sa_m = sa_matches[-1]
                sa_text = sa_m.group(0)
                name_city_state = before[:sa_m.start()].strip()
            else:
                sa_text = ""
                name_city_state = before

            # State = last 2-letter token
            tokens = name_city_state.split()
            state_idx = None
            for idx in range(len(tokens) - 1, -1, -1):
                if re.fullmatch(r"[A-Z]{2}", tokens[idx]):
                    state_idx = idx
                    break
            if state_idx is None or state_idx < 1:
                continue
            state = tokens[state_idx]
            name_city = tokens[:state_idx]

            # City = trailing 1-3 tokens. Default to 1; expand for known
            # multi-word cities ("NEW YORK", "SALT LAKE CITY", etc).
            n = 1
            if len(name_city) >= 2 and name_city[-2].upper().rstrip(",") in MULTI_WORD_CITY_HEADS:
                n = 2
            if len(name_city) >= 3 and name_city[-3].upper().rstrip(",") in THREE_WORD_CITY_HEADS:
                n = 3
            # Don't let city eat into a corp suffix (LLC, INC, etc.)
            while n > 0 and any(t.upper().rstrip(",.") in CORP_TOKENS for t in name_city[-n:]):
                n -= 1
            if n == 0:
                continue
            city = " ".join(name_city[-n:])
            name = " ".join(name_city[:-n]).strip()
            name = re.sub(r"^[,.\s]+|[,.\s]+$", "", name).strip()

            if len(name) < 4 or name.upper() == "ALLOCATEE":
                continue

            records.append({
                "awardee_name": name,
                "awardee_city": city.title(),
                "awardee_state": state,
                "service_area": sa_text,
                "award_amount": float(amount),
            })
    return records


def parse_award_book(pdf_bytes: bytes) -> list[dict]:
    records: list[dict] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for pi in ALLOCATEE_PAGE_RANGE:
            if pi >= len(pdf.pages):
                break
            records.extend(_extract_records_from_page(pdf.pages[pi]))

    # Dedupe by (name, amount). Award Book lists each allocatee exactly once.
    seen: set[tuple[str, float]] = set()
    unique: list[dict] = []
    for r in records:
        key = (r["awardee_name"], r["award_amount"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)
    return unique


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="Parse and print, don't write to DB")
    args = ap.parse_args()

    print(f"Downloading CY 2024-2025 NMTC Award Book...")
    resp = requests.get(PDF_URL, timeout=60, headers={"User-Agent": "cd-command-center ETL"})
    resp.raise_for_status()
    print(f"  {len(resp.content) // 1024} KB")

    records = parse_award_book(resp.content)
    total = sum(r["award_amount"] for r in records)
    print(f"Parsed {len(records)} allocatees, total ${total:,.0f}")
    print("(Round actuals: 142 allocatees / $10.0B — gap is PDF parse loss on edge cases.)")

    if args.dry_run:
        for r in sorted(records, key=lambda x: x["awardee_name"]):
            print(f"  {r['awardee_name'][:55]:<55} {r['awardee_city'][:18]:<18} "
                  f"{r['awardee_state']} ${r['award_amount']:>12,.0f}")
        return

    db.init_db()
    loaded = 0
    for r in records:
        rec = {
            "awardee_name":  r["awardee_name"],
            "award_year":    AWARD_YEAR,
            "program":       "NMTC",
            "awardee_state": r["awardee_state"],
            "awardee_city":  r["awardee_city"],
            "award_amount":  r["award_amount"],
            "award_type":    "Allocation",
            "purpose":       f"Service area: {r['service_area']}" if r["service_area"] else None,
        }
        try:
            db.upsert_cdfi_award(rec)
            loaded += 1
        except Exception as e:
            print(f"  DB error for {r['awardee_name']}: {e}")

    print(f"Loaded {loaded} NMTC awards (year={AWARD_YEAR}) into cdfi_awards.")


if __name__ == "__main__":
    main()
