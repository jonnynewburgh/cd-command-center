"""
utils/pdf_extractor.py — Extract financial data from audit and 990 PDF documents.

Uses pdfplumber to extract text from PDFs, then regex patterns to find key
financial statement line items. Designed for nonprofit audit reports and
IRS Form 990 PDFs.

WHY regex over a structured parser:
  Nonprofit audit PDFs have no standard format — each CPA firm uses different
  layouts and terminology. Regex pattern matching on extracted text is more
  robust than trying to parse table structures, though it will miss some items.
  That's why the app also lets the user manually override any extracted value.

COVERAGE:
  - Balance sheet / statement of financial position:
      cash and cash equivalents, total current assets (if stated),
      total current liabilities, total liabilities,
      unrestricted net assets, total net assets
  - Statement of cash flows:
      net cash from operating activities
  - Income statement:
      total revenue, total expenses

The extractor tries multiple label variants per field since different auditors
use different terminology (e.g., "cash equivalents" vs "cash and cash equivalents").

Returns a dict with field names matching the financial_ratios table schema.
Missing fields are None (not zero) so the caller can distinguish "not found"
from "actually zero".
"""

import re
import json
import os
from typing import Optional

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False


# ---------------------------------------------------------------------------
# Label patterns for each financial line item we want to extract.
# Each entry is (canonical_name, [regex_patterns_to_try]).
# Patterns are tried in order; first match wins.
# ---------------------------------------------------------------------------

FIELD_PATTERNS = [
    # --- Balance sheet items ---
    ("cash_and_equivalents", [
        r"cash\s+and\s+cash\s+equivalents?\s*[\$\s]*([0-9,]+)",
        r"cash\s+and\s+equivalents?\s*[\$\s]*([0-9,]+)",
        r"^cash\s*[\$\s]*([0-9,]+)",
    ]),
    ("total_current_assets", [
        r"total\s+current\s+assets?\s*[\$\s]*([0-9,]+)",
        r"current\s+assets?,?\s+total\s*[\$\s]*([0-9,]+)",
    ]),
    ("current_liabilities", [
        r"total\s+current\s+liabilities?\s*[\$\s]*([0-9,]+)",
        r"current\s+liabilities?,?\s+total\s*[\$\s]*([0-9,]+)",
    ]),
    ("total_liabilities", [
        r"total\s+liabilities?\s*[\$\s]*([0-9,]+)",
        r"liabilities?,?\s+total\s*[\$\s]*([0-9,]+)",
    ]),
    ("unrestricted_net_assets", [
        r"without\s+donor\s+restrictions?\s*[\$\s]*([0-9,()-]+)",
        r"unrestricted\s+net\s+assets?\s*[\$\s]*([0-9,()-]+)",
        r"net\s+assets?\s+without\s+restrictions?\s*[\$\s]*([0-9,()-]+)",
    ]),
    ("total_net_assets", [
        r"total\s+net\s+assets?\s*[\$\s]*([0-9,()-]+)",
        r"net\s+assets?,?\s+total\s*[\$\s]*([0-9,()-]+)",
    ]),
    # --- Cash flow statement ---
    ("operating_cash_flow", [
        r"net\s+cash\s+(?:provided\s+by|used\s+in)\s+operating\s+activities?\s*[\$\s]*([0-9,()-]+)",
        r"cash\s+flows?\s+from\s+operating\s+activities?.*?net\s*[\$\s]*([0-9,()-]+)",
        r"operating\s+activities?.*?net\s+cash\s*[\$\s]*([0-9,()-]+)",
    ]),
    # --- Income statement ---
    ("total_revenue", [
        r"total\s+(?:revenues?|support\s+and\s+revenues?|support,?\s+revenues?)\s*[\$\s]*([0-9,]+)",
        r"revenues?\s+and\s+support,?\s+total\s*[\$\s]*([0-9,]+)",
    ]),
    ("total_expenses", [
        r"total\s+(?:expenses?|operating\s+expenses?|functional\s+expenses?)\s*[\$\s]*([0-9,]+)",
        r"expenses?,?\s+total\s*[\$\s]*([0-9,]+)",
    ]),
]


# ---------------------------------------------------------------------------
# Core extraction functions
# ---------------------------------------------------------------------------

def extract_text_from_pdf(filepath: str) -> str:
    """
    Extract all text from a PDF file using pdfplumber.
    Returns empty string if pdfplumber is not installed or extraction fails.
    """
    if not HAS_PDFPLUMBER:
        return ""

    if not os.path.exists(filepath):
        return ""

    try:
        text_parts = []
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
        return "\n".join(text_parts)
    except Exception:
        return ""


def _parse_number(raw: str) -> Optional[float]:
    """
    Convert a raw number string from a financial statement to a float.
    Handles commas, parentheses (negatives), and leading/trailing whitespace.
    e.g. "1,234,567" → 1234567.0
         "(234,567)" → -234567.0
    """
    if raw is None:
        return None
    raw = raw.strip().replace(",", "")
    # Parentheses denote negative numbers in accounting
    if raw.startswith("(") and raw.endswith(")"):
        raw = "-" + raw[1:-1]
    try:
        return float(raw)
    except ValueError:
        return None


def extract_financials_from_text(text: str) -> dict:
    """
    Run all field patterns against the extracted PDF text.
    Returns a dict of {field_name: float_or_None}.

    Searches line by line (case-insensitive) for each pattern.
    For each field, the first matching pattern wins.
    """
    # Normalize whitespace but keep newlines as search boundaries
    lines = text.lower().replace("\t", " ")
    # Collapse multiple spaces (but not newlines)
    lines = re.sub(r"[ ]{2,}", " ", lines)

    results = {}

    for field_name, patterns in FIELD_PATTERNS:
        value = None
        for pattern in patterns:
            # Search across the full text (. doesn't match newline by default)
            match = re.search(pattern, lines, re.IGNORECASE | re.MULTILINE)
            if match:
                raw = match.group(1)
                value = _parse_number(raw)
                if value is not None:
                    break  # first successful match wins

        results[field_name] = value

    return results


def extract_from_pdf(filepath: str) -> dict:
    """
    High-level function: extract text from a PDF, then run financial patterns.
    Returns a dict with all field names (missing values are None).
    Also returns 'extraction_note' describing coverage.
    """
    if not HAS_PDFPLUMBER:
        return {
            "extraction_note": "pdfplumber not installed — install with: pip install pdfplumber"
        }

    text = extract_text_from_pdf(filepath)
    if not text:
        return {"extraction_note": "No text extracted from PDF (may be a scanned image — OCR not supported)"}

    results = extract_financials_from_text(text)
    found_count = sum(1 for v in results.values() if v is not None)
    total = len(FIELD_PATTERNS)
    results["extraction_note"] = f"Extracted {found_count}/{total} fields from PDF text"
    return results


def compute_acid_ratio_from_audit(extracted: dict) -> Optional[float]:
    """
    Compute acid ratio from audit-extracted data.
    Prefers: cash_and_equivalents / current_liabilities
    Falls back to: total_current_assets / current_liabilities (weaker approximation)
    """
    cl = extracted.get("current_liabilities")
    if cl is None or cl == 0:
        return None

    cash = extracted.get("cash_and_equivalents")
    if cash is not None:
        return round(cash / cl, 3)

    # Weaker fallback: use total current assets (includes receivables, inventory)
    tca = extracted.get("total_current_assets")
    if tca is not None:
        return round(tca / cl, 3)

    return None


def build_ratio_updates_from_audit(ein: str, fiscal_year: int, extracted: dict) -> dict:
    """
    Given audit-extracted financials, return a dict ready for upsert_financial_ratios().
    Only populates audit-quality fields — does not overwrite 990-based estimates.
    """
    acid_audit = compute_acid_ratio_from_audit(extracted)

    # Leverage = unrestricted net assets / total liabilities
    unrest = extracted.get("unrestricted_net_assets")
    total_liab = extracted.get("total_liabilities")
    leverage = None
    if unrest is not None and total_liab and total_liab > 0:
        leverage = round(unrest / total_liab, 3)

    return {
        "ein":                       ein,
        "fiscal_year":               fiscal_year,
        "acid_ratio_audit":          acid_audit,
        "current_liabilities_audit": extracted.get("current_liabilities"),
        "leverage_ratio":            leverage,          # audit overrides 990 estimate
        "cash_and_equivalents":      extracted.get("cash_and_equivalents"),
        "unrestricted_net_assets":   extracted.get("unrestricted_net_assets"),
        "total_liabilities":         extracted.get("total_liabilities"),
        "has_audit_data":            1,
        "data_source":               "Audit",
    }


def to_json(extracted: dict) -> str:
    """Serialize extracted data dict to a JSON string for storage in the documents table."""
    return json.dumps(
        {k: v for k, v in extracted.items() if k != "extraction_note"},
        default=str,
    )


def from_json(json_str: str) -> dict:
    """Deserialize extracted data from the documents table."""
    if not json_str:
        return {}
    try:
        return json.loads(json_str)
    except Exception:
        return {}
