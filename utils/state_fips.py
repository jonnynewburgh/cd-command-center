"""
utils/state_fips.py — US state FIPS code lookups.

Consolidated from duplicate definitions across load_census_tracts.py,
fetch_enrollment_trends.py, and fetch_lea_accountability.py.
"""

# 2-letter abbreviation -> 2-digit FIPS string (Census API format)
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56",
    # Territories
    "PR": "72", "AS": "60", "GU": "66", "MP": "69", "VI": "78",
}

# Reverse map: FIPS string -> abbreviation
FIPS_STATE = {v: k for k, v in STATE_FIPS.items()}

# Integer version for APIs that expect int FIPS (e.g. Urban Institute)
STATE_FIPS_INT = {k: int(v) for k, v in STATE_FIPS.items()}


def state_to_fips(state: str) -> str:
    """Return 2-digit FIPS string for a state abbreviation, or empty string."""
    return STATE_FIPS.get(state.upper(), "")


def state_to_fips_int(state: str) -> int:
    """Return FIPS code as int for a state abbreviation, or 0."""
    return STATE_FIPS_INT.get(state.upper(), 0)


# ---------------------------------------------------------------------------
# State-name → 2-letter abbreviation. The CDFI Fund publishes NMTC project
# data with full state names ("Georgia"), while every other facility table
# in this repo uses 2-letter codes. The loader normalizes through this map
# so /nmtc/projects?states=GA matches the rest of the API.
# ---------------------------------------------------------------------------
STATE_NAME_TO_ABBREV = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "district of columbia": "DC", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
    "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    # Territories
    "puerto rico": "PR", "american samoa": "AS", "guam": "GU",
    "northern mariana islands": "MP",
    "us virgin islands": "VI", "u.s. virgin islands": "VI",
    "united states virgin islands": "VI", "virgin islands": "VI",
}


def state_name_to_abbrev(value):
    """Normalize a state value to its 2-letter abbreviation.

    Accepts 2-letter codes (returned as-is, uppercased) or full state names
    in any case ('georgia', 'GEORGIA', 'Georgia' all -> 'GA'). Returns the
    original value unchanged when it doesn't match either form, so caller
    code can decide whether to treat that as an error.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) == 2 and s.upper() in STATE_FIPS:
        return s.upper()
    return STATE_NAME_TO_ABBREV.get(s.lower(), s)
