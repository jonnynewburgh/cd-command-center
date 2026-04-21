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
