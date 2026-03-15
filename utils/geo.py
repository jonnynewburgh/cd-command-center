"""
utils/geo.py — Geography helper functions.

Handles census tract lookups, distance calculations, and coordinate utilities.
"""

import math
import requests


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance in miles between two lat/lon points.

    Args:
        lat1, lon1: coordinates of point 1
        lat2, lon2: coordinates of point 2

    Returns:
        distance in miles
    """
    R = 3958.8  # Earth's radius in miles

    # Convert to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return round(R * c, 2)


def filter_by_radius(df, center_lat: float, center_lon: float, radius_miles: float):
    """
    Filter a DataFrame of facilities to those within radius_miles of a center point.

    The DataFrame must have 'latitude' and 'longitude' columns.
    Adds a 'distance_miles' column to the result.

    Args:
        df: pandas DataFrame with latitude and longitude columns
        center_lat, center_lon: center of the search radius
        radius_miles: maximum distance in miles

    Returns:
        Filtered DataFrame with a 'distance_miles' column, sorted by distance
    """
    import pandas as pd

    df = df.copy()

    # Drop rows without coordinates
    df = df.dropna(subset=["latitude", "longitude"])

    # Calculate distance for each row
    df["distance_miles"] = df.apply(
        lambda row: haversine_distance(center_lat, center_lon, row["latitude"], row["longitude"]),
        axis=1,
    )

    # Filter to radius
    df = df[df["distance_miles"] <= radius_miles]
    df = df.sort_values("distance_miles")

    return df


def geocode_address(address: str) -> dict:
    """
    Geocode an address using the Census Bureau's free geocoding API.
    Returns a dict with 'lat', 'lon', and 'census_tract_id' if found,
    or an empty dict if geocoding fails.

    Args:
        address: full street address string

    Returns:
        dict with keys: lat, lon, census_tract_id, matched_address
    """
    url = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
    params = {
        "address": address,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "layers": "Census Tracts",
        "format": "json",
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        matches = data.get("result", {}).get("addressMatches", [])
        if not matches:
            return {}

        match = matches[0]
        coords = match.get("coordinates", {})
        tracts = match.get("geographies", {}).get("Census Tracts", [])

        tract_id = ""
        if tracts:
            tract = tracts[0]
            # 11-digit FIPS: state (2) + county (3) + tract (6)
            tract_id = tract.get("STATE", "") + tract.get("COUNTY", "") + tract.get("TRACT", "")

        return {
            "lat": coords.get("y"),
            "lon": coords.get("x"),
            "census_tract_id": tract_id,
            "matched_address": match.get("matchedAddress", ""),
        }

    except Exception:
        return {}


def format_census_tract(raw_id: str) -> str:
    """
    Format a raw census tract FIPS code for display.
    e.g., '06037201300' → '06-037-201300'
    """
    if not raw_id or len(raw_id) < 11:
        return raw_id or ""
    return f"{raw_id[:2]}-{raw_id[2:5]}-{raw_id[5:]}"
