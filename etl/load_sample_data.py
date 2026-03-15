"""
etl/load_sample_data.py — Load synthetic sample data for development/testing.

Run this to populate the database with fake-but-realistic data so you can
use the dashboard before you have real data files.

Usage:
    python etl/load_sample_data.py
"""

import sys
import os
import random
import sqlite3
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

random.seed(42)
np.random.seed(42)

# Sample data parameters
STATES = ["CA", "TX", "NY", "FL", "IL", "OH", "PA", "GA", "NC", "AZ"]
SCHOOL_SUFFIXES = [
    "Academy", "Charter School", "Preparatory Academy", "Leadership Academy",
    "STEM Academy", "Arts Academy", "Community School", "College Prep",
]
FIRST_NAMES = [
    "Sunrise", "Urban", "Excel", "Achieve", "Thrive", "Pioneer", "Horizon",
    "Mosaic", "Legacy", "Summit", "Venture", "Bridge", "Pathways", "Success",
    "Elevate", "Aspire", "Beacon", "Catalyst", "Discovery", "Empower",
]

# Approximate lat/lon ranges for each state (center + spread)
STATE_GEO = {
    "CA": (36.7, -119.4, 4.0, 5.0),
    "TX": (31.0, -99.0, 4.0, 6.0),
    "NY": (42.9, -75.5, 2.0, 2.5),
    "FL": (27.8, -81.5, 3.0, 3.0),
    "IL": (40.6, -89.2, 2.5, 2.0),
    "OH": (40.4, -82.7, 1.5, 1.5),
    "PA": (40.9, -77.8, 1.5, 2.0),
    "GA": (32.6, -83.4, 2.0, 2.0),
    "NC": (35.5, -79.4, 1.5, 2.5),
    "AZ": (34.0, -111.9, 3.0, 3.0),
}


def random_lat_lon(state):
    lat_center, lon_center, lat_spread, lon_spread = STATE_GEO[state]
    lat = lat_center + np.random.uniform(-lat_spread, lat_spread)
    lon = lon_center + np.random.uniform(-lon_spread, lon_spread)
    return round(lat, 6), round(lon, 6)


def fake_census_tract(state):
    """Generate a plausible-looking 11-digit FIPS tract ID."""
    state_fips = {"CA": "06", "TX": "48", "NY": "36", "FL": "12", "IL": "17",
                  "OH": "39", "PA": "42", "GA": "13", "NC": "37", "AZ": "04"}
    return f"{state_fips[state]}{random.randint(100, 999):03d}{random.randint(100000, 999999):06d}"


def generate_charter_schools(n=200):
    schools = []
    for i in range(n):
        state = random.choice(STATES)
        lat, lon = random_lat_lon(state)
        enrollment = int(np.random.lognormal(5.5, 0.7))  # realistic distribution
        enrollment = max(50, min(enrollment, 2000))

        year_opened = random.randint(2000, 2022)
        is_closed = random.random() < 0.15  # 15% closed
        year_closed = random.randint(year_opened + 1, 2023) if is_closed else None
        status = "Closed" if is_closed else "Open"

        # Demographic percentages that sum to ~100
        pct_black = round(np.random.beta(2, 5) * 100, 1)
        pct_hispanic = round(np.random.beta(2, 5) * 100, 1)
        pct_white = round(max(0, 100 - pct_black - pct_hispanic - random.uniform(5, 15)), 1)

        # Survival score — open schools tend to score higher
        base_score = 0.7 if not is_closed else 0.3
        survival_score = round(min(1.0, max(0.0, np.random.normal(base_score, 0.15))), 3)
        if survival_score >= 0.65:
            tier = "Low"
        elif survival_score >= 0.40:
            tier = "Medium"
        else:
            tier = "High"

        lea_id = f"LEA{state}{random.randint(1000, 9999)}"

        schools.append({
            "nces_id": f"NCES{i+1:06d}",
            "school_name": f"{random.choice(FIRST_NAMES)} {random.choice(SCHOOL_SUFFIXES)}",
            "lea_name": f"{state} Unified School District {random.randint(1, 50)}",
            "lea_id": lea_id,
            "state": state,
            "city": f"City{random.randint(1, 50)}",
            "address": f"{random.randint(100, 9999)} Main St",
            "zip_code": f"{random.randint(10000, 99999)}",
            "county": f"{state} County {random.randint(1, 20)}",
            "census_tract_id": fake_census_tract(state),
            "latitude": lat,
            "longitude": lon,
            "enrollment": enrollment,
            "grade_low": random.choice(["K", "1", "6", "9"]),
            "grade_high": random.choice(["5", "8", "12"]),
            "pct_free_reduced_lunch": round(np.random.beta(4, 2) * 100, 1),
            "pct_ell": round(np.random.beta(1.5, 5) * 40, 1),
            "pct_sped": round(np.random.beta(2, 8) * 25, 1),
            "pct_black": pct_black,
            "pct_hispanic": pct_hispanic,
            "pct_white": pct_white,
            "school_status": status,
            "year_opened": year_opened,
            "year_closed": year_closed,
            "survival_score": survival_score,
            "survival_risk_tier": tier,
            "data_year": 2023,
        })
    return schools


def generate_census_tracts(schools):
    """Generate census tract records for each unique tract in the schools data."""
    tract_ids = list({s["census_tract_id"] for s in schools})
    tracts = []
    for tract_id in tract_ids:
        state = next((s["state"] for s in schools if s["census_tract_id"] == tract_id), "CA")
        poverty_rate = round(np.random.beta(2, 7) * 60, 1)  # 0–60%
        median_income = int(np.random.lognormal(10.8, 0.4))  # around $49k
        is_eligible = 1 if poverty_rate >= 20 or median_income <= 60000 else 0
        reason = ""
        if poverty_rate >= 20:
            reason = "Poverty"
        elif median_income <= 60000:
            reason = "Income"

        tracts.append({
            "census_tract_id": tract_id,
            "state_fips": tract_id[:2],
            "county_fips": tract_id[:5],
            "tract_name": f"Tract {tract_id[5:]}",
            "total_population": random.randint(1000, 8000),
            "median_household_income": median_income,
            "poverty_rate": poverty_rate,
            "pct_minority": round(np.random.beta(3, 3) * 100, 1),
            "unemployment_rate": round(np.random.beta(2, 10) * 30, 1),
            "is_nmtc_eligible": is_eligible,
            "nmtc_eligibility_reason": reason,
            "county_name": f"{state} County {random.randint(1, 20)}",
            "state": state,
            "data_year": 2023,
        })
    return tracts


def generate_lea_records(schools):
    """Generate LEA accountability records for each unique LEA in the schools data."""
    lea_ids = list({s["lea_id"] for s in schools if s["lea_id"]})
    records = []
    for lea_id in lea_ids:
        state = next((s["state"] for s in schools if s["lea_id"] == lea_id), "CA")
        lea_name = next((s["lea_name"] for s in schools if s["lea_id"] == lea_id), "Unknown LEA")
        score = round(np.random.uniform(40, 100), 1)
        rating = "A" if score >= 85 else "B" if score >= 70 else "C" if score >= 55 else "D"
        records.append({
            "lea_id": lea_id,
            "lea_name": lea_name,
            "state": state,
            "accountability_score": score,
            "accountability_rating": rating,
            "proficiency_reading": round(np.random.uniform(20, 80), 1),
            "proficiency_math": round(np.random.uniform(20, 80), 1),
            "graduation_rate": round(np.random.uniform(60, 99), 1),
            "data_year": 2023,
        })
    return records


def main():
    print("Initializing database schema...")
    db.init_db()

    print("Generating sample charter schools...")
    schools = generate_charter_schools(200)

    print("Generating sample census tracts...")
    tracts = generate_census_tracts(schools)

    print("Generating sample LEA accountability records...")
    lea_records = generate_lea_records(schools)

    print(f"Loading {len(schools)} schools...")
    for s in schools:
        db.upsert_charter_school(s)

    print(f"Loading {len(tracts)} census tracts...")
    for t in tracts:
        db.upsert_census_tract(t)

    print(f"Loading {len(lea_records)} LEA records...")
    for r in lea_records:
        db.upsert_lea_accountability(r)

    print("Sample data load complete.")
    print(f"  Schools: {len(schools)}")
    print(f"  Census tracts: {len(tracts)}")
    print(f"  LEA records: {len(lea_records)}")


if __name__ == "__main__":
    main()
