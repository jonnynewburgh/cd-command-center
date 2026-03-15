"""
pages/02_nmtc_tracker.py — NMTC Tracker and Census Tract Explorer.

Phase 2 tool for community development deal origination. Shows:
  - Census tract NMTC eligibility tiers (LIC / Severely Distressed / Deep Distress)
  - Map with eligible tracts and optional charter school overlay
  - Past NMTC projects from the CDFI Fund public data release
  - CDE allocation data for the selected geography
  - Geography search by address or state/county
  - CSV export of filtered results
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_folium import st_folium

import db
from utils.maps import make_nmtc_map
from utils.export import df_to_csv_bytes
from utils.geo import geocode_address, filter_by_radius

st.set_page_config(page_title="NMTC Tracker | CD Command Center", layout="wide")

st.title("NMTC Tracker")
st.markdown(
    "Explore NMTC-eligible census tracts by geography and distress level. "
    "Overlay charter schools and past NMTC projects to identify deal opportunities."
)

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

st.sidebar.header("Filters")

# State filter — only show states with census tract data
available_states = db.get_census_tract_states()
selected_states = st.sidebar.multiselect(
    "State(s)",
    options=available_states,
    default=[],
    help="Leave empty to show all states with data loaded",
)

# NMTC eligibility tier filter
st.sidebar.markdown("**NMTC Eligibility Tier**")
show_deep = st.sidebar.checkbox("Deep Distress (poverty ≥ 40% or MFI ≤ 50% AMI)", value=True)
show_severe = st.sidebar.checkbox("Severely Distressed (poverty ≥ 30% or MFI ≤ 60% AMI)", value=True)
show_lic = st.sidebar.checkbox("LIC — Low-Income Community (poverty ≥ 20% or MFI ≤ 80% AMI)", value=True)
show_not_eligible = st.sidebar.checkbox("Not Eligible", value=False)

selected_tiers = []
if show_deep:
    selected_tiers.append("Deep Distress")
if show_severe:
    selected_tiers.append("Severely Distressed")
if show_lic:
    selected_tiers.append("LIC")
if show_not_eligible:
    selected_tiers.append("Not Eligible")

# Poverty rate filter
st.sidebar.markdown("**Poverty Rate**")
min_poverty = st.sidebar.slider("Minimum poverty rate (%)", 0, 100, 0)

# Income filter
max_income = st.sidebar.number_input(
    "Max median family income ($)",
    min_value=0,
    value=200_000,
    step=5_000,
    help="Filter to tracts with median family income at or below this value",
)

# Asset class overlays
st.sidebar.markdown("---")
st.sidebar.markdown("**Overlays**")
show_schools = st.sidebar.checkbox(
    "Charter schools",
    value=True,
    help="Show charter schools on the map",
)
show_projects = st.sidebar.checkbox(
    "Past NMTC projects",
    value=True,
    help="Show CDFI Fund NMTC project locations (requires data load)",
)

# Geography search
st.sidebar.markdown("---")
st.sidebar.markdown("**Radius Search** (optional)")
radius_address = st.sidebar.text_input(
    "Address or place",
    placeholder="e.g. 250 W 55th St, New York NY",
)
radius_miles = st.sidebar.slider("Radius (miles)", 1, 100, 25)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

filter_args = {
    "states": selected_states if selected_states else None,
    "min_poverty_rate": min_poverty if min_poverty > 0 else None,
    "max_median_income": max_income if max_income < 200_000 else None,
    "eligibility_tiers": selected_tiers if selected_tiers else None,
}

with st.spinner("Loading census tract data..."):
    tracts_df = db.get_census_tracts(**filter_args)

# Load overlay data
schools_df = pd.DataFrame()
if show_schools:
    school_filter_states = selected_states if selected_states else None
    schools_df = db.get_charter_schools(states=school_filter_states, school_status=["Open"])

projects_df = pd.DataFrame()
if show_projects:
    projects_df = db.get_nmtc_projects(
        states=selected_states if selected_states else None
    )

# Geocode and apply radius search
geocoded = None
if radius_address.strip():
    with st.spinner(f"Geocoding '{radius_address}'..."):
        geocoded = geocode_address(radius_address.strip())

    if geocoded:
        st.sidebar.success(
            f"Found: {geocoded.get('matched_address', radius_address)}\n\n"
            f"Tract: {geocoded.get('census_tract_id', '—')}"
        )
        # Filter schools and projects by radius; tracts don't have lat/lon so we
        # filter them by the census_tract_id of the geocoded location instead
        if not schools_df.empty:
            schools_df = filter_by_radius(
                schools_df,
                center_lat=geocoded["lat"],
                center_lon=geocoded["lon"],
                radius_miles=radius_miles,
            )
        if not projects_df.empty:
            projects_df = filter_by_radius(
                projects_df,
                center_lat=geocoded["lat"],
                center_lon=geocoded["lon"],
                radius_miles=radius_miles,
            )
    else:
        st.sidebar.warning("Could not geocode that address. Showing all results.")

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------

col1, col2, col3, col4 = st.columns(4)

eligible_count = (
    tracts_df[tracts_df["nmtc_eligibility_tier"] != "Not Eligible"].shape[0]
    if not tracts_df.empty
    else 0
)
deep_count = (
    (tracts_df["nmtc_eligibility_tier"] == "Deep Distress").sum()
    if not tracts_df.empty
    else 0
)

col1.metric("Tracts shown", f"{len(tracts_df):,}")
col2.metric("NMTC eligible", f"{eligible_count:,}")
col3.metric("Deep Distress", f"{deep_count:,}")

if not projects_df.empty and "qlici_amount" in projects_df.columns:
    total_qlici = projects_df["qlici_amount"].sum()
    col4.metric("Total QLICI (shown)", f"${total_qlici/1e6:.0f}M" if total_qlici > 0 else "$0")
else:
    col4.metric("NMTC projects", f"{len(projects_df):,}")

# ---------------------------------------------------------------------------
# Data availability notices
# ---------------------------------------------------------------------------

if tracts_df.empty:
    st.info(
        "No census tract data loaded yet. Run:\n\n"
        "```\npython etl/load_census_tracts.py --states CA TX NY\n```"
    )

if show_projects and projects_df.empty:
    st.caption(
        "No NMTC project data loaded. Download the CDFI Fund public data file from "
        "https://www.cdfifund.gov/documents/data-releases and run "
        "`python etl/load_nmtc_data.py --file data/raw/nmtc_public_data_2024.xlsx`"
    )

# ---------------------------------------------------------------------------
# Map view
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Map View")

st.caption(
    "Census tracts with charter school and NMTC project overlays. "
    "Load census data to enable tract-level coloring."
)

map_center_lat = geocoded["lat"] if geocoded else None
map_center_lon = geocoded["lon"] if geocoded else None
map_zoom = 11 if geocoded else (7 if selected_states and len(selected_states) == 1 else 5)

nmtc_map = make_nmtc_map(
    tracts_df=tracts_df,
    schools_df=schools_df if show_schools else None,
    projects_df=projects_df if show_projects else None,
    center_lat=map_center_lat,
    center_lon=map_center_lon,
    zoom=map_zoom,
)

st_folium(nmtc_map, width="100%", height=500, returned_objects=[])

# ---------------------------------------------------------------------------
# NMTC eligibility tier distribution chart
# ---------------------------------------------------------------------------

if not tracts_df.empty and "nmtc_eligibility_tier" in tracts_df.columns:
    st.markdown("---")
    st.subheader("Eligibility Distribution")

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        tier_counts = tracts_df["nmtc_eligibility_tier"].value_counts().reset_index()
        tier_counts.columns = ["Tier", "Tracts"]
        tier_order = ["Deep Distress", "Severely Distressed", "LIC", "Not Eligible"]
        tier_counts["Tier"] = pd.Categorical(tier_counts["Tier"], categories=tier_order, ordered=True)
        tier_counts = tier_counts.sort_values("Tier")

        fig = px.bar(
            tier_counts,
            x="Tier",
            y="Tracts",
            color="Tier",
            color_discrete_map={
                "Deep Distress":        "#d62728",
                "Severely Distressed":  "#ff7f0e",
                "LIC":                  "#2ca02c",
                "Not Eligible":         "#cccccc",
            },
            labels={"Tracts": "Number of Tracts"},
        )
        fig.update_layout(height=300, margin=dict(t=10, b=10), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with chart_col2:
        st.markdown("**Poverty Rate Distribution**")
        if tracts_df["poverty_rate"].notna().any():
            fig2 = px.histogram(
                tracts_df[tracts_df["poverty_rate"].notna()],
                x="poverty_rate",
                nbins=30,
                color="nmtc_eligibility_tier",
                color_discrete_map={
                    "Deep Distress":        "#d62728",
                    "Severely Distressed":  "#ff7f0e",
                    "LIC":                  "#2ca02c",
                    "Not Eligible":         "#cccccc",
                },
                labels={"poverty_rate": "Poverty Rate (%)", "nmtc_eligibility_tier": "Tier"},
            )
            fig2.update_layout(height=300, margin=dict(t=10, b=10))
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.caption("No poverty rate data.")

# ---------------------------------------------------------------------------
# Eligible Tracts table
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Census Tracts")

if tracts_df.empty:
    st.info("No tracts to display.")
else:
    display_cols = {
        "census_tract_id": "Tract FIPS",
        "state": "State",
        "county_fips": "County FIPS",
        "tract_name": "Name",
        "poverty_rate": "Poverty Rate %",
        "median_family_income": "Median Family Income",
        "median_household_income": "Median HH Income",
        "unemployment_rate": "Unemployment %",
        "total_population": "Population",
        "nmtc_eligibility_tier": "NMTC Tier",
        "nmtc_eligibility_reason": "Reason",
    }
    show_cols = [c for c in display_cols.keys() if c in tracts_df.columns]

    st.dataframe(
        tracts_df[show_cols].rename(columns=display_cols),
        use_container_width=True,
        height=350,
    )

    csv_bytes = df_to_csv_bytes(tracts_df[show_cols].rename(columns=display_cols))
    st.download_button(
        "Download Tracts CSV",
        data=csv_bytes,
        file_name="nmtc_eligible_tracts.csv",
        mime="text/csv",
    )

# ---------------------------------------------------------------------------
# Charter schools in eligible tracts
# ---------------------------------------------------------------------------

if show_schools and not schools_df.empty and not tracts_df.empty:
    st.markdown("---")
    st.subheader("Charter Schools in Filtered Tracts")

    # Find schools whose census_tract_id is in our filtered tracts
    eligible_tract_ids = set(tracts_df["census_tract_id"].dropna().tolist())
    schools_in_tracts = schools_df[
        schools_df["census_tract_id"].isin(eligible_tract_ids)
    ]

    if schools_in_tracts.empty:
        st.caption(
            "No charter schools matched to these census tracts. "
            "This may be because charter schools don't have census_tract_id populated yet "
            "(run the NCES ETL with real data to populate this)."
        )
    else:
        st.markdown(
            f"**{len(schools_in_tracts):,}** charter schools are located in the filtered census tracts."
        )
        school_display = {
            "school_name": "School",
            "state": "State",
            "city": "City",
            "enrollment": "Enrollment",
            "school_status": "Status",
            "survival_risk_tier": "Risk",
            "pct_free_reduced_lunch": "% FRL",
            "census_tract_id": "Census Tract",
        }
        show = [c for c in school_display if c in schools_in_tracts.columns]
        st.dataframe(
            schools_in_tracts[show].rename(columns=school_display),
            use_container_width=True,
            height=300,
        )

# ---------------------------------------------------------------------------
# Past NMTC Projects table
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Past NMTC Projects")

if projects_df.empty:
    st.info(
        "No NMTC project data. Load the CDFI Fund public data file to see past investments."
    )
else:
    proj_display = {
        "project_name": "Project",
        "state": "State",
        "city": "City",
        "project_type": "Type",
        "qlici_amount": "QLICI Amount",
        "total_investment": "Total Investment",
        "fiscal_year": "Year",
        "cde_name": "CDE",
        "census_tract_id": "Census Tract",
        "jobs_created": "Jobs Created",
    }
    show_proj = [c for c in proj_display if c in projects_df.columns]
    st.dataframe(
        projects_df[show_proj].rename(columns=proj_display),
        use_container_width=True,
        height=350,
    )

    csv_proj = df_to_csv_bytes(projects_df[show_proj].rename(columns=proj_display))
    st.download_button(
        "Download Projects CSV",
        data=csv_proj,
        file_name="nmtc_projects.csv",
        mime="text/csv",
    )

# ---------------------------------------------------------------------------
# CDE Allocations table
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("CDE Allocations")

cde_df = db.get_cde_allocations(states=selected_states if selected_states else None)

if cde_df.empty:
    st.info(
        "No CDE allocation data. Load the CDFI Fund public data file to see CDEs."
    )
else:
    cde_display = {
        "cde_name": "CDE Name",
        "state": "State",
        "city": "City",
        "allocation_amount": "Allocation Amount",
        "allocation_year": "Year",
        "round_number": "Round",
        "service_areas": "Service Areas",
    }
    show_cde = [c for c in cde_display if c in cde_df.columns]
    st.dataframe(
        cde_df[show_cde].rename(columns=cde_display),
        use_container_width=True,
        height=300,
    )

    csv_cde = df_to_csv_bytes(cde_df[show_cde].rename(columns=cde_display))
    st.download_button(
        "Download CDE Allocations CSV",
        data=csv_cde,
        file_name="cde_allocations.csv",
        mime="text/csv",
    )
