"""
pages/01_charter_schools.py — Charter School deal origination view.

This is the primary Phase 1 tool. It shows:
  - A map of charter schools colored by survival risk
  - Sidebar filters (state, enrollment, risk tier, demographics)
  - A data table of filtered results
  - Census tract NMTC eligibility overlay
  - CSV export of filtered results
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_folium import st_folium

import db
from utils.maps import make_charter_school_map
from utils.export import df_to_csv_bytes, format_school_export
from utils.geo import filter_by_radius, geocode_address

st.set_page_config(page_title="Charter Schools | CD Command Center", layout="wide")

st.title("Charter Schools")
st.markdown(
    "Map and filter charter schools by location, enrollment, demographics, and survival risk. "
    "Click any marker for school details."
)

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

st.sidebar.header("Filters")

# State filter
available_states = db.get_charter_school_states()
selected_states = st.sidebar.multiselect(
    "State(s)",
    options=available_states,
    default=[],
    help="Leave empty to show all states",
)

# School status
status_options = ["Open", "Closed", "Pending"]
selected_status = st.sidebar.multiselect(
    "School Status",
    options=status_options,
    default=["Open"],
)

# Risk tier
st.sidebar.markdown("**Survival Risk Tier**")
show_low = st.sidebar.checkbox("Low risk", value=True)
show_medium = st.sidebar.checkbox("Medium risk", value=True)
show_high = st.sidebar.checkbox("High risk", value=True)
show_unknown = st.sidebar.checkbox("Unknown", value=True)

selected_tiers = []
if show_low:
    selected_tiers.append("Low")
if show_medium:
    selected_tiers.append("Medium")
if show_high:
    selected_tiers.append("High")
if show_unknown:
    selected_tiers.append("Unknown")

# Enrollment range
st.sidebar.markdown("**Enrollment**")
enroll_min = st.sidebar.number_input("Min enrollment", min_value=0, value=0, step=50)
enroll_max = st.sidebar.number_input(
    "Max enrollment", min_value=0, value=5000, step=50
)

# FRL filter (poverty proxy)
frl_threshold = st.sidebar.slider(
    "Min % Free/Reduced Lunch",
    min_value=0,
    max_value=100,
    value=0,
    help="Show only schools where at least this % of students qualify for FRL",
)

# Geography radius search
st.sidebar.markdown("---")
st.sidebar.markdown("**Radius Search** (optional)")
radius_address = st.sidebar.text_input(
    "Address or place",
    placeholder="e.g. 123 Main St, Chicago IL",
)
radius_miles = st.sidebar.slider("Radius (miles)", min_value=1, max_value=100, value=25)

# ---------------------------------------------------------------------------
# Load and filter data
# ---------------------------------------------------------------------------

# Build filter args for db query
filter_args = {
    "states": selected_states if selected_states else None,
    "min_enrollment": enroll_min if enroll_min > 0 else None,
    "max_enrollment": enroll_max if enroll_max < 5000 else None,
    "risk_tiers": selected_tiers if selected_tiers else None,
    "school_status": selected_status if selected_status else None,
}

with st.spinner("Loading charter school data..."):
    df = db.get_charter_schools(**filter_args)

# Apply FRL filter (not in db.py query because it's a simple threshold)
if frl_threshold > 0:
    df = df[df["pct_free_reduced_lunch"].fillna(0) >= frl_threshold]

# Apply radius search if an address was entered
geocoded = None
if radius_address.strip():
    with st.spinner(f"Geocoding '{radius_address}'..."):
        geocoded = geocode_address(radius_address.strip())

    if geocoded:
        st.sidebar.success(
            f"Found: {geocoded.get('matched_address', radius_address)}\n\n"
            f"Census tract: {geocoded.get('census_tract_id', '—')}"
        )
        df = filter_by_radius(
            df,
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
col1.metric("Schools shown", f"{len(df):,}")
col2.metric("Open", f"{(df['school_status'] == 'Open').sum():,}" if not df.empty else "0")
col3.metric(
    "High risk",
    f"{(df['survival_risk_tier'] == 'High').sum():,}" if not df.empty else "0",
)
if not df.empty and "enrollment" in df.columns:
    total_enrollment = df["enrollment"].sum()
    col4.metric(
        "Total enrollment",
        f"{int(total_enrollment):,}" if not pd.isna(total_enrollment) else "—",
    )

# ---------------------------------------------------------------------------
# Map view
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Map View")

if df.empty:
    st.info("No schools match the current filters.")
else:
    map_center_lat = geocoded["lat"] if geocoded else None
    map_center_lon = geocoded["lon"] if geocoded else None
    zoom = 10 if geocoded else 5

    school_map = make_charter_school_map(
        df,
        center_lat=map_center_lat,
        center_lon=map_center_lon,
        zoom=zoom,
    )

    # st_folium renders the interactive map in Streamlit
    st_folium(school_map, width="100%", height=500, returned_objects=[])

# ---------------------------------------------------------------------------
# NMTC Eligibility summary for filtered schools
# ---------------------------------------------------------------------------

if not df.empty and "census_tract_id" in df.columns:
    tract_ids = df["census_tract_id"].dropna().unique().tolist()
    if tract_ids:
        # Quick summary — load census tract data for the visible schools
        # (We do this via a db call to keep SQL out of this file)
        nmtc_eligible_tracts = db.get_nmtc_eligible_tracts(
            states=selected_states if selected_states else None
        )

        if not nmtc_eligible_tracts.empty:
            eligible_ids = set(nmtc_eligible_tracts["census_tract_id"].tolist())
            df["in_nmtc_tract"] = df["census_tract_id"].isin(eligible_ids)
            nmtc_count = df["in_nmtc_tract"].sum()

            st.markdown(
                f"**{nmtc_count:,} of {len(df):,}** schools shown are in NMTC-eligible census tracts."
            )

# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Distributions")

if not df.empty:
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.markdown("**Survival Score Distribution**")
        if "survival_score" in df.columns and df["survival_score"].notna().any():
            fig = px.histogram(
                df[df["survival_score"].notna()],
                x="survival_score",
                nbins=20,
                color="survival_risk_tier",
                color_discrete_map={
                    "Low": "#2ca02c",
                    "Medium": "#ff7f0e",
                    "High": "#d62728",
                    "Unknown": "#7f7f7f",
                },
                labels={"survival_score": "Survival Score", "survival_risk_tier": "Risk"},
            )
            fig.update_layout(height=300, margin=dict(t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("No survival score data available.")

    with chart_col2:
        st.markdown("**Enrollment Distribution**")
        if "enrollment" in df.columns and df["enrollment"].notna().any():
            fig = px.histogram(
                df[df["enrollment"].notna()],
                x="enrollment",
                nbins=25,
                color="survival_risk_tier",
                color_discrete_map={
                    "Low": "#2ca02c",
                    "Medium": "#ff7f0e",
                    "High": "#d62728",
                    "Unknown": "#7f7f7f",
                },
                labels={"enrollment": "Enrollment", "survival_risk_tier": "Risk"},
            )
            fig.update_layout(height=300, margin=dict(t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("No enrollment data available.")

# ---------------------------------------------------------------------------
# Data table
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("School List")

if df.empty:
    st.info("No schools match the current filters.")
else:
    # Which columns to show in the table (keep it readable)
    display_cols = [
        "school_name", "state", "city", "enrollment", "school_status",
        "survival_score", "survival_risk_tier", "pct_free_reduced_lunch",
        "accountability_score", "lea_name", "census_tract_id",
    ]
    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(
        df[display_cols].rename(columns={
            "school_name": "School",
            "state": "State",
            "city": "City",
            "enrollment": "Enrollment",
            "school_status": "Status",
            "survival_score": "Survival Score",
            "survival_risk_tier": "Risk Tier",
            "pct_free_reduced_lunch": "% FRL",
            "accountability_score": "LEA Score",
            "lea_name": "LEA",
            "census_tract_id": "Census Tract",
        }),
        use_container_width=True,
        height=400,
    )

    # CSV export
    export_df = format_school_export(df)
    csv_bytes = df_to_csv_bytes(export_df)
    st.download_button(
        label="Download CSV",
        data=csv_bytes,
        file_name="charter_schools_filtered.csv",
        mime="text/csv",
    )
