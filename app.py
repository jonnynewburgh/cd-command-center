"""
app.py — CD Command Center: unified GIS-style dashboard.

Single-page layout with layer toggles, global search, and comparison.
Run with: streamlit run app.py
"""

import streamlit as st
import pandas as pd
from streamlit_folium import st_folium

import db
from utils.maps import make_unified_map
from utils.export import df_to_csv_bytes
from utils.geo import geocode_address, filter_by_radius

# ---------------------------------------------------------------------------
# Page config + init
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="CD Command Center",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)

db.init_db()

# Initialize session state for comparison
if "compare_items" not in st.session_state:
    st.session_state["compare_items"] = []

# ---------------------------------------------------------------------------
# Sidebar: Global search + Layer toggles + Filters
# ---------------------------------------------------------------------------

st.sidebar.title("CD Command Center")

# --- Global search ---
search_query = st.sidebar.text_input(
    "Search",
    placeholder="School, project, CDE, city...",
    help="Search across schools, NMTC projects, and CDEs",
)

st.sidebar.markdown("---")

# --- Layer toggles ---
st.sidebar.markdown("**Data Layers**")
show_schools = st.sidebar.checkbox("Schools", value=True)
show_nmtc_projects = st.sidebar.checkbox("NMTC Projects", value=True)
show_cde = st.sidebar.checkbox("CDE Allocations", value=False)

st.sidebar.markdown("---")

# --- Shared filters ---
st.sidebar.markdown("**Filters**")

# State filter — combine states from all data sources
all_states = sorted(set(db.get_school_states() + db.get_census_tract_states()))
selected_states = st.sidebar.multiselect(
    "State(s)",
    options=all_states,
    default=[],
    help="Leave empty to show all states",
)

# --- School filters (shown when Schools layer is on) ---
if show_schools:
    st.sidebar.markdown("**School Filters**")

    # Charter vs all
    school_type_filter = st.sidebar.radio(
        "School type",
        ["All public schools", "Charter schools only", "Traditional public only"],
        index=0,
    )

    selected_status = st.sidebar.multiselect(
        "Status",
        ["Open", "Closed", "Pending"],
        default=["Open"],
    )

    # Enrollment — with 2,000+ endpoint
    enrollment_range = st.sidebar.radio(
        "Enrollment",
        ["All", "Under 500", "500 – 2,000", "2,000+"],
        index=0,
        horizontal=True,
    )

    # Risk tier (charter schools only)
    if school_type_filter != "Traditional public only":
        st.sidebar.markdown("**Survival Risk**")
        risk_low = st.sidebar.checkbox("Low risk", value=True, key="risk_low")
        risk_med = st.sidebar.checkbox("Medium risk", value=True, key="risk_med")
        risk_high = st.sidebar.checkbox("High risk", value=True, key="risk_high")
        risk_unknown = st.sidebar.checkbox("Unknown", value=True, key="risk_unk")

    # FRL filter
    frl_threshold = st.sidebar.slider(
        "Min % Free/Reduced Lunch", 0, 100, 0,
        help="Show only schools with at least this % FRL",
    )

# --- NMTC filters (shown when NMTC layer is on) ---
if show_nmtc_projects or show_cde:
    st.sidebar.markdown("**NMTC Filters**")

    st.sidebar.markdown("Eligibility Tier")
    tier_deep = st.sidebar.checkbox("Deep Distress", value=True, key="t_deep")
    tier_severe = st.sidebar.checkbox("Severely Distressed", value=True, key="t_severe")
    tier_lic = st.sidebar.checkbox("LIC", value=True, key="t_lic")

    min_poverty = st.sidebar.slider("Min poverty rate (%)", 0, 100, 0, key="pov_slider")

# --- Radius search ---
st.sidebar.markdown("---")
st.sidebar.markdown("**Radius Search** (optional)")
radius_address = st.sidebar.text_input(
    "Address or place",
    placeholder="e.g. 250 W 55th St, New York NY",
)
radius_miles = st.sidebar.slider("Radius (miles)", 1, 100, 25)

# ---------------------------------------------------------------------------
# Load data based on active layers and search
# ---------------------------------------------------------------------------

# If search is active, use search results instead of normal queries
search_active = bool(search_query and search_query.strip())
search_results = None
if search_active:
    search_results = db.search_all(search_query)

# Build filter args
state_filter = selected_states if selected_states else None

# --- Schools data ---
schools_df = pd.DataFrame()
if show_schools:
    if search_active and search_results:
        schools_df = search_results["schools"]
    else:
        # Determine charter filter
        charter_only = False
        if school_type_filter == "Charter schools only":
            charter_only = True

        # Enrollment range
        min_enroll = None
        max_enroll = None
        if enrollment_range == "Under 500":
            max_enroll = 499
        elif enrollment_range == "500 – 2,000":
            min_enroll = 500
            max_enroll = 2000
        elif enrollment_range == "2,000+":
            min_enroll = 2000

        # Risk tiers
        risk_tiers = None
        if school_type_filter != "Traditional public only":
            risk_tiers = []
            if risk_low:
                risk_tiers.append("Low")
            if risk_med:
                risk_tiers.append("Medium")
            if risk_high:
                risk_tiers.append("High")
            if risk_unknown:
                risk_tiers.append("Unknown")

        schools_df = db.get_schools(
            states=state_filter,
            min_enrollment=min_enroll,
            max_enrollment=max_enroll,
            risk_tiers=risk_tiers if risk_tiers else None,
            school_status=selected_status if selected_status else None,
            charter_only=charter_only,
        )

        # Filter traditional-only
        if school_type_filter == "Traditional public only" and "is_charter" in schools_df.columns:
            schools_df = schools_df[schools_df["is_charter"] == 0]

        # Apply FRL filter
        if frl_threshold > 0 and not schools_df.empty:
            schools_df = schools_df[schools_df["pct_free_reduced_lunch"].fillna(0) >= frl_threshold]

# --- NMTC Projects data ---
projects_df = pd.DataFrame()
if show_nmtc_projects:
    if search_active and search_results:
        projects_df = search_results["projects"]
    else:
        projects_df = db.get_nmtc_projects(states=state_filter)

# --- CDE data ---
cde_df = pd.DataFrame()
if show_cde:
    if search_active and search_results:
        cde_df = search_results["cdes"]
    else:
        cde_df = db.get_cde_allocations(states=state_filter)

# --- Census tract data (for metrics, not map markers) ---
tracts_df = pd.DataFrame()
if show_nmtc_projects or show_cde:
    selected_tiers = []
    if tier_deep:
        selected_tiers.append("Deep Distress")
    if tier_severe:
        selected_tiers.append("Severely Distressed")
    if tier_lic:
        selected_tiers.append("LIC")

    tracts_df = db.get_census_tracts(
        states=state_filter,
        eligibility_tiers=selected_tiers if selected_tiers else None,
        min_poverty_rate=min_poverty if min_poverty > 0 else None,
    )

# --- Geocode + radius filter ---
geocoded = None
if radius_address and radius_address.strip():
    geocoded = geocode_address(radius_address.strip())

    if geocoded:
        st.sidebar.success(
            f"Found: {geocoded.get('matched_address', radius_address)}\n\n"
            f"Tract: {geocoded.get('census_tract_id', '—')}"
        )
        if not schools_df.empty:
            schools_df = filter_by_radius(
                schools_df, geocoded["lat"], geocoded["lon"], radius_miles,
            )
        if not projects_df.empty:
            projects_df = filter_by_radius(
                projects_df, geocoded["lat"], geocoded["lon"], radius_miles,
            )
    else:
        st.sidebar.warning("Could not geocode that address.")

# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------

st.title("CD Command Center")

if search_active:
    st.info(f"Showing search results for: **{search_query}**")

# --- Summary metrics ---
col1, col2, col3, col4 = st.columns(4)

if show_schools:
    col1.metric("Schools", f"{len(schools_df):,}")
    if not schools_df.empty:
        open_count = (schools_df["school_status"] == "Open").sum() if "school_status" in schools_df.columns else 0
        col2.metric("Open", f"{open_count:,}")
    else:
        col2.metric("Open", "0")

if show_nmtc_projects:
    col3.metric("NMTC Projects", f"{len(projects_df):,}")
    if not projects_df.empty and "qlici_amount" in projects_df.columns:
        total_qlici = projects_df["qlici_amount"].sum()
        col4.metric("Total QLICI", f"${total_qlici/1e6:.0f}M" if total_qlici and total_qlici > 0 else "$0")
    else:
        col4.metric("Total QLICI", "$0")

if not show_schools and not show_nmtc_projects:
    if show_cde:
        col1.metric("CDEs", f"{len(cde_df):,}")

# Tract metrics
if not tracts_df.empty:
    eligible = tracts_df[tracts_df.get("nmtc_eligibility_tier", pd.Series()) != "Not Eligible"].shape[0] if "nmtc_eligibility_tier" in tracts_df.columns else 0
    st.caption(f"Census tracts loaded: {len(tracts_df):,} | NMTC eligible: {eligible:,}")

# --- No data notice ---
if schools_df.empty and projects_df.empty and cde_df.empty:
    st.info(
        "No data loaded yet. Run these commands to load data:\n\n"
        "```\n"
        "python etl/fetch_nces_schools.py --states TX\n"
        "python etl/load_census_tracts.py --states TX\n"
        "```"
    )

# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

st.markdown("---")

# Determine map params
map_lat = geocoded["lat"] if geocoded else None
map_lon = geocoded["lon"] if geocoded else None
map_zoom = 10 if geocoded else (7 if selected_states and len(selected_states) == 1 else 5)

# Only build map if there's data to show
has_map_data = (
    (not schools_df.empty and show_schools) or
    (not projects_df.empty and show_nmtc_projects)
)

if has_map_data:
    unified_map = make_unified_map(
        schools_df=schools_df if show_schools else None,
        projects_df=projects_df if show_nmtc_projects else None,
        tracts_df=tracts_df,
        center_lat=map_lat,
        center_lon=map_lon,
        zoom=map_zoom,
    )
    st_folium(unified_map, width="100%", height=500, returned_objects=[])

# ---------------------------------------------------------------------------
# Survival Model Explanation (for charter school users)
# ---------------------------------------------------------------------------

if show_schools:
    with st.expander("How is the Survival Score calculated?"):
        st.markdown("""
The survival score estimates the probability (0–100%) that a charter school
remains open over the next few years. It is **only calculated for charter schools**.

**Current method: Rule-based heuristic** (no trained model yet)

| Factor | Effect on Score |
|--------|----------------|
| Enrollment > 500 | +8% |
| Enrollment > 200 | +4% |
| Enrollment < 100 | -5% |
| Open < 3 years | -10% (young schools fail more) |
| Open > 10 years | +6% |
| FRL > 80% | -5% |
| LEA score > 80 | +4% |
| LEA score < 50 | -5% |

**Risk tiers:**
- **Low** (score >= 65%): School appears stable
- **Medium** (40–65%): Some risk factors present
- **High** (< 40%): Multiple risk factors or already closed

Scores will improve when a trained model is built from historical closure data.
Traditional public schools do not receive survival scores.
        """)

# ---------------------------------------------------------------------------
# Data table — switchable between Schools / Projects / CDEs
# ---------------------------------------------------------------------------

st.markdown("---")

# Determine which tables to show options for
table_options = []
if show_schools and not schools_df.empty:
    table_options.append("Schools")
if show_nmtc_projects and not projects_df.empty:
    table_options.append("NMTC Projects")
if show_cde and not cde_df.empty:
    table_options.append("CDE Allocations")

if table_options:
    # Table selector
    active_table = st.radio(
        "Showing",
        table_options,
        horizontal=True,
        label_visibility="collapsed",
    )

    # Quick table filter
    table_filter = st.text_input(
        "Filter table",
        placeholder="Type to filter rows...",
        label_visibility="collapsed",
    )

    if active_table == "Schools":
        display_df = schools_df.copy()

        # Column selection
        display_cols = {
            "school_name": "School",
            "state": "State",
            "city": "City",
            "enrollment": "Enrollment",
            "is_charter": "Charter",
            "school_status": "Status",
            "survival_score": "Survival Score",
            "survival_risk_tier": "Risk",
            "pct_free_reduced_lunch": "% FRL",
            "pct_black": "% Black",
            "pct_hispanic": "% Hispanic",
            "accountability_score": "LEA Score",
            "lea_name": "LEA",
            "census_tract_id": "Census Tract",
        }
        show_cols = [c for c in display_cols if c in display_df.columns]
        display_df = display_df[show_cols].rename(columns=display_cols)

        # Format is_charter as Yes/No
        if "Charter" in display_df.columns:
            display_df["Charter"] = display_df["Charter"].map({1: "Yes", 0: "No", None: "—"})

        # Apply table filter
        if table_filter:
            mask = display_df.apply(
                lambda row: row.astype(str).str.contains(table_filter, case=False).any(), axis=1
            )
            display_df = display_df[mask]

        st.dataframe(display_df, use_container_width=True, height=400)

        # Export
        csv_bytes = df_to_csv_bytes(display_df)
        st.download_button(
            "Download Schools CSV", data=csv_bytes,
            file_name="schools_filtered.csv", mime="text/csv",
        )

    elif active_table == "NMTC Projects":
        display_df = projects_df.copy()
        proj_cols = {
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
        show_cols = [c for c in proj_cols if c in display_df.columns]
        display_df = display_df[show_cols].rename(columns=proj_cols)

        if table_filter:
            mask = display_df.apply(
                lambda row: row.astype(str).str.contains(table_filter, case=False).any(), axis=1
            )
            display_df = display_df[mask]

        st.dataframe(display_df, use_container_width=True, height=400)

        csv_bytes = df_to_csv_bytes(display_df)
        st.download_button(
            "Download Projects CSV", data=csv_bytes,
            file_name="nmtc_projects.csv", mime="text/csv",
        )

    elif active_table == "CDE Allocations":
        display_df = cde_df.copy()
        cde_cols = {
            "cde_name": "CDE Name",
            "state": "State",
            "city": "City",
            "allocation_amount": "Allocation Amount",
            "allocation_year": "Year",
            "round_number": "Round",
            "service_areas": "Service Areas",
        }
        show_cols = [c for c in cde_cols if c in display_df.columns]
        display_df = display_df[show_cols].rename(columns=cde_cols)

        if table_filter:
            mask = display_df.apply(
                lambda row: row.astype(str).str.contains(table_filter, case=False).any(), axis=1
            )
            display_df = display_df[mask]

        st.dataframe(display_df, use_container_width=True, height=400)

        csv_bytes = df_to_csv_bytes(display_df)
        st.download_button(
            "Download CDE CSV", data=csv_bytes,
            file_name="cde_allocations.csv", mime="text/csv",
        )

# ---------------------------------------------------------------------------
# Comparison panel
# ---------------------------------------------------------------------------

compare_items = st.session_state.get("compare_items", [])

if show_schools and not schools_df.empty:
    st.markdown("---")
    st.subheader("Compare")

    # Let user pick schools to compare
    school_names = schools_df["school_name"].dropna().tolist()
    if school_names:
        selected_for_compare = st.multiselect(
            "Select schools to compare (up to 4)",
            options=school_names[:500],  # limit options for performance
            max_selections=4,
            help="Pick 2–4 schools to see them side by side",
        )

        if len(selected_for_compare) >= 2:
            compare_df = schools_df[schools_df["school_name"].isin(selected_for_compare)]

            # Build comparison table
            compare_metrics = {
                "school_name": "School",
                "state": "State",
                "city": "City",
                "enrollment": "Enrollment",
                "is_charter": "Charter",
                "school_status": "Status",
                "survival_score": "Survival Score",
                "survival_risk_tier": "Risk Tier",
                "pct_free_reduced_lunch": "% FRL",
                "pct_black": "% Black",
                "pct_hispanic": "% Hispanic",
                "census_tract_id": "Census Tract",
                "lea_name": "LEA",
            }
            show_metrics = [c for c in compare_metrics if c in compare_df.columns]
            compare_display = compare_df[show_metrics].rename(columns=compare_metrics).T
            compare_display.columns = [f"School {i+1}" for i in range(len(compare_display.columns))]

            st.dataframe(compare_display, use_container_width=True)

# ---------------------------------------------------------------------------
# Census tract summary (when NMTC layers active)
# ---------------------------------------------------------------------------

if (show_nmtc_projects or show_cde) and not tracts_df.empty:
    st.markdown("---")
    st.subheader("Census Tract Summary")

    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Total Tracts", f"{len(tracts_df):,}")

    if "nmtc_eligibility_tier" in tracts_df.columns:
        eligible = (tracts_df["nmtc_eligibility_tier"] != "Not Eligible").sum()
        deep = (tracts_df["nmtc_eligibility_tier"] == "Deep Distress").sum()
        severe = (tracts_df["nmtc_eligibility_tier"] == "Severely Distressed").sum()
        t2.metric("NMTC Eligible", f"{eligible:,}")
        t3.metric("Severely Distressed", f"{severe:,}")
        t4.metric("Deep Distress", f"{deep:,}")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown(
    """
    **Build phases:**
    - ✅ **Phase 1**: Schools + LEA accountability data
    - ✅ **Phase 2**: NMTC tracker + census demographics
    - ✅ **Phase 2.5**: Unified GIS layout + all public schools
    - ⬜ Phase 3: FQHC / health centers
    - ⬜ Phase 4: ECE facility data
    - ⬜ Phase 5: 990 / philanthropy data
    - ⬜ Phase 6: Auth + PostgreSQL migration
    """
)
