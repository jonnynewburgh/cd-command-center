"""
app.py — CD Command Center: unified GIS-style dashboard.

Single-page layout with layer toggles, global search, and comparison.
Run with: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
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

# Initialize session state for comparison and site detail
if "compare_items" not in st.session_state:
    st.session_state["compare_items"] = []
if "detail_site" not in st.session_state:
    st.session_state["detail_site"] = None

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

# --- Layer toggles — alphabetical order, all on by default ---
st.sidebar.markdown("**Data Layers**")
show_ece = st.sidebar.checkbox("ECE / Child Care Centers", value=True)
show_fqhc = st.sidebar.checkbox("Health Centers (FQHCs)", value=True)
show_nmtc_projects = st.sidebar.checkbox("NMTC Projects", value=True)
show_cde = st.sidebar.checkbox("CDE Allocations", value=True)
show_schools = st.sidebar.checkbox("Schools", value=True)

st.sidebar.markdown("---")

# --- Shared filters ---
st.sidebar.markdown("**Filters**")

# State filter — combine states from all data sources
all_states = sorted(set(
    db.get_school_states() + db.get_census_tract_states()
    + db.get_fqhc_states() + db.get_ece_states()
))
selected_states = st.sidebar.multiselect(
    "State(s)",
    options=all_states,
    default=[],
    help="Leave empty to show all states",
)

# NMTC-eligible tracts filter — applies across all asset classes
nmtc_eligible_filter = st.sidebar.checkbox(
    "NMTC-eligible tracts only",
    value=False,
    key="nmtc_elig_filter",
    help="Show only facilities whose census tract qualifies as LIC, Severely Distressed, or Deep Distress",
)

oz_filter = st.sidebar.checkbox(
    "Opportunity Zones only",
    value=False,
    key="oz_filter",
    help="Show only facilities in Treasury-designated Opportunity Zones",
)

# --- School filters (collapsed by default so they don't dominate the sidebar) ---
if show_schools:
    school_expander = st.sidebar.expander("School filters", expanded=False)
    with school_expander:
        # Charter vs all
        school_type_filter = st.radio(
            "School type",
            ["All public schools", "Charter schools only", "Traditional public only"],
            index=0,
        )

        selected_status = st.multiselect(
            "Status",
            ["Open", "Closed", "Pending"],
            default=["Open"],
        )

        # Enrollment slider — 2,000 on the right means "no upper limit"
        enroll_range = st.slider(
            "Enrollment",
            min_value=0,
            max_value=2000,
            value=(0, 2000),
            step=50,
            help="Drag handles to filter by enrollment. Right edge (2,000) includes all larger schools.",
        )

        # Risk tier (charter schools only)
        if school_type_filter != "Traditional public only":
            st.markdown("**Survival Risk**")
            risk_low = st.checkbox("Low risk", value=True, key="risk_low")
            risk_med = st.checkbox("Medium risk", value=True, key="risk_med")
            risk_high = st.checkbox("High risk", value=True, key="risk_high")
            risk_unknown = st.checkbox("Unknown", value=True, key="risk_unk")

        # FRL filter
        frl_threshold = st.slider(
            "Min % Free/Reduced Lunch", 0, 100, 0,
            help="Show only schools with at least this % FRL",
        )

        # Survival Score explainer — tucked away here since it's charter-specific
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

# --- NMTC filters (shown when NMTC layer is on) ---
if show_nmtc_projects or show_cde:
    st.sidebar.markdown("**NMTC Filters**")

    st.sidebar.markdown("Eligibility Tier")
    tier_deep = st.sidebar.checkbox("Deep Distress", value=True, key="t_deep")
    tier_severe = st.sidebar.checkbox("Severely Distressed", value=True, key="t_severe")
    tier_lic = st.sidebar.checkbox("LIC", value=True, key="t_lic")

    min_poverty = st.sidebar.slider("Min poverty rate (%)", 0, 100, 0, key="pov_slider")

# --- FQHC filters ---
if show_fqhc:
    st.sidebar.markdown("**FQHC Filters**")
    fqhc_active_only = st.sidebar.checkbox("Active sites only", value=True, key="fqhc_active")

# --- ECE filters ---
if show_ece:
    st.sidebar.markdown("**ECE Filters**")
    ece_active_only = st.sidebar.checkbox("Active licenses only", value=True, key="ece_active")
    ece_subsidy_only = st.sidebar.checkbox(
        "Accepts subsidies (CCDF)", value=False, key="ece_subsidy"
    )
    ece_min_capacity = st.sidebar.number_input(
        "Min capacity", min_value=0, max_value=500, value=0, step=5, key="ece_capacity",
        help="Show only centers licensed for at least this many children",
    )

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

        # Enrollment range — slider value of 0 means no lower bound; 2000 means no upper bound
        min_enroll = enroll_range[0] if enroll_range[0] > 0 else None
        max_enroll = enroll_range[1] if enroll_range[1] < 2000 else None

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
            nmtc_eligible_only=nmtc_eligible_filter,
        )

        # Filter traditional-only
        if school_type_filter == "Traditional public only" and "is_charter" in schools_df.columns:
            schools_df = schools_df[schools_df["is_charter"] == 0]

        # Apply FRL filter
        if frl_threshold > 0 and not schools_df.empty:
            schools_df = schools_df[schools_df["pct_free_reduced_lunch"].fillna(0) >= frl_threshold]

        # Apply OZ filter
        if oz_filter and not schools_df.empty and "census_tract_id" in schools_df.columns:
            oz_tracts = {
                row["census_tract_id"]
                for _, row in db.get_census_tracts(
                    states=state_filter, nmtc_eligible_only=False
                ).iterrows()
                if row.get("is_opportunity_zone") == 1
            }
            schools_df = schools_df[schools_df["census_tract_id"].isin(oz_tracts)]

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

# --- FQHC data ---
fqhc_df = pd.DataFrame()
if show_fqhc:
    if search_active and search_results:
        fqhc_df = search_results.get("fqhc", pd.DataFrame())
    else:
        fqhc_df = db.get_fqhc(
            states=state_filter,
            active_only=fqhc_active_only,
        )

# --- ECE data ---
ece_df = pd.DataFrame()
if show_ece:
    if search_active and search_results:
        ece_df = search_results.get("ece", pd.DataFrame())
    else:
        ece_df = db.get_ece_centers(
            states=state_filter,
            active_only=ece_active_only,
            accepts_subsidies=True if ece_subsidy_only else None,
            min_capacity=ece_min_capacity if ece_min_capacity > 0 else None,
        )

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
        if not fqhc_df.empty:
            fqhc_df = filter_by_radius(
                fqhc_df, geocoded["lat"], geocoded["lon"], radius_miles,
            )
        if not ece_df.empty:
            ece_df = filter_by_radius(
                ece_df, geocoded["lat"], geocoded["lon"], radius_miles,
            )
    else:
        st.sidebar.warning("Could not geocode that address.")

# ---------------------------------------------------------------------------
# Detail render helper functions (used by Site Detail tab)
# ---------------------------------------------------------------------------

def _fmt_dollar(value):
    """Format a dollar amount: $1.2M for millions, $500K for thousands."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    try:
        v = float(value)
        if v >= 1_000_000:
            return f"${v/1_000_000:.1f}M"
        if v >= 1_000:
            return f"${v/1_000:.0f}K"
        return f"${v:,.0f}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_pct(value, decimals=1):
    """Format a percentage value."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    try:
        return f"{float(value):.{decimals}f}%"
    except (TypeError, ValueError):
        return str(value)


def _render_990_section(rec: dict):
    """
    Render a 990 financial health panel for a nonprofit facility.
    Pass the dict returned by db.get_990_for_school() or db.get_990_for_fqhc().
    Does nothing if the dict is empty (no 990 data found yet).
    """
    if not rec:
        st.caption(
            "No 990 data linked yet. Run `python etl/fetch_990_data.py` to fetch from ProPublica."
        )
        return

    revenue  = rec.get("total_revenue")
    expenses = rec.get("total_expenses")
    assets   = rec.get("total_assets")
    net      = rec.get("net_income")
    prog_exp = rec.get("program_service_expenses")
    tax_year = rec.get("tax_year")

    # Program expense ratio = program spending / total expenses
    # A healthy nonprofit typically spends 75%+ on programs
    prog_ratio = None
    if prog_exp and expenses and float(expenses) > 0:
        prog_ratio = float(prog_exp) / float(expenses)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Revenue",   _fmt_dollar(revenue))
    m2.metric("Total Expenses",  _fmt_dollar(expenses))
    m3.metric("Net Income",      _fmt_dollar(net))
    m4.metric("Total Assets",    _fmt_dollar(assets))

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown(f"**Program Expense Ratio:** {_fmt_pct(prog_ratio * 100) if prog_ratio is not None else '—'}")
        st.caption("Share of spending on mission programs (higher = better)")
    with col_r:
        st.markdown(f"**Officer Compensation:** {_fmt_dollar(rec.get('officer_compensation'))}")
        st.caption(f"**NTEE Code:** {rec.get('ntee_code') or '—'} · **Tax Year:** {tax_year or '—'}")

    if rec.get("filing_pdf_url"):
        st.markdown(f"[View 990 PDF on ProPublica ↗]({rec['filing_pdf_url']})")
    elif rec.get("ein"):
        st.markdown(
            f"[View on ProPublica ↗](https://projects.propublica.org/nonprofits/organizations/{rec['ein']})"
        )


def _render_census_context(census_tract_id):
    """Show census tract demographics, NMTC eligibility, OZ status, and EJ indicators."""
    if not census_tract_id:
        return
    tract = db.get_census_tract(census_tract_id)
    if not tract:
        st.caption(f"Census tract: {census_tract_id} (no demographic data loaded)")
        return
    st.markdown(f"**Census Tract {census_tract_id}**")

    # NMTC eligibility badge
    tier = tract.get("nmtc_eligibility_tier", "Unknown")
    tier_icons = {"Deep Distress": "🔴", "Severely Distressed": "🟠", "LIC": "🟡", "Not Eligible": "⚪"}
    badges = [f"NMTC: {tier_icons.get(tier, '⚪')} **{tier}**"]

    # Opportunity Zone badge
    if tract.get("is_opportunity_zone") == 1:
        badges.append("🌟 **Opportunity Zone**")

    st.markdown("  ·  ".join(badges))

    # Core demographic metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Poverty Rate", _fmt_pct(tract.get("poverty_rate")))
    c2.metric("Median HH Income", _fmt_dollar(tract.get("median_household_income")))
    pop = tract.get("total_population")
    c3.metric("Population", f"{int(pop):,}" if pop else "—")

    # 5-year trend (show if historical data loaded)
    if tract.get("poverty_rate_5yr_ago") is not None:
        pov_now = tract.get("poverty_rate")
        pov_then = tract.get("poverty_rate_5yr_ago")
        inc_chg = tract.get("income_change_pct")
        trend_parts = []
        if pov_now is not None and pov_then is not None:
            delta = pov_now - pov_then
            arrow = "↑" if delta > 0 else "↓"
            trend_parts.append(f"Poverty {arrow}{abs(delta):.1f}pp (5yr)")
        if inc_chg is not None:
            arrow = "↑" if inc_chg > 0 else "↓"
            trend_parts.append(f"Income {arrow}{abs(inc_chg):.1f}% (5yr)")
        if trend_parts:
            st.caption("5-year trend: " + "  ·  ".join(trend_parts))

    # EJScreen environmental indicators (collapsed to save space)
    ej_index = tract.get("ej_index")
    if ej_index is not None:
        with st.expander(f"Environmental Justice (EJ Index: {ej_index:.0f}th percentile)"):
            ej_cols = st.columns(3)
            ej_cols[0].metric("PM2.5", f"{tract['pm25_percentile']:.0f}th pct" if tract.get("pm25_percentile") is not None else "—")
            ej_cols[1].metric("Diesel", f"{tract['diesel_percentile']:.0f}th pct" if tract.get("diesel_percentile") is not None else "—")
            ej_cols[2].metric("Lead Paint", f"{tract['lead_paint_percentile']:.0f}th pct" if tract.get("lead_paint_percentile") is not None else "—")
            ej_cols2 = st.columns(2)
            ej_cols2[0].metric("Superfund", f"{tract['superfund_percentile']:.0f}th pct" if tract.get("superfund_percentile") is not None else "—")
            ej_cols2[1].metric("Wastewater", f"{tract['wastewater_percentile']:.0f}th pct" if tract.get("wastewater_percentile") is not None else "—")


def _render_nearby_facilities(nearby: dict):
    """Show nearby schools, FQHCs, ECE centers, and NMTC projects in collapsible sections."""
    schools_near = nearby.get("schools", pd.DataFrame())
    fqhc_near = nearby.get("fqhc", pd.DataFrame())
    ece_near = nearby.get("ece", pd.DataFrame())
    nmtc_near = nearby.get("nmtc", pd.DataFrame())

    if schools_near.empty and fqhc_near.empty and ece_near.empty and nmtc_near.empty:
        st.caption("No other facilities found within 1 mile.")
        return

    if not schools_near.empty:
        with st.expander(f"Nearby schools ({len(schools_near)})"):
            cols = [c for c in ["school_name", "city", "state", "is_charter", "school_status", "enrollment", "distance_miles"] if c in schools_near.columns]
            st.dataframe(schools_near[cols].rename(columns={"school_name": "School", "is_charter": "Charter", "school_status": "Status", "distance_miles": "Miles"}), use_container_width=True)

    if not fqhc_near.empty:
        with st.expander(f"Nearby health centers ({len(fqhc_near)})"):
            cols = [c for c in ["site_name", "health_center_name", "city", "state", "site_type", "distance_miles"] if c in fqhc_near.columns]
            st.dataframe(fqhc_near[cols].rename(columns={"site_name": "Site", "health_center_name": "Health Center", "distance_miles": "Miles"}), use_container_width=True)

    if not ece_near.empty:
        with st.expander(f"Nearby ECE centers ({len(ece_near)})"):
            cols = [c for c in ["provider_name", "facility_type", "city", "state", "capacity", "distance_miles"] if c in ece_near.columns]
            st.dataframe(ece_near[cols].rename(columns={"provider_name": "Provider", "facility_type": "Type", "distance_miles": "Miles"}), use_container_width=True)

    if not nmtc_near.empty:
        with st.expander(f"Nearby NMTC projects ({len(nmtc_near)})"):
            cols = [c for c in ["project_name", "cde_name", "project_type", "qlici_amount", "fiscal_year", "distance_miles"] if c in nmtc_near.columns]
            st.dataframe(nmtc_near[cols].rename(columns={"project_name": "Project", "cde_name": "CDE", "qlici_amount": "QLICI", "distance_miles": "Miles"}), use_container_width=True)


def _render_deal_signals(school: dict):
    """
    Render a compact row of deal-relevant flags at the top of a school detail view.

    This is the first thing a deal originator sees — designed to answer in 3 seconds:
    'Is there a deal here?' before reading the full detail.

    Signals shown:
    - NMTC eligibility of the census tract (biggest filter for CD finance)
    - Whether 990 financial data is linked (can we underwrite this operator?)
    - Charter survival risk tier (is this school stable?)
    - Whether the school is open (closed schools are usually not deals)
    """
    signals = []

    # NMTC tract eligibility — show from either the join column or a census_tract lookup
    nmtc_tier = school.get("nmtc_eligibility_tier")
    if not nmtc_tier and school.get("census_tract_id"):
        tract = db.get_census_tract(school["census_tract_id"])
        nmtc_tier = tract.get("nmtc_eligibility_tier") if tract else None

    if nmtc_tier and nmtc_tier != "Not Eligible":
        tier_icons = {"Deep Distress": "🔴", "Severely Distressed": "🟠", "LIC": "🟡"}
        signals.append(f"{tier_icons.get(nmtc_tier, '🟡')} **{nmtc_tier}** tract")
    elif nmtc_tier == "Not Eligible":
        signals.append("⚪ Not NMTC-eligible tract")
    else:
        signals.append("⚪ Tract eligibility unknown")

    # 990 data availability
    has_990 = school.get("has_990") == 1 or (school.get("ein") is not None and school.get("ein") != "")
    if has_990:
        signals.append("📄 990 data linked")
    elif school.get("is_charter") == 1:
        signals.append("📄 No 990 linked")

    # Survival risk (charters only)
    if school.get("is_charter") == 1:
        tier = school.get("survival_risk_tier", "Unknown")
        risk_icons = {"Low": "🟢", "Medium": "🟡", "High": "🔴", "Unknown": "⚫"}
        score = school.get("survival_score")
        score_str = f" ({score*100:.0f}%)" if score is not None and not (isinstance(score, float) and pd.isna(score)) else ""
        signals.append(f"{risk_icons.get(tier, '⚫')} {tier} survival risk{score_str}")

    if signals:
        st.markdown(" &nbsp;·&nbsp; ".join(signals))


def _render_school_detail(school_id):
    """Render a full detail view for a school given its integer primary key id."""
    school = db.get_school_by_id(school_id)
    if not school:
        st.error(f"School not found (id={school_id})")
        return

    is_charter = school.get("is_charter") == 1
    school_type_label = "Charter School" if is_charter else "Traditional Public School"
    status = school.get("school_status", "Unknown")
    status_icon = "✅" if status == "Open" else ("❌" if status == "Closed" else "⏳")

    st.markdown(f"## {school.get('school_name', 'Unknown School')}")
    st.markdown(f"🏫 {school_type_label} · {school.get('city', '—')}, {school.get('state', '—')} · {status_icon} {status}")
    if school.get("address"):
        st.caption(f"📍 {school['address']}, {school.get('city', '')}, {school.get('state', '')} {school.get('zip_code', '')}")

    # --- Deal Signals banner ---
    # Quick at-a-glance flags for deal origination: is this worth digging into?
    _render_deal_signals(school)

    st.markdown("---")

    grades = f"{school.get('grade_low', '?')} – {school.get('grade_high', '?')}"
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Enrollment", f"{int(school['enrollment']):,}" if school.get("enrollment") else "—")
    m2.metric("Grades", grades)
    m3.metric("% Free/Reduced Lunch", _fmt_pct(school.get("pct_free_reduced_lunch")))
    if is_charter:
        score = school.get("survival_score")
        tier = school.get("survival_risk_tier", "Unknown")
        score_str = f"{score*100:.0f}%" if score is not None and not (isinstance(score, float) and pd.isna(score)) else "—"
        m4.metric("Survival Score", score_str, delta=f"Risk: {tier}", delta_color="off")

    st.markdown("---")

    col_demo, col_acct = st.columns(2)
    with col_demo:
        st.markdown("**Student Demographics**")
        for label, key in [("% Black", "pct_black"), ("% Hispanic", "pct_hispanic"), ("% White", "pct_white"), ("% ELL", "pct_ell"), ("% SPED", "pct_sped")]:
            st.markdown(f"- **{label}:** {_fmt_pct(school.get(key))}")

    with col_acct:
        st.markdown("**LEA Accountability**")
        lea_id = school.get("lea_id")
        if lea_id:
            acct_df = db.get_lea_accountability(lea_ids=[lea_id])
            if not acct_df.empty:
                row = acct_df.sort_values("data_year", ascending=False).iloc[0]
                st.markdown(f"- **District:** {school.get('lea_name', lea_id)}")
                if row.get("accountability_rating"):
                    st.markdown(f"- **Rating:** {row['accountability_rating']}")
                if row.get("accountability_score") is not None:
                    st.markdown(f"- **Score:** {_fmt_pct(row.get('accountability_score'))}")
                st.markdown(f"- **Reading proficiency:** {_fmt_pct(row.get('proficiency_reading'))}")
                st.markdown(f"- **Math proficiency:** {_fmt_pct(row.get('proficiency_math'))}")
                st.markdown(f"- **Graduation rate:** {_fmt_pct(row.get('graduation_rate'))}")
                st.caption(f"Data year: {int(row['data_year']) if row.get('data_year') else '—'}")
            else:
                st.caption(f"No accountability data for {school.get('lea_name', lea_id)}. Load with `python etl/fetch_edfacts.py`")
        else:
            st.caption("No LEA ID available.")

    if is_charter:
        st.markdown("---")
        st.markdown("**990 / Financial Health**")
        nces_id = school.get("nces_id")
        ein = school.get("ein")
        if nces_id:
            _render_990_section(db.get_990_for_school(nces_id))
        else:
            st.caption("No NCES ID — cannot look up 990.")

        # Operator profile — show all other schools run by same org if EIN is linked
        if ein:
            other_schools = db.get_operator_schools(ein)
            # Exclude the current school from the list
            if not other_schools.empty and "id" in other_schools.columns:
                other_schools = other_schools[other_schools["id"] != school_id]
            if not other_schools.empty:
                st.markdown("---")
                st.markdown(f"**Other Schools by Same Operator** (EIN {ein})")
                op_cols = [c for c in ["school_name", "city", "state", "school_status", "enrollment", "survival_risk_tier"] if c in other_schools.columns]
                st.dataframe(
                    other_schools[op_cols].rename(columns={"school_name": "School", "school_status": "Status", "enrollment": "Enrollment", "survival_risk_tier": "Risk"}),
                    use_container_width=True,
                )

            # 990 multi-year trend chart
            history_df = db.get_990_history(ein)
            if not history_df.empty and len(history_df) > 1 and "tax_year" in history_df.columns:
                st.markdown("---")
                st.markdown("**Financial Trend (990 History)**")
                trend_cols = [c for c in ["total_revenue", "total_expenses", "net_income"] if c in history_df.columns]
                if trend_cols:
                    trend_df = history_df[["tax_year"] + trend_cols].sort_values("tax_year")
                    trend_melted = trend_df.melt(id_vars="tax_year", value_vars=trend_cols, var_name="Metric", value_name="Amount")
                    fig = px.line(trend_melted, x="tax_year", y="Amount", color="Metric",
                                  labels={"tax_year": "Tax Year", "Amount": "$ Amount"},
                                  title="Revenue / Expense Trend")
                    fig.update_layout(height=300, margin=dict(t=40, b=20))
                    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.markdown("**Census Tract Context**")
    _render_census_context(school.get("census_tract_id"))

    st.markdown("---")
    st.markdown("**Nearby Facilities** (within 1 mile)")
    lat, lon = school.get("latitude"), school.get("longitude")
    if lat and lon:
        nearby = db.get_nearby_facilities(float(lat), float(lon), radius_miles=1.0)
        # Exclude this school from the nearby schools list
        if not nearby["schools"].empty and "id" in nearby["schools"].columns:
            nearby["schools"] = nearby["schools"][nearby["schools"]["id"] != school_id]
        _render_nearby_facilities(nearby)
    else:
        st.caption("No coordinates available — cannot show nearby facilities.")


def _render_fqhc_detail(bhcmis_id):
    """Render a full detail view for a FQHC health center site."""
    site = db.get_fqhc_by_id(bhcmis_id)
    if not site:
        st.error(f"Health center not found (id={bhcmis_id})")
        return

    is_active = site.get("is_active") == 1
    status_label = "✅ Active" if is_active else "❌ Inactive"
    st.markdown(f"## {site.get('site_name') or site.get('health_center_name', 'Unknown Site')}")
    st.markdown(f"🏥 {site.get('health_center_type', 'Health Center')} · {site.get('city', '—')}, {site.get('state', '—')} · {status_label}")
    if site.get("site_address"):
        st.caption(f"📍 {site['site_address']}, {site.get('city', '')}, {site.get('state', '')} {site.get('zip_code', '')}")

    st.markdown("---")
    m1, m2, m3 = st.columns(3)
    m1.metric("Site Type", site.get("site_type", "—"))
    m2.metric("Total Patients", f"{int(site['total_patients']):,}" if site.get("total_patients") else "—")
    m3.metric("Patients ≤200% FPL", f"{int(site['patients_below_200pct_poverty']):,}" if site.get("patients_below_200pct_poverty") else "—")

    st.markdown("---")
    st.markdown("**990 / Financial Health**")
    _render_990_section(db.get_990_for_fqhc(bhcmis_id))

    ein = site.get("ein")
    if ein:
        # Other sites run by the same health center organization
        other_sites = db.get_operator_fqhc(ein)
        if not other_sites.empty and "bhcmis_id" in other_sites.columns:
            other_sites = other_sites[other_sites["bhcmis_id"] != bhcmis_id]
        if not other_sites.empty:
            st.markdown("---")
            st.markdown(f"**Other Sites — Same Organization** (EIN {ein})")
            op_cols = [c for c in ["site_name", "city", "state", "site_type", "total_patients"] if c in other_sites.columns]
            st.dataframe(
                other_sites[op_cols].rename(columns={"site_name": "Site", "site_type": "Type", "total_patients": "Patients"}),
                use_container_width=True,
            )

        # 990 multi-year trend chart
        history_df = db.get_990_history(ein)
        if not history_df.empty and len(history_df) > 1 and "tax_year" in history_df.columns:
            st.markdown("---")
            st.markdown("**Financial Trend (990 History)**")
            trend_cols = [c for c in ["total_revenue", "total_expenses", "net_income"] if c in history_df.columns]
            if trend_cols:
                trend_df = history_df[["tax_year"] + trend_cols].sort_values("tax_year")
                trend_melted = trend_df.melt(id_vars="tax_year", value_vars=trend_cols, var_name="Metric", value_name="Amount")
                fig = px.line(trend_melted, x="tax_year", y="Amount", color="Metric",
                              labels={"tax_year": "Tax Year", "Amount": "$ Amount"},
                              title="Revenue / Expense Trend")
                fig.update_layout(height=300, margin=dict(t=40, b=20))
                st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.markdown("**Census Tract Context**")
    _render_census_context(site.get("census_tract_id"))

    st.markdown("---")
    st.markdown("**Nearby Facilities** (within 1 mile)")
    lat, lon = site.get("latitude"), site.get("longitude")
    if lat and lon:
        nearby = db.get_nearby_facilities(float(lat), float(lon), radius_miles=1.0)
        if not nearby["fqhc"].empty and "bhcmis_id" in nearby["fqhc"].columns:
            nearby["fqhc"] = nearby["fqhc"][nearby["fqhc"]["bhcmis_id"] != bhcmis_id]
        _render_nearby_facilities(nearby)
    else:
        st.caption("No coordinates available.")


def _render_ece_detail(license_id):
    """Render a full detail view for an ECE / child care center."""
    center = db.get_ece_by_id(license_id)
    if not center:
        st.error(f"ECE center not found (id={license_id})")
        return

    st.markdown(f"## {center.get('provider_name', 'Unknown Center')}")
    st.markdown(f"🧒 {center.get('facility_type', 'ECE Center')} · {center.get('city', '—')}, {center.get('state', '—')} · {center.get('license_status', '—')}")
    if center.get("address"):
        st.caption(f"📍 {center['address']}, {center.get('city', '')}, {center.get('state', '')} {center.get('zip_code', '')}")
    if center.get("operator_name") and center["operator_name"] != center.get("provider_name"):
        st.caption(f"Operator: {center['operator_name']}")

    st.markdown("---")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Licensed Capacity", center.get("capacity", "—"))
    m2.metric("Quality Rating", center.get("star_rating") or "—")
    m3.metric("Accepts Subsidies", "Yes" if center.get("accepts_subsidies") == 1 else "No")
    m4.metric("Ages Served", center.get("ages_served") or "—")

    st.markdown("---")
    st.markdown("**Census Tract Context**")
    _render_census_context(center.get("census_tract_id"))

    st.markdown("---")
    st.markdown("**Nearby Facilities** (within 1 mile)")
    lat, lon = center.get("latitude"), center.get("longitude")
    if lat and lon:
        nearby = db.get_nearby_facilities(float(lat), float(lon), radius_miles=1.0)
        if not nearby["ece"].empty and "license_id" in nearby["ece"].columns:
            nearby["ece"] = nearby["ece"][nearby["ece"]["license_id"] != license_id]
        _render_nearby_facilities(nearby)
    else:
        st.caption("No coordinates available.")


def _render_nmtc_detail(cdfi_project_id):
    """Render a full detail view for an NMTC project."""
    project = db.get_nmtc_project_by_id(cdfi_project_id)
    if not project:
        st.error(f"NMTC project not found (id={cdfi_project_id})")
        return

    st.markdown(f"## {project.get('project_name', 'Unknown Project')}")
    st.markdown(f"💰 {project.get('project_type', 'NMTC Project')} · {project.get('city', '—')}, {project.get('state', '—')} · FY {project.get('fiscal_year', '—')}")
    if project.get("address"):
        st.caption(f"📍 {project['address']}, {project.get('city', '')}, {project.get('state', '')} {project.get('zip_code', '')}")
    st.caption(f"CDE: {project.get('cde_name', '—')}")

    st.markdown("---")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("QLICI Amount", _fmt_dollar(project.get("qlici_amount")))
    m2.metric("Total Investment", _fmt_dollar(project.get("total_investment")))
    m3.metric("Jobs Created", project.get("jobs_created", "—"))
    m4.metric("Jobs Retained", project.get("jobs_retained", "—"))

    if project.get("project_description"):
        with st.expander("Project description"):
            st.markdown(project["project_description"])

    st.markdown("---")
    st.markdown("**Census Tract Context**")
    _render_census_context(project.get("census_tract_id"))

    cde_name = project.get("cde_name")
    if cde_name:
        st.markdown("---")
        st.markdown(f"**Other projects by {cde_name}**")
        cde_projects = db.get_nmtc_projects_by_cde(cde_name)
        if not cde_projects.empty and "cdfi_project_id" in cde_projects.columns:
            cde_projects = cde_projects[cde_projects["cdfi_project_id"] != cdfi_project_id]
        if not cde_projects.empty:
            show_cols = [c for c in ["project_name", "state", "city", "project_type", "qlici_amount", "fiscal_year"] if c in cde_projects.columns]
            st.dataframe(cde_projects[show_cols].rename(columns={"project_name": "Project", "qlici_amount": "QLICI", "fiscal_year": "FY"}), use_container_width=True)
        else:
            st.caption("No other projects found for this CDE.")

    # Peer comps — similar deals by project type and state, comparable QLICI size
    qlici = project.get("qlici_amount")
    project_type = project.get("project_type")
    state = project.get("state")
    if qlici and project_type and state:
        qlici_min = float(qlici) * 0.5
        qlici_max = float(qlici) * 2.0
        peers = db.get_peer_nmtc_projects(
            project_type=project_type,
            state=state,
            qlici_min=qlici_min,
            qlici_max=qlici_max,
            exclude_id=cdfi_project_id,
        )
        if not peers.empty:
            st.markdown("---")
            st.markdown(f"**Comparable Deals** ({project_type} in {state}, QLICI ½×–2× this deal)")
            peer_cols = [c for c in ["project_name", "cde_name", "city", "qlici_amount", "fiscal_year", "jobs_created"] if c in peers.columns]
            st.dataframe(
                peers[peer_cols].rename(columns={"project_name": "Project", "cde_name": "CDE", "qlici_amount": "QLICI", "fiscal_year": "FY", "jobs_created": "Jobs"}),
                use_container_width=True,
            )

    st.markdown("---")
    st.markdown("**Nearby Facilities** (within 1 mile)")
    lat, lon = project.get("latitude"), project.get("longitude")
    if lat and lon:
        nearby = db.get_nearby_facilities(float(lat), float(lon), radius_miles=1.0)
        if not nearby["nmtc"].empty and "cdfi_project_id" in nearby["nmtc"].columns:
            nearby["nmtc"] = nearby["nmtc"][nearby["nmtc"]["cdfi_project_id"] != cdfi_project_id]
        _render_nearby_facilities(nearby)
    else:
        st.caption("No coordinates available.")


# ---------------------------------------------------------------------------
# Main area — two tabs: Dashboard and Site Detail
# ---------------------------------------------------------------------------

st.title("CD Command Center")

tab_dashboard, tab_detail, tab_tools = st.tabs(["📊 Dashboard", "🔍 Site Detail", "🧮 Tools"])

with tab_dashboard:
    if search_active:
        st.info(f"Showing search results for: **{search_query}**")

    # --- Summary metrics — one count per asset class, equal weight ---
    m_ece, m_fqhc, m_nmtc, m_cde, m_schools = st.columns(5)
    m_ece.metric("ECE Centers", f"{len(ece_df):,}")
    m_fqhc.metric("Health Centers", f"{len(fqhc_df):,}")
    m_nmtc.metric("NMTC Projects", f"{len(projects_df):,}")
    m_cde.metric("CDE Allocations", f"{len(cde_df):,}")
    m_schools.metric("Schools", f"{len(schools_df):,}")

    # Tract metrics
    if not tracts_df.empty:
        eligible = tracts_df[tracts_df.get("nmtc_eligibility_tier", pd.Series()) != "Not Eligible"].shape[0] if "nmtc_eligibility_tier" in tracts_df.columns else 0
        st.caption(f"Census tracts loaded: {len(tracts_df):,} | NMTC eligible: {eligible:,}")

    # Per-layer empty state notices (only when that layer is explicitly toggled on but has no data)
    notices = []
    if show_fqhc and fqhc_df.empty:
        notices.append("**Health Centers:** No data loaded — run `python etl/fetch_fqhc.py --states CA` to fetch HRSA data.")
    if show_ece and ece_df.empty:
        notices.append("**ECE Centers:** No data loaded — download your state's licensing file and run `python etl/load_ece_data.py --file ... --state CA`.")
    if show_cde and cde_df.empty:
        notices.append("**CDE Allocations:** No data loaded — run `python etl/load_nmtc_data.py --file data/raw/nmtc_public_data_2024.xlsx`.")
    if notices:
        st.warning("\n\n".join(notices))

    # --- No data notice (all layers empty) ---
    if schools_df.empty and projects_df.empty and cde_df.empty and fqhc_df.empty and ece_df.empty:
        st.info(
            "No data loaded yet. Run these commands to get started:\n\n"
            "```\n"
            "python etl/fetch_nces_schools.py --states CA\n"
            "python etl/load_census_tracts.py --states CA\n"
            "python etl/fetch_fqhc.py --states CA\n"
            "```"
        )

    # -----------------------------------------------------------------------
    # Map
    # -----------------------------------------------------------------------

    st.markdown("---")

    map_lat = geocoded["lat"] if geocoded else None
    map_lon = geocoded["lon"] if geocoded else None
    map_zoom = 10 if geocoded else (7 if selected_states and len(selected_states) == 1 else 5)

    has_map_data = (
        (not schools_df.empty and show_schools) or
        (not projects_df.empty and show_nmtc_projects) or
        (not fqhc_df.empty and show_fqhc) or
        (not ece_df.empty and show_ece)
    )

    if has_map_data:
        unified_map = make_unified_map(
            schools_df=schools_df if show_schools else None,
            projects_df=projects_df if show_nmtc_projects else None,
            fqhc_df=fqhc_df if show_fqhc else None,
            ece_df=ece_df if show_ece else None,
            tracts_df=tracts_df,
            center_lat=map_lat,
            center_lon=map_lon,
            zoom=map_zoom,
        )
        st_folium(unified_map, width="100%", height=500, returned_objects=[])


    # -----------------------------------------------------------------------
    # Data table — unified view across all active asset classes
    # -----------------------------------------------------------------------

    st.markdown("---")

    # Build a unified table from all active datasets.
    # Each asset class contributes: Asset Type, Name, State, City, Key Metric.
    # Listed in this order so no single class dominates: ECE → FQHCs → NMTC → CDE → Schools.
    unified_frames = []

    if show_ece and not ece_df.empty:
        ece_names = ece_df.get("provider_name", pd.Series([""] * len(ece_df), index=ece_df.index))
        ece_cap = ece_df.get("capacity", pd.Series([None] * len(ece_df), index=ece_df.index))
        unified_frames.append(pd.DataFrame({
            "Asset Type": "ECE Center",
            "Name": ece_names,
            "State": ece_df.get("state", pd.Series([""] * len(ece_df), index=ece_df.index)),
            "City": ece_df.get("city", pd.Series([""] * len(ece_df), index=ece_df.index)),
            "Key Metric": ece_cap.apply(lambda x: f"{int(x):,} capacity" if pd.notna(x) and x else "—"),
            "NMTC Tier": ece_df.get("nmtc_eligibility_tier", pd.Series([""] * len(ece_df), index=ece_df.index)),
            "_id_type": "ece",
            "_id_val": ece_df.get("license_id", pd.Series([""] * len(ece_df), index=ece_df.index)).astype(str),
        }))

    if show_fqhc and not fqhc_df.empty:
        # Prefer site_name, fall back to health_center_name
        if "site_name" in fqhc_df.columns and "health_center_name" in fqhc_df.columns:
            fqhc_names = fqhc_df["site_name"].where(fqhc_df["site_name"].notna() & (fqhc_df["site_name"] != ""), fqhc_df["health_center_name"])
        else:
            fqhc_names = fqhc_df.get("site_name", fqhc_df.get("health_center_name", pd.Series([""] * len(fqhc_df), index=fqhc_df.index)))
        fqhc_patients = fqhc_df.get("total_patients", pd.Series([None] * len(fqhc_df), index=fqhc_df.index))
        unified_frames.append(pd.DataFrame({
            "Asset Type": "Health Center",
            "Name": fqhc_names,
            "State": fqhc_df.get("state", pd.Series([""] * len(fqhc_df), index=fqhc_df.index)),
            "City": fqhc_df.get("city", pd.Series([""] * len(fqhc_df), index=fqhc_df.index)),
            "Key Metric": fqhc_patients.apply(lambda x: f"{int(x):,} patients" if pd.notna(x) and x else "—"),
            "NMTC Tier": fqhc_df.get("nmtc_eligibility_tier", pd.Series([""] * len(fqhc_df), index=fqhc_df.index)),
            "_id_type": "fqhc",
            "_id_val": fqhc_df.get("bhcmis_id", pd.Series([""] * len(fqhc_df), index=fqhc_df.index)).astype(str),
        }))

    if show_nmtc_projects and not projects_df.empty:
        proj_qlici = projects_df.get("qlici_amount", pd.Series([None] * len(projects_df), index=projects_df.index))
        unified_frames.append(pd.DataFrame({
            "Asset Type": "NMTC Project",
            "Name": projects_df.get("project_name", pd.Series([""] * len(projects_df), index=projects_df.index)),
            "State": projects_df.get("state", pd.Series([""] * len(projects_df), index=projects_df.index)),
            "City": projects_df.get("city", pd.Series([""] * len(projects_df), index=projects_df.index)),
            "Key Metric": proj_qlici.apply(lambda x: f"${x/1e6:.1f}M QLICI" if pd.notna(x) and x else "—"),
            "NMTC Tier": projects_df.get("nmtc_eligibility_tier", pd.Series([""] * len(projects_df), index=projects_df.index)),
            "_id_type": "nmtc",
            "_id_val": projects_df.get("cdfi_project_id", pd.Series([""] * len(projects_df), index=projects_df.index)).astype(str),
        }))

    if show_cde and not cde_df.empty:
        cde_alloc = cde_df.get("allocation_amount", pd.Series([None] * len(cde_df), index=cde_df.index))
        unified_frames.append(pd.DataFrame({
            "Asset Type": "CDE Allocation",
            "Name": cde_df.get("cde_name", pd.Series([""] * len(cde_df), index=cde_df.index)),
            "State": cde_df.get("state", pd.Series([""] * len(cde_df), index=cde_df.index)),
            "City": cde_df.get("city", pd.Series([""] * len(cde_df), index=cde_df.index)),
            "Key Metric": cde_alloc.apply(lambda x: f"${x/1e6:.1f}M allocation" if pd.notna(x) and x else "—"),
            "NMTC Tier": "",
            "_id_type": "cde",
            "_id_val": cde_df.get("id", pd.Series([""] * len(cde_df), index=cde_df.index)).astype(str),
        }))

    if show_schools and not schools_df.empty:
        sch_enroll = schools_df.get("enrollment", pd.Series([None] * len(schools_df), index=schools_df.index))
        unified_frames.append(pd.DataFrame({
            "Asset Type": "School",
            "Name": schools_df.get("school_name", pd.Series([""] * len(schools_df), index=schools_df.index)),
            "State": schools_df.get("state", pd.Series([""] * len(schools_df), index=schools_df.index)),
            "City": schools_df.get("city", pd.Series([""] * len(schools_df), index=schools_df.index)),
            "Key Metric": sch_enroll.apply(lambda x: f"{int(x):,} students" if pd.notna(x) and x else "—"),
            "NMTC Tier": schools_df.get("nmtc_eligibility_tier", pd.Series([""] * len(schools_df), index=schools_df.index)),
            "_id_type": "school",
            "_id_val": schools_df.get("id", pd.Series([None] * len(schools_df), index=schools_df.index)).apply(
                lambda x: str(int(x)) if pd.notna(x) else ""
            ),
        }))

    if unified_frames:
        unified_df = pd.concat(unified_frames, ignore_index=True)

        # NMTC Eligible column — checkmark if the census tract qualifies
        unified_df["NMTC Eligible"] = unified_df["NMTC Tier"].isin(
            ["LIC", "Severely Distressed", "Deep Distress"]
        ).map({True: "✓", False: ""})

        # Asset type filter
        active_types = sorted(unified_df["Asset Type"].unique().tolist())
        selected_asset_types = st.multiselect(
            "Asset types shown",
            options=active_types,
            default=active_types,
            key="unified_type_filter",
            label_visibility="collapsed",
        )
        if selected_asset_types:
            unified_df = unified_df[unified_df["Asset Type"].isin(selected_asset_types)]

        # Text filter
        table_filter = st.text_input(
            "Filter table",
            placeholder="Type to filter rows...",
            label_visibility="collapsed",
        )
        if table_filter:
            search_cols = unified_df[["Asset Type", "Name", "State", "City", "Key Metric"]]
            mask = search_cols.apply(
                lambda row: row.astype(str).str.contains(table_filter, case=False).any(), axis=1
            )
            unified_df = unified_df[mask]

        # Display the unified table
        display_cols = [c for c in ["Asset Type", "Name", "State", "City", "Key Metric", "NMTC Eligible"] if c in unified_df.columns]
        st.dataframe(unified_df[display_cols], use_container_width=True, height=400)

        # CSV download
        csv_bytes = df_to_csv_bytes(unified_df[display_cols])
        st.download_button(
            "Download CSV", data=csv_bytes,
            file_name="cd_command_center_data.csv", mime="text/csv",
        )

        # View in Site Detail — pick any row from the filtered table
        if not unified_df.empty and "_id_type" in unified_df.columns:
            detail_labels = unified_df.apply(
                lambda r: f"{r['Asset Type']}: {r['Name']} ({r.get('City', '')}, {r.get('State', '')})",
                axis=1,
            ).tolist()[:300]
            id_types = unified_df["_id_type"].tolist()[:300]
            id_vals = unified_df["_id_val"].tolist()[:300]
            # Build a label → (type, id) map (last occurrence wins for duplicate names)
            detail_map = {label: (t, v) for label, t, v in zip(detail_labels, id_types, id_vals)}

            chosen_label = st.selectbox(
                "Select a row to view in Site Detail →",
                ["—"] + detail_labels,
                key="unified_detail_select",
                label_visibility="collapsed",
            )
            if chosen_label != "—" and chosen_label in detail_map:
                id_type, id_val = detail_map[chosen_label]
                if id_type and id_val:
                    id_final = int(id_val) if id_type == "school" else str(id_val)
                    st.session_state["detail_site"] = {"type": id_type, "id": id_final}
                    st.info("✓ Selected. Switch to the **Site Detail** tab to view full details.")

    # -----------------------------------------------------------------------
    # Comparison panel
    # -----------------------------------------------------------------------

    compare_items = st.session_state.get("compare_items", [])

    # Comparison panels — ECE, Health Centers, Schools (same order as layer toggles)
    if show_ece and not ece_df.empty:
        st.markdown("---")
        st.subheader("Compare ECE Centers")

        ece_names = ece_df["provider_name"].dropna().tolist()
        if ece_names:
            selected_ece = st.multiselect(
                "Select ECE centers to compare (up to 4)",
                options=ece_names[:500],
                max_selections=4,
                key="ece_compare",
            )

            if len(selected_ece) >= 2:
                ece_compare_df = ece_df[ece_df["provider_name"].isin(selected_ece)]
                ece_metrics = {
                    "provider_name": "Provider",
                    "operator_name": "Operator",
                    "state": "State",
                    "city": "City",
                    "facility_type": "Type",
                    "license_status": "Status",
                    "capacity": "Capacity",
                    "ages_served": "Ages",
                    "accepts_subsidies": "Subsidies",
                    "star_rating": "Quality Rating",
                    "census_tract_id": "Census Tract",
                }
                ece_show = [c for c in ece_metrics if c in ece_compare_df.columns]
                ece_display = ece_compare_df[ece_show].rename(columns=ece_metrics).T
                ece_display.columns = [f"Center {i+1}" for i in range(len(ece_display.columns))]
                st.dataframe(ece_display, use_container_width=True)

    if show_fqhc and not fqhc_df.empty:
        st.markdown("---")
        st.subheader("Compare Health Centers")

        hc_names = fqhc_df.apply(
            lambda r: r.get("site_name") or r.get("health_center_name", ""), axis=1
        ).dropna().tolist()

        if hc_names:
            selected_hc = st.multiselect(
                "Select health centers to compare (up to 4)",
                options=hc_names[:500],
                max_selections=4,
                key="hc_compare",
            )

            if len(selected_hc) >= 2:
                mask = (
                    fqhc_df.get("site_name", pd.Series()).isin(selected_hc) |
                    fqhc_df.get("health_center_name", pd.Series()).isin(selected_hc)
                )
                hc_compare_df = fqhc_df[mask]
                hc_metrics = {
                    "health_center_name": "Health Center",
                    "site_name": "Site",
                    "state": "State",
                    "city": "City",
                    "site_type": "Site Type",
                    "health_center_type": "HC Type",
                    "total_patients": "Total Patients",
                    "census_tract_id": "Census Tract",
                }
                hc_show = [c for c in hc_metrics if c in hc_compare_df.columns]
                hc_display = hc_compare_df[hc_show].rename(columns=hc_metrics).T
                hc_display.columns = [f"Site {i+1}" for i in range(len(hc_display.columns))]
                st.dataframe(hc_display, use_container_width=True)

    if show_schools and not schools_df.empty:
        st.markdown("---")
        st.subheader("Compare Schools")

        school_names = schools_df["school_name"].dropna().tolist()
        if school_names:
            selected_for_compare = st.multiselect(
                "Select schools to compare (up to 4)",
                options=school_names[:500],
                max_selections=4,
                help="Pick 2–4 schools to see them side by side",
            )

            if len(selected_for_compare) >= 2:
                compare_df = schools_df[schools_df["school_name"].isin(selected_for_compare)]
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

    # -----------------------------------------------------------------------
    # Census tract summary (when NMTC layers active)
    # -----------------------------------------------------------------------

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

    # -----------------------------------------------------------------------
    # Footer
    # -----------------------------------------------------------------------

    st.markdown("---")
    st.markdown(
        """
        **Build phases:**
        - ✅ **Phase 1**: Schools + LEA accountability data
        - ✅ **Phase 2**: NMTC tracker + census demographics
        - ✅ **Phase 2.5**: Unified GIS layout + all public schools
        - ✅ **Phase 3**: FQHC / health centers
        - ✅ **Phase 4**: ECE / child care facility data
        - ✅ **Phase 5**: 990 / philanthropy data
        - ✅ **Phase 5.5**: Deal analysis tools — OZ overlay, EJScreen, peer comps, operator profiles, pro forma calculator, gap analysis, CDFI directory, state programs
        - ⬜ Phase 6: Auth + PostgreSQL migration
        """
    )

# ---------------------------------------------------------------------------
# Site Detail tab
# ---------------------------------------------------------------------------

with tab_detail:
    st.markdown("### Site Detail")
    st.caption("Search for any school, health center, ECE center, or NMTC project to see full details.")

    # --- Search box ---
    detail_search = st.text_input(
        "Search for a site",
        placeholder="Type a school name, health center, ECE provider, or NMTC project...",
        key="detail_search_input",
    )

    if detail_search and detail_search.strip():
        results = db.search_all(detail_search.strip())

        # Flatten search results into a unified list with type labels
        options = []
        for school_row in results.get("schools", pd.DataFrame()).itertuples(index=False):
            name = getattr(school_row, "school_name", None) or "Unknown School"
            state = getattr(school_row, "state", "")
            city = getattr(school_row, "city", "")
            row_id = getattr(school_row, "id", None)
            if row_id is not None:
                options.append({"label": f"🏫 {name} — {city}, {state}", "type": "school", "id": int(row_id)})

        for proj_row in results.get("projects", pd.DataFrame()).itertuples(index=False):
            name = getattr(proj_row, "project_name", None) or "Unknown Project"
            state = getattr(proj_row, "state", "")
            proj_id = getattr(proj_row, "cdfi_project_id", None)
            if proj_id is not None:
                options.append({"label": f"💰 {name} — {state}", "type": "nmtc", "id": str(proj_id)})

        for hc_row in results.get("fqhc", pd.DataFrame()).itertuples(index=False):
            name = getattr(hc_row, "site_name", None) or getattr(hc_row, "health_center_name", None) or "Unknown Site"
            state = getattr(hc_row, "state", "")
            city = getattr(hc_row, "city", "")
            hc_id = getattr(hc_row, "bhcmis_id", None)
            if hc_id is not None:
                options.append({"label": f"🏥 {name} — {city}, {state}", "type": "fqhc", "id": str(hc_id)})

        for ece_row in results.get("ece", pd.DataFrame()).itertuples(index=False):
            name = getattr(ece_row, "provider_name", None) or "Unknown Center"
            state = getattr(ece_row, "state", "")
            city = getattr(ece_row, "city", "")
            lic_id = getattr(ece_row, "license_id", None)
            if lic_id is not None:
                options.append({"label": f"🧒 {name} — {city}, {state}", "type": "ece", "id": str(lic_id)})

        if options:
            option_labels = [o["label"] for o in options]
            chosen_label = st.selectbox("Select a site", option_labels, key="detail_site_select")
            chosen = next((o for o in options if o["label"] == chosen_label), None)
            if chosen:
                # Set session state whenever a selection is made
                st.session_state["detail_site"] = {"type": chosen["type"], "id": chosen["id"]}
        else:
            st.info("No results found. Try a different search term.")

    # --- Render selected site detail ---
    current_site = st.session_state.get("detail_site")
    if current_site:
        st.markdown("---")
        site_type = current_site.get("type")
        site_id = current_site.get("id")

        if site_type == "school":
            _render_school_detail(int(site_id))
        elif site_type == "fqhc":
            _render_fqhc_detail(str(site_id))
        elif site_type == "ece":
            _render_ece_detail(str(site_id))
        elif site_type == "nmtc":
            _render_nmtc_detail(str(site_id))
        else:
            st.error(f"Unknown site type: {site_type}")

        if st.button("Clear / search for another site", key="clear_detail"):
            st.session_state["detail_site"] = None
            st.rerun()
    elif not detail_search:
        st.info("Use the search box above, or select a site from the data tables in the Dashboard tab.")

# ---------------------------------------------------------------------------
# Tools tab — pro forma calculator, gap analysis, state programs, CDFI directory
# ---------------------------------------------------------------------------

with tab_tools:

    tool_tabs = st.tabs([
        "📐 NMTC Pro Forma",
        "🗺️ Service Gap Analysis",
        "🏦 CDFI Directory",
        "📋 State Programs",
    ])

    # -----------------------------------------------------------------------
    # NMTC Pro Forma Calculator
    # -----------------------------------------------------------------------

    with tool_tabs[0]:
        st.subheader("NMTC Structure Calculator")
        st.caption(
            "Estimate the federal tax credit, equity proceeds, and net borrower benefit "
            "for a New Markets Tax Credit deal."
        )

        with st.expander("How NMTC deals work", expanded=False):
            st.markdown("""
**The basic structure:**
1. A CDE receives an NMTC allocation from Treasury
2. A tax credit investor provides equity to the CDE (the CDE is the pass-through entity)
3. The investor earns 39% of the NMTC allocation in federal tax credits over 7 years
4. The CDE combines the equity with a leverage loan and makes a QLICI (Qualified Low-Income Community Investment) to the project
5. At the end of the 7-year compliance period, the investor exits and the leverage loan is paid off or forgiven

**Key terms:**
- **QLICI amount:** The total investment into the project (equity + leverage)
- **Credit amount:** 39% of QLICI, earned over 7 years (5% in years 1-3, 6% in years 4-7)
- **Equity price:** What the investor pays per dollar of credit (typically $0.75–$0.90)
- **Net benefit:** The "subsidy" the project receives — how much cheaper the NMTC financing is vs. market
            """)

        st.markdown("---")
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("**Inputs**")
            qlici_amount = st.number_input(
                "QLICI Amount ($M)",
                min_value=0.5, max_value=500.0, value=10.0, step=0.5,
                help="Total Qualified Low-Income Community Investment into the project",
            )
            equity_price = st.slider(
                "Equity price (¢ per $1 of credit)",
                min_value=0.60, max_value=1.00, value=0.85, step=0.01,
                format="$%.2f",
                help="What the tax credit investor pays per dollar of federal credit. Typically $0.75–$0.90.",
            )
            leverage_rate = st.slider(
                "Leverage loan interest rate (%)",
                min_value=0.0, max_value=10.0, value=4.5, step=0.25,
                help="Interest rate on the leverage loan that goes into the QLICI alongside investor equity",
            )
            cde_fee_pct = st.slider(
                "CDE fee (% of QLICI)",
                min_value=0.0, max_value=5.0, value=1.0, step=0.25,
                help="Fee the CDE charges for using its allocation and acting as pass-through entity",
            )
            leverage_pct = st.slider(
                "Leverage loan as % of QLICI",
                min_value=0, max_value=90, value=55, step=5,
                help="What share of the QLICI is funded by the leverage loan (vs. investor equity). Typically 55-70%.",
            )

        with col_right:
            st.markdown("**Outputs**")

            # Calculations
            qlici_dollars = qlici_amount * 1_000_000
            credit_amount = qlici_dollars * 0.39          # 39% NMTC credit rate
            equity_proceeds = credit_amount * equity_price # investor pays equity_price per credit dollar
            leverage_loan = qlici_dollars * (leverage_pct / 100)
            equity_from_investor = qlici_dollars - leverage_loan
            cde_fee = qlici_dollars * (cde_fee_pct / 100)
            annual_interest = leverage_loan * (leverage_rate / 100)
            # Net benefit = equity proceeds above par minus CDE fees
            net_benefit = equity_proceeds - cde_fee
            # Effective subsidy rate = net benefit / project cost
            subsidy_rate = (net_benefit / qlici_dollars * 100) if qlici_dollars > 0 else 0

            m1, m2 = st.columns(2)
            m1.metric("Federal Tax Credits", _fmt_dollar(credit_amount))
            m2.metric("Equity Proceeds", _fmt_dollar(equity_proceeds),
                      help="Credits × equity price — the cash the investor puts in upfront")

            m3, m4 = st.columns(2)
            m3.metric("Leverage Loan", _fmt_dollar(leverage_loan))
            m4.metric("Annual Interest Cost", _fmt_dollar(annual_interest))

            m5, m6 = st.columns(2)
            m5.metric("CDE Fee", _fmt_dollar(cde_fee))
            m6.metric("Net Benefit to Borrower", _fmt_dollar(net_benefit),
                      delta=f"{subsidy_rate:.1f}% effective subsidy",
                      delta_color="normal")

            st.markdown("---")
            st.markdown("**Deal Summary**")
            st.markdown(f"""
| Item | Amount |
|------|--------|
| QLICI (total project investment) | {_fmt_dollar(qlici_dollars)} |
| Leverage loan ({leverage_pct}%) | {_fmt_dollar(leverage_loan)} |
| Investor equity needed | {_fmt_dollar(equity_from_investor)} |
| Federal tax credits generated | {_fmt_dollar(credit_amount)} |
| Investor pays (equity price × credits) | {_fmt_dollar(equity_proceeds)} |
| CDE fee | {_fmt_dollar(cde_fee)} |
| **Net borrower benefit** | **{_fmt_dollar(net_benefit)}** |
| Annual leverage interest | {_fmt_dollar(annual_interest)} |
""")

    # -----------------------------------------------------------------------
    # Service Gap Analysis
    # -----------------------------------------------------------------------

    with tool_tabs[1]:
        st.subheader("Service Gap Analysis")
        st.caption(
            "Find census tracts with high poverty and no nearby facilities. "
            "Useful for identifying underserved markets for new facility investments."
        )

        gap_col1, gap_col2, gap_col3 = st.columns(3)
        with gap_col1:
            gap_asset_class = st.selectbox(
                "Asset class",
                options=["ece", "fqhc", "schools"],
                format_func=lambda x: {"ece": "ECE / Child Care Centers", "fqhc": "Health Centers (FQHC)", "schools": "Schools"}.get(x, x),
                key="gap_asset_class",
            )
        with gap_col2:
            gap_min_poverty = st.slider(
                "Min poverty rate (%)",
                min_value=10, max_value=50, value=20, step=5,
                key="gap_min_poverty",
                help="Only include tracts with at least this poverty rate",
            )
        with gap_col3:
            gap_states = st.multiselect(
                "State(s)",
                options=sorted(set(db.get_census_tract_states())),
                key="gap_states",
                help="Leave empty for all states (slow on large datasets)",
            )

        if st.button("Run Gap Analysis", key="run_gap_analysis"):
            with st.spinner("Analyzing..."):
                gaps_df = db.get_service_gaps(
                    states=gap_states if gap_states else None,
                    asset_class=gap_asset_class,
                    min_poverty_rate=float(gap_min_poverty),
                )

            if gaps_df.empty:
                st.info(
                    "No gaps found with current filters, or no census tract / facility data loaded. "
                    "Run `python etl/load_census_tracts.py` to load tract demographics."
                )
            else:
                asset_label = {"ece": "ECE centers", "fqhc": "health centers", "schools": "schools"}[gap_asset_class]
                st.success(f"Found {len(gaps_df):,} high-need tracts with zero {asset_label}.")

                # Summary metrics
                gm1, gm2, gm3 = st.columns(3)
                gm1.metric("Gap tracts", f"{len(gaps_df):,}")
                total_pop = gaps_df["total_population"].sum() if "total_population" in gaps_df.columns else 0
                gm2.metric("Population in gap tracts", f"{int(total_pop):,}" if total_pop else "—")
                oz_gaps = gaps_df[gaps_df.get("is_opportunity_zone", pd.Series(0, index=gaps_df.index)) == 1].shape[0] if "is_opportunity_zone" in gaps_df.columns else 0
                gm3.metric("Also in Opportunity Zone", f"{oz_gaps:,}")

                # Display table
                display_gap_cols = [c for c in [
                    "state", "county_name", "census_tract_id", "total_population",
                    "poverty_rate", "median_household_income", "nmtc_eligibility_tier",
                    "need_score"
                ] if c in gaps_df.columns]
                st.dataframe(
                    gaps_df[display_gap_cols].rename(columns={
                        "county_name": "County", "census_tract_id": "Tract",
                        "total_population": "Population", "poverty_rate": "Poverty %",
                        "median_household_income": "Median HH Income",
                        "nmtc_eligibility_tier": "NMTC Tier", "need_score": "Need Score",
                    }),
                    use_container_width=True, height=400,
                )

                csv_bytes = df_to_csv_bytes(gaps_df[display_gap_cols])
                st.download_button(
                    "Download gaps CSV", data=csv_bytes,
                    file_name=f"service_gaps_{gap_asset_class}.csv", mime="text/csv",
                )
        else:
            st.info("Set filters and click **Run Gap Analysis** to find underserved tracts.")

    # -----------------------------------------------------------------------
    # CDFI Directory
    # -----------------------------------------------------------------------

    with tool_tabs[2]:
        st.subheader("CDFI Directory")
        st.caption("Certified Community Development Financial Institutions from the CDFI Fund.")

        cdfi_states_available = db.get_cdfi_states()
        if not cdfi_states_available:
            st.info(
                "No CDFI data loaded yet. Download the certified CDFI list from the CDFI Fund "
                "and run:\n```\npython etl/load_cdfi_directory.py --file data/raw/cdfi_certified_list.xlsx\n```"
            )
        else:
            cdfi_filter_col1, cdfi_filter_col2 = st.columns(2)
            with cdfi_filter_col1:
                cdfi_state_filter = st.multiselect(
                    "State(s)",
                    options=cdfi_states_available,
                    key="cdfi_state_filter",
                )
            with cdfi_filter_col2:
                cdfi_type_options = ["All types", "Loan Fund", "Credit Union", "Community Development Bank", "Venture Capital"]
                cdfi_type_filter = st.selectbox("CDFI type", cdfi_type_options, key="cdfi_type_filter")

            cdfis_df = db.get_cdfis(
                states=cdfi_state_filter if cdfi_state_filter else None,
                cdfi_type=None if cdfi_type_filter == "All types" else cdfi_type_filter,
            )

            if not cdfis_df.empty:
                st.metric("CDFIs shown", len(cdfis_df))
                show_cdfi_cols = [c for c in [
                    "cdfi_name", "city", "state", "cdfi_type", "total_assets",
                    "primary_markets", "target_populations", "website"
                ] if c in cdfis_df.columns]
                st.dataframe(
                    cdfis_df[show_cdfi_cols].rename(columns={
                        "cdfi_name": "CDFI", "cdfi_type": "Type",
                        "total_assets": "Total Assets", "primary_markets": "Markets",
                        "target_populations": "Target Populations",
                    }),
                    use_container_width=True, height=400,
                )
                st.download_button(
                    "Download CSV", data=df_to_csv_bytes(cdfis_df[show_cdfi_cols]),
                    file_name="cdfi_directory.csv", mime="text/csv",
                )
            else:
                st.info("No CDFIs match the current filters.")

    # -----------------------------------------------------------------------
    # State Incentive Programs
    # -----------------------------------------------------------------------

    with tool_tabs[3]:
        st.subheader("State Incentive Programs")
        st.caption(
            "State-level financing programs that can stack with NMTC: historic tax credits, "
            "state NMTCs, LIHTC, and other community development incentives."
        )

        program_states = db.get_program_states()
        if not program_states:
            st.info(
                "No state program data loaded yet. Run:\n"
                "```\npython etl/load_state_programs.py\n```"
            )
        else:
            prog_state_select = st.selectbox(
                "Select a state",
                ["(All states)"] + program_states,
                key="prog_state_select",
            )
            prog_type_filter = st.multiselect(
                "Program type",
                ["Historic Tax Credit", "State NMTC", "LIHTC", "Grant", "Loan", "Other"],
                key="prog_type_filter",
            )

            programs_df = db.get_state_programs(
                state=None if prog_state_select == "(All states)" else prog_state_select
            )

            if prog_type_filter and not programs_df.empty and "program_type" in programs_df.columns:
                programs_df = programs_df[programs_df["program_type"].isin(prog_type_filter)]

            if not programs_df.empty:
                st.metric("Programs shown", len(programs_df))
                show_prog_cols = [c for c in [
                    "state", "program_name", "program_type", "eligible_uses",
                    "max_credit_pct", "max_amount", "administering_agency", "website", "notes"
                ] if c in programs_df.columns]
                st.dataframe(
                    programs_df[show_prog_cols].rename(columns={
                        "program_name": "Program", "program_type": "Type",
                        "eligible_uses": "Eligible Uses", "max_credit_pct": "Max Credit %",
                        "max_amount": "Max Amount", "administering_agency": "Agency",
                    }),
                    use_container_width=True, height=400,
                )
            else:
                st.info("No programs match current filters.")
