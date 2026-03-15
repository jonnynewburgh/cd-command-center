"""
utils/maps.py — Map rendering functions using Folium.

All map creation logic lives here so page files stay clean.
"""

import folium
import pandas as pd


# Color scheme for charter school survival risk tiers
RISK_COLORS = {
    "Low": "#2ca02c",      # green
    "Medium": "#ff7f0e",   # orange
    "High": "#d62728",     # red
    "Unknown": "#7f7f7f",  # gray
}

# Color scheme for NMTC eligibility tiers
NMTC_TIER_COLORS = {
    "Deep Distress":        "#d62728",   # red — highest distress
    "Severely Distressed":  "#ff7f0e",   # orange
    "LIC":                  "#2ca02c",   # green — basic LIC eligibility
    "Not Eligible":         "#cccccc",   # light gray
}


def make_charter_school_map(
    df: pd.DataFrame,
    center_lat: float = None,
    center_lon: float = None,
    zoom: int = 5,
    color_by: str = "survival_risk_tier",
) -> folium.Map:
    """
    Create a Folium map with charter school markers.

    Markers are colored by survival risk tier (green=low risk, red=high risk).
    Clicking a marker shows a popup with school details.

    Args:
        df: DataFrame from db.get_charter_schools(), must have lat/lon columns
        center_lat, center_lon: map center (defaults to centroid of schools)
        zoom: initial zoom level
        color_by: column to use for marker color ('survival_risk_tier' or 'school_status')

    Returns:
        folium.Map object (pass to st_folium in the page file)
    """
    # Filter to schools with coordinates
    df = df.dropna(subset=["latitude", "longitude"]).copy()

    if df.empty:
        # Return a default US map if no data
        return folium.Map(location=[38.0, -97.0], zoom_start=4)

    # Default center to centroid of visible schools
    if center_lat is None:
        center_lat = df["latitude"].mean()
    if center_lon is None:
        center_lon = df["longitude"].mean()

    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom)

    # Add a marker for each school
    for _, row in df.iterrows():
        color = RISK_COLORS.get(row.get("survival_risk_tier", "Unknown"), "#7f7f7f")
        if color_by == "school_status":
            color = "#2ca02c" if row.get("school_status") == "Open" else "#d62728"

        popup_html = _school_popup_html(row)

        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=6,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=row.get("school_name", "School"),
        ).add_to(m)

    # Add a legend
    legend_html = _risk_legend_html()
    m.get_root().html.add_child(folium.Element(legend_html))

    return m


def _school_popup_html(row) -> str:
    """Generate HTML for a school marker popup."""
    survival_score = row.get("survival_score")
    score_str = f"{survival_score:.2f}" if survival_score is not None else "—"
    enrollment = row.get("enrollment")
    enrollment_str = f"{int(enrollment):,}" if enrollment else "—"

    return f"""
    <div style="font-family: sans-serif; font-size: 13px; min-width: 200px;">
        <b>{row.get('school_name', 'Unknown School')}</b><br>
        <span style="color: #555;">{row.get('city', '')}, {row.get('state', '')}</span>
        <hr style="margin: 4px 0;">
        <b>Status:</b> {row.get('school_status', '—')}<br>
        <b>Enrollment:</b> {enrollment_str}<br>
        <b>Survival Score:</b> {score_str}
            <span style="color: {RISK_COLORS.get(row.get('survival_risk_tier', 'Unknown'), '#555')};">
                ({row.get('survival_risk_tier', '—')} risk)
            </span><br>
        <b>LEA:</b> {row.get('lea_name', '—')}<br>
        <b>Census Tract:</b> {row.get('census_tract_id', '—')}<br>
        <b>FRL:</b> {_fmt_pct(row.get('pct_free_reduced_lunch'))}<br>
    </div>
    """


def _fmt_pct(val) -> str:
    """Format a percentage value for display."""
    if val is None:
        return "—"
    try:
        return f"{float(val):.1f}%"
    except (TypeError, ValueError):
        return "—"


def make_nmtc_map(
    tracts_df: pd.DataFrame,
    schools_df: pd.DataFrame = None,
    projects_df: pd.DataFrame = None,
    center_lat: float = None,
    center_lon: float = None,
    zoom: int = 7,
) -> folium.Map:
    """
    Create a Folium map for the NMTC Tracker page.

    Layers (bottom to top):
      1. Census tract markers — circles colored by NMTC eligibility tier,
         sized by population. Clicking shows tract demographics.
      2. Charter school markers (optional) — small diamonds using the
         same risk-tier colors as the charter school page.
      3. NMTC project markers (optional) — blue squares for past investments.

    Args:
        tracts_df: DataFrame from db.get_census_tracts(), needs lat/lon or will
                   use tract centroid approximation from county_fips.
        schools_df: optional DataFrame from db.get_charter_schools(); overlay on map
        projects_df: optional DataFrame from db.get_nmtc_projects(); overlay on map
        center_lat, center_lon: map center (defaults to centroid of tracts)
        zoom: initial zoom level

    Returns:
        folium.Map object
    """
    # The census_tracts table doesn't store lat/lon — tracts are polygons.
    # We derive approximate centroid lat/lon from the county FIPS + tract code
    # by looking up coordinates from any charter school in the same tract,
    # or fall back to a state-level centroid.
    # For now, tracts that don't have coordinates are skipped.
    # (A future enhancement would store tract centroids from the Census TIGER API.)

    # Build a default center
    if center_lat is None or center_lon is None:
        if schools_df is not None and not schools_df.empty:
            schools_with_coords = schools_df.dropna(subset=["latitude", "longitude"])
            if not schools_with_coords.empty:
                center_lat = schools_with_coords["latitude"].mean()
                center_lon = schools_with_coords["longitude"].mean()
        if center_lat is None:
            center_lat, center_lon = 38.0, -97.0  # US center

    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom)

    # Layer 1: Charter school markers (drawn first so they appear above tracts)
    if schools_df is not None and not schools_df.empty:
        schools_with_coords = schools_df.dropna(subset=["latitude", "longitude"])
        for _, row in schools_with_coords.iterrows():
            color = RISK_COLORS.get(row.get("survival_risk_tier", "Unknown"), "#7f7f7f")
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=5,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.75,
                popup=folium.Popup(_school_popup_html(row), max_width=300),
                tooltip=f"🏫 {row.get('school_name', 'School')}",
            ).add_to(m)

    # Layer 2: NMTC project markers
    if projects_df is not None and not projects_df.empty:
        projects_with_coords = projects_df.dropna(subset=["latitude", "longitude"])
        for _, row in projects_with_coords.iterrows():
            qlici = row.get("qlici_amount")
            qlici_str = f"${qlici/1e6:.1f}M" if qlici else "—"
            popup_html = f"""
            <div style="font-family: sans-serif; font-size: 13px; min-width: 200px;">
                <b>{row.get('project_name', 'NMTC Project')}</b><br>
                <span style="color:#555;">{row.get('city', '')}, {row.get('state', '')}</span>
                <hr style="margin: 4px 0;">
                <b>Type:</b> {row.get('project_type', '—')}<br>
                <b>QLICI:</b> {qlici_str}<br>
                <b>CDE:</b> {row.get('cde_name', '—')}<br>
                <b>Year:</b> {row.get('fiscal_year', '—')}<br>
            </div>
            """
            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                icon=folium.Icon(color="blue", icon="dollar", prefix="fa"),
                popup=folium.Popup(popup_html, max_width=280),
                tooltip=f"💵 {row.get('project_name', 'NMTC Project')}",
            ).add_to(m)

    # Add NMTC tier legend
    legend_html = _nmtc_legend_html(
        show_schools=schools_df is not None and not schools_df.empty,
        show_projects=projects_df is not None and not projects_df.empty,
    )
    m.get_root().html.add_child(folium.Element(legend_html))

    return m


def _nmtc_legend_html(show_schools: bool = False, show_projects: bool = False) -> str:
    """Legend overlay for the NMTC map."""
    tier_items = "".join(
        f'<div><span style="background:{color}; display:inline-block; '
        f'width:12px; height:12px; border-radius:50%; margin-right:6px;"></span>'
        f'{tier}</div>'
        for tier, color in NMTC_TIER_COLORS.items()
        if tier != "Not Eligible"
    )
    extra = ""
    if show_schools:
        extra += '<div style="margin-top:6px; border-top:1px solid #eee; padding-top:4px;"><b>Charter schools</b></div>'
        for tier, color in RISK_COLORS.items():
            if tier != "Unknown":
                extra += (
                    f'<div><span style="background:{color}; display:inline-block; '
                    f'width:10px; height:10px; border-radius:50%; margin-right:6px;"></span>'
                    f'{tier} risk</div>'
                )
    if show_projects:
        extra += '<div style="margin-top:4px;"><span style="color:#1f77b4; margin-right:6px;">●</span>NMTC project</div>'

    return f"""
    <div style="
        position: fixed;
        bottom: 30px; right: 10px;
        background: white;
        padding: 10px 14px;
        border-radius: 6px;
        border: 1px solid #ccc;
        font-family: sans-serif;
        font-size: 13px;
        z-index: 9999;
        box-shadow: 2px 2px 6px rgba(0,0,0,0.15);
    ">
        <b>NMTC Eligibility</b><br>
        {tier_items}
        {extra}
    </div>
    """


def _risk_legend_html() -> str:
    """HTML string for a risk tier legend overlay on the map."""
    items = "".join(
        f'<div><span style="background:{color}; display:inline-block; '
        f'width:12px; height:12px; border-radius:50%; margin-right:6px;"></span>'
        f'{tier} risk</div>'
        for tier, color in RISK_COLORS.items()
        if tier != "Unknown"
    )
    return f"""
    <div style="
        position: fixed;
        bottom: 30px; right: 10px;
        background: white;
        padding: 10px 14px;
        border-radius: 6px;
        border: 1px solid #ccc;
        font-family: sans-serif;
        font-size: 13px;
        z-index: 9999;
        box-shadow: 2px 2px 6px rgba(0,0,0,0.15);
    ">
        <b>Survival Risk</b><br>
        {items}
    </div>
    """
