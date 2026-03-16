"""
utils/maps.py — Map rendering functions using Folium.

All map creation logic lives here so the app file stays clean.
"""

import folium
from folium import FeatureGroup
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


def make_unified_map(
    schools_df: pd.DataFrame = None,
    tracts_df: pd.DataFrame = None,
    projects_df: pd.DataFrame = None,
    center_lat: float = None,
    center_lon: float = None,
    zoom: int = 5,
) -> folium.Map:
    """
    Create a single Folium map with toggleable layers for the unified GIS view.

    Layers (each is a FeatureGroup that can be toggled via LayerControl):
      1. Schools — circle markers colored by survival risk tier (charters)
         or blue (traditional public)
      2. NMTC Projects — blue dollar-icon markers for past investments
      3. Census Tracts — (future: polygon overlay; for now, not rendered as
         tracts don't have centroid coordinates in the DB)

    Args:
        schools_df: DataFrame from db.get_schools()
        tracts_df: DataFrame from db.get_census_tracts() (used for data, not drawn)
        projects_df: DataFrame from db.get_nmtc_projects()
        center_lat, center_lon: map center (defaults to centroid of data or US center)
        zoom: initial zoom level

    Returns:
        folium.Map object with LayerControl
    """
    # Determine map center
    if center_lat is None or center_lon is None:
        if schools_df is not None and not schools_df.empty:
            coords = schools_df.dropna(subset=["latitude", "longitude"])
            if not coords.empty:
                center_lat = coords["latitude"].mean()
                center_lon = coords["longitude"].mean()
        if center_lat is None:
            center_lat, center_lon = 38.0, -97.0  # US center

    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom)

    # --- Layer: Schools ---
    if schools_df is not None and not schools_df.empty:
        school_layer = FeatureGroup(name="Schools", show=True)
        schools_with_coords = schools_df.dropna(subset=["latitude", "longitude"])

        for _, row in schools_with_coords.iterrows():
            is_charter = row.get("is_charter", 1)
            if is_charter:
                color = RISK_COLORS.get(row.get("survival_risk_tier", "Unknown"), "#7f7f7f")
            else:
                color = "#1f77b4"  # blue for traditional public schools

            popup_html = _school_popup_html(row)
            label = row.get("school_name", "School")
            prefix = "Charter" if is_charter else "Public"

            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=5,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.75,
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{prefix}: {label}",
            ).add_to(school_layer)

        school_layer.add_to(m)

    # --- Layer: NMTC Projects ---
    if projects_df is not None and not projects_df.empty:
        project_layer = FeatureGroup(name="NMTC Projects", show=True)
        projects_with_coords = projects_df.dropna(subset=["latitude", "longitude"])

        for _, row in projects_with_coords.iterrows():
            qlici = row.get("qlici_amount")
            qlici_str = f"${qlici/1e6:.1f}M" if qlici and qlici == qlici else "—"
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
                tooltip=f"NMTC: {row.get('project_name', 'Project')}",
            ).add_to(project_layer)

        project_layer.add_to(m)

    # Add Folium's built-in layer toggle control
    folium.LayerControl(collapsed=False).add_to(m)

    # Add legend
    legend_html = _unified_legend_html(
        show_schools=schools_df is not None and not schools_df.empty,
        show_projects=projects_df is not None and not projects_df.empty,
        has_charters=_has_charters(schools_df),
    )
    m.get_root().html.add_child(folium.Element(legend_html))

    return m


def _has_charters(df) -> bool:
    """Check if a schools DataFrame contains charter schools."""
    if df is None or df.empty:
        return False
    if "is_charter" in df.columns:
        return (df["is_charter"] == 1).any()
    return True  # old data without is_charter column assumed to be charters


def _school_popup_html(row) -> str:
    """Generate HTML for a school marker popup."""
    survival_score = row.get("survival_score")
    score_str = f"{survival_score:.2f}" if survival_score is not None and survival_score == survival_score else "—"
    enrollment = row.get("enrollment")
    enrollment_str = f"{int(enrollment):,}" if enrollment and enrollment == enrollment else "—"
    is_charter = row.get("is_charter", 1)
    school_type = "Charter" if is_charter else "Public"

    # Build survival info only for charter schools
    survival_html = ""
    if is_charter:
        risk_tier = row.get("survival_risk_tier", "—")
        risk_color = RISK_COLORS.get(risk_tier, "#555")
        survival_html = f"""
        <b>Survival Score:</b> {score_str}
            <span style="color: {risk_color};">
                ({risk_tier} risk)
            </span><br>
        """

    return f"""
    <div style="font-family: sans-serif; font-size: 13px; min-width: 200px;">
        <b>{row.get('school_name', 'Unknown School')}</b><br>
        <span style="color: #555;">{row.get('city', '') or ''}, {row.get('state', '')}</span>
        <hr style="margin: 4px 0;">
        <b>Type:</b> {school_type}<br>
        <b>Status:</b> {row.get('school_status', '—')}<br>
        <b>Enrollment:</b> {enrollment_str}<br>
        {survival_html}
        <b>LEA:</b> {row.get('lea_name', '—')}<br>
        <b>Census Tract:</b> {row.get('census_tract_id', '—') or '—'}<br>
        <b>FRL:</b> {_fmt_pct(row.get('pct_free_reduced_lunch'))}<br>
    </div>
    """


def _fmt_pct(val) -> str:
    """Format a percentage value for display."""
    if val is None:
        return "—"
    try:
        v = float(val)
        if v != v:  # NaN check
            return "—"
        return f"{v:.1f}%"
    except (TypeError, ValueError):
        return "—"


def _unified_legend_html(
    show_schools: bool = False,
    show_projects: bool = False,
    has_charters: bool = False,
) -> str:
    """Legend overlay for the unified map."""
    items = ""

    if show_schools and has_charters:
        items += '<div style="margin-bottom:4px;"><b>Charter School Risk</b></div>'
        for tier, color in RISK_COLORS.items():
            if tier != "Unknown":
                items += (
                    f'<div><span style="background:{color}; display:inline-block; '
                    f'width:10px; height:10px; border-radius:50%; margin-right:6px;"></span>'
                    f'{tier} risk</div>'
                )

    if show_schools:
        items += (
            '<div style="margin-top:4px;">'
            '<span style="background:#1f77b4; display:inline-block; '
            'width:10px; height:10px; border-radius:50%; margin-right:6px;"></span>'
            'Traditional public</div>'
        )

    if show_projects:
        items += (
            '<div style="margin-top:4px;">'
            '<span style="color:#1f77b4; margin-right:6px;">&#x1f4b5;</span>'
            'NMTC project</div>'
        )

    if not items:
        return ""

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
        {items}
    </div>
    """


# ---------------------------------------------------------------------------
# Legacy functions (kept for backward compatibility)
# ---------------------------------------------------------------------------

def make_charter_school_map(
    df: pd.DataFrame,
    center_lat: float = None,
    center_lon: float = None,
    zoom: int = 5,
    color_by: str = "survival_risk_tier",
) -> folium.Map:
    """Legacy wrapper — calls make_unified_map with schools only."""
    return make_unified_map(
        schools_df=df,
        center_lat=center_lat,
        center_lon=center_lon,
        zoom=zoom,
    )


def make_nmtc_map(
    tracts_df: pd.DataFrame,
    schools_df: pd.DataFrame = None,
    projects_df: pd.DataFrame = None,
    center_lat: float = None,
    center_lon: float = None,
    zoom: int = 7,
) -> folium.Map:
    """Legacy wrapper — calls make_unified_map."""
    return make_unified_map(
        schools_df=schools_df,
        tracts_df=tracts_df,
        projects_df=projects_df,
        center_lat=center_lat,
        center_lon=center_lon,
        zoom=zoom,
    )
