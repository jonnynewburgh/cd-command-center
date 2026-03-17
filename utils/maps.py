"""
utils/maps.py — Map rendering functions using Folium.

All map creation logic lives here so the app file stays clean.
"""

import folium
from folium import FeatureGroup
from folium.plugins import MarkerCluster
import pandas as pd

# Above this count, use clustered markers for schools to keep the map fast.
# Lower = clusters kick in sooner = faster initial render with many schools.
SCHOOL_CLUSTER_THRESHOLD = 200


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
    fqhc_df: pd.DataFrame = None,
    ece_df: pd.DataFrame = None,
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
      3. FQHCs — green circle markers for health center sites
      4. ECE Centers — orange circle markers for child care facilities
      5. Census Tracts — (future: polygon overlay; for now, not rendered as
         tracts don't have centroid coordinates in the DB)

    Args:
        schools_df: DataFrame from db.get_schools()
        tracts_df: DataFrame from db.get_census_tracts() (used for data, not drawn)
        projects_df: DataFrame from db.get_nmtc_projects()
        fqhc_df: DataFrame from db.get_fqhc()
        ece_df: DataFrame from db.get_ece_centers()
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

    # Layers are added in this order: ECE → FQHCs → NMTC → Schools.
    # In Folium's LayerControl, later-added layers appear higher in the list.
    # Schools are last so they don't appear as the "default" or most prominent layer.

    # --- Layer: ECE Centers ---
    if ece_df is not None and not ece_df.empty:
        ece_layer = FeatureGroup(name="ECE Centers", show=True)
        ece_with_coords = ece_df.dropna(subset=["latitude", "longitude"])

        for row in ece_with_coords.itertuples(index=False):
            capacity = getattr(row, "capacity", None)
            capacity_str = f"{int(capacity):,}" if capacity and capacity == capacity else "—"
            rating = getattr(row, "star_rating", None)
            rating_str = f"{rating:.1f} ★" if rating and rating == rating else "—"
            popup_html = f"""
            <div style="font-family: sans-serif; font-size: 13px; min-width: 200px;">
                <b>{getattr(row, 'provider_name', 'ECE Center')}</b><br>
                <span style="color:#555;">{getattr(row, 'city', '—')}, {getattr(row, 'state', '')}</span>
                <hr style="margin: 4px 0;">
                <b>Type:</b> {getattr(row, 'facility_type', '—')}<br>
                <b>Status:</b> {getattr(row, 'license_status', '—')}<br>
                <b>Capacity:</b> {capacity_str}<br>
                <b>Ages:</b> {getattr(row, 'ages_served', '—')}<br>
                <b>Quality Rating:</b> {rating_str}<br>
                <b>Accepts Subsidies:</b> {'Yes' if getattr(row, 'accepts_subsidies', 0) == 1 else '—'}<br>
                <b>Census Tract:</b> {getattr(row, 'census_tract_id', '—') or '—'}<br>
            </div>
            """
            folium.CircleMarker(
                location=[row.latitude, row.longitude],
                radius=5,
                color="#ff7f0e",      # orange — distinct from schools (blue/red) and FQHCs (green)
                fill=True,
                fill_color="#ff7f0e",
                fill_opacity=0.7,
                popup=folium.Popup(popup_html, max_width=280),
                tooltip=f"ECE: {getattr(row, 'provider_name', 'Child Care Center')}",
            ).add_to(ece_layer)

        ece_layer.add_to(m)

    # --- Layer: FQHCs ---
    if fqhc_df is not None and not fqhc_df.empty:
        fqhc_layer = FeatureGroup(name="Health Centers (FQHCs)", show=True)
        fqhc_with_coords = fqhc_df.dropna(subset=["latitude", "longitude"])

        for row in fqhc_with_coords.itertuples(index=False):
            patients = getattr(row, "total_patients", None)
            patients_str = f"{int(patients):,}" if patients and patients == patients else "—"
            site = getattr(row, "site_name", None) or getattr(row, "health_center_name", "Health Center")
            popup_html = f"""
            <div style="font-family: sans-serif; font-size: 13px; min-width: 200px;">
                <b>{site}</b><br>
                <span style="color:#555;">{getattr(row, 'health_center_name', '')}</span>
                <hr style="margin: 4px 0;">
                <b>Type:</b> {getattr(row, 'site_type', '—')}<br>
                <b>City:</b> {getattr(row, 'city', '—')}, {getattr(row, 'state', '')}<br>
                <b>Total Patients:</b> {patients_str}<br>
                <b>Census Tract:</b> {getattr(row, 'census_tract_id', '—') or '—'}<br>
            </div>
            """
            folium.CircleMarker(
                location=[row.latitude, row.longitude],
                radius=6,
                color="#2ca02c",      # green
                fill=True,
                fill_color="#2ca02c",
                fill_opacity=0.7,
                popup=folium.Popup(popup_html, max_width=280),
                tooltip=f"FQHC: {site}",
            ).add_to(fqhc_layer)

        fqhc_layer.add_to(m)

    # --- Layer: NMTC Projects ---
    if projects_df is not None and not projects_df.empty:
        project_layer = FeatureGroup(name="NMTC Projects", show=True)
        projects_with_coords = projects_df.dropna(subset=["latitude", "longitude"])

        for row in projects_with_coords.itertuples(index=False):
            qlici = getattr(row, "qlici_amount", None)
            qlici_str = f"${qlici/1e6:.1f}M" if qlici and qlici == qlici else "—"
            popup_html = f"""
            <div style="font-family: sans-serif; font-size: 13px; min-width: 200px;">
                <b>{getattr(row, 'project_name', 'NMTC Project')}</b><br>
                <span style="color:#555;">{getattr(row, 'city', '')}, {getattr(row, 'state', '')}</span>
                <hr style="margin: 4px 0;">
                <b>Type:</b> {getattr(row, 'project_type', '—')}<br>
                <b>QLICI:</b> {qlici_str}<br>
                <b>CDE:</b> {getattr(row, 'cde_name', '—')}<br>
                <b>Year:</b> {getattr(row, 'fiscal_year', '—')}<br>
            </div>
            """
            folium.Marker(
                location=[row.latitude, row.longitude],
                icon=folium.Icon(color="blue", icon="dollar", prefix="fa"),
                popup=folium.Popup(popup_html, max_width=280),
                tooltip=f"NMTC: {getattr(row, 'project_name', 'Project')}",
            ).add_to(project_layer)

        project_layer.add_to(m)

    # --- Layer: Schools (added last so it appears at top of LayerControl list) ---
    if schools_df is not None and not schools_df.empty:
        school_layer = FeatureGroup(name="Schools", show=True)
        schools_with_coords = schools_df.dropna(subset=["latitude", "longitude"])

        # Use clustered markers when there are many schools to keep the map responsive.
        # MarkerCluster groups nearby markers at low zoom and expands them as you zoom in.
        use_cluster = len(schools_with_coords) > SCHOOL_CLUSTER_THRESHOLD
        if use_cluster:
            cluster = MarkerCluster(
                name="Schools (clustered)",
                options={"maxClusterRadius": 40, "disableClusteringAtZoom": 12},
            )
            marker_target = cluster
        else:
            marker_target = school_layer

        # itertuples() is ~5-10x faster than iterrows() for 500+ rows because it
        # returns namedtuples instead of full pandas Series objects.
        for row in schools_with_coords.itertuples(index=False):
            is_charter = getattr(row, "is_charter", 1)
            if is_charter:
                color = RISK_COLORS.get(getattr(row, "survival_risk_tier", "Unknown"), "#7f7f7f")
            else:
                color = "#1f77b4"  # blue for traditional public schools

            popup_html = _school_popup_html(row)
            label = getattr(row, "school_name", "School")
            prefix = "Charter" if is_charter else "Public"

            folium.CircleMarker(
                location=[row.latitude, row.longitude],
                radius=5,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.75,
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{prefix}: {label}",
            ).add_to(marker_target)

        if use_cluster:
            cluster.add_to(school_layer)

        school_layer.add_to(m)

    # Add Folium's built-in layer toggle control
    folium.LayerControl(collapsed=False).add_to(m)

    # Add legend
    legend_html = _unified_legend_html(
        show_schools=schools_df is not None and not schools_df.empty,
        show_projects=projects_df is not None and not projects_df.empty,
        show_fqhc=fqhc_df is not None and not fqhc_df.empty,
        show_ece=ece_df is not None and not ece_df.empty,
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
    """
    Generate HTML for a school marker popup.
    Works with both pandas Series (from iterrows) and namedtuples (from itertuples).
    Uses getattr() with a default so it's compatible with both.
    """
    def _get(key, default=None):
        return getattr(row, key, default)

    survival_score = _get("survival_score")
    score_str = f"{survival_score:.2f}" if survival_score is not None and survival_score == survival_score else "—"
    enrollment = _get("enrollment")
    enrollment_str = f"{int(enrollment):,}" if enrollment and enrollment == enrollment else "—"
    is_charter = _get("is_charter", 1)
    school_type = "Charter" if is_charter else "Public"

    # Build survival info only for charter schools
    survival_html = ""
    if is_charter:
        risk_tier = _get("survival_risk_tier", "—")
        risk_color = RISK_COLORS.get(risk_tier, "#555")
        survival_html = f"""
        <b>Survival Score:</b> {score_str}
            <span style="color: {risk_color};">
                ({risk_tier} risk)
            </span><br>
        """

    return f"""
    <div style="font-family: sans-serif; font-size: 13px; min-width: 200px;">
        <b>{_get('school_name', 'Unknown School')}</b><br>
        <span style="color: #555;">{_get('city', '') or ''}, {_get('state', '')}</span>
        <hr style="margin: 4px 0;">
        <b>Type:</b> {school_type}<br>
        <b>Status:</b> {_get('school_status', '—')}<br>
        <b>Enrollment:</b> {enrollment_str}<br>
        {survival_html}
        <b>LEA:</b> {_get('lea_name', '—')}<br>
        <b>Census Tract:</b> {_get('census_tract_id', '—') or '—'}<br>
        <b>FRL:</b> {_fmt_pct(_get('pct_free_reduced_lunch'))}<br>
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
    show_fqhc: bool = False,
    show_ece: bool = False,
    has_charters: bool = False,
) -> str:
    """Legend overlay for the unified map."""
    # Legend order matches the layer toggle order in the sidebar: ECE → FQHCs → NMTC → Schools
    items = ""

    if show_ece:
        items += (
            '<div style="margin-top:4px;">'
            '<span style="background:#ff7f0e; display:inline-block; '
            'width:10px; height:10px; border-radius:50%; margin-right:6px;"></span>'
            'ECE / child care center</div>'
        )

    if show_fqhc:
        items += (
            '<div style="margin-top:4px;">'
            '<span style="background:#2ca02c; display:inline-block; '
            'width:10px; height:10px; border-radius:50%; margin-right:6px;"></span>'
            'Health center (FQHC)</div>'
        )

    if show_projects:
        items += (
            '<div style="margin-top:4px;">'
            '<span style="color:#1f77b4; margin-right:6px;">&#x1f4b5;</span>'
            'NMTC project</div>'
        )

    if show_schools:
        items += (
            '<div style="margin-top:4px;">'
            '<span style="background:#1f77b4; display:inline-block; '
            'width:10px; height:10px; border-radius:50%; margin-right:6px;"></span>'
            'Traditional public school</div>'
        )

    if show_schools and has_charters:
        items += '<div style="margin-top:6px; margin-bottom:2px;"><b>Charter school risk</b></div>'
        for tier, color in RISK_COLORS.items():
            if tier != "Unknown":
                items += (
                    f'<div><span style="background:{color}; display:inline-block; '
                    f'width:10px; height:10px; border-radius:50%; margin-right:6px;"></span>'
                    f'{tier} risk</div>'
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
