"""
app.py — CD Command Center main entry point.

Run with:
    streamlit run app.py
"""

import streamlit as st
import db

# Initialize the database (creates tables if they don't exist yet)
db.init_db()

st.set_page_config(
    page_title="CD Command Center",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🏗️ CD Command Center")
st.markdown(
    """
    **Community Development Finance Deal Origination Dashboard**

    Use the sidebar to navigate between tools. Each page focuses on a specific
    asset class or data source.
    """
)

# Show high-level summary if we have charter school data
summary = db.get_charter_school_summary()
if summary and summary.get("total_schools", 0) > 0:
    st.markdown("---")
    st.subheader("Phase 1: Charter Schools")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Schools", f"{summary.get('total_schools', 0):,}")
    col2.metric("Open Schools", f"{summary.get('open_schools', 0):,}")
    col3.metric("High Risk", f"{summary.get('high_risk_schools', 0):,}")
    avg_score = summary.get("avg_survival_score")
    col4.metric(
        "Avg Survival Score",
        f"{avg_score:.2f}" if avg_score is not None else "—"
    )
    total_enrollment = summary.get("total_enrollment")
    if total_enrollment:
        st.caption(f"Total enrollment: {int(total_enrollment):,} students")
else:
    st.info(
        "No data loaded yet. Run `python etl/load_sample_data.py` to load sample data, "
        "or `python etl/load_charter_schools.py --file <your-csv>` to load real data."
    )

st.markdown("---")
st.markdown(
    """
    **Build phases:**
    - ✅ **Phase 1** (current): Charter schools + LEA accountability data
    - ⬜ Phase 2: NMTC tracker + census demographics
    - ⬜ Phase 3: FQHC / health centers
    - ⬜ Phase 4: ECE facility data
    - ⬜ Phase 5: 990 / philanthropy data
    - ⬜ Phase 6: Auth + PostgreSQL migration
    """
)
