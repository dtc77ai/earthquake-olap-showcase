"""Moon phase analysis page showing correlation between lunar cycles and earthquakes."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import duckdb
import streamlit as st

from src.app.components.charts import create_moon_phase_polar_chart
from src.olap.queries import OLAPQueries
from src.utils.config import get_config

# Page config
st.set_page_config(page_title="Moon Phase Analysis", page_icon="🌙", layout="wide")

# Load config
config = get_config()

st.title("🌙 Moon Phase Analysis")
st.markdown("Explore the relationship between lunar cycles and seismic activity")

# Connect to database
db_path = config.get_duckdb_path()

if not db_path.exists():
    st.error("❌ Database not found. Please run the ETL pipeline first.")
    st.code("python scripts/run_etl.py", language="bash")
    st.stop()

try:
    conn = duckdb.connect(str(db_path), read_only=True)
    queries = OLAPQueries()

    # Sidebar filter with session state
    st.sidebar.header("🔍 Moon Phase Filters")

    # Initialize session state
    if "moon_min_magnitude" not in st.session_state:
        st.session_state.moon_min_magnitude = 5.5

    min_magnitude = st.sidebar.slider(
        "Minimum Magnitude",
        min_value=5.0,
        max_value=10.0,
        value=st.session_state.moon_min_magnitude,
        step=0.1,
        help="Filter earthquakes by minimum magnitude",
        key="moon_magnitude_slider",
    )

    # Update session state
    st.session_state.moon_min_magnitude = min_magnitude

    st.sidebar.info(f"Analyzing earthquakes with magnitude ≥ {min_magnitude}")

    # Reset button
    if st.sidebar.button("🔄 Reset Filters", key="reset_moon_filters"):
        st.session_state.moon_min_magnitude = 5.5
        st.rerun()

    # Information box
    st.info(
        "🌙 **About Moon Phases:** This analysis explores whether there's any correlation "
        "between lunar cycles and earthquake activity. While scientifically debated, "
        "some studies suggest tidal forces during full and new moons might influence seismic activity."
    )

    # Get data - use filtered query for accurate chart display
    moon_data = queries.get_moon_phase_filtered(conn, min_magnitude=min_magnitude)

    if moon_data.empty:
        st.warning("No data available for the selected magnitude range. Try lowering the minimum magnitude.")
        conn.close()
        st.stop()

    # Show the data returned by the query, for debugging purposes
    # st.write("### Debug: Raw Data")
    # st.dataframe(moon_data)
    # st.write(f"Total events in data: {moon_data['event_count'].sum()}")

    # Main polar chart
    st.header("📊 Earthquake Distribution by Lunar Cycle")

    fig = create_moon_phase_polar_chart(moon_data)
    st.plotly_chart(fig, use_container_width=True)

    # Explanation
    st.markdown("""
    ### 🔍 How to Read This Chart
    
    - **Circle represents the lunar cycle** - Starting from New Moon at the top, moving clockwise
    - **Bar height** - Number of earthquakes during each moon phase
    - **Colors** - Magnitude groups:
      - 🟢 Green: 1-3 (Minor/Light)
      - 🟡 Yellow: 4 (Moderate)
      - 🟠 Orange: 5 (Moderate-Strong)
      - 🔴 Red: 6-7 (Strong/Major)
      - 🟣 Purple: 8-9 (Great)
    - **Hover** - See detailed statistics for each segment
    """)

    st.markdown("---")

    # Statistics by moon phase
    st.header("📈 Statistics by Moon Phase")

    # Aggregate by moon phase
    phase_stats = (
        moon_data.groupby("moon_phase_name")
        .agg(
            {
                "event_count": "sum",
                "avg_magnitude": "mean",
                "max_magnitude": "max",
                "avg_depth": "mean",
            }
        )
        .reset_index()
    )

    # Sort by moon phase order
    phase_order = [
        "New Moon",
        "Waxing Crescent",
        "First Quarter",
        "Waxing Gibbous",
        "Full Moon",
        "Waning Gibbous",
        "Last Quarter",
        "Waning Crescent",
    ]

    phase_stats["sort_order"] = phase_stats["moon_phase_name"].apply(
        lambda x: phase_order.index(x) if x in phase_order else 999
    )
    phase_stats = phase_stats.sort_values("sort_order").drop("sort_order", axis=1)

    # Display table
    display_df = phase_stats.rename(
        columns={
            "moon_phase_name": "Moon Phase",
            "event_count": "Total Events",
            "avg_magnitude": "Avg Magnitude",
            "max_magnitude": "Max Magnitude",
            "avg_depth": "Avg Depth (km)",
        }
    )

    st.dataframe(display_df, width="stretch", hide_index=True)

    st.markdown("---")

    # Magnitude group breakdown
    st.header("📊 Magnitude Group Distribution")

    col1, col2, col3, col4, col5 = st.columns(5)

    magnitude_groups = [
        ("1-3", "#2ecc71", "🟢"),
        ("4", "#f1c40f", "🟡"),
        ("5", "#e67e22", "🟠"),
        ("6-7", "#e74c3c", "🔴"),
        ("8-9", "#9b59b6", "🟣"),
    ]

    for idx, (mag_group, color, emoji) in enumerate(magnitude_groups):
        group_data = moon_data[moon_data["magnitude_group"] == mag_group]
        total_events = group_data["event_count"].sum()
        
        if moon_data["event_count"].sum() > 0:
            percentage = total_events / moon_data["event_count"].sum() * 100
        else:
            percentage = 0

        with [col1, col2, col3, col4, col5][idx]:
            st.metric(
                f"{emoji} Mag {mag_group}",
                f"{total_events:,}",
                delta=f"{percentage:.1f}%",
            )

    st.markdown("---")

    # Scientific context
    st.header("🔬 Scientific Context")

    st.markdown("""
    ### Does the Moon Affect Earthquakes?
    
    **The Debate:**
    - **Tidal Forces**: The Moon's gravitational pull creates tides in Earth's oceans and can also affect the solid Earth
    - **Stress Changes**: Some studies suggest lunar tidal stresses might trigger earthquakes on faults already close to failure
    - **Statistical Evidence**: Most large-scale studies find no significant correlation, but some regional studies show weak patterns
    
    **What This Data Shows:**
    - Visualize the distribution of earthquakes across lunar phases
    - Compare different magnitude ranges
    - Identify any patterns in your dataset
    
    **Important Note:** Correlation does not imply causation. Any patterns observed should be interpreted with caution and 
    would require rigorous statistical analysis to determine significance.
    
    ### 📚 Further Reading
    - [USGS: Can the position of the moon affect seismicity?](https://www.usgs.gov/faqs/can-position-moon-or-planets-affect-seismicity-are-there-more-earthquakes-morningin-eveningat)
    - Research papers on tidal triggering of earthquakes
    """)

    conn.close()

except Exception as e:
    st.error(f"❌ Error loading data: {e}")
    import traceback

    st.code(traceback.format_exc())
