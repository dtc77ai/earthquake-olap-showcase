"""Overview page with key metrics and summary statistics."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import duckdb
import streamlit as st

from src.app.components.charts import (
    create_depth_analysis_chart,
    create_magnitude_distribution_chart,
)
from src.olap.queries import OLAPQueries
from src.utils.config import get_config

# Page config
st.set_page_config(page_title="Overview", page_icon="📊", layout="wide")

# Load config
config = get_config()

st.title("📊 Overview Dashboard")
st.markdown("Key metrics and summary statistics from the earthquake dataset")

# Connect to database
db_path = config.get_duckdb_path()

if not db_path.exists():
    st.error("❌ Database not found. Please run the ETL pipeline first.")
    st.code("python scripts/run_etl.py", language="bash")
    st.stop()

try:
    conn = duckdb.connect(str(db_path), read_only=True)
    queries = OLAPQueries()

    # Key Metrics
    st.header("🎯 Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    # Total events
    result = conn.execute("SELECT COUNT(*) FROM fact_earthquakes").fetchone()
    total_events = result[0] if result else 0

    with col1:
        st.metric("Total Earthquakes", f"{total_events:,}")

    # Date range
    result = conn.execute("""
        SELECT 
            MIN(t.datetime) as min_date,
            MAX(t.datetime) as max_date
        FROM fact_earthquakes f
        JOIN dim_time t ON f.time_id = t.time_id
    """).fetchone()

    if result:
        date_range_days = (result[1] - result[0]).days
        with col2:
            st.metric("Date Range (days)", f"{date_range_days:,}")

    # Average magnitude
    result = conn.execute("""
        SELECT AVG(m.magnitude)
        FROM fact_earthquakes f
        JOIN dim_magnitude m ON f.magnitude_id = m.magnitude_id
    """).fetchone()

    avg_magnitude = result[0] if result else 0

    with col3:
        st.metric("Average Magnitude", f"{avg_magnitude:.2f}")

    # Max magnitude
    result = conn.execute("""
        SELECT MAX(m.magnitude)
        FROM fact_earthquakes f
        JOIN dim_magnitude m ON f.magnitude_id = m.magnitude_id
    """).fetchone()

    max_magnitude = result[0] if result else 0

    with col4:
        st.metric("Maximum Magnitude", f"{max_magnitude:.2f}")

    st.markdown("---")

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Magnitude Distribution")
        mag_dist = queries.get_magnitude_distribution(conn)
        if not mag_dist.empty:
            fig = create_magnitude_distribution_chart(mag_dist)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available")

    with col2:
        st.subheader("Depth Analysis")
        depth_analysis = queries.get_depth_analysis(conn)
        if not depth_analysis.empty:
            fig = create_depth_analysis_chart(depth_analysis)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available")

    st.markdown("---")

    # Top Events Table
    st.header("🔝 Top Magnitude Events")

    top_events = queries.get_top_magnitude_events(conn, limit=10)

    if not top_events.empty:
        # Format the dataframe for display
        display_df = top_events[["datetime", "place", "magnitude", "depth"]].copy()
        display_df["datetime"] = display_df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        display_df.columns = ["Date & Time", "Location", "Magnitude", "Depth (km)"]

        st.dataframe(display_df, width="stretch", hide_index=True)
    else:
        st.info("No events found")

    # Regional Statistics
    st.header("🌎 Regional Statistics")

    regional_stats = queries.get_events_by_region(conn, top_n=10)

    if not regional_stats.empty:
        display_df = regional_stats[["region", "event_count", "avg_magnitude", "max_magnitude"]].copy()
        display_df.columns = ["Region", "Event Count", "Avg Magnitude", "Max Magnitude"]

        st.dataframe(display_df, width="stretch", hide_index=True)
    else:
        st.info("No regional data available")

    conn.close()

except Exception as e:
    st.error(f"❌ Error loading data: {e}")
    import traceback

    st.code(traceback.format_exc())
