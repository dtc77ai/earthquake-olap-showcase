"""Analysis page with detailed temporal and magnitude patterns."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import duckdb
import pandas as pd
import streamlit as st

from src.app.components.charts import (
    create_energy_release_chart,
    create_hourly_pattern_chart,
    create_magnitude_vs_depth_scatter,
    create_regional_comparison_chart,
    create_temporal_trend_chart,
)
from src.olap.queries import OLAPQueries
from src.utils.config import get_config

# Page config
st.set_page_config(page_title="Analysis", page_icon="📈", layout="wide")

# Load config
config = get_config()

st.title("📈 Detailed Analysis")
st.markdown("Deep dive into earthquake patterns and trends")

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
    st.sidebar.header("🔍 Analysis Filters")
    
    # Initialize session state for analysis filters
    if "analysis_min_magnitude" not in st.session_state:
        st.session_state.analysis_min_magnitude = 5.5
    
    min_magnitude = st.sidebar.slider(
        "Minimum Magnitude",
        min_value=5.0,
        max_value=10.0,
        value=st.session_state.analysis_min_magnitude,
        step=0.1,
        help="Filter earthquakes by minimum magnitude (applies to all tabs)",
        key="analysis_magnitude_slider",
    )
    
    # Update session state
    st.session_state.analysis_min_magnitude = min_magnitude
    
    st.sidebar.info(f"Analyzing earthquakes with magnitude ≥ {min_magnitude}")

    if st.sidebar.button("🔄 Reset Filters", key="reset_analysis_filters"):
        st.session_state.analysis_min_magnitude = 5.5
        st.rerun()

    # Tabs for different analyses
    tab1, tab2, tab3, tab4 = st.tabs(["📅 Temporal", "📊 Magnitude", "🌍 Regional", "⚡ Energy"])

    with tab1:
        st.header("Temporal Analysis")
        st.markdown("Explore how earthquake activity varies over time")

        # Temporal trends
        st.subheader("Daily Activity Trends")
        
        temporal_query = f"""
        SELECT
            t.date,
            t.year,
            t.month,
            t.day_of_week,
            COUNT(*) AS daily_event_count,
            AVG(m.magnitude) AS daily_avg_magnitude,
            MAX(m.magnitude) AS daily_max_magnitude,
            SUM(m.energy_joules) AS daily_total_energy,
            COUNT(DISTINCT l.region) AS affected_regions
        FROM fact_earthquakes f
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN dim_magnitude m ON f.magnitude_id = m.magnitude_id
        JOIN dim_location l ON f.location_id = l.location_id
        WHERE m.magnitude >= {min_magnitude}
        GROUP BY t.date, t.year, t.month, t.day_of_week
        ORDER BY t.date
        """
        
        temporal_data = conn.execute(temporal_query).df()

        if not temporal_data.empty:
            fig = create_temporal_trend_chart(temporal_data)
            st.plotly_chart(fig, use_container_width=True, key="temporal_trend_chart")

            # Statistics
            col1, col2, col3 = st.columns(3)

            with col1:
                avg_daily = temporal_data["daily_event_count"].mean()
                st.metric("Avg Daily Events", f"{avg_daily:.1f}")

            with col2:
                max_daily = temporal_data["daily_event_count"].max()
                st.metric("Max Daily Events", f"{max_daily:.0f}")

            with col3:
                total_days = len(temporal_data)
                st.metric("Total Days", f"{total_days:,}")
            
            st.markdown("---")
            
            # Day of week distribution
            st.subheader("Distribution by Day of Week")
            
            from src.app.components.charts import create_day_of_week_chart
            
            fig_dow = create_day_of_week_chart(temporal_data)
            st.plotly_chart(fig_dow, use_container_width=True, key="day_of_week_chart")
            
            st.info(
                "💡 **Insight:** This chart shows if earthquake activity varies by day of the week. "
                "Since earthquakes are natural phenomena, we expect a relatively uniform distribution. "
                "Any significant variation might be due to detection/reporting patterns rather than actual seismic activity."
            )
            
        else:
            st.info("No temporal data available for the selected magnitude range")

        st.markdown("---")

        # Hourly patterns
        st.subheader("Hourly Activity Patterns")
        
        hourly_query = f"""
        SELECT
            hour,
            SUM(event_count) AS total_events,
            AVG(avg_magnitude) AS avg_magnitude
        FROM cube_time_magnitude
        WHERE avg_magnitude >= {min_magnitude}
        GROUP BY hour
        ORDER BY hour
        """
        
        hourly_data = conn.execute(hourly_query).df()

        if not hourly_data.empty:
            fig = create_hourly_pattern_chart(hourly_data)
            st.plotly_chart(fig, use_container_width=True, key="hourly_pattern_chart")

            st.info(
                "💡 **Note:** This chart shows the distribution of earthquakes by hour of day. "
                "Earthquakes are natural phenomena and should show no significant hourly pattern."
            )
        else:
            st.info("No hourly data available for the selected magnitude range")

        st.markdown("---")

        # Seasonal patterns
        st.subheader("Seasonal Patterns")
        
        seasonal_query = f"""
        SELECT
            season,
            SUM(event_count) AS total_events,
            AVG(avg_magnitude) AS avg_magnitude,
            AVG(avg_depth) AS avg_depth
        FROM cube_time_magnitude
        WHERE avg_magnitude >= {min_magnitude}
        GROUP BY season
        ORDER BY 
            CASE season
                WHEN 'Spring' THEN 1
                WHEN 'Summer' THEN 2
                WHEN 'Fall' THEN 3
                WHEN 'Winter' THEN 4
            END
        """
        
        seasonal_data = conn.execute(seasonal_query).df()

        if not seasonal_data.empty:
            # Format the data for display
            seasonal_data['total_events'] = seasonal_data['total_events'].astype(int)
            seasonal_data['avg_magnitude'] = seasonal_data['avg_magnitude'].round(2)
            
            # Display polar chart first (larger size)
            from src.app.components.charts import create_seasonal_polar_chart
            
            fig = create_seasonal_polar_chart(seasonal_data)
            st.plotly_chart(fig, use_container_width=True, key="seasonal_polar_chart")
            
            # Table below the chart
            st.markdown("#### Seasonal Statistics")
            
            # Prepare display dataframe with totals
            display_df = seasonal_data[["season", "total_events", "avg_magnitude"]].copy()
            display_df.columns = ["Season", "Total Events", "Avg Magnitude"]
            
            # Add totals row
            totals_row = pd.DataFrame([{
                "Season": "**TOTAL**",
                "Total Events": int(display_df["Total Events"].sum()),
                "Avg Magnitude": round(seasonal_data["avg_magnitude"].mean(), 2)
            }])
            
            display_df = pd.concat([display_df, totals_row], ignore_index=True)
            
            # Display table
            st.dataframe(
                display_df,
                width="stretch",
                hide_index=True,
            )
            
            # Season legend
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown("🌸 **Spring** (Mar-May)")
            with col2:
                st.markdown("☀️ **Summer** (Jun-Aug)")
            with col3:
                st.markdown("🍂 **Fall** (Sep-Nov)")
            with col4:
                st.markdown("❄️ **Winter** (Dec-Feb)")
            
            st.info(
                "💡 **Note:** Earthquakes are natural phenomena and should show "
                "relatively uniform distribution across seasons."
            )
        else:
            st.info("No seasonal data available for the selected magnitude range")



    with tab2:
        st.header("Magnitude Analysis")
        st.markdown("Analyze earthquake magnitudes and their relationships")

        # Magnitude distribution with filter
        st.subheader("Magnitude Distribution")
        
        mag_dist_query = f"""
        SELECT
            magnitude_category,
            SUM(event_count) AS total_events,
            AVG(avg_magnitude) AS avg_magnitude,
            AVG(avg_depth) AS avg_depth
        FROM cube_time_magnitude
        WHERE avg_magnitude >= {min_magnitude}
        GROUP BY magnitude_category
        ORDER BY 
            CASE magnitude_category
                WHEN 'Minor' THEN 1
                WHEN 'Light' THEN 2
                WHEN 'Moderate' THEN 3
                WHEN 'Strong' THEN 4
                WHEN 'Major' THEN 5
                WHEN 'Great' THEN 6
            END
        """
        
        mag_dist = conn.execute(mag_dist_query).df()

        if not mag_dist.empty:
            col1, col2 = st.columns([2, 1])

            with col1:
                import plotly.express as px

                fig = px.pie(
                    mag_dist,
                    values="total_events",
                    names="magnitude_category",
                    title="Distribution by Category",
                    color_discrete_sequence=px.colors.sequential.Reds,
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.table(
                    mag_dist.rename(
                        columns={
                            "magnitude_category": "Category",
                            "total_events": "Events",
                            "avg_magnitude": "Avg Mag",
                        }
                    )
                )
        else:
            st.info("No magnitude data available for the selected magnitude range")

        st.markdown("---")

        # Magnitude vs Depth with filter
        st.subheader("Magnitude vs Depth Relationship")

        scatter_query = f"""
        SELECT 
            m.magnitude,
            f.depth,
            m.magnitude_category
        FROM fact_earthquakes f
        JOIN dim_magnitude m ON f.magnitude_id = m.magnitude_id
        WHERE m.magnitude >= {min_magnitude}
        LIMIT 5000
        """
        
        scatter_data = conn.execute(scatter_query).df()

        if not scatter_data.empty:
            fig = create_magnitude_vs_depth_scatter(scatter_data)
            st.plotly_chart(fig, use_container_width=True)

            st.info(
                "💡 **Insight:** Deeper earthquakes often have different characteristics. "
                "Shallow earthquakes (< 70km) are generally more destructive at the surface."
            )
        else:
            st.info("No scatter data available for the selected magnitude range")

    with tab3:
        st.header("Regional Analysis")
        st.markdown("Compare earthquake activity across different regions")

        # Regional comparison with filter
        st.subheader("Most Active Regions")

        top_n = st.slider("Number of regions to display", min_value=5, max_value=20, value=10)

        regional_query = f"""
        SELECT
            region,
            SUM(event_count) AS event_count,
            AVG(avg_magnitude) AS avg_magnitude,
            MAX(max_magnitude) AS max_magnitude,
            AVG(center_latitude) AS center_latitude,
            AVG(center_longitude) AS center_longitude
        FROM cube_location_magnitude
        WHERE avg_magnitude >= {min_magnitude}
        GROUP BY region
        ORDER BY event_count DESC
        LIMIT {top_n}
        """
        
        regional_data = conn.execute(regional_query).df()

        if not regional_data.empty:
            fig = create_regional_comparison_chart(regional_data, top_n=top_n)
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")

            # Detailed regional table
            st.subheader("Regional Statistics")
            display_df = regional_data[
                ["region", "event_count", "avg_magnitude", "max_magnitude"]
            ].copy()
            display_df.columns = ["Region", "Event Count", "Avg Magnitude", "Max Magnitude"]

            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No regional data available for the selected magnitude range")

    with tab4:
        st.header("Energy Release Analysis")
        st.markdown("Analyze the seismic energy released by earthquakes")

        # Energy over time with filter
        st.subheader("Energy Release Over Time")
        
        energy_query = f"""
        SELECT
            t.date,
            t.year,
            t.month,
            t.day_of_week,
            COUNT(*) AS daily_event_count,
            AVG(m.magnitude) AS daily_avg_magnitude,
            MAX(m.magnitude) AS daily_max_magnitude,
            SUM(m.energy_joules) AS daily_total_energy,
            COUNT(DISTINCT l.region) AS affected_regions
        FROM fact_earthquakes f
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN dim_magnitude m ON f.magnitude_id = m.magnitude_id
        JOIN dim_location l ON f.location_id = l.location_id
        WHERE m.magnitude >= {min_magnitude}
        GROUP BY t.date, t.year, t.month, t.day_of_week
        ORDER BY t.date
        """
        
        temporal_data = conn.execute(energy_query).df()

        if not temporal_data.empty and "daily_total_energy" in temporal_data.columns:
            fig = create_energy_release_chart(temporal_data)
            st.plotly_chart(fig, use_container_width=True)

            # Energy statistics
            col1, col2, col3 = st.columns(3)

            with col1:
                total_energy = temporal_data["daily_total_energy"].sum()
                st.metric("Total Energy (Joules)", f"{total_energy:.2e}")

            with col2:
                avg_daily_energy = temporal_data["daily_total_energy"].mean()
                st.metric("Avg Daily Energy", f"{avg_daily_energy:.2e}")

            with col3:
                max_daily_energy = temporal_data["daily_total_energy"].max()
                st.metric("Max Daily Energy", f"{max_daily_energy:.2e}")

            st.markdown("---")

            st.info(
                "💡 **About Seismic Energy:** The energy released by an earthquake increases "
                "exponentially with magnitude. A magnitude 7 earthquake releases about 32 times "
                "more energy than a magnitude 6."
            )
        else:
            st.info("No energy data available for the selected magnitude range")

    conn.close()

except Exception as e:
    st.error(f"❌ Error loading data: {e}")
    import traceback

    st.code(traceback.format_exc())
