"""Maps page with interactive earthquake visualizations."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import duckdb
import folium
import streamlit as st
from streamlit_folium import folium_static

from src.olap.queries import OLAPQueries
from src.utils.config import get_config

# Page config
st.set_page_config(page_title="Maps", page_icon="🗺️", layout="wide")

# Load config
config = get_config()

st.title("🗺️ Interactive Earthquake Maps")
st.markdown("Visualize earthquake locations and intensities on interactive maps")

# Connect to database
db_path = config.get_duckdb_path()

if not db_path.exists():
    st.error("❌ Database not found. Please run the ETL pipeline first.")
    st.code("python scripts/run_etl.py", language="bash")
    st.stop()

try:
    conn = duckdb.connect(str(db_path), read_only=True)
    queries = OLAPQueries()

    # Sidebar filters with session state
    st.sidebar.header("🔍 Map Filters")

    # Get date range from database
    date_range_query = conn.execute("""
        SELECT 
            MIN(t.date) as min_date,
            MAX(t.date) as max_date
        FROM fact_earthquakes f
        JOIN dim_time t ON f.time_id = t.time_id
    """).fetchone()

    if date_range_query:
        db_min_date = date_range_query[0]
        db_max_date = date_range_query[1]
    else:
        st.error("Unable to retrieve date range from database")
        conn.close()
        st.stop()

    # Display available date range
    st.sidebar.info(
        f"📅 **Available Data:**\n\n"
        f"From: {db_min_date}\n\n"
        f"To: {db_max_date}"
    )

    st.sidebar.markdown("---")

    # Initialize session state for map filters
    if "map_min_magnitude" not in st.session_state:
        st.session_state.map_min_magnitude = 5.5
    
    if "map_max_events" not in st.session_state:
        st.session_state.map_max_events = 1000
    
    if "map_start_date" not in st.session_state:
        st.session_state.map_start_date = db_min_date
    
    if "map_end_date" not in st.session_state:
        st.session_state.map_end_date = db_max_date

    # Date range filter
    st.sidebar.subheader("📅 Date Range")

    col1, col2 = st.sidebar.columns(2)

    with col1:
        start_date = st.date_input(
            "Start Date",
            value=st.session_state.map_start_date,
            min_value=db_min_date,
            max_value=db_max_date,
            key="map_start_date_picker",
            help="Select start date for filtering",
        )

    with col2:
        end_date = st.date_input(
            "End Date",
            value=st.session_state.map_end_date,
            min_value=db_min_date,
            max_value=db_max_date,
            key="map_end_date_picker",
            help="Select end date for filtering",
        )

    # Handle date input types (can be tuple or date)
    if isinstance(start_date, tuple):
        start_date = start_date[0] if len(start_date) > 0 else db_min_date
    
    if isinstance(end_date, tuple):
        end_date = end_date[0] if len(end_date) > 0 else db_max_date

    # Validate date range
    if start_date > end_date:
        st.sidebar.error("⚠️ Start date must be before end date!")
        start_date = db_min_date
        end_date = db_max_date

    # Update session state
    st.session_state.map_start_date = start_date
    st.session_state.map_end_date = end_date

    st.sidebar.markdown("---")

    # Magnitude filter
    st.sidebar.subheader("📊 Magnitude")
    
    min_magnitude = st.sidebar.slider(
        "Minimum Magnitude",
        min_value=5.0,
        max_value=10.0,
        value=st.session_state.map_min_magnitude,
        step=0.1,
        help="Filter earthquakes by minimum magnitude",
        key="map_magnitude_slider",
    )
    
    # Update session state
    st.session_state.map_min_magnitude = min_magnitude

    st.sidebar.markdown("---")

    # Limit filter
    st.sidebar.subheader("🔢 Display Limit")
    
    max_events = st.sidebar.slider(
        "Maximum Events to Display",
        min_value=100,
        max_value=5000,
        value=st.session_state.map_max_events,
        step=100,
        help="Limit the number of events shown (for performance)",
        key="map_max_events_slider",
    )
    
    # Update session state
    st.session_state.map_max_events = max_events

    st.sidebar.markdown("---")

    # Reset button
    if st.sidebar.button("🔄 Reset All Filters", key="reset_map_filters"):
        st.session_state.map_min_magnitude = 5.5
        st.session_state.map_max_events = 1000
        st.session_state.map_start_date = db_min_date
        st.session_state.map_end_date = db_max_date
        st.rerun()

    # Display active filters summary
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🎯 Active Filters")
    st.sidebar.markdown(f"**Date Range:** {start_date} to {end_date}")
    st.sidebar.markdown(f"**Min Magnitude:** {min_magnitude}")
    st.sidebar.markdown(f"**Max Events:** {max_events:,}")

    # Get map data with date filter
    with st.spinner(f"Loading earthquakes..."):
        map_data_query = f"""
        SELECT
            f.event_id,
            t.datetime,
            l.latitude,
            l.longitude,
            l.place,
            l.region,
            m.magnitude,
            m.magnitude_category,
            f.depth,
            f.depth_category
        FROM fact_earthquakes f
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN dim_location l ON f.location_id = l.location_id
        JOIN dim_magnitude m ON f.magnitude_id = m.magnitude_id
        WHERE m.magnitude >= {min_magnitude}
            AND t.date >= '{start_date}'
            AND t.date <= '{end_date}'
        ORDER BY m.magnitude DESC, t.datetime DESC
        LIMIT {max_events}
        """
        
        map_data = conn.execute(map_data_query).df()

    if map_data.empty:
        st.warning(
            "No earthquakes found with the selected filters. "
            "Try adjusting the date range or lowering the minimum magnitude."
        )
        conn.close()
        st.stop()

    # Show filter results
    days_in_range = (end_date - start_date).days + 1
    st.success(
        f"✅ Loaded {len(map_data):,} earthquakes "
        f"(Magnitude ≥ {min_magnitude}, {days_in_range} days: {start_date} to {end_date})"
    )

    # Color mapping for magnitude
    def get_color(magnitude):
        if magnitude < 3:
            return "green"
        elif magnitude < 4:
            return "blue"
        elif magnitude < 5:
            return "orange"
        elif magnitude < 6:
            return "red"
        else:
            return "darkred"

    # Create tabs for different map views
    tab1, tab2, tab3 = st.tabs(["🌍 Global View", "📍 Clustered View", "🔥 Heatmap"])

    with tab1:
        st.subheader("Global Earthquake Distribution")
        st.markdown("Each circle represents an earthquake, sized and colored by magnitude")

        # Create base map
        m = folium.Map(
            location=[20, 0],
            zoom_start=2,
            tiles="OpenStreetMap",
            width="100%",
            height="100%",
        )

        # Add markers
        for idx, row in map_data.iterrows():
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=max(3, row["magnitude"] * 1.5),
                popup=folium.Popup(
                    f"""
                    <div style="font-family: Arial; width: 200px;">
                        <h4 style="margin: 0 0 10px 0;">Magnitude {row['magnitude']}</h4>
                        <p style="margin: 5px 0;"><b>Depth:</b> {row['depth']:.1f} km</p>
                        <p style="margin: 5px 0;"><b>Location:</b> {row['place']}</p>
                        <p style="margin: 5px 0;"><b>Date:</b> {str(row['datetime'])[:19]}</p>
                        <p style="margin: 5px 0;"><b>Category:</b> {row['magnitude_category']}</p>
                    </div>
                    """,
                    max_width=250,
                ),
                tooltip=f"M{row['magnitude']:.1f}",
                color=get_color(row["magnitude"]),
                fill=True,
                fillColor=get_color(row["magnitude"]),
                fillOpacity=0.7,
                weight=2,
            ).add_to(m)

        # Display map using folium_static for better compatibility
        folium_static(m, width=1400, height=600)

        # Legend
        st.markdown("---")
        st.markdown("### 🎨 Magnitude Legend")
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.markdown("🟢 **< 3.0** Minor")
        with col2:
            st.markdown("🔵 **3.0-4.0** Light")
        with col3:
            st.markdown("🟠 **4.0-5.0** Moderate")
        with col4:
            st.markdown("🔴 **5.0-6.0** Strong")
        with col5:
            st.markdown("🔴 **≥ 6.0** Major+")

    with tab2:
        st.subheader("Clustered Earthquake View")
        st.markdown("Markers are grouped into clusters for better visualization at different zoom levels")

        # Create map with marker cluster
        from folium.plugins import MarkerCluster

        m = folium.Map(
            location=[20, 0],
            zoom_start=2,
            tiles="OpenStreetMap",
            width="100%",
            height="100%",
        )

        # Create marker cluster
        marker_cluster = MarkerCluster(
            name="Earthquake Clusters",
            overlay=True,
            control=True,
            show=True,
        ).add_to(m)

        # Add markers to cluster
        for idx, row in map_data.iterrows():
            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                popup=folium.Popup(
                    f"""
                    <div style="font-family: Arial; width: 200px;">
                        <h4 style="margin: 0 0 10px 0;">Magnitude {row['magnitude']}</h4>
                        <p style="margin: 5px 0;"><b>Depth:</b> {row['depth']:.1f} km</p>
                        <p style="margin: 5px 0;"><b>Location:</b> {row['place']}</p>
                        <p style="margin: 5px 0;"><b>Date:</b> {str(row['datetime'])[:19]}</p>
                        <p style="margin: 5px 0;"><b>Category:</b> {row['magnitude_category']}</p>
                    </div>
                    """,
                    max_width=250,
                ),
                tooltip=f"M{row['magnitude']:.1f}",
                icon=folium.Icon(color=get_color(row["magnitude"]), icon="info-sign"),
            ).add_to(marker_cluster)

        # Display map
        folium_static(m, width=1400, height=600)

        st.info("💡 **Tip:** Zoom in to see individual markers. Zoom out to see clusters with event counts.")

    with tab3:
        st.subheader("Earthquake Density Heatmap")
        st.markdown("Visualize earthquake concentration using a heatmap weighted by magnitude")

        from folium.plugins import HeatMap

        # Create map
        m = folium.Map(
            location=[20, 0],
            zoom_start=2,
            tiles="OpenStreetMap",
            width="100%",
            height="100%",
        )

        # Prepare heatmap data (lat, lon, weight)
        heat_data = [
            [row["latitude"], row["longitude"], float(row["magnitude"])]
            for idx, row in map_data.iterrows()
        ]

        # Add heatmap
        HeatMap(
            heat_data,
            min_opacity=0.4,
            #max_val=map_data["magnitude"].max(),
            radius=20,
            blur=25,
            gradient={0.4: "blue", 0.65: "lime", 0.8: "orange", 1.0: "red"},
        ).add_to(m)

        # Display map
        folium_static(m, width=1400, height=600)

        st.info(
            "💡 **Heatmap Insight:** Red/orange areas indicate high concentration of seismic activity, "
            "typically along tectonic plate boundaries like the Pacific Ring of Fire."
        )

    # Statistics panel
    st.markdown("---")
    st.header("📊 Filtered Data Statistics")

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Events Displayed", f"{len(map_data):,}")

    with col2:
        days_span = (end_date - start_date).days + 1
        st.metric("Date Range (days)", f"{days_span:,}")

    with col3:
        avg_mag = map_data["magnitude"].mean()
        st.metric("Avg Magnitude", f"{avg_mag:.2f}")

    with col4:
        max_mag = map_data["magnitude"].max()
        max_event = map_data[map_data["magnitude"] == max_mag].iloc[0]
        st.metric("Max Magnitude", f"{max_mag:.2f}")

    with col5:
        avg_depth = map_data["depth"].mean()
        st.metric("Avg Depth (km)", f"{avg_depth:.1f}")

    # Additional insights
    st.markdown("---")
    st.subheader("🔍 Data Insights")

    col1, col2 = st.columns(2)

    with col1:
        # Depth distribution
        shallow = len(map_data[map_data["depth"] < 70])
        intermediate = len(map_data[(map_data["depth"] >= 70) & (map_data["depth"] < 300)])
        deep = len(map_data[map_data["depth"] >= 300])

        st.markdown("**Depth Distribution:**")
        st.markdown(f"- Shallow (< 70 km): {shallow:,} ({shallow/len(map_data)*100:.1f}%)")
        st.markdown(f"- Intermediate (70-300 km): {intermediate:,} ({intermediate/len(map_data)*100:.1f}%)")
        st.markdown(f"- Deep (≥ 300 km): {deep:,} ({deep/len(map_data)*100:.1f}%)")

    with col2:
        # Magnitude distribution
        minor = len(map_data[map_data["magnitude"] < 4])
        moderate = len(map_data[(map_data["magnitude"] >= 4) & (map_data["magnitude"] < 5)])
        strong = len(map_data[(map_data["magnitude"] >= 5) & (map_data["magnitude"] < 6)])
        major = len(map_data[map_data["magnitude"] >= 6])

        st.markdown("**Magnitude Distribution:**")
        st.markdown(f"- Minor/Light (< 4.0): {minor:,} ({minor/len(map_data)*100:.1f}%)")
        st.markdown(f"- Moderate (4.0-5.0): {moderate:,} ({moderate/len(map_data)*100:.1f}%)")
        st.markdown(f"- Strong (5.0-6.0): {strong:,} ({strong/len(map_data)*100:.1f}%)")
        st.markdown(f"- Major+ (≥ 6.0): {major:,} ({major/len(map_data)*100:.1f}%)")

    conn.close()

except Exception as e:
    st.error(f"❌ Error loading data: {e}")
    import traceback

    st.code(traceback.format_exc())
