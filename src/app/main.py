"""Main Streamlit application for Earthquake OLAP Analytics."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st

from src.utils.config import get_config
from src.utils.logger import setup_logging

# Setup
setup_logging()
config = get_config()

# Page configuration
st.set_page_config(
    page_title=config.streamlit.page_title,
    page_icon=config.streamlit.page_icon,
    layout=config.streamlit.layout,
    initial_sidebar_state=config.streamlit.initial_sidebar_state,
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)

# Main page
st.markdown('<h1 class="main-header">🌍 Earthquake OLAP Analytics</h1>', unsafe_allow_html=True)

st.markdown("""
## Welcome to the Earthquake Data Analytics Platform

This application demonstrates the power of **open-source tools** for data engineering and analytics:

### 🛠️ Technology Stack
- **Python** - Data processing and orchestration
- **DuckDB** - High-performance OLAP database
- **Polars** - Lightning-fast data transformations
- **Streamlit** - Interactive web application
- **Plotly & Folium** - Rich visualizations and maps

### 📊 What You Can Do Here
1. **Overview** - Explore key metrics and summary statistics
2. **Analysis** - Dive deep into temporal and magnitude patterns
3. **Maps** - Visualize earthquake locations and intensities

### 🎯 Purpose
This project showcases how **open-source tools** can replace commercial solutions like:
- SQL Server → **DuckDB**
- Power BI → **Streamlit + Plotly**
- SSIS/Data Factory → **Python + Polars**

All running locally, with full transparency and reproducibility! 🚀

---

### 📈 Data Source
**USGS Earthquake Catalog** - Real-time seismic data from around the world

Use the sidebar to navigate between different views.
""")

# Sidebar
with st.sidebar:
    st.image("https://earthquake.usgs.gov/theme/images/usgs-logo.svg", width=200)
    
    st.markdown("---")
    
    st.markdown("### 📊 Quick Stats")
    
    # Check if database exists
    db_path = config.get_duckdb_path()
    
    if db_path.exists():
        import duckdb
        
        try:
            conn = duckdb.connect(str(db_path), read_only=True)
            
            # Get basic stats
            result = conn.execute("SELECT COUNT(*) FROM fact_earthquakes").fetchone()
            total_events = result[0] if result else 0
            
            result = conn.execute("""
                SELECT 
                    MIN(t.datetime) as min_date,
                    MAX(t.datetime) as max_date
                FROM fact_earthquakes f
                JOIN dim_time t ON f.time_id = t.time_id
            """).fetchone()
            
            if result:
                min_date = result[0]
                max_date = result[1]
            else:
                min_date = max_date = None
            
            conn.close()
            
            # Display stats with better contrast
            st.markdown(f"**Total Earthquakes:**")
            st.markdown(f"{total_events:,}")
            
            if min_date and max_date:
                st.markdown(f"**Date Range:**")
                st.markdown(f"{min_date.date()} to {max_date.date()}")
            
            st.success("✅ Database loaded")
            
        except Exception as e:
            st.error(f"❌ Error loading data: {e}")
    else:
        st.warning("⚠️ Database not found. Please run ETL pipeline first.")
        st.code("python scripts/run_etl.py", language="bash")
    
    st.markdown("---")
    
    st.markdown("### ℹ️ About")
    st.markdown(f"""
    **Version:** {config.app.version}
    
    **GitHub:** [View Source](https://github.com/dtc77ai/earthquake-olap-showcase)
    """)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 2rem;'>
    <p>Data provided by USGS Earthquake Hazards Program</p>
    <p>This is a demonstration project for educational purposes</p>
</div>
""", unsafe_allow_html=True)
