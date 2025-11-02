"""Pre-defined analytical queries for the OLAP system."""

from typing import Any, List, Optional

import duckdb
import pandas as pd

from src.utils.config import Config, get_config
from src.utils.logger import LoggerMixin


class OLAPQueries(LoggerMixin):
    """Execute pre-defined analytical queries on the OLAP system."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize query executor.

        Args:
            config: Configuration object (uses global config if None)
        """
        self.config = config or get_config()

    def get_top_magnitude_events(
        self, conn: duckdb.DuckDBPyConnection, limit: int = 10
    ) -> pd.DataFrame:
        """Get top magnitude earthquake events.

        Args:
            conn: DuckDB connection
            limit: Number of results to return

        Returns:
            DataFrame with top events
        """
        sql = f"""
        SELECT
            f.event_id,
            t.datetime,
            l.place,
            l.region,
            m.magnitude,
            m.magnitude_category,
            f.depth,
            f.depth_category,
            l.latitude,
            l.longitude
        FROM fact_earthquakes f
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN dim_location l ON f.location_id = l.location_id
        JOIN dim_magnitude m ON f.magnitude_id = m.magnitude_id
        ORDER BY m.magnitude DESC
        LIMIT {limit}
        """

        return conn.execute(sql).df()

    def get_events_by_region(
        self, conn: duckdb.DuckDBPyConnection, top_n: int = 10
    ) -> pd.DataFrame:
        """Get earthquake count by region.

        Args:
            conn: DuckDB connection
            top_n: Number of top regions to return

        Returns:
            DataFrame with regional statistics
        """
        sql = f"""
        SELECT
            region,
            event_count,
            avg_magnitude,
            max_magnitude,
            center_latitude,
            center_longitude
        FROM cube_location_magnitude
        GROUP BY region, event_count, avg_magnitude, max_magnitude, 
                 center_latitude, center_longitude
        ORDER BY event_count DESC
        LIMIT {top_n}
        """

        return conn.execute(sql).df()

    def get_temporal_trends(self, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Get temporal trends of earthquake activity.

        Args:
            conn: DuckDB connection

        Returns:
            DataFrame with temporal trends
        """
        sql = """
        SELECT
            date,
            daily_event_count,
            daily_avg_magnitude,
            daily_max_magnitude,
            daily_total_energy,
            affected_regions
        FROM cube_temporal_trends
        ORDER BY date
        """

        return conn.execute(sql).df()

    def get_magnitude_distribution(self, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Get distribution of earthquakes by magnitude category.

        Args:
            conn: DuckDB connection

        Returns:
            DataFrame with magnitude distribution
        """
        sql = """
        SELECT
            magnitude_category,
            SUM(event_count) AS total_events,
            AVG(avg_magnitude) AS avg_magnitude,
            AVG(avg_depth) AS avg_depth
        FROM cube_time_magnitude
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

        return conn.execute(sql).df()

    def get_depth_analysis(self, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Get analysis of earthquakes by depth category.

        Args:
            conn: DuckDB connection

        Returns:
            DataFrame with depth analysis
        """
        sql = """
        SELECT
            depth_category,
            SUM(event_count) AS total_events,
            AVG(avg_depth) AS avg_depth,
            AVG(avg_magnitude) AS avg_magnitude,
            AVG(avg_stations) AS avg_stations
        FROM cube_depth_analysis
        GROUP BY depth_category
        ORDER BY 
            CASE depth_category
                WHEN 'Shallow' THEN 1
                WHEN 'Intermediate' THEN 2
                WHEN 'Deep' THEN 3
            END
        """

        return conn.execute(sql).df()

    def get_hourly_patterns(self, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Get earthquake patterns by hour of day.

        Args:
            conn: DuckDB connection

        Returns:
            DataFrame with hourly patterns
        """
        sql = """
        SELECT
            hour,
            SUM(event_count) AS total_events,
            AVG(avg_magnitude) AS avg_magnitude
        FROM cube_time_magnitude
        GROUP BY hour
        ORDER BY hour
        """

        return conn.execute(sql).df()

    def get_seasonal_patterns(self, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Get earthquake patterns by season.

        Args:
            conn: DuckDB connection

        Returns:
            DataFrame with seasonal patterns
        """
        sql = """
        SELECT
            season,
            SUM(event_count) AS total_events,
            AVG(avg_magnitude) AS avg_magnitude,
            AVG(avg_depth) AS avg_depth
        FROM cube_time_magnitude
        GROUP BY season
        ORDER BY 
            CASE season
                WHEN 'Spring' THEN 1
                WHEN 'Summer' THEN 2
                WHEN 'Fall' THEN 3
                WHEN 'Winter' THEN 4
            END
        """

        return conn.execute(sql).df()

    def get_moon_phase_analysis(
        self, conn: duckdb.DuckDBPyConnection, min_magnitude: Optional[float] = None
    ) -> pd.DataFrame:
        """Get earthquake distribution by moon phase and magnitude group.

        Args:
            conn: DuckDB connection
            min_magnitude: Minimum magnitude filter

        Returns:
            DataFrame with moon phase analysis
        """
        where_clause = ""
        if min_magnitude is not None:
            where_clause = f"WHERE avg_magnitude >= {min_magnitude}"

        sql = f"""
        SELECT
            moon_phase_name,
            moon_phase,
            magnitude_group,
            event_count,
            avg_magnitude,
            max_magnitude,
            avg_depth
        FROM cube_moon_phase
        {where_clause}
        ORDER BY moon_phase, magnitude_group
        """

        return conn.execute(sql).df()

    def get_moon_phase_filtered(
        self, conn: duckdb.DuckDBPyConnection, min_magnitude: Optional[float] = None
    ) -> pd.DataFrame:
        """Get earthquake distribution by moon phase with magnitude filter applied.

        Args:
            conn: DuckDB connection
            min_magnitude: Minimum magnitude filter

        Returns:
            DataFrame with moon phase analysis (filtered)
        """
        where_clause = ""
        if min_magnitude is not None:
            where_clause = f"WHERE m.magnitude >= {min_magnitude}"

        sql = f"""
        SELECT
            f.moon_phase_name,
            f.moon_phase,
            CASE 
                WHEN m.magnitude < 4.0 THEN '1-3'
                WHEN m.magnitude >= 4.0 AND m.magnitude < 5.0 THEN '4'
                WHEN m.magnitude >= 5.0 AND m.magnitude < 6.0 THEN '5'
                WHEN m.magnitude >= 6.0 AND m.magnitude < 8.0 THEN '6-7'
                ELSE '8-9'
            END AS magnitude_group,
            COUNT(*) AS event_count,
            AVG(m.magnitude) AS avg_magnitude,
            MAX(m.magnitude) AS max_magnitude,
            AVG(f.depth) AS avg_depth
        FROM fact_earthquakes f
        JOIN dim_magnitude m ON f.magnitude_id = m.magnitude_id
        {where_clause}
        GROUP BY f.moon_phase_name, f.moon_phase, magnitude_group
        ORDER BY f.moon_phase, magnitude_group
        """

        return conn.execute(sql).df()

    def get_events_for_map(
        self, 
        conn: duckdb.DuckDBPyConnection,
        min_magnitude: Optional[float] = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """Get earthquake events formatted for map visualization.

        Args:
            conn: DuckDB connection
            min_magnitude: Minimum magnitude filter
            limit: Maximum number of events to return

        Returns:
            DataFrame with map-ready data
        """
        where_clause = ""
        if min_magnitude is not None:
            where_clause = f"WHERE m.magnitude >= {min_magnitude}"

        sql = f"""
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
        {where_clause}
        ORDER BY m.magnitude DESC, t.datetime DESC
        LIMIT {limit}
        """

        return conn.execute(sql).df()
