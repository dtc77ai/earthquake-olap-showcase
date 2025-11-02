"""OLAP cube creation and management."""

from typing import Optional

import duckdb

from src.utils.config import Config, get_config
from src.utils.logger import LoggerMixin, print_info, print_success


class OLAPCube(LoggerMixin):
    """Create and manage OLAP cubes for multi-dimensional analysis."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize cube manager.

        Args:
            config: Configuration object (uses global config if None)
        """
        self.config = config or get_config()
        self.schema_config = self.config.duckdb.schema_tables

    def create_cubes(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create all OLAP cubes.

        Args:
            conn: DuckDB connection
        """
        self.logger.info("Creating OLAP cubes")
        print_info("Creating OLAP cubes for analytics...")

        self._create_time_magnitude_cube(conn)
        self._create_location_magnitude_cube(conn)
        self._create_depth_analysis_cube(conn)
        self._create_temporal_trends_cube(conn)
        self._create_moon_phase_cube(conn) 

        print_success("OLAP cubes created successfully")

    def _create_time_magnitude_cube(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create cube for time-based magnitude analysis.

        Args:
            conn: DuckDB connection
        """
        self.logger.info("Creating cube_time_magnitude")

        sql = """
        CREATE OR REPLACE TABLE cube_time_magnitude AS
        SELECT
            t.year,
            t.month,
            t.day_name,
            t.hour,
            t.season,
            t.is_weekend,
            m.magnitude_category,
            COUNT(*) AS event_count,
            AVG(m.magnitude) AS avg_magnitude,
            MIN(m.magnitude) AS min_magnitude,
            MAX(m.magnitude) AS max_magnitude,
            AVG(f.depth) AS avg_depth,
            SUM(m.energy_joules) AS total_energy
        FROM fact_earthquakes f
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN dim_magnitude m ON f.magnitude_id = m.magnitude_id
        GROUP BY 
            t.year, t.month, t.day_name, t.hour, 
            t.season, t.is_weekend, m.magnitude_category
        """

        conn.execute(sql)
        result = conn.execute("SELECT COUNT(*) FROM cube_time_magnitude").fetchone()
        count = result[0] if result else 0
        self.logger.info(f"Created cube_time_magnitude with {count:,} aggregations")

    def _create_location_magnitude_cube(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create cube for location-based magnitude analysis.

        Args:
            conn: DuckDB connection
        """
        self.logger.info("Creating cube_location_magnitude")

        sql = """
        CREATE OR REPLACE TABLE cube_location_magnitude AS
        SELECT
            l.region,
            l.hemisphere_ns,
            l.hemisphere_ew,
            l.climate_zone,
            m.magnitude_category,
            COUNT(*) AS event_count,
            AVG(m.magnitude) AS avg_magnitude,
            MAX(m.magnitude) AS max_magnitude,
            AVG(f.depth) AS avg_depth,
            AVG(l.latitude) AS center_latitude,
            AVG(l.longitude) AS center_longitude
        FROM fact_earthquakes f
        JOIN dim_location l ON f.location_id = l.location_id
        JOIN dim_magnitude m ON f.magnitude_id = m.magnitude_id
        GROUP BY 
            l.region, l.hemisphere_ns, l.hemisphere_ew, 
            l.climate_zone, m.magnitude_category
        """

        conn.execute(sql)
        result = conn.execute("SELECT COUNT(*) FROM cube_location_magnitude").fetchone()
        count = result[0] if result else 0
        self.logger.info(f"Created cube_location_magnitude with {count:,} aggregations")

    def _create_depth_analysis_cube(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create cube for depth-based analysis.

        Args:
            conn: DuckDB connection
        """
        self.logger.info("Creating cube_depth_analysis")

        sql = """
        CREATE OR REPLACE TABLE cube_depth_analysis AS
        SELECT
            f.depth_category,
            m.magnitude_category,
            t.season,
            COUNT(*) AS event_count,
            AVG(f.depth) AS avg_depth,
            AVG(m.magnitude) AS avg_magnitude,
            AVG(f.num_stations) AS avg_stations,
            AVG(f.azimuthal_gap) AS avg_gap,
            AVG(f.horizontal_error) AS avg_horizontal_error,
            AVG(f.depth_error) AS avg_depth_error
        FROM fact_earthquakes f
        JOIN dim_magnitude m ON f.magnitude_id = m.magnitude_id
        JOIN dim_time t ON f.time_id = t.time_id
        GROUP BY f.depth_category, m.magnitude_category, t.season
        """

        conn.execute(sql)
        result = conn.execute("SELECT COUNT(*) FROM cube_depth_analysis").fetchone()
        count = result[0] if result else 0
        self.logger.info(f"Created cube_depth_analysis with {count:,} aggregations")

    def _create_temporal_trends_cube(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create cube for temporal trend analysis.

        Args:
            conn: DuckDB connection
        """
        self.logger.info("Creating cube_temporal_trends")

        sql = """
        CREATE OR REPLACE TABLE cube_temporal_trends AS
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
        GROUP BY t.date, t.year, t.month, t.day_of_week
        ORDER BY t.date
        """

        conn.execute(sql)
        result = conn.execute("SELECT COUNT(*) FROM cube_temporal_trends").fetchone()
        count = result[0] if result else 0
        self.logger.info(f"Created cube_temporal_trends with {count:,} aggregations")

    def _create_moon_phase_cube(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create cube for moon phase analysis.

        Args:
            conn: DuckDB connection
        """
        self.logger.info("Creating cube_moon_phase")

        sql = """
        CREATE OR REPLACE TABLE cube_moon_phase AS
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
        GROUP BY f.moon_phase_name, f.moon_phase, magnitude_group
        ORDER BY f.moon_phase
        """

        conn.execute(sql)
        result = conn.execute("SELECT COUNT(*) FROM cube_moon_phase").fetchone()
        count = result[0] if result else 0
        self.logger.info(f"Created cube_moon_phase with {count:,} aggregations")

    def get_cube_summary(self, conn: duckdb.DuckDBPyConnection) -> dict:
        """Get summary of all cubes.

        Args:
            conn: DuckDB connection

        Returns:
            Dictionary with cube summaries
        """
        cubes = [
            "cube_time_magnitude",
            "cube_location_magnitude",
            "cube_depth_analysis",
            "cube_temporal_trends",
            "cube_moon_phase",
        ]

        summary = {}
        for cube in cubes:
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {cube}").fetchone()
                count = result[0] if result else 0
                summary[cube] = {"row_count": count, "exists": True}
            except Exception as e:
                summary[cube] = {"exists": False, "error": str(e)}

        return summary
