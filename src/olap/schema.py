"""OLAP schema definition for earthquake data warehouse."""

from typing import Optional

import duckdb

from src.utils.config import Config, get_config
from src.utils.logger import LoggerMixin, print_info, print_success


class OLAPSchema(LoggerMixin):
    """Define and create OLAP star schema for earthquake analytics."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize schema manager.

        Args:
            config: Configuration object (uses global config if None)
        """
        self.config = config or get_config()
        self.schema_config = self.config.duckdb.schema_tables

    def create_star_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create complete star schema with dimensions and fact table.

        Args:
            conn: DuckDB connection
        """
        self.logger.info("Creating OLAP star schema")
        print_info("Creating star schema for OLAP analytics...")

        # Create dimensions
        self._create_dim_time(conn)
        self._create_dim_location(conn)
        self._create_dim_magnitude(conn)

        # Create fact table
        self._create_fact_earthquakes(conn)

        print_success("Star schema created successfully")

    def _create_dim_time(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create time dimension table.

        Args:
            conn: DuckDB connection
        """
        self.logger.info("Creating dim_time")

        dim_time_table = self.schema_config.get("dim_time", "dim_time")

        sql = f"""
        CREATE OR REPLACE TABLE {dim_time_table} AS
        SELECT DISTINCT
            ROW_NUMBER() OVER (ORDER BY datetime) AS time_id,
            datetime,
            DATE_TRUNC('day', datetime) AS date,
            year,
            month,
            day,
            hour,
            day_of_week,
            CASE day_of_week
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
                WHEN 7 THEN 'Sunday'
            END AS day_name,
            CASE 
                WHEN month IN (12, 1, 2) THEN 'Winter'
                WHEN month IN (3, 4, 5) THEN 'Spring'
                WHEN month IN (6, 7, 8) THEN 'Summer'
                ELSE 'Fall'
            END AS season,
            CASE 
                WHEN day_of_week IN (6, 7) THEN true
                ELSE false
            END AS is_weekend
        FROM (
            SELECT DISTINCT datetime, year, month, day, hour, day_of_week
            FROM raw_earthquakes
            WHERE datetime IS NOT NULL
        )
        ORDER BY datetime
        """

        conn.execute(sql)
        result = conn.execute(f"SELECT COUNT(*) FROM {dim_time_table}").fetchone()
        count = result[0] if result else 0
        self.logger.info(f"Created {dim_time_table} with {count:,} records")

    def _create_dim_location(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create location dimension table.

        Args:
            conn: DuckDB connection
        """
        self.logger.info("Creating dim_location")

        dim_location_table = self.schema_config.get("dim_location", "dim_location")

        sql = f"""
        CREATE OR REPLACE TABLE {dim_location_table} AS
        SELECT DISTINCT
            ROW_NUMBER() OVER (ORDER BY latitude, longitude, place, region) AS location_id,
            latitude,
            longitude,
            place,
            region,
            -- Hemisphere classification
            CASE 
                WHEN latitude >= 0 THEN 'Northern'
                ELSE 'Southern'
            END AS hemisphere_ns,
            CASE 
                WHEN longitude >= 0 THEN 'Eastern'
                ELSE 'Western'
            END AS hemisphere_ew,
            -- Approximate geographic zones
            CASE 
                WHEN latitude BETWEEN -23.5 AND 23.5 THEN 'Tropical'
                WHEN latitude BETWEEN 23.5 AND 66.5 OR latitude BETWEEN -66.5 AND -23.5 THEN 'Temperate'
                ELSE 'Polar'
            END AS climate_zone
        FROM (
            SELECT DISTINCT 
                latitude, 
                longitude, 
                COALESCE(place, 'Unknown') as place,
                COALESCE(region, 'Unknown') as region
            FROM raw_earthquakes
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        )
        """

        conn.execute(sql)
        result = conn.execute(f"SELECT COUNT(*) FROM {dim_location_table}").fetchone()
        count = result[0] if result else 0
        self.logger.info(f"Created {dim_location_table} with {count:,} records")

    def _create_dim_magnitude(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create magnitude dimension table.

        Args:
            conn: DuckDB connection
        """
        self.logger.info("Creating dim_magnitude")

        dim_magnitude_table = self.schema_config.get("dim_magnitude", "dim_magnitude")

        sql = f"""
        CREATE OR REPLACE TABLE {dim_magnitude_table} AS
        SELECT DISTINCT
            ROW_NUMBER() OVER (ORDER BY magnitude, magnitude_type, magnitude_category) AS magnitude_id,
            magnitude,
            magnitude_category,
            magnitude_type,
            -- Richter scale effects description
            CASE 
                WHEN magnitude < 2.0 THEN 'Micro - Not felt'
                WHEN magnitude < 3.0 THEN 'Minor - Rarely felt'
                WHEN magnitude < 4.0 THEN 'Light - Often felt, rarely causes damage'
                WHEN magnitude < 5.0 THEN 'Moderate - Notable shaking, slight damage'
                WHEN magnitude < 6.0 THEN 'Strong - Can cause damage in populated areas'
                WHEN magnitude < 7.0 THEN 'Major - Serious damage over large areas'
                WHEN magnitude < 8.0 THEN 'Great - Serious damage over very large areas'
                ELSE 'Epic - Devastating over extremely large areas'
            END AS effects_description,
            -- Energy release (approximate, in joules)
            POWER(10, (1.5 * magnitude + 4.8)) AS energy_joules
        FROM (
            SELECT DISTINCT 
                magnitude,
                COALESCE(magnitude_type, 'Unknown') as magnitude_type,
                magnitude_category
            FROM raw_earthquakes
            WHERE magnitude IS NOT NULL
        )
        """

        conn.execute(sql)
        result = conn.execute(f"SELECT COUNT(*) FROM {dim_magnitude_table}").fetchone()
        count = result[0] if result else 0
        self.logger.info(f"Created {dim_magnitude_table} with {count:,} records")

    def _create_fact_earthquakes(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create fact table linking all dimensions.

        Args:
            conn: DuckDB connection
        """
        self.logger.info("Creating fact_earthquakes")

        fact_table = self.schema_config.get("fact_table", "fact_earthquakes")
        dim_time = self.schema_config.get("dim_time", "dim_time")
        dim_location = self.schema_config.get("dim_location", "dim_location")
        dim_magnitude = self.schema_config.get("dim_magnitude", "dim_magnitude")

        # Drop existing table first
        conn.execute(f"DROP TABLE IF EXISTS {fact_table}")

        # Create fact table with proper joins and type casting
        sql = f"""
        CREATE TABLE {fact_table} AS
        SELECT
            r.event_id AS earthquake_id,
            t.time_id,
            l.location_id,
            m.magnitude_id,
            r.event_id,
            COALESCE(r.depth, 0.0) AS depth,
            COALESCE(r.depth_category, 'Unknown') AS depth_category,
            COALESCE(CAST(r.num_stations AS INTEGER), 0) AS num_stations,
            COALESCE(CAST(r.azimuthal_gap AS DOUBLE), 0.0) AS azimuthal_gap,
            COALESCE(CAST(r.min_distance AS DOUBLE), 0.0) AS min_distance,
            COALESCE(CAST(r.rms AS DOUBLE), 0.0) AS rms,
            COALESCE(CAST(r.horizontal_error AS DOUBLE), 0.0) AS horizontal_error,
            COALESCE(CAST(r.depth_error AS DOUBLE), 0.0) AS depth_error,
            COALESCE(CAST(r.magnitude_error AS DOUBLE), 0.0) AS magnitude_error,
            COALESCE(r.network, 'Unknown') AS network,
            COALESCE(r.status, 'Unknown') AS status,
            COALESCE(r.event_type, 'Unknown') AS event_type,
            COALESCE(CAST(r.moon_phase AS DOUBLE), 0.0) AS moon_phase,
            COALESCE(r.moon_phase_name, 'Unknown') AS moon_phase_name
        FROM raw_earthquakes r
        LEFT JOIN {dim_time} t 
            ON r.datetime = t.datetime
        LEFT JOIN {dim_location} l 
            ON r.latitude = l.latitude 
            AND r.longitude = l.longitude
            AND COALESCE(r.place, 'Unknown') = l.place
            AND COALESCE(r.region, 'Unknown') = l.region
        LEFT JOIN {dim_magnitude} m 
            ON r.magnitude = m.magnitude
            AND COALESCE(r.magnitude_type, 'Unknown') = m.magnitude_type
            AND r.magnitude_category = m.magnitude_category
        WHERE r.datetime IS NOT NULL 
            AND r.latitude IS NOT NULL 
            AND r.longitude IS NOT NULL
            AND r.magnitude IS NOT NULL
        """

        conn.execute(sql)
        
        # Get counts
        result = conn.execute(f"SELECT COUNT(*) FROM {fact_table}").fetchone()
        count = result[0] if result else 0
        
        # Check for duplicates
        dup_result = conn.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT event_id) as duplicates
            FROM {fact_table}
        """).fetchone()
        duplicates = dup_result[0] if dup_result else 0
        
        self.logger.info(f"Created {fact_table} with {count:,} records ({duplicates} duplicates)")
        
        if duplicates > 0:
            self.logger.warning(f"Removing {duplicates} duplicate event_ids")
            # Deduplicate
            conn.execute(f"""
                CREATE OR REPLACE TABLE {fact_table} AS
                SELECT DISTINCT ON (event_id) *
                FROM {fact_table}
                ORDER BY event_id
            """)
            
            result = conn.execute(f"SELECT COUNT(*) FROM {fact_table}").fetchone()
            final_count = result[0] if result else 0
            self.logger.info(f"After deduplication: {final_count:,} records")

    def validate_schema(self, conn: duckdb.DuckDBPyConnection) -> dict:
        """Validate that the schema was created correctly.

        Args:
            conn: DuckDB connection

        Returns:
            Dictionary with validation results
        """
        self.logger.info("Validating schema")

        validation = {
            "dim_time": self._validate_table(conn, self.schema_config.get("dim_time", "dim_time")),
            "dim_location": self._validate_table(conn, self.schema_config.get("dim_location", "dim_location")),
            "dim_magnitude": self._validate_table(conn, self.schema_config.get("dim_magnitude", "dim_magnitude")),
            "fact_earthquakes": self._validate_table(conn, self.schema_config.get("fact_table", "fact_earthquakes")),
        }

        all_valid = all(v["exists"] for v in validation.values())

        if all_valid:
            self.logger.info("Schema validation passed")
            print_success("Schema validation passed")
        else:
            self.logger.error("Schema validation failed")

        return validation

    def _validate_table(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> dict:
        """Validate a single table.

        Args:
            conn: DuckDB connection
            table_name: Name of table to validate

        Returns:
            Dictionary with validation info
        """
        try:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            count = result[0] if result else 0
            return {
                "exists": True,
                "row_count": count,
            }
        except Exception as e:
            self.logger.error(f"Table {table_name} validation failed: {e}")
            return {
                "exists": False,
                "error": str(e),
            }

    def get_schema_summary(self, conn: duckdb.DuckDBPyConnection) -> dict:
        """Get summary of the OLAP schema.

        Args:
            conn: DuckDB connection

        Returns:
            Dictionary with schema summary
        """
        summary = {}

        tables = [
            self.schema_config.get("dim_time", "dim_time"),
            self.schema_config.get("dim_location", "dim_location"),
            self.schema_config.get("dim_magnitude", "dim_magnitude"),
            self.schema_config.get("fact_table", "fact_earthquakes"),
        ]

        for table in tables:
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                count = result[0] if result else 0
                columns = conn.execute(f"DESCRIBE {table}").fetchall()
                summary[table] = {
                    "row_count": count,
                    "column_count": len(columns),
                    "columns": [col[0] for col in columns],
                }
            except Exception as e:
                summary[table] = {"error": str(e)}

        return summary
