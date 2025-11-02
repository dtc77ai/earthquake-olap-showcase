"""Data transformation module for cleaning and enriching earthquake data."""

from skyfield import almanac
from skyfield.api import load

from datetime import datetime
from typing import Optional, Union

import polars as pl
import pandas as pd

from src.utils.config import Config, get_config
from src.utils.logger import LoggerMixin, print_info, print_success, print_warning


class DataTransformer(LoggerMixin):
    """Transform and clean earthquake data."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize transformer.

        Args:
            config: Configuration object (uses global config if None)
        """
        self.config = config or get_config()
        self.validation_config = self.config.etl.validation

    def transform(self, df: Union[pl.DataFrame, pd.DataFrame]) -> pl.DataFrame:
        """Apply all transformations to the data.

        Args:
            df: Input DataFrame

        Returns:
            Transformed Polars DataFrame
        """
        self.logger.info("Starting data transformation")
        print_info("Transforming earthquake data...")

        # Convert to Polars if needed
        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)

        initial_rows = len(df)

        # Apply transformations
        df = self._standardize_columns(df)
        df = self._clean_data(df)
        df = self._validate_data(df)
        df = self._enrich_data(df)
        df = self._add_moon_phase(df)

        final_rows = len(df)
        removed_rows = initial_rows - final_rows

        if removed_rows > 0:
            self.logger.warning(f"Removed {removed_rows:,} invalid rows")
            print_warning(f"Removed {removed_rows:,} invalid rows")

        self.logger.info(f"Transformation complete: {final_rows:,} rows")
        print_success(f"Transformation complete: {final_rows:,} valid rows")

        return df

    def _standardize_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Standardize column names and types.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with standardized columns
        """
        self.logger.info("Standardizing columns")

        # Expected columns from USGS earthquake data
        column_mapping = {
            "time": "time",
            "latitude": "latitude",
            "longitude": "longitude",
            "depth": "depth",
            "mag": "magnitude",
            "magType": "magnitude_type",
            "nst": "num_stations",
            "gap": "azimuthal_gap",
            "dmin": "min_distance",
            "rms": "rms",
            "net": "network",
            "id": "event_id",
            "updated": "updated",
            "place": "place",
            "type": "event_type",
            "horizontalError": "horizontal_error",
            "depthError": "depth_error",
            "magError": "magnitude_error",
            "magNst": "magnitude_stations",
            "status": "status",
            "locationSource": "location_source",
            "magSource": "magnitude_source",
        }

        # Rename columns that exist
        existing_renames = {
            old: new for old, new in column_mapping.items() if old in df.columns
        }
        df = df.rename(existing_renames)

        return df

    def _clean_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean the data by handling nulls and invalid values.

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        self.logger.info("Cleaning data")

        # Remove rows with null critical fields
        critical_fields = ["time", "latitude", "longitude", "magnitude"]
        existing_critical = [f for f in critical_fields if f in df.columns]

        if existing_critical:
            df = df.drop_nulls(subset=existing_critical)

        # Fill nulls in optional fields
        if "depth" in df.columns:
            df = df.with_columns(pl.col("depth").fill_null(0.0))

        if "place" in df.columns:
            df = df.with_columns(pl.col("place").fill_null("Unknown"))

        return df

    def _validate_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate data against configured rules.

        Args:
            df: Input DataFrame

        Returns:
            Validated DataFrame with invalid rows removed
        """
        self.logger.info("Validating data")

        # Validate magnitude
        if "magnitude" in df.columns:
            min_mag = self.validation_config.get("min_magnitude", -2.0)
            max_mag = self.validation_config.get("max_magnitude", 10.0)

            df = df.filter(
                (pl.col("magnitude") >= min_mag) & (pl.col("magnitude") <= max_mag)
            )

        # Validate depth
        if "depth" in df.columns:
            min_depth = self.validation_config.get("min_depth", -10.0)
            max_depth = self.validation_config.get("max_depth", 1000.0)

            df = df.filter((pl.col("depth") >= min_depth) & (pl.col("depth") <= max_depth))

        # Validate coordinates
        if "latitude" in df.columns and "longitude" in df.columns:
            df = df.filter(
                (pl.col("latitude") >= -90)
                & (pl.col("latitude") <= 90)
                & (pl.col("longitude") >= -180)
                & (pl.col("longitude") <= 180)
            )

        return df

    def _enrich_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Enrich data with derived fields.

        Args:
            df: Input DataFrame

        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")

        # Parse time field and extract components
        if "time" in df.columns:
            df = df.with_columns([
                pl.col("time").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.3fZ").alias("datetime"),
            ])

            df = df.with_columns([
                pl.col("datetime").dt.year().alias("year"),
                pl.col("datetime").dt.month().alias("month"),
                pl.col("datetime").dt.day().alias("day"),
                pl.col("datetime").dt.hour().alias("hour"),
                pl.col("datetime").dt.weekday().alias("day_of_week"),
            ])

        # Add magnitude categories
        if "magnitude" in df.columns:
            df = df.with_columns([
                pl.when(pl.col("magnitude") < 3.0)
                .then(pl.lit("Minor"))
                .when(pl.col("magnitude") < 5.0)
                .then(pl.lit("Light"))
                .when(pl.col("magnitude") < 6.0)
                .then(pl.lit("Moderate"))
                .when(pl.col("magnitude") < 7.0)
                .then(pl.lit("Strong"))
                .when(pl.col("magnitude") < 8.0)
                .then(pl.lit("Major"))
                .otherwise(pl.lit("Great"))
                .alias("magnitude_category")
            ])

        # Add depth categories
        if "depth" in df.columns:
            df = df.with_columns([
                pl.when(pl.col("depth") < 70)
                .then(pl.lit("Shallow"))
                .when(pl.col("depth") < 300)
                .then(pl.lit("Intermediate"))
                .otherwise(pl.lit("Deep"))
                .alias("depth_category")
            ])

        # Extract region from place (simplified)
        if "place" in df.columns:
            df = df.with_columns([
                pl.col("place")
                .str.split(" of ")
                .list.last()
                .alias("region")
            ])

        return df

    def _add_moon_phase(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add moon phase information to the data.

        Args:
            df: Input DataFrame with datetime column

        Returns:
            DataFrame with moon phase columns added
        """
        self.logger.info("Adding moon phase data")

        if "datetime" not in df.columns:
            self.logger.warning("No datetime column found, skipping moon phase enrichment")
            return df

        # Load ephemeris data (downloads if not cached)
        from skyfield.api import utc
        
        ts = load.timescale()
        eph = load('de421.bsp')  # JPL ephemeris

        # Convert datetime to list for processing
        datetimes = df["datetime"].to_list()

        moon_phases = []
        moon_phase_names = []

        for dt in datetimes:
            try:
                # Ensure datetime is timezone-aware (assume UTC if naive)
                if dt.tzinfo is None:
                    dt_utc = dt.replace(tzinfo=utc)
                else:
                    dt_utc = dt.astimezone(utc)
                
                # Convert to skyfield time
                t = ts.from_datetime(dt_utc)

                # Calculate moon phase (0 = new moon, 0.5 = full moon, 1 = new moon)
                earth = eph['earth']
                sun = eph['sun']
                moon = eph['moon']

                # Calculate phase
                phase = almanac.moon_phase(eph, t)
                phase_value = float(phase.degrees) / 360.0  # type: ignore[arg-type]  # Normalize to 0-1

                moon_phases.append(phase_value)

                # Categorize phase
                if phase_value < 0.0625 or phase_value >= 0.9375:
                    phase_name = "New Moon"
                elif 0.0625 <= phase_value < 0.1875:
                    phase_name = "Waxing Crescent"
                elif 0.1875 <= phase_value < 0.3125:
                    phase_name = "First Quarter"
                elif 0.3125 <= phase_value < 0.4375:
                    phase_name = "Waxing Gibbous"
                elif 0.4375 <= phase_value < 0.5625:
                    phase_name = "Full Moon"
                elif 0.5625 <= phase_value < 0.6875:
                    phase_name = "Waning Gibbous"
                elif 0.6875 <= phase_value < 0.8125:
                    phase_name = "Last Quarter"
                else:
                    phase_name = "Waning Crescent"

                moon_phase_names.append(phase_name)
                
            except Exception as e:
                self.logger.warning(f"Error calculating moon phase for {dt}: {e}")
                moon_phases.append(0.0)
                moon_phase_names.append("Unknown")

        # Add to dataframe
        df = df.with_columns([
            pl.Series("moon_phase", moon_phases),
            pl.Series("moon_phase_name", moon_phase_names),
        ])

        self.logger.info("Moon phase data added successfully")

        return df

    def get_summary_statistics(self, df: pl.DataFrame) -> dict:
        """Get summary statistics of the transformed data.

        Args:
            df: Transformed DataFrame

        Returns:
            Dictionary with summary statistics
        """
        stats = {
            "total_rows": len(df),
            "date_range": None,
            "magnitude_range": None,
            "depth_range": None,
            "unique_regions": None,
        }

        if "datetime" in df.columns:
            stats["date_range"] = {
                "min": df["datetime"].min(),
                "max": df["datetime"].max(),
            }

        if "magnitude" in df.columns:
            stats["magnitude_range"] = {
                "min": df["magnitude"].min(),
                "max": df["magnitude"].max(),
                "mean": df["magnitude"].mean(),
            }

        if "depth" in df.columns:
            stats["depth_range"] = {
                "min": df["depth"].min(),
                "max": df["depth"].max(),
                "mean": df["depth"].mean(),
            }

        if "region" in df.columns:
            stats["unique_regions"] = df["region"].n_unique()

        return stats
