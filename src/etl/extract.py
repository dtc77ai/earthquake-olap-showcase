"""Data extraction module for parsing CSV files."""

from pathlib import Path
from typing import List, Optional, Union

import pandas as pd
import polars as pl

from src.utils.config import Config, get_config
from src.utils.logger import LoggerMixin, print_info, print_success, print_warning


class DataExtractor(LoggerMixin):
    """Extract and parse earthquake data from CSV files."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize extractor.

        Args:
            config: Configuration object (uses global config if None)
        """
        self.config = config or get_config()

    def extract_csv(self, file_path: Path, use_polars: bool = True) -> pl.DataFrame | pd.DataFrame:
        """Extract data from CSV file.

        Args:
            file_path: Path to CSV file
            use_polars: Use Polars for faster processing (default: True)

        Returns:
            DataFrame with extracted data

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        self.logger.info(f"Extracting data from: {file_path}")
        print_info(f"Reading CSV file: {file_path.name}")

        file_size = file_path.stat().st_size
        self.logger.info(f"File size: {file_size:,} bytes")

        if use_polars:
            df = self._extract_with_polars(file_path)
        else:
            df = self._extract_with_pandas(file_path)

        row_count = len(df)
        col_count = len(df.columns)

        if row_count == 0:
            self.logger.warning(f"File {file_path.name} is empty (only headers)")
            print_warning(f"File {file_path.name} is empty - skipping")
            return df  # Return empty dataframe

        self.logger.info(f"Extracted {row_count:,} rows and {col_count} columns")
        print_success(f"Extracted {row_count:,} rows × {col_count} columns")

        return df

    def _extract_with_polars(self, file_path: Path) -> pl.DataFrame:
        """Extract using Polars (faster for large files).

        Args:
            file_path: Path to CSV file

        Returns:
            Polars DataFrame
        """
        try:
            df = pl.read_csv(
                file_path,
                infer_schema_length=10000,
                ignore_errors=True,
            )
            self.logger.info("Extraction completed with Polars")
            return df
        except Exception as e:
            self.logger.error(f"Polars extraction failed: {e}")
            self.logger.info("Falling back to Pandas")
            return pl.from_pandas(self._extract_with_pandas(file_path))

    def _extract_with_pandas(self, file_path: Path) -> pd.DataFrame:
        """Extract using Pandas (fallback option).

        Args:
            file_path: Path to CSV file

        Returns:
            Pandas DataFrame
        """
        df = pd.read_csv(file_path, low_memory=False)
        self.logger.info("Extraction completed with Pandas")
        return df

    def get_schema_info(self, df: Union[pl.DataFrame, pd.DataFrame]) -> dict:
        """Get schema information from DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            Dictionary with schema information
        """
        if isinstance(df, pl.DataFrame):
            return {
                "columns": df.columns,
                "dtypes": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
                "shape": df.shape,
                "null_counts": df.null_count().to_dicts()[0],
            }
        else:  # pandas
            return {
                "columns": df.columns.tolist(),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "shape": df.shape,
                "null_counts": df.isnull().sum().to_dict(),
            }

    def preview_data(
        self, df: Union[pl.DataFrame, pd.DataFrame], n_rows: int = 5
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """Preview first n rows of data.

        Args:
            df: Input DataFrame
            n_rows: Number of rows to preview

        Returns:
            DataFrame with first n rows
        """
        if isinstance(df, pl.DataFrame):
            return df.head(n_rows)
        else:
            return df.head(n_rows)

    def extract_multiple_csv(
        self, file_paths: List[Path], use_polars: bool = True
    ) -> pl.DataFrame:
        """Extract and combine data from multiple CSV files.

        Args:
            file_paths: List of paths to CSV files
            use_polars: Use Polars for faster processing (default: True)

        Returns:
            Combined DataFrame with extracted data

        Raises:
            FileNotFoundError: If any file doesn't exist
            RuntimeError: If all files are empty
        """
        self.logger.info(f"Extracting data from {len(file_paths)} file(s)")
        print_info(f"Reading {len(file_paths)} CSV file(s)...")

        dfs = []
        total_rows = 0

        for idx, file_path in enumerate(file_paths, 1):
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")

            self.logger.info(f"Processing file {idx}/{len(file_paths)}: {file_path.name}")
            print_info(f"File {idx}/{len(file_paths)}: {file_path.name}")

            df = self.extract_csv(file_path, use_polars=use_polars)
            
            # Skip empty dataframes
            if len(df) == 0:
                self.logger.warning(f"Skipping empty file: {file_path.name}")
                continue
            
            # Convert to Polars if needed
            if isinstance(df, pd.DataFrame):
                df = pl.from_pandas(df)
            
            dfs.append(df)
            total_rows += len(df)

        # Check if we have any data
        if not dfs:
            raise RuntimeError("All files are empty - no data to process")

        # Combine all dataframes
        self.logger.info(f"Combining {len(dfs)} dataframe(s)...")
        print_info("Combining data from all files...")

        if len(dfs) == 1:
            combined_df = dfs[0]
        else:
            combined_df = pl.concat(dfs, how="vertical")

        # Remove duplicates based on event_id if it exists
        if "id" in combined_df.columns:
            initial_count = len(combined_df)
            combined_df = combined_df.unique(subset=["id"], keep="first")
            duplicates_removed = initial_count - len(combined_df)
            
            if duplicates_removed > 0:
                self.logger.info(f"Removed {duplicates_removed:,} duplicate events")
                print_warning(f"Removed {duplicates_removed:,} duplicate events")

        row_count = len(combined_df)
        col_count = len(combined_df.columns)

        self.logger.info(f"Combined total: {row_count:,} rows and {col_count} columns")
        print_success(f"Combined: {row_count:,} rows × {col_count} columns")

        return combined_df
