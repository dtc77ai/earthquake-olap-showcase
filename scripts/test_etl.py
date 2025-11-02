"""Test script for ETL pipeline."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl.download import DataDownloader
from src.etl.extract import DataExtractor
from src.etl.load import DataLoader
from src.etl.transform import DataTransformer
from src.utils.logger import print_section, setup_logging


def main():
    """Test ETL pipeline."""
    setup_logging()

    # Download
    print_section("Step 1: Download Data")
    downloader = DataDownloader()
    file_path = downloader.download()

    # Extract
    print_section("Step 2: Extract Data")
    extractor = DataExtractor()
    df = extractor.extract_csv(file_path)

    print("\nSchema Info:")
    schema = extractor.get_schema_info(df)
    print(f"Shape: {schema['shape']}")
    print(f"Columns: {len(schema['columns'])}")

    # Transform
    print_section("Step 3: Transform Data")
    transformer = DataTransformer()
    df_transformed = transformer.transform(df)

    print("\nSummary Statistics:")
    stats = transformer.get_summary_statistics(df_transformed)
    for key, value in stats.items():
        print(f"{key}: {value}")

    # Load
    print_section("Step 4: Load into DuckDB")
    with DataLoader() as loader:
        loader.load_raw_data(df_transformed)
        loader.create_indexes()

        # Get table info
        info = loader.get_table_info()
        print(f"\nTable: {info['table_name']}")
        print(f"Rows: {info['row_count']:,}")
        print(f"Columns: {len(info['columns'])}")

        # Export to Parquet
        parquet_path = loader.export_to_parquet()

    print_section("ETL Pipeline Complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
