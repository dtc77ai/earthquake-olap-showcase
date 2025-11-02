"""Main ETL pipeline script with benchmarking."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.benchmark.metrics import BenchmarkContext, BenchmarkTracker, format_bytes
from src.etl.download import DataDownloader
from src.etl.extract import DataExtractor
from src.etl.load import DataLoader
from src.etl.transform import DataTransformer
from src.utils.logger import print_section, setup_logging

from src.utils.config import get_config

def main():
    """Run complete ETL pipeline with benchmarking."""
    setup_logging()

    config = get_config()

    # Initialize benchmark tracker
    tracker = BenchmarkTracker()
    print_section("🚀 Starting ETL Pipeline with Benchmarking")

    try:
        # Step 1: Download
        print_section("Step 1: Download Data")
        with BenchmarkContext(tracker, "download"):
            downloader = DataDownloader()
            file_paths = downloader.download()  # Now returns list of paths

            # Record file sizes
            total_size = sum(fp.stat().st_size for fp in file_paths)
            tracker.record_data_info("raw_file_count", len(file_paths))
            tracker.record_data_info("raw_file_size_bytes", total_size)
            tracker.record_data_info("raw_file_size_formatted", format_bytes(total_size))

        # Step 2: Extract
        print_section("Step 2: Extract Data")
        with BenchmarkContext(tracker, "extract"):
            extractor = DataExtractor()
            
            if len(file_paths) == 1:
                df = extractor.extract_csv(file_paths[0])
            else:
                df = extractor.extract_multiple_csv(file_paths)

            # Record row count
            row_count = len(df)
            col_count = len(df.columns)
            tracker.record_data_info("raw_row_count", row_count)
            tracker.record_data_info("raw_column_count", col_count)

        # Step 3: Transform
        print_section("Step 3: Transform Data")
        with BenchmarkContext(tracker, "transform"):
            transformer = DataTransformer()
            df_transformed = transformer.transform(df)

            # Record transformed data info
            transformed_rows = len(df_transformed)
            tracker.record_data_info("transformed_row_count", transformed_rows)
            tracker.record_data_info("rows_removed", row_count - transformed_rows)

            # Get summary statistics
            stats = transformer.get_summary_statistics(df_transformed)
            tracker.record_data_info("summary_statistics", stats)

        # Step 4: Load
        print_section("Step 4: Load into DuckDB")
        with BenchmarkContext(tracker, "load"):
            with DataLoader() as loader:
                loader.load_raw_data(df_transformed)
                loader.create_indexes()

                # Export to Parquet
                parquet_path = loader.export_to_parquet()
                parquet_size = parquet_path.stat().st_size
                tracker.record_data_info("parquet_file_size_bytes", parquet_size)
                tracker.record_data_info("parquet_file_size_formatted", format_bytes(parquet_size))

                # Calculate compression ratio (compare to total raw file size)
                raw_total_size = tracker.result.data_info.get("raw_file_size_bytes", 0)
                if raw_total_size > 0:
                    compression_ratio = (1 - parquet_size / raw_total_size) * 100
                    tracker.record_data_info("compression_ratio_percent", f"{compression_ratio:.2f}%")

        # Step 5: Build OLAP Layer
        print_section("Step 5: Build OLAP Layer")
        
        # Import OLAP modules here (after path is set)
        import duckdb
        from src.olap.cube import OLAPCube
        from src.olap.schema import OLAPSchema
        
        with BenchmarkContext(tracker, "olap_schema"):
            db_conn = duckdb.connect(str(config.get_duckdb_path()))
            
            # Configure DuckDB for large datasets
            temp_dir = Path(config.duckdb.temp_directory)
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            db_conn.execute(f"SET memory_limit='{config.duckdb.memory_limit}'")
            db_conn.execute(f"SET threads={config.duckdb.threads}")
            db_conn.execute(f"SET temp_directory='{temp_dir}'")
            db_conn.execute(f"SET max_temp_directory_size='{config.duckdb.max_temp_directory_size}'")
            db_conn.execute(f"SET preserve_insertion_order={str(config.duckdb.preserve_insertion_order).lower()}")
            
            schema = OLAPSchema()
            schema.create_star_schema(db_conn)
            
            # Record schema info
            summary = schema.get_schema_summary(db_conn)
            for table_name, info in summary.items():
                if "error" not in info:
                    tracker.record_data_info(f"{table_name}_rows", info["row_count"])
        
        with BenchmarkContext(tracker, "olap_cubes"):
            cube = OLAPCube()
            cube.create_cubes(db_conn)
            
            # Record cube info
            cube_summary = cube.get_cube_summary(db_conn)
            for cube_name, info in cube_summary.items():
                if info["exists"]:
                    tracker.record_data_info(f"{cube_name}_aggregations", info["row_count"])
            
            db_conn.close()

        # Record memory usage
        memory_usage = tracker.get_memory_usage()
        tracker.record_data_info("peak_memory_usage_mb", f"{memory_usage['rss_mb']:.2f}")

        # Print and save results
        print_section("📊 Benchmark Results")
        tracker.print_summary()

        # Save to file
        results_path = tracker.save_results()
        print(f"\n💾 Detailed results saved to: {results_path}")

        print_section("✅ ETL Pipeline Complete!")
        return 0

    except Exception as e:
        print(f"\n❌ Error during ETL pipeline: {e}")
        tracker.logger.error(f"ETL pipeline failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
