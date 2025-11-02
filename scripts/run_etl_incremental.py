"""Incremental ETL pipeline that processes data year by year."""

import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

from src.benchmark.metrics import BenchmarkContext, BenchmarkTracker, format_bytes
from src.etl.download import DataDownloader
from src.etl.extract import DataExtractor
from src.etl.load import DataLoader
from src.etl.transform import DataTransformer
from src.olap.cube import OLAPCube
from src.olap.schema import OLAPSchema
from src.utils.config import get_config
from src.utils.data_manager import DataManager
from src.utils.logger import print_section, print_warning, setup_logging


def process_year(year: int, config, tracker: BenchmarkTracker, data_manager: DataManager):
    """Process a single year of data.

    Args:
        year: Year to process
        config: Configuration object
        tracker: Benchmark tracker
        data_manager: Data manager

    Returns:
        Number of rows processed
    """
    print_section(f"Processing Year {year}")

    # Override config params for this year
    year_params = config.data_source.params.copy()
    year_params["starttime"] = f"{year}-01-01"
    year_params["endtime"] = f"{year}-12-31"

    # Build URL
    base_url = config.data_source.base_url
    param_str = "&".join(f"{key}={value}" for key, value in year_params.items())
    url = f"{base_url}?{param_str}"

    # Download
    with BenchmarkContext(tracker, f"download_{year}"):
        downloader = DataDownloader(config)
        output_path = config.paths.raw_dir / f"earthquakes_{year}.csv"
        file_paths = [downloader._download_single_file(url, output_path, force=False)]

        total_size = sum(fp.stat().st_size for fp in file_paths)
        tracker.record_data_info(f"year_{year}_file_size", format_bytes(total_size))

    # Extract
    with BenchmarkContext(tracker, f"extract_{year}"):
        extractor = DataExtractor(config)
        df = extractor.extract_csv(file_paths[0])

        if len(df) == 0:
            print_section(f"⚠️ Year {year} has no data - skipping")
            return 0

        row_count = len(df)
        tracker.record_data_info(f"year_{year}_raw_rows", row_count)

    # Transform
    with BenchmarkContext(tracker, f"transform_{year}"):
        transformer = DataTransformer(config)
        df_transformed = transformer.transform(df)

        transformed_rows = len(df_transformed)
        tracker.record_data_info(f"year_{year}_transformed_rows", transformed_rows)

    # Load to temporary table
    with BenchmarkContext(tracker, f"load_{year}"):
        with DataLoader(config) as loader:
            # Load to year-specific table
            temp_table = f"raw_earthquakes_{year}"
            loader.load_raw_data(df_transformed, table_name=temp_table)

    # Record year details
    data_manager.record_year_details(year, {
        "row_count": transformed_rows,
        "file_size_bytes": total_size,
        "date_range": (f"{year}-01-01", f"{year}-12-31")
    })

    # Mark as loaded
    data_manager.mark_year_loaded(year)

    return transformed_rows


def cleanup_old_yearly_tables(conn: duckdb.DuckDBPyConnection):
    """Remove old yearly tables after merging.

    Args:
        conn: DuckDB connection
    """
    print_section("Cleaning Old Yearly Tables")
    
    # Get all tables
    tables = conn.execute("SHOW TABLES").fetchall()
    
    # Drop yearly tables
    dropped = 0
    for table in tables:
        table_name = table[0]
        if table_name.startswith("raw_earthquakes_"):
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            dropped += 1
    
    if dropped > 0:
        print_section(f"🗑️ Dropped {dropped} old yearly table(s)")


def merge_yearly_tables(conn: duckdb.DuckDBPyConnection, config, years: list):
    """Merge all yearly tables into main raw_earthquakes table.

    Args:
        conn: DuckDB connection (reuse existing)
        config: Configuration object
        years: List of years to merge
    """
    print_section("Merging Yearly Tables")

    # Build list of year tables
    table_list = [f"raw_earthquakes_{year}" for year in years]

    # Check which tables actually exist
    existing_tables = []
    for table in table_list:
        try:
            result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            count = result[0] if result else 0
            if count > 0:
                existing_tables.append(table)
                from src.utils.logger import print_info
                print_info(f"Found {table}: {count:,} rows")
        except:
            pass

    if not existing_tables:
        print_section("⚠️ No yearly tables found")
        return

    # Create or replace main table with union of all years
    print_section(f"Merging {len(existing_tables)} yearly table(s)")

    union_query = " UNION ALL ".join([f"SELECT * FROM {table}" for table in existing_tables])

    conn.execute("DROP TABLE IF EXISTS raw_earthquakes")
    
    # Create with deduplication on event_id
    conn.execute(f"""
        CREATE TABLE raw_earthquakes AS 
        SELECT DISTINCT ON (event_id) * 
        FROM ({union_query})
        ORDER BY event_id, datetime DESC
    """)

    # Verify
    result = conn.execute("SELECT COUNT(*) FROM raw_earthquakes").fetchone()
    total_rows = result[0] if result else 0
    from src.utils.logger import print_success
    print_success(f"✅ Merged table has {total_rows:,} total rows (deduplicated)")


def table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check if a table exists.

    Args:
        conn: DuckDB connection
        table_name: Name of table to check

    Returns:
        True if table exists
    """
    try:
        conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except:
        return False


def main():
    """Run incremental ETL pipeline."""
    setup_logging()
    config = get_config()

    tracker = BenchmarkTracker()
    data_manager = DataManager(config)

    print_section("🚀 Starting Incremental ETL Pipeline")

    # Validate metadata against actual database FIRST
    db_path = config.get_duckdb_path()
    if db_path.exists():
        validation_conn = duckdb.connect(str(db_path), read_only=True)
        actual_loaded_years = data_manager.validate_loaded_years(validation_conn)
        validation_conn.close()
        
        # Force cleanup
        import gc
        gc.collect()
        time.sleep(0.5)

    # Show current status (after validation)
    summary = data_manager.get_summary()
    if summary["total_years"] > 0:
        print_section("📊 Current Data Status (Validated)")
        print(f"Loaded years: {summary['loaded_years']}")
        print(f"Total events: {summary['total_events']:,}")
        if summary['gaps']:
            print(f"Gaps: {summary['gaps']}")

    # Determine years to load
    if config.data_source.years_to_load:
        requested_years = config.data_source.years_to_load
    elif config.data_source.start_year and config.data_source.end_year:
        requested_years = list(range(config.data_source.start_year, config.data_source.end_year + 1))
    else:
        current_year = datetime.now().year
        requested_years = [current_year]
        print_warning(f"No years specified in config, using current year: {current_year}")

    # Get actually loaded years (validated)
    loaded_years_set = set(data_manager.get_loaded_years())
    
    years_to_process = []
    for year in requested_years:
        if year not in loaded_years_set:
            years_to_process.append(year)

    if years_to_process:
        print_section(f"📅 Will process {len(years_to_process)} year(s): {years_to_process}")

    try:
        # Process each year (only if there are new years)
        if years_to_process:
            total_rows = 0
            for year in years_to_process:
                rows = process_year(year, config, tracker, data_manager)
                total_rows += rows
                
                # Force cleanup after each year
                import gc
                gc.collect()
                time.sleep(0.1)

        # Force final cleanup before opening main connection
        import gc
        gc.collect()
        time.sleep(0.5)

        # Now open connection for merge and OLAP
        db_conn = duckdb.connect(str(config.get_duckdb_path()))
        
        # Configure DuckDB
        temp_dir = Path(config.duckdb.temp_directory)
        temp_dir.mkdir(parents=True, exist_ok=True)

        db_conn.execute(f"SET memory_limit='{config.duckdb.memory_limit}'")
        db_conn.execute(f"SET threads={config.duckdb.threads}")
        db_conn.execute(f"SET temp_directory='{temp_dir}'")
        db_conn.execute(f"SET max_temp_directory_size='{config.duckdb.max_temp_directory_size}'")
        db_conn.execute(f"SET preserve_insertion_order={str(config.duckdb.preserve_insertion_order).lower()}")

        # Check if OLAP layer exists (using current connection)
        olap_exists = table_exists(db_conn, "fact_earthquakes")
        
        # Check if raw_earthquakes needs to be rebuilt
        raw_exists = table_exists(db_conn, "raw_earthquakes")
        
        # Count rows in raw_earthquakes if it exists
        raw_row_count = 0
        if raw_exists:
            result = db_conn.execute("SELECT COUNT(*) FROM raw_earthquakes").fetchone()
            raw_row_count = result[0] if result else 0
        
        # Expected total from metadata (after validation)
        expected_total = summary.get("total_events", 0)
        
        # Determine if we need to merge
        needs_merge = (
            years_to_process or  # New years were processed
            not raw_exists or    # raw_earthquakes doesn't exist
            raw_row_count != expected_total  # Row count mismatch
        )

        # Merge yearly tables if needed (BEFORE cleanup)
        if needs_merge:
            from src.utils.logger import print_info
            if raw_row_count != expected_total and raw_exists:
                print_info(f"⚠️ Row count mismatch: {raw_row_count:,} in table vs {expected_total:,} expected - rebuilding...")
            
            # Merge ALL loaded years (not just newly processed)
            all_loaded_years = sorted(data_manager.get_loaded_years())
            merge_yearly_tables(db_conn, config, all_loaded_years)

        # NOW clean old yearly tables (after merge)
        cleanup_old_yearly_tables(db_conn)

        # Rebuild OLAP layer if needed
        needs_olap_rebuild = not olap_exists or needs_merge
        
        if needs_olap_rebuild:
            print_section("Step 5: Rebuild OLAP Layer")

            with BenchmarkContext(tracker, "olap_schema"):
                schema = OLAPSchema(config)
                schema.create_star_schema(db_conn)

                summary_result = schema.get_schema_summary(db_conn)
                for table_name, info in summary_result.items():
                    if "error" not in info:
                        tracker.record_data_info(f"{table_name}_rows", info["row_count"])

            with BenchmarkContext(tracker, "olap_cubes"):
                cube = OLAPCube(config)
                cube.create_cubes(db_conn)

                cube_summary = cube.get_cube_summary(db_conn)
                for cube_name, info in cube_summary.items():
                    if info["exists"]:
                        tracker.record_data_info(f"{cube_name}_aggregations", info["row_count"])
        else:
            print_section("✅ All requested years already loaded and OLAP layer exists")

        # Close connection
        db_conn.close()

        # Record memory usage
        memory_usage = tracker.get_memory_usage()
        tracker.record_data_info("peak_memory_usage_mb", f"{memory_usage['rss_mb']:.2f}")

        # Print and save results (only if we did something)
        if years_to_process or needs_merge or needs_olap_rebuild:
            print_section("📊 Benchmark Results")
            tracker.print_summary()

            results_path = tracker.save_results()
            print(f"\n💾 Detailed results saved to: {results_path}")

        # Show final summary
        final_summary = data_manager.get_summary()
        print_section("✅ ETL Pipeline Complete!")
        print(f"Total years loaded: {final_summary['total_years']}")
        print(f"Years: {final_summary['loaded_years']}")
        print(f"Total events: {final_summary['total_events']:,}")

        return 0

    except Exception as e:
        print(f"\n❌ Error during ETL pipeline: {e}")
        tracker.logger.error(f"ETL pipeline failed: {e}", exc_info=True)
        return 1


def table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check if a table exists.

    Args:
        conn: DuckDB connection
        table_name: Name of table to check

    Returns:
        True if table exists
    """
    try:
        conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except:
        return False


if __name__ == "__main__":
    sys.exit(main())
