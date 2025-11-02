"""Standalone benchmark script."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from src.benchmark.metrics import BenchmarkContext, BenchmarkTracker
from src.olap.cube import OLAPCube
from src.olap.queries import OLAPQueries
from src.olap.schema import OLAPSchema
from src.utils.config import get_config
from src.utils.logger import print_section, setup_logging


def main():
    """Run comprehensive benchmark including OLAP operations."""
    setup_logging()
    config = get_config()

    tracker = BenchmarkTracker()
    print_section("🎯 Starting Comprehensive Benchmark")

    try:
        # Connect to DuckDB
        conn = duckdb.connect(str(config.get_duckdb_path()))

        # Benchmark: Create Star Schema
        print_section("Creating Star Schema")
        with BenchmarkContext(tracker, "create_star_schema"):
            schema = OLAPSchema()
            schema.create_star_schema(conn)

            # Record schema info
            summary = schema.get_schema_summary(conn)
            for table_name, info in summary.items():
                if "error" not in info:
                    tracker.record_data_info(f"{table_name}_rows", info["row_count"])

        # Benchmark: Create OLAP Cubes
        print_section("Creating OLAP Cubes")
        with BenchmarkContext(tracker, "create_olap_cubes"):
            cube = OLAPCube()
            cube.create_cubes(conn)

            # Record cube info
            cube_summary = cube.get_cube_summary(conn)
            for cube_name, info in cube_summary.items():
                if info["exists"]:
                    tracker.record_data_info(f"{cube_name}_aggregations", info["row_count"])

        # Benchmark: Query Performance
        print_section("Testing Query Performance")
        queries = OLAPQueries()

        with BenchmarkContext(tracker, "query_top_magnitude"):
            queries.get_top_magnitude_events(conn, limit=100)

        with BenchmarkContext(tracker, "query_regional_stats"):
            queries.get_events_by_region(conn, top_n=50)

        with BenchmarkContext(tracker, "query_temporal_trends"):
            queries.get_temporal_trends(conn)

        with BenchmarkContext(tracker, "query_magnitude_distribution"):
            queries.get_magnitude_distribution(conn)

        with BenchmarkContext(tracker, "query_depth_analysis"):
            queries.get_depth_analysis(conn)

        with BenchmarkContext(tracker, "query_map_data"):
            queries.get_events_for_map(conn, min_magnitude=4.0, limit=1000)

        # Get database size
        db_path = config.get_duckdb_path()
        if db_path.exists():
            db_size = db_path.stat().st_size
            tracker.record_data_info("database_size_mb", f"{db_size / (1024**2):.2f}")

        conn.close()

        # Print and save results
        print_section("📊 Benchmark Results")
        tracker.print_summary()

        results_path = tracker.save_results()
        print(f"\n💾 Detailed results saved to: {results_path}")

        print_section("✅ Benchmark Complete!")
        return 0

    except Exception as e:
        print(f"\n❌ Error during benchmark: {e}")
        tracker.logger.error(f"Benchmark failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
