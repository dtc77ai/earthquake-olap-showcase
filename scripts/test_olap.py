"""Test script for OLAP layer."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from src.olap.cube import OLAPCube
from src.olap.queries import OLAPQueries
from src.olap.schema import OLAPSchema
from src.utils.config import get_config
from src.utils.logger import print_section, setup_logging


def main():
    """Test OLAP layer."""
    setup_logging()
    config = get_config()

    # Connect to DuckDB
    print_section("Connecting to DuckDB")
    conn = duckdb.connect(str(config.get_duckdb_path()))

    # Create schema
    print_section("Creating Star Schema")
    schema = OLAPSchema()
    schema.create_star_schema(conn)

    # Validate schema
    validation = schema.validate_schema(conn)
    print("\nValidation Results:")
    for table, result in validation.items():
        if result["exists"]:
            print(f"  ✓ {table}: {result['row_count']:,} rows")
        else:
            print(f"  ✗ {table}: {result.get('error', 'Not found')}")

    # Get schema summary
    summary = schema.get_schema_summary(conn)
    print("\nSchema Summary:")
    for table, info in summary.items():
        if "error" not in info:
            print(f"  {table}: {info['row_count']:,} rows, {info['column_count']} columns")

    # Create cubes
    print_section("Creating OLAP Cubes")
    cube = OLAPCube()
    cube.create_cubes(conn)

    # Get cube summary
    cube_summary = cube.get_cube_summary(conn)
    print("\nCube Summary:")
    for cube_name, info in cube_summary.items():
        if info["exists"]:
            print(f"  ✓ {cube_name}: {info['row_count']:,} aggregations")

    # Test queries
    print_section("Testing Analytical Queries")
    queries = OLAPQueries()

    print("\nTop 5 Magnitude Events:")
    top_events = queries.get_top_magnitude_events(conn, limit=5)
    print(top_events[["datetime", "place", "magnitude", "depth"]].to_string(index=False))

    print("\nMagnitude Distribution:")
    mag_dist = queries.get_magnitude_distribution(conn)
    print(mag_dist.to_string(index=False))

    print("\nDepth Analysis:")
    depth_analysis = queries.get_depth_analysis(conn)
    print(depth_analysis.to_string(index=False))

    conn.close()

    print_section("OLAP Layer Test Complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
