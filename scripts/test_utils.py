"""Quick test script for utility modules."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.config import get_config
from src.utils.logger import (
    get_logger,
    print_error,
    print_info,
    print_section,
    print_success,
    print_warning,
    setup_logging,
)


def main():
    """Test utility modules."""
    # Setup logging
    print_section("Testing Logging Setup")
    setup_logging()
    logger = get_logger(__name__)

    logger.info("Logger initialized successfully")
    print_success("Logging setup complete")

    # Test configuration
    print_section("Testing Configuration")
    try:
        config = get_config()
        print_info(f"App Name: {config.app.name}")
        print_info(f"App Version: {config.app.version}")
        print_info(f"DuckDB Path: {config.get_duckdb_path()}")
        print_info(f"Data Source URL: {config.get_data_source_url()}")
        print_success("Configuration loaded successfully")

        logger.info(f"Configuration loaded: {config.app.name} v{config.app.version}")
    except Exception as e:
        print_error(f"Configuration error: {e}")
        logger.error(f"Configuration error: {e}", exc_info=True)
        return 1

    # Test directory creation
    print_section("Testing Directory Setup")
    try:
        config.setup_directories()
        print_success("All directories created successfully")
        logger.info("Directory structure verified")

        # List created directories
        for field_name in config.paths.model_fields:
            path = getattr(config.paths, field_name)
            if isinstance(path, Path) and path.exists():
                print_info(f"✓ {path}")
    except Exception as e:
        print_error(f"Directory setup error: {e}")
        logger.error(f"Directory setup error: {e}", exc_info=True)
        return 1

    # Test logging levels
    print_section("Testing Logging Levels")
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")

    print_section("All Tests Passed!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
