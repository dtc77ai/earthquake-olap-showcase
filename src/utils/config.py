"""Configuration management for the earthquake OLAP showcase."""

import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PathsConfig(BaseSettings):
    """Path configuration."""

    data_dir: Path = Field(default=Path("data"))
    raw_dir: Path = Field(default=Path("data/raw"))
    processed_dir: Path = Field(default=Path("data/processed"))
    duckdb_dir: Path = Field(default=Path("data/duckdb"))
    duckdb_file: Path = Field(default=Path("data/duckdb/earthquakes.duckdb"))
    cache_dir: Path = Field(default=Path("data/cache"))

    def ensure_directories(self) -> None:
        """Create all required directories if they don't exist."""
        for field_name in self.model_fields:
            path = getattr(self, field_name)
            if field_name.endswith("_dir"):
                path.mkdir(parents=True, exist_ok=True)
            elif field_name.endswith("_file"):
                path.parent.mkdir(parents=True, exist_ok=True)


class DataSourceConfig(BaseSettings):
    """Data source configuration."""

    name: str = Field(default="USGS Earthquake Data")
    base_url: str = Field(
        default="https://earthquake.usgs.gov/fdsnws/event/1/query"
    )
    format: str = Field(default="csv")
    params: Dict[str, Any] = Field(default_factory=dict)
    use_api: bool = Field(default=True)
    feed_url: str = Field(
        default="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv"
    )
    
    # Year-based loading fields
    years_to_load: Optional[List[int]] = Field(default=None)
    start_year: Optional[int] = Field(default=None)
    end_year: Optional[int] = Field(default=None)


class DuckDBConfig(BaseSettings):
    """DuckDB configuration."""

    database: str = Field(default="earthquakes.duckdb")
    memory_limit: str = Field(default="8GB")
    threads: int = Field(default=4)
    temp_directory: str = Field(default="data/duckdb/temp")
    max_temp_directory_size: str = Field(default="50GB")
    preserve_insertion_order: bool = Field(default=False)
    schema_tables: Dict[str, str] = Field(default_factory=dict, alias="schema")
    indexes: list = Field(default_factory=list)


class ETLConfig(BaseSettings):
    """ETL configuration."""

    chunk_size: int = Field(default=100000)
    download_timeout: int = Field(default=300)
    retry_attempts: int = Field(default=3)
    retry_delay: int = Field(default=5)
    validation: Dict[str, float] = Field(default_factory=dict)


class BenchmarkConfig(BaseSettings):
    """Benchmark configuration."""

    enabled: bool = Field(default=True)
    output_dir: Path = Field(default=Path("benchmark_results"))
    metrics: list = Field(default_factory=list)


class StreamlitConfig(BaseSettings):
    """Streamlit configuration."""

    page_title: str = Field(default="🌍 Earthquake OLAP Analytics")
    page_icon: str = Field(default="🌍")
    layout: Literal["centered", "wide"] = Field(default="wide")
    initial_sidebar_state: Literal["auto", "expanded", "collapsed"] = Field(default="expanded")
    map: Dict[str, Any] = Field(default_factory=dict)


class AppConfig(BaseSettings):
    """Application configuration."""

    name: str = Field(default="Earthquake OLAP Analytics")
    version: str = Field(default="0.1.0")
    debug: bool = Field(default=False)


class Config(BaseSettings):
    """Main configuration class that loads from YAML and environment variables."""

    model_config = SettingsConfigDict(
        #env_file=".env",
        #env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="allow",
        populate_by_name=True,  # Allow using alias names
    )

    app: AppConfig = Field(default_factory=AppConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)
    data_source: DataSourceConfig = Field(default_factory=DataSourceConfig)
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)
    etl: ETLConfig = Field(default_factory=ETLConfig)
    benchmark: BenchmarkConfig = Field(default_factory=BenchmarkConfig)
    streamlit: StreamlitConfig = Field(default_factory=StreamlitConfig)

    @classmethod
    def from_yaml(cls, config_path: str = "config/config.yaml") -> "Config":
        """Load configuration from YAML file and merge with environment variables.

        Args:
            config_path: Path to the YAML configuration file

        Returns:
            Config instance with merged settings
        """
        config_file = Path(config_path)

        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_file, "r") as f:
            yaml_config = yaml.safe_load(f)

        # Convert nested dict to flat dict for Pydantic
        return cls(**yaml_config)

    def get_duckdb_path(self) -> Path:
        """Get the full path to the DuckDB database file."""
        return self.paths.duckdb_file

    def get_data_source_url(self) -> str:
        """Get the data source URL with parameters.

        Returns:
            Complete URL with query parameters if using API, or feed URL
        """
        if self.data_source.use_api:
            # Build URL with parameters
            params = "&".join(
                f"{key}={value}" for key, value in self.data_source.params.items()
            )
            return f"{self.data_source.base_url}?{params}"
        else:
            return self.data_source.feed_url


    def setup_directories(self) -> None:
        """Create all required directories."""
        self.paths.ensure_directories()

        # Create logs directory
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)

        # Create benchmark results directory if benchmarking is enabled
        if self.benchmark.enabled:
            self.benchmark.output_dir.mkdir(parents=True, exist_ok=True)


# Global configuration instance
_config: Optional[Config] = None


def get_config(reload: bool = False) -> Config:
    """Get the global configuration instance.

    Args:
        reload: If True, reload configuration from file

    Returns:
        Config instance
    """
    global _config

    if _config is None or reload:
        try:
            _config = Config.from_yaml()
            _config.setup_directories()
        except FileNotFoundError:
            # Fallback to default configuration
            _config = Config()
            _config.setup_directories()

    return _config


def reload_config() -> Config:
    """Reload configuration from file.

    Returns:
        Reloaded Config instance
    """
    return get_config(reload=True)
