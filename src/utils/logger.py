"""Logging configuration and utilities."""

import logging
import logging.config
import sys
from pathlib import Path
from typing import Optional

import yaml
from rich.console import Console
from rich.logging import RichHandler


def setup_logging(
    config_path: str = "config/logging.yaml",
    default_level: int = logging.INFO,
    use_rich: bool = True,
) -> None:
    """Setup logging configuration.

    Args:
        config_path: Path to logging configuration YAML file
        default_level: Default logging level if config file not found
        use_rich: Use rich console handler for prettier output
    """
    config_file = Path(config_path)

    # Ensure logs directory exists
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    if config_file.exists():
        try:
            with open(config_file, "r") as f:
                config = yaml.safe_load(f)
                logging.config.dictConfig(config)
        except Exception as e:
            print(f"Error loading logging config: {e}", file=sys.stderr)
            _setup_basic_logging(default_level, use_rich)
    else:
        _setup_basic_logging(default_level, use_rich)


def _setup_basic_logging(level: int = logging.INFO, use_rich: bool = True) -> None:
    """Setup basic logging configuration as fallback.

    Args:
        level: Logging level
        use_rich: Use rich console handler
    """
    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    if use_rich:
        # Use Rich handler for beautiful console output
        console_handler = RichHandler(
            rich_tracebacks=True,
            markup=True,
            show_time=True,
            show_path=True,
        )
        console_handler.setLevel(level)
    else:
        # Standard console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(formatter)

    # File handler
    file_handler = logging.FileHandler("logs/earthquake_olap.log")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)

    # Configure root logger
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name.

    Args:
        name: Logger name (typically __name__ of the module)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class LoggerMixin:
    """Mixin class to add logging capability to any class."""

    @property
    def logger(self) -> logging.Logger:
        """Get logger for this class."""
        name = f"{self.__class__.__module__}.{self.__class__.__name__}"
        return get_logger(name)


# Rich console for pretty printing (optional utility)
console = Console()


def print_info(message: str) -> None:
    """Print info message with rich formatting."""
    console.print(f"[blue]ℹ[/blue] {message}")


def print_success(message: str) -> None:
    """Print success message with rich formatting."""
    console.print(f"[green]✓[/green] {message}")


def print_warning(message: str) -> None:
    """Print warning message with rich formatting."""
    console.print(f"[yellow]⚠[/yellow] {message}")


def print_error(message: str) -> None:
    """Print error message with rich formatting."""
    console.print(f"[red]✗[/red] {message}")


def print_section(title: str) -> None:
    """Print a section header."""
    console.rule(f"[bold blue]{title}[/bold blue]")
