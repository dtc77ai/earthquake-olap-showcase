"""Benchmarking and performance metrics tracking."""

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import psutil

from src.utils.config import Config, get_config
from src.utils.logger import LoggerMixin, print_info, print_success


@dataclass
class BenchmarkMetric:
    """Single benchmark metric."""

    name: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    duration: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def stop(self) -> float:
        """Stop the metric timer.

        Returns:
            Duration in seconds
        """
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        return self.duration

    def to_dict(self) -> dict:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "name": self.name,
            "duration_seconds": self.duration,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),
            "end_time": datetime.fromtimestamp(self.end_time).isoformat()
            if self.end_time
            else None,
            "metadata": self.metadata,
        }


@dataclass
class BenchmarkResult:
    """Complete benchmark results."""

    run_id: str
    timestamp: str
    metrics: Dict[str, BenchmarkMetric] = field(default_factory=dict)
    system_info: Dict[str, Any] = field(default_factory=dict)
    data_info: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "metrics": {name: metric.to_dict() for name, metric in self.metrics.items()},
            "system_info": self.system_info,
            "data_info": self.data_info,
            "summary": self._generate_summary(),
        }

    def _generate_summary(self) -> dict:
        """Generate summary statistics.

        Returns:
            Summary dictionary
        """
        total_duration = sum(
            m.duration for m in self.metrics.values() if m.duration is not None
        )

        return {
            "total_duration_seconds": total_duration,
            "total_duration_minutes": total_duration / 60,
            "metric_count": len(self.metrics),
            "completed_metrics": sum(
                1 for m in self.metrics.values() if m.duration is not None
            ),
        }


class BenchmarkTracker(LoggerMixin):
    """Track and record benchmark metrics."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize benchmark tracker.

        Args:
            config: Configuration object (uses global config if None)
        """
        self.config = config or get_config()
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.result = BenchmarkResult(
            run_id=self.run_id,
            timestamp=datetime.now().isoformat(),
            system_info=self._collect_system_info(),
        )

    def start_metric(self, name: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Start tracking a metric.

        Args:
            name: Metric name
            metadata: Optional metadata to attach
        """
        self.logger.info(f"Starting metric: {name}")
        self.result.metrics[name] = BenchmarkMetric(
            name=name, metadata=metadata or {}
        )

    def stop_metric(self, name: str, metadata: Optional[Dict[str, Any]] = None) -> float:
        """Stop tracking a metric.

        Args:
            name: Metric name
            metadata: Optional additional metadata

        Returns:
            Duration in seconds
        """
        if name not in self.result.metrics:
            self.logger.warning(f"Metric {name} was not started")
            return 0.0

        metric = self.result.metrics[name]
        duration = metric.stop()

        if metadata:
            metric.metadata.update(metadata)

        self.logger.info(f"Completed metric: {name} ({duration:.2f}s)")
        return duration

    def record_data_info(self, key: str, value: Any) -> None:
        """Record data-related information.

        Args:
            key: Information key
            value: Information value
        """
        self.result.data_info[key] = value

    def _collect_system_info(self) -> dict:
        """Collect system information.

        Returns:
            Dictionary with system info
        """
        return {
            "cpu_count": psutil.cpu_count(),
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_total_gb": psutil.virtual_memory().total / (1024**3),
            "memory_available_gb": psutil.virtual_memory().available / (1024**3),
            "disk_usage_percent": psutil.disk_usage("/").percent,
        }

    def get_memory_usage(self) -> dict:
        """Get current memory usage.

        Returns:
            Dictionary with memory metrics
        """
        process = psutil.Process()
        memory_info = process.memory_info()

        return {
            "rss_mb": memory_info.rss / (1024**2),
            "vms_mb": memory_info.vms / (1024**2),
            "percent": process.memory_percent(),
        }

    def save_results(self, output_path: Optional[Path] = None) -> Path:
        """Save benchmark results to JSON file.

        Args:
            output_path: Path to save results (auto-generated if None)

        Returns:
            Path to saved file
        """
        if output_path is None:
            if self.config.benchmark.enabled:
                output_dir = self.config.benchmark.output_dir
                output_dir.mkdir(parents=True, exist_ok=True)
            else:
                output_dir = Path("benchmark_results")
                output_dir.mkdir(parents=True, exist_ok=True)

            output_path = output_dir / f"benchmark_{self.run_id}.json"

        self.logger.info(f"Saving benchmark results to {output_path}")

        # Convert to dict and handle datetime serialization
        result_dict = self._serialize_result(self.result.to_dict())

        with open(output_path, "w") as f:
            json.dump(result_dict, f, indent=2, default=str)

        print_success(f"Benchmark results saved to {output_path.name}")
        return output_path

    def _serialize_result(self, obj: Any) -> Any:
        """Recursively serialize objects to JSON-compatible format.

        Args:
            obj: Object to serialize

        Returns:
            JSON-serializable object
        """
        if isinstance(obj, dict):
            return {key: self._serialize_result(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_result(item) for item in obj]
        elif isinstance(obj, (datetime, )):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat'):  # Handle other date/time types
            return obj.isoformat()
        elif isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        else:
            return str(obj)

    def print_summary(self) -> None:
        """Print a formatted summary of benchmark results."""
        from rich.console import Console
        from rich.table import Table

        console = Console()

        # Summary table
        table = Table(title="🎯 Benchmark Summary", show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="cyan")
        table.add_column("Duration", justify="right", style="green")
        table.add_column("Details", style="yellow")

        for name, metric in self.result.metrics.items():
            if metric.duration is not None:
                duration_str = f"{metric.duration:.2f}s"
                details = ", ".join(f"{k}={v}" for k, v in metric.metadata.items())
                table.add_row(name, duration_str, details or "-")

        console.print(table)

        # Summary stats
        summary = self.result._generate_summary()
        console.print(f"\n[bold]Total Duration:[/bold] {summary['total_duration_seconds']:.2f}s ({summary['total_duration_minutes']:.2f}m)")

        # Data info
        if self.result.data_info:
            console.print("\n[bold]Data Information:[/bold]")
            for key, value in self.result.data_info.items():
                console.print(f"  {key}: {value}")

        # System info
        console.print("\n[bold]System Information:[/bold]")
        for key, value in self.result.system_info.items():
            if isinstance(value, float):
                console.print(f"  {key}: {value:.2f}")
            else:
                console.print(f"  {key}: {value}")


class BenchmarkContext:
    """Context manager for easy metric tracking."""

    def __init__(self, tracker: BenchmarkTracker, name: str, metadata: Optional[Dict[str, Any]] = None):
        """Initialize context.

        Args:
            tracker: BenchmarkTracker instance
            name: Metric name
            metadata: Optional metadata
        """
        self.tracker = tracker
        self.name = name
        self.metadata = metadata or {}

    def __enter__(self):
        """Enter context."""
        self.tracker.start_metric(self.name, self.metadata)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context."""
        if exc_type is not None:
            self.metadata["error"] = str(exc_val)
            self.metadata["failed"] = True

        self.tracker.stop_metric(self.name, self.metadata)


def format_bytes(bytes_value: float) -> str:
    """Format bytes to human-readable string.

    Args:
        bytes_value: Number of bytes

    Returns:
        Formatted string (e.g., "1.5 GB")
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def format_duration(seconds: float) -> str:
    """Format duration to human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "2m 30s")
    """
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
