"""Data download module for earthquake data."""

import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import httpx
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from src.utils.config import Config, get_config
from src.utils.logger import LoggerMixin, print_error, print_info, print_success, print_warning


class DataDownloader(LoggerMixin):
    """Download earthquake data from USGS."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize downloader.

        Args:
            config: Configuration object (uses global config if None)
        """
        self.config = config or get_config()
        self.timeout = self.config.etl.download_timeout
        self.retry_attempts = self.config.etl.retry_attempts
        self.retry_delay = self.config.etl.retry_delay

    def download(
        self,
        url: Optional[str] = None,
        output_path: Optional[Path] = None,
        force: bool = False,
    ) -> List[Path]:
        """Download data from URL, splitting into chunks if date range > 12 months.

        Args:
            url: URL to download from (uses config default if None)
            output_path: Path to save downloaded file (auto-generated if None)
            force: Force re-download even if file exists

        Returns:
            List of paths to downloaded files

        Raises:
            httpx.HTTPError: If download fails after all retries
        """
        # Check if we're using API with date range
        if self.config.data_source.use_api:
            return self._download_with_chunking(force)
        else:
            # Single feed URL download
            url = url or self.config.get_data_source_url()
            output_path = output_path or self._get_default_output_path()
            downloaded_file = self._download_single_file(url, output_path, force)
            return [downloaded_file]

    def _download_with_chunking(self, force: bool = False) -> List[Path]:
        """Download data in chunks if date range exceeds 12 months.

        Args:
            force: Force re-download even if files exist

        Returns:
            List of paths to downloaded files
        """
        params = self.config.data_source.params
        start_date = datetime.strptime(params["starttime"], "%Y-%m-%d")
        end_date = datetime.strptime(params["endtime"], "%Y-%m-%d")

        # Calculate date range in days
        date_range = (end_date - start_date).days

        # If less than 365 days, download as single file
        if date_range <= 365:
            self.logger.info(f"Date range is {date_range} days, downloading as single file")
            url = self.config.get_data_source_url()
            output_path = self._get_default_output_path()
            downloaded_file = self._download_single_file(url, output_path, force)
            return [downloaded_file]

        # Split into 12-month chunks
        self.logger.info(f"Date range is {date_range} days, splitting into 12-month chunks")
        print_info(f"📅 Date range spans {date_range} days - splitting into chunks...")

        chunks = self._create_date_chunks(start_date, end_date)
        downloaded_files = []

        for idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
            self.logger.info(f"Downloading chunk {idx}/{len(chunks)}: {chunk_start} to {chunk_end}")
            print_info(f"Chunk {idx}/{len(chunks)}: {chunk_start.date()} to {chunk_end.date()}")

            # Build URL for this chunk
            chunk_params = params.copy()
            chunk_params["starttime"] = chunk_start.strftime("%Y-%m-%d")
            chunk_params["endtime"] = chunk_end.strftime("%Y-%m-%d")

            url = self._build_url_with_params(chunk_params)
            output_path = self.config.paths.raw_dir / f"earthquakes_{chunk_start.strftime('%Y%m%d')}_{chunk_end.strftime('%Y%m%d')}.csv"

            try:
                downloaded_file = self._download_single_file(url, output_path, force)
                downloaded_files.append(downloaded_file)
            except Exception as e:
                self.logger.error(f"Failed to download chunk {idx}: {e}")
                print_error(f"Failed to download chunk {idx}: {e}")
                # Continue with other chunks
                continue

        if not downloaded_files:
            raise RuntimeError("All chunk downloads failed")

        print_success(f"✅ Downloaded {len(downloaded_files)} file(s) successfully")
        return downloaded_files

    def _create_date_chunks(
        self, start_date: datetime, end_date: datetime, chunk_months: int = 12
    ) -> List[tuple]:
        """Split date range into chunks.

        Args:
            start_date: Start date
            end_date: End date
            chunk_months: Number of months per chunk

        Returns:
            List of (start, end) date tuples
        """
        chunks = []
        current_start = start_date

        while current_start < end_date:
            # Calculate chunk end (12 months from start or end_date, whichever is earlier)
            chunk_end = current_start + timedelta(days=chunk_months * 30)  # Approximate
            if chunk_end > end_date:
                chunk_end = end_date

            chunks.append((current_start, chunk_end))
            current_start = chunk_end + timedelta(days=1)

        return chunks

    def _build_url_with_params(self, params: dict) -> str:
        """Build URL with query parameters.

        Args:
            params: Dictionary of query parameters

        Returns:
            Complete URL with parameters
        """
        base_url = self.config.data_source.base_url
        param_str = "&".join(f"{key}={value}" for key, value in params.items())
        return f"{base_url}?{param_str}"

    def _download_single_file(
        self, url: str, output_path: Path, force: bool = False
    ) -> Path:
        """Download a single file.

        Args:
            url: URL to download from
            output_path: Path to save file
            force: Force re-download even if exists

        Returns:
            Path to downloaded file
        """
        # Check if file already exists
        if output_path.exists() and not force:
            file_size = output_path.stat().st_size
            self.logger.info(f"File already exists: {output_path} ({file_size:,} bytes)")
            print_info(f"Using cached file: {output_path.name}")
            return output_path

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Downloading from: {url}")

        # Attempt download with retries
        for attempt in range(1, self.retry_attempts + 1):
            try:
                return self._download_with_progress(url, output_path)
            except Exception as e:
                self.logger.warning(f"Download attempt {attempt} failed: {e}")

                if attempt < self.retry_attempts:
                    print_info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    print_error(f"Download failed after {self.retry_attempts} attempts")
                    raise

        raise RuntimeError("Download failed")  # Should never reach here

    def _download_with_progress(self, url: str, output_path: Path) -> Path:
        """Download file with progress bar.

        Args:
            url: URL to download from
            output_path: Path to save file

        Returns:
            Path to downloaded file
        """
        with httpx.stream("GET", url, timeout=self.timeout, follow_redirects=True) as response:
            response.raise_for_status()

            # Get total file size if available
            total_size = int(response.headers.get("content-length", 0))

            # Create progress bar
            progress = Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
            )

            with progress:
                task = progress.add_task(
                    f"Downloading {output_path.name}",
                    total=total_size,
                )

                with open(output_path, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)
                        progress.update(task, advance=len(chunk))

        file_size = output_path.stat().st_size
        self.logger.info(f"Download complete: {output_path} ({file_size:,} bytes)")
        print_success(f"Downloaded {file_size:,} bytes to {output_path.name}")

        return output_path

    def _get_default_output_path(self) -> Path:
        """Generate default output path based on data source.

        Returns:
            Path for output file
        """
        if self.config.data_source.use_api:
            params = self.config.data_source.params
            start = params.get("starttime", "").replace("-", "")
            end = params.get("endtime", "").replace("-", "")
            filename = f"earthquakes_{start}_{end}.csv"
        else:
            url = self.config.data_source.feed_url
            filename = url.split("/")[-1]
            if "." not in filename:
                filename = "earthquakes.csv"

        return self.config.paths.raw_dir / filename

    def get_cached_files(self) -> List[Path]:
        """Get list of cached CSV files.

        Returns:
            List of paths to cached files
        """
        raw_dir = self.config.paths.raw_dir
        if not raw_dir.exists():
            return []

        return sorted(raw_dir.glob("earthquakes_*.csv"))
