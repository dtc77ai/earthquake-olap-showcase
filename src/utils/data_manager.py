"""Data management for incremental loading and year tracking."""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
import duckdb

from src.utils.config import Config, get_config
from src.utils.logger import LoggerMixin, print_info, print_success, print_warning


class DataManager(LoggerMixin):
    """Manage incremental data loading and year tracking."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize data manager.

        Args:
            config: Configuration object (uses global config if None)
        """
        self.config = config or get_config()
        self.metadata_file = self.config.paths.data_dir / "metadata.json"

    def get_loaded_years(self) -> Set[int]:
        """Get set of years that have been loaded.

        Returns:
            Set of year integers
        """
        metadata = self._load_metadata()
        return set(metadata.get("loaded_years", []))

    def get_years_to_load(self, start_year: int, end_year: int) -> List[int]:
        """Determine which years need to be loaded.

        Args:
            start_year: Start year (inclusive)
            end_year: End year (inclusive)

        Returns:
            List of years that need loading
        """
        requested_years = set(range(start_year, end_year + 1))
        loaded_years = self.get_loaded_years()
        
        years_to_load = sorted(requested_years - loaded_years)
        
        if years_to_load:
            self.logger.info(f"Years to load: {years_to_load}")
            print_info(f"📅 Need to load {len(years_to_load)} year(s): {years_to_load}")
        else:
            self.logger.info("All requested years already loaded")
            print_success("✅ All requested years already loaded")
        
        return years_to_load

    def mark_year_loaded(self, year: int) -> None:
        """Mark a year as successfully loaded.

        Args:
            year: Year to mark as loaded
        """
        metadata = self._load_metadata()
        
        if "loaded_years" not in metadata:
            metadata["loaded_years"] = []
        
        if year not in metadata["loaded_years"]:
            metadata["loaded_years"].append(year)
            metadata["loaded_years"].sort()
        
        metadata["last_updated"] = datetime.now().isoformat()
        
        self._save_metadata(metadata)
        self.logger.info(f"Marked year {year} as loaded")

    def get_year_info(self, year: int) -> Optional[Dict]:
        """Get information about a specific loaded year.

        Args:
            year: Year to query

        Returns:
            Dictionary with year info or None if not loaded
        """
        metadata = self._load_metadata()
        year_data = metadata.get("year_details", {}).get(str(year))
        return year_data

    def record_year_details(self, year: int, details: Dict) -> None:
        """Record detailed information about a loaded year.

        Args:
            year: Year
            details: Dictionary with details (row_count, date_range, etc.)
        """
        metadata = self._load_metadata()
        
        if "year_details" not in metadata:
            metadata["year_details"] = {}
        
        metadata["year_details"][str(year)] = {
            **details,
            "loaded_at": datetime.now().isoformat()
        }
        
        self._save_metadata(metadata)

    def get_summary(self) -> Dict:
        """Get summary of loaded data.

        Returns:
            Dictionary with summary statistics
        """
        metadata = self._load_metadata()
        loaded_years = metadata.get("loaded_years", [])
        
        if not loaded_years:
            return {
                "total_years": 0,
                "year_range": None,
                "gaps": [],
                "total_events": 0
            }
        
        # Calculate gaps
        gaps = []
        for i in range(len(loaded_years) - 1):
            gap_size = loaded_years[i + 1] - loaded_years[i] - 1
            if gap_size > 0:
                gaps.append((loaded_years[i] + 1, loaded_years[i + 1] - 1))
        
        # Calculate total events
        year_details = metadata.get("year_details", {})
        total_events = sum(
            details.get("row_count", 0) 
            for details in year_details.values()
        )
        
        return {
            "total_years": len(loaded_years),
            "year_range": (min(loaded_years), max(loaded_years)),
            "loaded_years": loaded_years,
            "gaps": gaps,
            "total_events": total_events,
            "last_updated": metadata.get("last_updated")
        }

    def clear_year(self, year: int) -> None:
        """Remove a year from loaded data (for reprocessing).

        Args:
            year: Year to remove
        """
        metadata = self._load_metadata()
        
        if "loaded_years" in metadata and year in metadata["loaded_years"]:
            metadata["loaded_years"].remove(year)
            self.logger.info(f"Removed year {year} from loaded years")
        
        if "year_details" in metadata and str(year) in metadata["year_details"]:
            del metadata["year_details"][str(year)]
        
        self._save_metadata(metadata)
        print_warning(f"Cleared year {year} - will be reprocessed on next run")

    def reset_all(self) -> None:
        """Clear all metadata (for complete reset)."""
        self._save_metadata({
            "loaded_years": [],
            "year_details": {},
            "last_updated": datetime.now().isoformat()
        })
        self.logger.info("Reset all metadata")
        print_warning("⚠️ All metadata cleared - all years will be reprocessed")

    def _load_metadata(self) -> Dict:
        """Load metadata from file.

        Returns:
            Metadata dictionary
        """
        if not self.metadata_file.exists():
            return {
                "loaded_years": [],
                "year_details": {},
                "created_at": datetime.now().isoformat()
            }
        
        try:
            with open(self.metadata_file, "r") as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading metadata: {e}")
            return {"loaded_years": [], "year_details": {}}

    def _save_metadata(self, metadata: Dict) -> None:
        """Save metadata to file.

        Args:
            metadata: Metadata dictionary
        """
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

    def validate_loaded_years(self, conn: duckdb.DuckDBPyConnection) -> List[int]:
        """Validate that yearly tables actually exist for loaded years.

        Args:
            conn: DuckDB connection

        Returns:
            List of years that are actually loaded (have tables)
        """
        metadata = self._load_metadata()
        claimed_years = set(metadata.get("loaded_years", []))
        
        # Check which tables actually exist
        actual_years = set()
        for year in claimed_years:
            table_name = f"raw_earthquakes_{year}"
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                if result and result[0] > 0:
                    actual_years.add(year)
            except:
                pass
        
        # Find missing years
        missing_years = claimed_years - actual_years
        
        if missing_years:
            self.logger.warning(f"Years in metadata but missing tables: {sorted(missing_years)}")
            # Remove missing years from metadata
            metadata["loaded_years"] = sorted(list(actual_years))
            
            # Remove from year_details too
            if "year_details" in metadata:
                for year in missing_years:
                    if str(year) in metadata["year_details"]:
                        del metadata["year_details"][str(year)]
            
            self._save_metadata(metadata)
            
            from src.utils.logger import print_warning
            print_warning(f"⚠️ Removed {len(missing_years)} missing year(s) from metadata: {sorted(missing_years)}")
        
        return sorted(list(actual_years))
