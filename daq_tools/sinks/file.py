import logging
import csv
import gzip
import time
from pathlib import Path
from datetime import datetime
from typing import Literal, Any

import aiofiles
import json

from ..models import DataPoint
from .base import AsyncSink, TryAgainError

logger = logging.getLogger(__name__)


class FileSink(AsyncSink):
    """
    Flexible file sink supporting JSONL and CSV with rotation and dynamic CSV headers.

    Column order for CSV: timestamp + alpha-sorted tags + alpha-sorted fields.
    New tags/fields automatically add columns (dynamic growth).
    rotation = "startup"      # "startup" (new file every launch), "daily", "hourly", "size", "none"

    """

    def __init__(self, config, base_data_dir):
        super().__init__(config, base_data_dir)

        sink_config = self.config

        self.base_path = Path(sink_config.get("output_path", "output/data"))
        self.format: Literal["jsonl", "csv"] = sink_config.get("format", "jsonl")
        self.rotation: Literal["none", "daily", "hourly", "size", "startup"] = sink_config.get("rotation", "startup")
        self.max_size_mb: float = sink_config.get("max_size_mb", 50.0)
        self.compress: bool = sink_config.get("compress", False)

        if not self.base_path:
            raise ValueError(f"FileSink '{self.name}' requires 'output_path'")

        self.base_path.parent.mkdir(parents=True, exist_ok=True)

        self._current_file: Path | None = None
        self._current_size = 0
        self._csv_writer = None
        self._csv_file_handle = None
        self._header_written = False
        self._fieldnames: list[str] = []

        logger.info(f"FileSink '{self.name}' initialized → format={self.format}, "
                   f"rotation={self.rotation}, base={self.base_path}")

    def _get_current_file_path(self, force_new: bool = False) -> Path:
        """Get the path for the current output file.
        
        If force_new=True, always create a new timestamped file.
        """
        parent = self.base_path.parent
        base_name = self.base_path.name

        if force_new or self.rotation == "startup":
            timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            suffix = f"_{timestamp}.jsonl" if self.format == "jsonl" else f"_{timestamp}.csv"
            return parent / (base_name + suffix)

        elif self.rotation == "none":
            suffix = ".jsonl" if self.format == "jsonl" else ".csv"
            return self.base_path.with_suffix(suffix)

        elif self.rotation == "daily":
            date_str = datetime.now().strftime("%Y-%m-%d")
            suffix = f"_{date_str}.jsonl" if self.format == "jsonl" else f"_{date_str}.csv"

        elif self.rotation == "hourly":
            date_str = datetime.now().strftime("%Y-%m-%d_%H")
            suffix = f"_{date_str}.jsonl" if self.format == "jsonl" else f"_{date_str}.csv"

        else:
            # fallback for size-only
            suffix = f"_{int(time.time())}.jsonl" if self.format == "jsonl" else f"_{int(time.time())}.csv"

        return parent / (base_name + suffix)

    async def _ensure_file_open(self, force_new: bool = False) -> None:
        """Ensure we have an open file handle. Rotate if needed."""
        target_path = self._get_current_file_path(force_new=force_new)

        # Rotate if target file changed OR size limit exceeded
        if (self._current_file != target_path or 
            (self._current_file and self._current_size > self.max_size_mb * 1024 * 1024)):
            
            await self._close_current_file()
            
            self._current_file = target_path
            self._current_size = 0
            self._header_written = False
            self._csv_writer = None

            logger.info(f"FileSink '{self.name}' → opened new file: {target_path.name} "
                       f"(size rotation triggered)" if self._current_size > 0 else "")

        # Open CSV handle if needed
        if self.format == "csv" and not self._csv_file_handle:
            self._csv_file_handle = open(target_path, "a", newline="", encoding="utf-8")
            self._csv_writer = csv.DictWriter(self._csv_file_handle, fieldnames=None)

    async def _close_current_file(self) -> None:
        """Close handles and compress if enabled."""
        if self._csv_file_handle:
            self._csv_file_handle.close()
            self._csv_file_handle = None
            self._csv_writer = None

        if self._current_file and self.compress and self._current_file.exists():
            try:
                gz_path = self._current_file.with_suffix(self._current_file.suffix + ".gz")
                with open(self._current_file, "rb") as f_in:
                    with gzip.open(gz_path, "wb") as f_out:
                        f_out.writelines(f_in)
                self._current_file.unlink()
                logger.info(f"Compressed → {gz_path.name}")
            except Exception as e:
                logger.warning(f"Compression failed: {e}")

    async def process_file(self, file_path: Path) -> None:
        """Process incoming .jsonl file and append to output."""
        await self._ensure_file_open()

        try:
            async with aiofiles.open(file_path, "r", encoding="utf-8") as src:
                async for line in src:
                    line = line.strip()
                    if not line:
                        continue

                    if self.format == "jsonl":
                        await self._append_jsonl_line(line)
                    else:
                        await self._append_csv_line(line)

            # Size-based rotation check
            if self.rotation in ("size", "startup") and self._current_size > self.max_size_mb * 1024 * 1024:
                logger.info(f"FileSink '{self.name}' size limit reached ({self._current_size / (1024*1024):.1f} MB), rotating...")
                await self._ensure_file_open(force_new=True)

            logger.debug(f"FileSink '{self.name}' processed {file_path.name}")

        except Exception as e:
            logger.warning(f"FileSink '{self.name}' transient error: {e}")
            raise TryAgainError(str(e)) from e

    async def _append_jsonl_line(self, line: str) -> None:
        """Append raw JSONL line."""
        async with aiofiles.open(self._current_file, "a", encoding="utf-8") as dst:
            await dst.write(line + "\n")
        self._current_size += len(line) + 1

    async def _append_csv_line(self, line: str) -> None:
        """Simple CSV writer with fixed schema after first row.
        
        - First row defines the column set (timestamp + sorted tags + sorted fields).
        - Later rows: drop any extra keys that weren't in the first row.
        - No dynamic column growth.
        - If schema changes, user should restart the ingestor (new file created).
        """
        try:
            dp: DataPoint = DataPoint.from_json(line)

            # Build full row from this DataPoint
            row: dict[str, Any] = {"timestamp": dp.time}

            # Add tags (cleaned)
            for k, v in dp.tags.items():
                clean_key = str(k).strip().replace(" ", "_").replace("-", "_").lower()
                row[clean_key] = json.dumps(v) if isinstance(v, (dict, list)) else v

            # Add fields (overwrite on collision — field wins)
            for k, v in dp.fields.items():
                clean_key = str(k).strip().replace(" ", "_").replace("-", "_").lower()
                row[clean_key] = json.dumps(v) if isinstance(v, (dict, list)) else v

            # Determine desired column order: timestamp + sorted tags + sorted fields
            non_time_keys = [k for k in row.keys() if k != "timestamp"]
            tag_set = set(str(k).strip().replace(" ", "_").replace("-", "_").lower() 
                         for k in dp.tags.keys())

            tag_keys = sorted(k for k in non_time_keys if k in tag_set)
            field_keys = sorted(k for k in non_time_keys if k not in tag_set)

            ordered_keys = ["timestamp"] + tag_keys + field_keys

            # Create row using only allowed columns (drop extras)
            final_row = {k: row.get(k) for k in ordered_keys}

            # === First row ever: define the schema ===
            if not self._header_written:
                self._fieldnames = ordered_keys
                self._csv_writer.fieldnames = ordered_keys
                self._csv_writer.writeheader()
                self._header_written = True
                logger.info(f"FileSink '{self.name}' wrote initial CSV header with {len(ordered_keys)} columns")

            # === Subsequent rows: drop any unknown keys ===
            else:
                # Safety: make sure we only use columns from the original header
                final_row = {k: final_row.get(k) for k in self._fieldnames}

            # Write the row
            self._csv_writer.writerow(final_row)
            self._csv_file_handle.flush()

            self._current_size += len(str(final_row)) + 100

        except Exception as e:
            await self._write_line_to_jail(line, e, self._current_file or Path("unknown"))
            logger.warning(f"CSV conversion failed: {e}")

    async def stop(self) -> None:
        """Final cleanup."""
        await self._close_current_file()
        await super().stop()