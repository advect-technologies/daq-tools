import logging
import csv
import gzip
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Literal, Any, List

import aiofiles
import asyncio

from ..models import DataPoint
from .base import AsyncSink, TryAgainError

logger = logging.getLogger(__name__)

def extract_timestamp_from_path(file_path: Path) -> datetime | None:
    """Extracts a timestamp in '%Y-%m-%d_%H%M%S' format from a file path."""
    pattern = r"\d{4}-\d{2}-\d{2}_\d{6}"
    match = re.search(pattern, str(file_path))
    if match:
        try:
            return datetime.strptime(match.group(), "%Y-%m-%d_%H%M%S")
        except ValueError:
            return None
    return None


class FileSink(AsyncSink):
    """
    File sink with embedded timestamp naming: YYYY-MM-DD_HHMMSS_active.<format>
    Rotation based on embedded time + real file size.
    NO automatic archive on shutdown — leave _active file for clean restart behavior.
    """

    def __init__(self, config, base_data_dir):
        super().__init__(config, base_data_dir)

        sink_config = self.config

        self.base_dir = Path(sink_config.get("output_path", "output/data"))
        self.format: Literal["jsonl", "csv"] = sink_config.get("format", "jsonl")
        self.rotation: Literal["daily", "hourly", "weekly", "size-only"] = sink_config.get("rotation", "size-only")
        self.max_size_mb: float = float(sink_config.get("max_size_mb", 50.0))
        self.compress: bool = sink_config.get("compress", False)
        self.startup: bool = sink_config.get("startup", False)
        self.suffix = '.jsonl' if self.format == 'jsonl' else '.csv'

        # JSONL buffering
        self._jsonl_buffer: List[str] = []
        self._buffer_size = 100  # TODO: make configurable via TOML

        self.base_dir.mkdir(parents=True, exist_ok=True)

        self._current_file: Path | None = None
        self._csv_file_handle = None
        self._csv_writer = None
        self._header_written = False
        self._fieldnames: List[str] = []

        self._startup_file_written: bool = not self.startup

        logger.info(
            f"FileSink '{self.name}' initialized → format={self.format}, rotation={self.rotation}, "
            f"max_size_mb={self.max_size_mb}, compress={self.compress}, startup={self.startup}"
        )

    # ... [ _find_existing_active_files, _archive_file, _sync_gzip_compress, 
    #       _should_rotate_existing_file, _generate_new_active_file_path, 
    #       _get_current_file_path, _ensure_file_open unchanged ] ...

    def _find_existing_active_files(self) -> List[Path]:
        """Return active files sorted newest first."""
        pattern = f"*_active.{self.format}"
        candidates = [
            p for p in self.base_dir.glob(pattern)
            if extract_timestamp_from_path(p) is not None
        ]
        candidates.sort(
            key=lambda x: extract_timestamp_from_path(x).timestamp() if extract_timestamp_from_path(x) else 0,
            reverse=True
        )
        return candidates

    async def _archive_file(self, file: Path) -> None:
        """Rename _active → final and compress only if enabled. Called only on rotation."""
        if not file.exists():
            return

        new_path = file.parent / f"{file.stem.strip('_active')}{file.suffix}"

        if self.compress:
            try:
                gz_path = new_path.with_suffix(new_path.suffix + ".gz")
                async with aiofiles.open(file, "rb") as f_in:
                    data = await f_in.read()
                await asyncio.to_thread(self._sync_gzip_compress, data, gz_path)
                await asyncio.to_thread(file.unlink)
                logger.info(f"Compressed {file.name} → {gz_path.name}")
            except Exception as e:
                logger.warning(f"Compression failed for {file}: {e}")
        else:
            await asyncio.to_thread(file.rename, new_path)
            logger.info(f"Archived {file.name} → {new_path.name}")

    @staticmethod
    def _sync_gzip_compress(data: bytes, output_path: Path) -> None:
        with gzip.open(output_path, "wb") as f_out:
            f_out.write(data)

    def _should_rotate_existing_file(self, existing_file: Path) -> bool:
        if not self._startup_file_written:
            self._startup_file_written = True
            return True

        file_time = extract_timestamp_from_path(existing_file)
        if file_time is None:
            return True

        now = datetime.now()

        match self.rotation:
            case "daily":
                return file_time.date() != now.date()
            case "hourly":
                return file_time.date() != now.date() or file_time.hour != now.hour
            case "weekly":
                return file_time.isocalendar().week != now.isocalendar().week
            case "size-only" | _:
                try:
                    return existing_file.stat().st_size / (1024 ** 2) >= self.max_size_mb
                except Exception:
                    return True
        return False

    def _generate_new_active_file_path(self) -> Path:
        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        return self.base_dir / f"{ts}_active{self.suffix}"

    def _get_current_file_path(self, force_new: bool = False) -> Path:
        """Decide which file to use, setting startup flag on any successful selection."""
        if force_new:
            new_path = self._generate_new_active_file_path()
            self._startup_file_written = True
            logger.info(f"FileSink '{self.name}' creating new file: {new_path.name}")
            return new_path

        candidates = self._find_existing_active_files()

        if candidates:
            existing = candidates[0]
            # Archive any extra older active files (safety net)
            for old_file in candidates[1:]:
                asyncio.create_task(self._archive_file(old_file))

            if not self._should_rotate_existing_file(existing):
                self._startup_file_written = True
                logger.debug(f"FileSink '{self.name}' resuming with: {existing.name}")
                return existing

        # Need to rotate or no suitable file
        new_path = self._generate_new_active_file_path()
        self._startup_file_written = True
        logger.info(f"FileSink '{self.name}' creating new file: {new_path.name}")
        return new_path

    async def _ensure_file_open(self, force_new: bool = False) -> None:
        target_path = self._get_current_file_path(force_new=force_new)

        if self._current_file != target_path:
            await self._close_current_file()

            self._current_file = target_path
            self._header_written = False
            self._csv_writer = None
            if self._csv_file_handle:
                self._csv_file_handle.close()
                self._csv_file_handle = None

            logger.info(f"FileSink '{self.name}' using file: {target_path.name}")

        # Ensure CSV handle
        if self.format == "csv" and (self._csv_file_handle is None or self._csv_file_handle.closed):
            self._csv_file_handle = open(target_path, "a", newline="", encoding="utf-8")
            self._csv_writer = csv.DictWriter(self._csv_file_handle, fieldnames=None)

    async def _close_current_file(self) -> None:
        """Flush pending data. Do NOT archive on normal close/shutdown."""
        if self._jsonl_buffer and self._current_file:
            await self._flush_jsonl_buffer()

        if self._csv_file_handle:
            self._csv_file_handle.flush()
            self._csv_file_handle.close()
            self._csv_file_handle = None
            self._csv_writer = None

    async def _flush_jsonl_buffer(self) -> None:
        """Flush buffered JSONL lines. Jail any lines that fail to write."""
        if not self._jsonl_buffer or not self._current_file:
            return

        try:
            async with aiofiles.open(self._current_file, "a", encoding="utf-8") as dst:
                await dst.writelines((DataPoint.from_json(line).to_json() + "\n" for line in self._jsonl_buffer))
            self._jsonl_buffer.clear()

        except Exception as e:
            logger.warning(f"Failed to flush JSONL buffer to {self._current_file}: {e}")
            # Jail the entire batch that failed to write
            for bad_line in self._jsonl_buffer:
                await self._write_line_to_jail(
                    bad_line, e, self._current_file
                )
            self._jsonl_buffer.clear()

    async def process_file(self, file_path: Path) -> None:
        await self._ensure_file_open()

        try:
            async with aiofiles.open(file_path, "r", encoding="utf-8") as src:
                async for line in src:
                    line = line.strip()
                    if not line:
                        continue

                    if self.format == "jsonl":
                        self._jsonl_buffer.append(line)
                        if len(self._jsonl_buffer) >= self._buffer_size:
                            await self._flush_jsonl_buffer()
                    else:
                        await self._append_csv_line(line)

            if self.format == "jsonl":
                await self._flush_jsonl_buffer()

            logger.debug(f"FileSink '{self.name}' processed {file_path.name}")

        except Exception as e:
            logger.warning(f"FileSink '{self.name}' error processing {file_path}: {e}")
            raise TryAgainError(str(e)) from e

    async def _append_csv_line(self, line: str) -> None:
        """Convert JSONL line to CSV row. Jail bad lines individually."""
        try:
            dp: DataPoint = DataPoint.from_json(line)
            row: dict[str, Any] = {"timestamp": dp.time}

            for k, v in dp.tags.items():
                clean_key = str(k).strip().replace(" ", "_").replace("-", "_").lower()
                row[clean_key] = json.dumps(v) if isinstance(v, (dict, list)) else v

            for k, v in dp.fields.items():
                clean_key = str(k).strip().replace(" ", "_").replace("-", "_").lower()
                row[clean_key] = json.dumps(v) if isinstance(v, (dict, list)) else v

            non_time = [k for k in row if k != "timestamp"]
            tag_set = {str(k).strip().replace(" ", "_").replace("-", "_").lower() for k in dp.tags}
            tag_keys = sorted(k for k in non_time if k in tag_set)
            field_keys = sorted(k for k in non_time if k not in tag_set)
            ordered_keys = ["timestamp"] + tag_keys + field_keys

            final_row = {k: row.get(k) for k in ordered_keys}

            if not self._header_written:
                self._fieldnames = ordered_keys
                if self._csv_writer:
                    self._csv_writer.fieldnames = ordered_keys
                    self._csv_writer.writeheader()
                self._header_written = True
                logger.info(f"FileSink '{self.name}' wrote CSV header ({len(ordered_keys)} columns)")
            else:
                final_row = {k: final_row.get(k) for k in self._fieldnames}

            if self._csv_writer and self._csv_file_handle:
                self._csv_writer.writerow(final_row)
                self._csv_file_handle.flush()

        except Exception as e:
            logger.warning(f"CSV conversion failed for line in {self._current_file}: {e}")
            await self._write_line_to_jail(line, e, self._current_file or Path("unknown"))

    async def stop(self) -> None:
        """Graceful shutdown: only flush, leave _active file intact."""
        await self._close_current_file()
        await super().stop()