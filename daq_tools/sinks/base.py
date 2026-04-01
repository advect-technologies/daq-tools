import asyncio
import logging
import random
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import aiofiles
import watchfiles
from watchfiles import Change

from ..config import SinkConfig
from ..models import DataPoint

logger = logging.getLogger(__name__)


class TryAgainError(Exception):
    """Raise this from process_file() for transient errors that should be retried."""
    pass


class AsyncSink(ABC):
    """
    Base class for independent async data sinks.

    Directory structure per sink:
      inbox/       - new files from orchestrator arrive here (watched)
      retry/       - files that failed transiently (periodic reprocessing)
      processed/   - optional archive for successful files (currently unused)
      dead_letter/ - not used for now (we leave files on exhaustion)
      line_jail/   - bad individual lines with timestamps
    """

    def __init__(self, config: SinkConfig, base_data_dir: Path):
        self.name = config.name
        self.sink_type = config.type
        self.config = config.config

        self.base_data_dir = Path(base_data_dir)

        # Per-sink directories
        self.sink_dir = self.base_data_dir / "sinks" / self.name
        self.inbox_dir = self.sink_dir / "inbox"
        self.retry_dir = self.sink_dir / "retry"
        self.processed_dir = self.sink_dir / "processed"
        self.dead_letter_dir = self.sink_dir / "dead_letter"
        self.line_jail_dir = self.sink_dir / "line_jail"

        self.max_retries = config.config.get("max_retries", 5)
        self.backoff_base = config.config.get("backoff_base_seconds", 2.0)
        self.retry_scan_interval = config.config.get("retry_scan_interval", 60.0)  # seconds
        self.retry_batch_limit = config.config.get("retry_batch_size", 10)

        self._watcher_task: asyncio.Task | None = None
        self._retry_task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        """Start watcher and retry scanner."""
        self._ensure_dirs()
        self._running = True

        self._watcher_task = asyncio.create_task(self._watcher_loop())
        self._retry_task = asyncio.create_task(self._retry_scanner_loop())

        logger.info(f"Sink '{self.name}' ({self.sink_type}) started. Inbox: {self.inbox_dir}")

    async def stop(self) -> None:
        """Graceful shutdown."""
        self._running = False

        tasks = [t for t in (self._watcher_task, self._retry_task) if t]
        for task in tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        logger.info(f"Sink '{self.name}' stopped.")

    def _ensure_dirs(self) -> None:
        for d in (self.inbox_dir, self.retry_dir, self.processed_dir,
                  self.dead_letter_dir, self.line_jail_dir):
            d.mkdir(parents=True, exist_ok=True)

    async def _watcher_loop(self) -> None:
        """Watch inbox/ for newly arrived files (creation events)."""
        async for changes in watchfiles.awatch(
            str(self.inbox_dir),
            watch_filter=watchfiles.DefaultFilter(
                ignore_dirs=("retry", "processed", "dead_letter", "line_jail")
            ),
        ):
            if not self._running:
                break

            for change, path_str in changes:
                if change != Change.added:
                    continue

                file_path = Path(path_str)
                if not file_path.name.endswith(".jsonl"):
                    continue

                await self._process_file_with_retry(file_path, is_retry=False)

    async def _retry_scanner_loop(self) -> None:
        """Periodically scan retry/ and reprocess a sample of files."""
        while self._running:
            try:
                await asyncio.sleep(self.retry_scan_interval)
                if not self._running:
                    break

                retry_files = list(self.retry_dir.glob("*.jsonl"))
                if not retry_files:
                    continue

                # Sample a batch
                batch_size = min(self.retry_batch_limit, len(retry_files))
                sample_files = random.sample(retry_files, batch_size)

                for file_path in sample_files:
                    if not self._running:
                        return
                    await self._process_file_with_retry(file_path, is_retry=True)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Error in retry scanner for sink '{self.name}': {e}")

    async def _process_file_with_retry(self, file_path: Path, is_retry: bool = False) -> None:
        """Process a file with retry logic using TryAgainError."""
        attempts = 0
        current_file = file_path

        while attempts < self.max_retries and self._running:
            attempts += 1
            try:
                await self.process_file(current_file)

                # SUCCESS: delete the file
                try:
                    current_file.unlink(missing_ok=True)
                except Exception:
                    pass

                logger.info(f"Sink '{self.name}' successfully processed {current_file.name}")
                return

            except TryAgainError as e:
                if attempts < self.max_retries:
                    delay = self.backoff_base * (2 ** (attempts - 1))
                    logger.warning(
                        f"Sink '{self.name}' transient failure on {current_file.name} "
                        f"(attempt {attempts}/{self.max_retries}), retrying in {delay:.1f}s: {e}"
                    )
                    await asyncio.sleep(delay)

                    # First failure: move from inbox to retry/
                    if not is_retry and attempts == 1:
                        retry_path = self.retry_dir / current_file.name
                        try:
                            current_file.replace(retry_path)
                            current_file = retry_path
                        except Exception as move_err:
                            logger.error(f"Failed to move {current_file} to retry/: {move_err}")
                else:
                    logger.warning(
                        f"Sink '{self.name}' exhausted retries for {current_file.name} "
                        f"({self.max_retries} attempts). Leaving file for future retry."
                    )
                    return  # Leave the file in retry/ or inbox

            except Exception as e:
                # Any other exception = permanent failure for now (or treat as TryAgainError in sink)
                logger.error(
                    f"Sink '{self.name}' unexpected error on {current_file.name} "
                    f"(attempt {attempts}): {e}"
                )
                # For now we leave the file (no dead_letter move)
                return

    @abstractmethod
    async def process_file(self, file_path: Path) -> None:
        """
        Concrete implementation:
        - Read jsonl lines with aiofiles
        - Optionally parse with DataPoint.from_json(line)
        - Send to destination
        - Raise TryAgainError for transient issues (network, etc.)
        - Raise other exceptions only for unrecoverable errors
        """
        ...

    async def _write_line_to_jail(self, original_line: str, error: Exception, original_file: Path) -> None:
        """Write bad line to line_jail with context."""
        timestamp = asyncio.get_running_loop().time()
        jail_file = self.line_jail_dir / f"{timestamp:.0f}_{original_file.name}.bad"

        async with aiofiles.open(jail_file, "a", encoding="utf-8") as f:
            await f.write(f"# ERROR: {type(error).__name__}: {error}\n")
            await f.write(f"# Original file: {original_file}\n")
            await f.write(f"# Timestamp: {timestamp}\n")
            await f.write(f"{original_line.strip()}\n\n")