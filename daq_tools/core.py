import asyncio
import logging
import shutil
from pathlib import Path
from typing import Any

import watchfiles
from watchfiles import Change

from .config import DAQConfig, load_config
from .sinks.registry import create_all_sinks
from .sinks.base import AsyncSink

logger = logging.getLogger(__name__)


class DAQIngestor:
    """
    Main orchestrator class for the DAQ JSONL ingestion pipeline.

    Public API:
        async with DAQIngestor(config) as ingestor:
            await asyncio.sleep(3600)  # run for 1 hour, or integrate into your app

    The ingestor:
    - Watches a single inbound directory for new *.jsonl files
    - Moves them to a central queue/
    - Fans out (copies) each file to every sink's inbox/
    - Starts all sinks (each with their own watcher + retry logic)
    - Handles graceful shutdown via TaskGroup
    """

    def __init__(self, config: DAQConfig):
        self.config = config

        self.base_data_dir = Path(config.inbound.data_dir).resolve()
        self.watch_dir = Path(config.inbound.watch_dir).resolve()
        self.queue_dir = self.base_data_dir / "queue"

        self.file_pattern = config.inbound.file_pattern

        # Will be populated during startup
        self.sinks: list[AsyncSink] = []
        self._watcher_task: asyncio.Task | None = None
        self._distributor_task: asyncio.Task | None = None
        self._running = False

    @classmethod
    def from_config_file(cls, config_path: str | Path) -> "DAQIngestor":
        """Convenience constructor from TOML file."""
        config = load_config(config_path)
        return cls(config)

    def _ensure_dirs(self) -> None:
        """Create all required directories."""
        self.queue_dir.mkdir(parents=True, exist_ok=True)
        self.base_data_dir.mkdir(parents=True, exist_ok=True)

        # Sink directories will be created by the sinks themselves

    async def start(self) -> None:
        """Start the ingestor and all sinks."""
        self._ensure_dirs()
        self._running = True

        # Create and start all sinks
        self.sinks = await create_all_sinks(self.config, self.base_data_dir)

        for sink in self.sinks:
            await sink.start()

        # Start watcher and distributor
        self._watcher_task = asyncio.create_task(self._watcher_loop())
        self._distributor_task = asyncio.create_task(self._distributor_loop())

        logger.info(f"DAQIngestor started. Watching {self.watch_dir} → queue → {len(self.sinks)} sinks")

    async def stop(self) -> None:
        """Graceful shutdown: stop watcher/distributor, then sinks."""
        self._running = False

        # Cancel orchestrator tasks
        for task in (self._watcher_task, self._distributor_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Stop all sinks (they will finish in-flight files)
        stop_tasks = [sink.stop() for sink in self.sinks]
        await asyncio.gather(*stop_tasks, return_exceptions=True)

        logger.info("DAQIngestor stopped gracefully.")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def _watcher_loop(self) -> None:
        """Watch inbound directory for new *.jsonl files (creation only)."""
        async for changes in watchfiles.awatch(
            str(self.watch_dir),
            watch_filter=watchfiles.DefaultFilter(
                ignore_dirs=("queue", "sinks")  # avoid watching our own dirs
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

                # Move new file to central queue (atomic on same filesystem)
                try:
                    queue_path = self.queue_dir / file_path.name
                    file_path.replace(queue_path)  # atomic move
                    logger.info(f"New file detected and queued: {file_path.name}")
                except Exception as e:
                    logger.error(f"Failed to move {file_path} to queue: {e}")

    async def _distributor_loop(self) -> None:
        """Periodically scan queue/ and fan-out copies to all sink inboxes."""
        while self._running:
            try:
                await asyncio.sleep(1.0)  # poll every second (efficient enough)

                for file_path in list(self.queue_dir.glob("*.jsonl")):
                    if not self._running:
                        return

                    await self._distribute_file(file_path)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Error in distributor loop: {e}")

    async def _distribute_file(self, file_path: Path) -> None:
        """Copy file to every sink's inbox/, then delete from queue if successful."""
        if not self.sinks:
            logger.warning("No sinks configured - file will stay in queue")
            return

        success = True
        copied_to = 0

        for sink in self.sinks:
            try:
                inbox_path = sink.inbox_dir / file_path.name

                # Use copy2 to preserve metadata
                shutil.copy2(file_path, inbox_path)
                copied_to += 1
                logger.debug(f"Distributed {file_path.name} → {sink.name}/inbox")

            except Exception as e:
                logger.error(f"Failed to distribute {file_path.name} to sink '{sink.name}': {e}")
                success = False
                break  # stop on first failure for this file

        # If successfully copied to ALL sinks, delete from central queue
        if success and copied_to == len(self.sinks):
            try:
                file_path.unlink()
                logger.info(f"Successfully distributed and removed from queue: {file_path.name}")
            except Exception as e:
                logger.warning(f"Failed to delete {file_path.name} from queue: {e}")
        elif copied_to > 0:
            logger.warning(f"Partial distribution for {file_path.name} ({copied_to}/{len(self.sinks)} sinks)")

    # Optional helper methods
    def get_sink(self, name: str) -> AsyncSink | None:
        """Get a sink by name (useful for advanced usage)."""
        for sink in self.sinks:
            if sink.name == name:
                return sink
        return None