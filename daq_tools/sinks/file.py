import logging
from pathlib import Path

import aiofiles

from ..models import DataPoint
from .base import AsyncSink, TryAgainError

logger = logging.getLogger(__name__)


class FileSink(AsyncSink):
    """
    Simple file sink that appends JSONL lines to a destination file.

    Config example:
    [[sinks]]
    name = "local_archive"
    type = "file"
    config = { "output_path": "/data/output/archive.jsonl", "format": "json" }  # or "line_protocol"
    """

    def __init__(self, config, base_data_dir):
        super().__init__(config, base_data_dir)
        self.output_path = Path(self.config.get("output_path"))
        self.format = self.config.get("format", "json")  # "json" or "line_protocol"

        if not self.output_path:
            raise ValueError(f"FileSink '{self.name}' requires 'output_path' in config")

        # Ensure parent directory exists
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    async def process_file(self, file_path: Path) -> None:
        """Read source jsonl and append to output file."""
        try:
            async with aiofiles.open(file_path, "r", encoding="utf-8") as src:
                async with aiofiles.open(self.output_path, "a", encoding="utf-8") as dst:
                    async for line in src:
                        line = line.strip()
                        if not line:
                            continue

                        if self.format == "line_protocol":
                            try:
                                dp = DataPoint.from_json(line)
                                out_line = dp.to_line_protocol()
                            except Exception as e:
                                await self._write_line_to_jail(line, e, file_path)
                                continue
                        else:
                            # Default: keep original JSON line
                            out_line = line

                        await dst.write(out_line + "\n")

            logger.debug(f"FileSink '{self.name}' appended {file_path.name} to {self.output_path}")

        except Exception as e:
            # File I/O errors are usually transient (disk full, permissions temporarily blocked, etc.)
            logger.warning(f"FileSink '{self.name}' transient error processing {file_path.name}: {e}")
            raise TryAgainError(str(e)) from e