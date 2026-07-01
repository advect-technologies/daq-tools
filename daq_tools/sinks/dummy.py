import logging
import shutil
from pathlib import Path
from random import randint

from ..config import SinkConfig
from .base import AsyncSink, BadFileError, TryAgainError

logger = logging.getLogger(__name__)


class DummySink(AsyncSink):
    """
    A dummy sink that does nothing except move files somewhere.

    Config Example:

    [[sinks]]
    name = "dummy"
    type = "dummy"
    config = {
        "destination_dir": ""
    }
    """

    def __init__(
        self, config: SinkConfig, transient_data_dir: Path, long_term_data_dir: Path
    ):
        super().__init__(config, transient_data_dir, long_term_data_dir)

        logger.info(f"DummySink '{self.name}' initialized")
        dest: str = self.sink_config.get("destination_dir", "DummySink")
        self.destination_dir: Path = Path(dest)
        self.destination_dir.mkdir(parents=True, exist_ok=True)

    async def process_file(self, file_path: Path) -> None:
        """Read JSONL file and copy data to destination directory."""

        try:
            if randint(0, 100) < 5:
                raise TryAgainError("Simulated retry error")

            shutil.copy2(file_path, self.destination_dir / file_path.name)
            logger.info(
                f"DummySink '{self.name}' successfully copied {file_path.name} to {self.destination_dir}"
            )

        except TryAgainError as e:
            logger.error(
                f"DummySink '{self.name}' temporarily failed processing {file_path.name}: {e}"
            )
            raise

        except Exception as e:
            logger.error(
                f"DummySink '{self.name}' unexpected error processing {file_path.name}: {e}"
            )
            raise BadFileError(f"Unexpected processing error: {e}") from e

    async def stop(self) -> None:
        await super().stop()
