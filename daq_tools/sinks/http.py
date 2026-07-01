import gzip
import json
import logging
from pathlib import Path
from typing import Any

import aiofiles
import aiohttp

from ..config import SinkConfig
from ..models import DataPoint, TimeRes
from .base import AsyncSink, BadFileError, TryAgainError

logger = logging.getLogger(__name__)


class HttpPostSink(AsyncSink):
    """
    Generic HTTP POST sink. Highly flexible for QuestDB, InfluxDB, custom APIs, etc.

    Config Examples:

    # QuestDB (Line Protocol - recommended)
    [[sinks]]
    name = "questdb"
    type = "http"
    config = {
        "url": "http://localhost:9000/write",
        "params": { "precision": "ns" },
        "format": "line_protocol",
        "batch_size": 5000,
        "compress": true,
        "timeout": 45
    }

    # InfluxDB v1
    [[sinks]]
    name = "influx_v1"
    type = "http"
    config = {
        "url": "http://localhost:8086/write",
        "params": { "db": "mydb", "precision": "ns" },
        "format": "line_protocol",
        "username": "admin",
        "password": "secret",
        "compress": true
    }

    # InfluxDB v2
    [[sinks]]
    name = "influx_v2"
    type = "http"
    config = {
        "url": "http://localhost:8086/api/v2/write",
        "params": { "org": "myorg", "bucket": "mybucket", "precision": "ns" },
        "format": "line_protocol",
        "headers": { "Authorization": "Token your-token-here" },
        "compress": true
    }

    # Generic JSON API
    [[sinks]]
    name = "http_json"
    type = "http"
    config = {
        "url": "https://api.example.com/ingest",
        "format": "json",
        "batch_size": 500,
        "headers": { "X-API-Key": "abc123" }
    }
    """

    def __init__(
        self, config: SinkConfig, transient_data_dir: Path, long_term_data_dir: Path
    ):
        super().__init__(config, transient_data_dir, long_term_data_dir)

        self.url: str = self.sink_config["url"]  # required
        self.params: dict[str, Any] = self.sink_config.get("params", {})
        self.username = self.sink_config.get("username")
        self.password = self.sink_config.get("password")
        self.compress: bool = self.sink_config.get("compress", False)
        self.format: str = self.sink_config.get(
            "format", "json"
        )  # "json" or "line_protocol"
        self.batch_size: int = self.sink_config.get("batch_size", 1000)
        self.timeout: int = self.sink_config.get("timeout", 30)
        self.extra_headers: dict = self.sink_config.get("headers", {})

        self.precision: str = self.sink_config.get("precision", TimeRes.S)
        if self.precision not in TimeRes:
            self.precision = TimeRes.S

        self._session: aiohttp.ClientSession | None = None

        logger.info(
            f"HttpPostSink '{self.name}' initialized → {self.url} "
            f"(format={self.format}, batch_size={self.batch_size}, "
            f"compress={self.compress}, params={bool(self.params)})"
        )

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Create or reuse aiohttp session with basic auth if configured."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            auth = (
                aiohttp.BasicAuth(self.username, self.password)
                if self.username and self.password
                else None
            )

            self._session = aiohttp.ClientSession(
                auth=auth,
                timeout=timeout,
                headers={"User-Agent": f"daq-tools/{self.name}"},
            )
        return self._session

    async def process_file(self, file_path: Path) -> None:
        """Read JSONL file and send data in batches via HTTP POST."""
        session = await self._ensure_session()

        batch: list = []
        processed_count = 0

        try:
            async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
                async for raw_line in f:
                    line = raw_line.strip()
                    if not line:
                        continue

                    try:
                        dp = DataPoint.from_json(line)
                    except Exception as e:
                        await self._write_line_to_jail(line, e, file_path)
                        continue

                    batch.append(dp)
                    processed_count += 1

                    if len(batch) >= self.batch_size:
                        await self._post_batch(session, batch)
                        batch.clear()

            # Final batch
            if batch:
                await self._post_batch(session, batch)

            logger.info(
                f"HttpPostSink '{self.name}' successfully posted {processed_count} points from {file_path.name}"
            )

        except TryAgainError as e:
            logger.error(
                f"HttpPostSink '{self.name}' temporarily failed processing {file_path.name}: {e}"
            )
            raise

        except aiohttp.ClientConnectionError as e:
            logger.error(
                f"HttpPostSink '{self.name}' connection error (will retry): {e}"
            )
            raise TryAgainError(f"Processing error: {e}") from e

        except BadFileError as e:
            logger.error(
                f"HttpPostSink '{self.name}' permanently failed processing {file_path.name}: {e}"
            )
            raise

        except Exception as e:
            logger.error(
                f"HttpPostSink '{self.name}' unexpected error processing {file_path.name}: {e}"
            )
            raise BadFileError(f"Unexpected processing error: {e}") from e

    async def _post_batch(self, session: aiohttp.ClientSession, batch: list) -> None:
        """Send one batch via HTTP POST."""
        if not batch:
            return

        # Prepare payload
        if self.format == "json":
            # Convert DataPoint objects to dicts
            payload_list = [
                item if isinstance(item, dict) else item.__dict__ for item in batch
            ]
            data = json.dumps(payload_list)
            content_type = "application/json"
        else:
            # Line protocol: join with newlines
            batch = [
                point.to_line_protocol(time_resolution=self.precision)
                for point in batch
            ]
            data = "\n".join(batch)
            content_type = "text/plain"

        headers = {"Content-Type": content_type, **self.extra_headers}

        if self.compress:
            post_data = gzip.compress(data.encode("utf-8"))
            headers["Content-Encoding"] = "gzip"
        else:
            post_data = data

        async with session.post(
            self.url,
            data=post_data,
            headers=headers,
            params=self.params,  # aiohttp handles this cleanly
        ) as resp:
            if resp.status >= 400:
                text = await resp.text()
                error_msg = f"HTTP {resp.status}: {text[:300]}"

                if 500 <= resp.status < 600 or resp.status in (429, 408):
                    raise TryAgainError(error_msg)
                else:
                    logger.error(
                        f"HttpPostSink '{self.name}' permanent error: {error_msg}"
                    )
                    raise BadFileError(error_msg)
            else:
                logger.debug(
                    f"HttpPostSink '{self.name}' posted batch of {len(batch)} points successfully"
                )

    async def stop(self) -> None:
        """Close HTTP session on shutdown."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        await super().stop()
