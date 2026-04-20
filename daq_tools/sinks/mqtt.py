import random
import logging
import string
from pathlib import Path
from typing import Any

import aiomqtt
import aiofiles

from ..models import DataPoint
from .base import AsyncSink, TryAgainError

logger = logging.getLogger(__name__)

class MqttSink(AsyncSink):
    """
    MQTT sink that publishes each DataPoint as Line Protocol (default) or JSON.

    Config example:
    [[sinks]]
    name = "mqtt_daemon"
    type = "mqtt"
    config = {
        "broker": "localhost",
        "port": 1883,
        "username": null,
        "password": null,
        "client_id": "daq-ingestor",
        "qos": 1,
        "measurement_map": {
            "temperature": "daq/sensors/temp",
            "pressure": "daq/sensors/pressure"
        },
        "fallback_topic": "daq/unknown",
        "format": "line_protocol"   # or "json"
    }
    """

    def __init__(self, config, base_data_dir):
        super().__init__(config, base_data_dir)

        self.broker = self.config.get("broker", "localhost")
        self.port = self.config.get("port", 1883)
        self.username = self.config.get("username")
        self.password = self.config.get("password")
        self.qos = self.config.get("qos", 1)

        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        self.client_id = self.config.get("client_id", f"daq-ingestor-{self.name}-{random_suffix}")       

        tls = self.config.get('tls',False)
        self.tls_params = aiomqtt.TLSParameters() if tls else None

        self.measurement_map: dict[str, str] = self.config.get("measurement_map", {})
        self.fallback_topic: str = self.config.get("fallback_topic", "daq/fallback")

        self.format = self.config.get("format", "json")  # "line_protocol" or "json"

        self._client: aiomqtt.Client | None = None

    async def _ensure_connected(self) -> None:
        """Create client only if we don't have one (or it was cleared after failure)."""
        if self._client is not None:
            return  # Reuse existing healthy client

        logger.info(f"MqttSink '{self.name}' connecting to {self.broker}:{self.port}")

        self._client = aiomqtt.Client(
            hostname=self.broker,
            port=self.port,
            username=self.username,
            password=self.password,
            identifier=self.client_id,
            tls_params=self.tls_params,
        )
        await self._client.__aenter__()

    async def process_file(self, file_path: Path) -> None:
        """Read jsonl file and publish each DataPoint to MQTT."""
        await self._ensure_connected()

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

                    topic = self._get_topic_for(dp.measurement)
                    payload = self._format_payload(dp)

                    # Publish — let any error bubble up to outer handler
                    await self._client.publish(
                        topic=topic,
                        payload=payload,
                        qos=self.qos,
                        retain=False
                    )

            logger.debug(f"MqttSink '{self.name}' successfully published {file_path.name}")

        except aiomqtt.MqttError as e:
            # Connection lost, network issue, broker unreachable, etc.
            logger.warning(f"MqttSink '{self.name}' MQTT error on {file_path.name}: {e}")

            # Critical: Force recreation of client on next retry
            if self._client is not None:
                try:
                    await self._client.__aexit__(None, None, None)
                except Exception:
                    pass  # best effort cleanup
                self._client = None

            raise TryAgainError(f"MQTT error: {e}") from e

        except Exception as e:
            # Unexpected non-MQTT errors (should be rare)
            logger.warning(f"MqttSink '{self.name}' unexpected error on {file_path.name}: {e}")
            raise TryAgainError(str(e)) from e

    def _get_topic_for(self, measurement: str) -> str:
        """Resolve topic using measurement map or fallback."""
        return self.measurement_map.get(measurement, self.fallback_topic)

    def _format_payload(self, dp: DataPoint) -> str | bytes:
        if self.format == "line_protocol":
            return dp.to_line_protocol()
        else:
            return dp.to_json()

    async def _connect(self) -> None:
        """Establish MQTT connection (called lazily)."""
        logger.info(f"MqttSink '{self.name}' connecting to {self.broker}:{self.port}")
        self._client = aiomqtt.Client(
            hostname=self.broker,
            port=self.port,
            username=self.username,
            password=self.password,
            identifier=self.client_id,
            tls_params=self.tls_params,
        )
        await self._client.__aenter__()  # aiomqtt uses async context manager

    async def stop(self) -> None:
        """Clean up MQTT connection on shutdown."""
        if self._client:
            try:
                await self._client.__aexit__(None, None, None)
            except Exception:
                pass
            self._client = None
        await super().stop()