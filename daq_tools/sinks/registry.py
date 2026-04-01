import logging
from pathlib import Path
from typing import Type

from ..config import SinkConfig, DAQConfig
from .base import AsyncSink
from .file import FileSink
from .mqtt import MqttSink

logger = logging.getLogger(__name__)

# Registry of known sink types → class
_SINK_REGISTRY: dict[str, Type[AsyncSink]] = {
    "file": FileSink,
    "mqtt": MqttSink,
    # Add more here later:
    # "http": HttpPostSink,
    # "sqlite": SqliteSink,
    # "questdb": QuestDBSink,
}


def register_sink_type(sink_type: str, sink_class: Type[AsyncSink]) -> None:
    """Register a new sink type dynamically (useful for plugins/extensions)."""
    if sink_type in _SINK_REGISTRY:
        logger.warning(f"Sink type '{sink_type}' is already registered. Overwriting.")
    _SINK_REGISTRY[sink_type] = sink_class
    logger.info(f"Registered sink type: {sink_type}")


def create_sink(sink_config: SinkConfig, base_data_dir: Path) -> AsyncSink:
    """
    Factory function to instantiate the correct AsyncSink from config.
    
    Raises ValueError if the sink type is unknown.
    """
    sink_type = sink_config.type.lower()

    if sink_type not in _SINK_REGISTRY:
        raise ValueError(
            f"Unknown sink type '{sink_type}'. "
            f"Supported types: {list(_SINK_REGISTRY.keys())}"
        )

    sink_class = _SINK_REGISTRY[sink_type]

    try:
        sink = sink_class(sink_config, base_data_dir)
        logger.debug(f"Created {sink_type} sink: {sink_config.name}")
        return sink
    except Exception as e:
        raise ValueError(f"Failed to create {sink_type} sink '{sink_config.name}': {e}") from e


async def create_all_sinks(config: "DAQConfig", base_data_dir: Path) -> list[AsyncSink]:
    """
    Create all sinks defined in the config.
    Called by the DAQIngestor during startup.
    """
    sinks: list[AsyncSink] = []

    for sink_cfg in config.sinks:
        try:
            sink = create_sink(sink_cfg, base_data_dir)
            sinks.append(sink)
        except Exception as e:
            logger.error(f"Failed to create sink '{sink_cfg.name}' (type: {sink_cfg.type}): {e}")
            # Continue creating other sinks — one bad sink shouldn't kill everything
            continue

    if not sinks:
        logger.warning("No valid sinks were created from config!")

    logger.info(f"Successfully created {len(sinks)} sink(s)")
    return sinks