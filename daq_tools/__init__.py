"""
DAQ Tools - Async JSONL file ingestor with fan-out to configurable sinks.
"""

from .config import DAQConfig, load_config
from .core import DAQIngestor
from .models import DataPoint, TimeRes
from .sinks.base import AsyncSink, TryAgainError
from .sinks.file import FileSink
from .sinks.mqtt import MqttSink
from .sinks.registry import create_sink, create_all_sinks, register_sink_type
from .utils import (
    get_public_ip,
    escape_lp_identifier,
    escape_lp_field_value,
)

__version__ = "0.2.3"
__all__ = [
    "DAQIngestor",
    "DAQConfig",
    "load_config",
    "DataPoint",
    "TimeRes",
    "AsyncSink",
    "TryAgainError",
    "FileSink",
    "MqttSink",
    "create_sink",
    "create_all_sinks",
    "register_sink_type",
    "get_public_ip",
    "escape_lp_identifier",
    "escape_lp_field_value",
]